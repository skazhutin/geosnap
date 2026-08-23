"""Bounded, restart-safe expansion of KartaView discovery hits by sequence.

The point-discovery loader typically returns one representative photo from many
sequences around each query point.  This module turns those sparse hits into a
small, deterministic request plan and fetches one page around each representative
``sequenceIndex``.  It deliberately does not perform discovery itself.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ml.ingestion.common import (
    RequestRateLimiter,
    RetryMetrics,
    read_json,
    request_with_retry,
    sanitize_error_message,
    write_json,
)
from ml.ingestion.parsers import parse_kartaview_item
from ml.ingestion.schema import MOSCOW_BOUNDS
from ml.ingestion.state import IngestionState, ingestion_config_fingerprint

logger = logging.getLogger(__name__)

KARTAVIEW_API_URL = "https://api.openstreetcam.org/2.0/photo/"
PLAN_SCHEMA_VERSION = 1
DEFAULT_HOTSPOTS_PER_AREA = 2
DEFAULT_SEQUENCES_PER_HOTSPOT = 3
DEFAULT_MAX_REQUESTS = 48
DEFAULT_ITEMS_PER_PAGE = 150
DEFAULT_MIN_REQUEST_INTERVAL_SEC = 45.0


@dataclass(frozen=True)
class SequencePageRequest:
    """One stable API request in the sequence-expansion plan."""

    key: str
    area_id: str
    hotspot_query: str
    hotspot_strength: int
    sequence_id: str
    sequence_strength: int
    representative_sequence_index: int
    page: int
    items_per_page: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlanSummary:
    discovery_records: int
    records_with_query: int
    records_with_sequence: int
    areas_discovered: int
    hotspots_discovered: int
    areas_planned: int
    hotspots_planned: int
    requests_before_global_limit: int
    requests_planned: int

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def _metadata_object(record: dict[str, Any]) -> dict[str, Any]:
    value = record.get("metadata_json")
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _first_present(record: dict[str, Any], metadata: dict[str, Any], names: tuple[str, ...]) -> Any:
    for container in (record, metadata):
        for name in names:
            value = container.get(name)
            if value not in (None, ""):
                return value
    return None


def _area_from_query(query: str) -> str | None:
    """Extract the area from loader keys such as ``config:dataset:area:000``."""

    parts = query.split(":")
    if len(parts) < 4 or parts[0] != "config" or not parts[-1].isdigit():
        return None
    area_id = parts[-2].strip()
    return area_id or None


def _non_negative_index(value: Any) -> int | None:
    try:
        index = int(value)
    except (TypeError, ValueError):
        return None
    return index if index >= 0 else None


def _stable_request_key(
    *, hotspot_query: str, sequence_id: str, page: int, items_per_page: int
) -> str:
    material = json.dumps(
        {
            "hotspot_query": hotspot_query,
            "items_per_page": items_per_page,
            "page": page,
            "sequence_id": sequence_id,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sequence:{hashlib.sha256(material).hexdigest()[:24]}"


def load_discovery_records(path: Path) -> list[dict[str, Any]]:
    """Read the discovery JSON array without silently accepting another shape."""

    payload = read_json(path, default=None)
    if not isinstance(payload, list):
        raise ValueError(f"{path} must contain a JSON array")
    return [record for record in payload if isinstance(record, dict)]


def build_sequence_plan(
    records: list[dict[str, Any]],
    *,
    items_per_page: int = DEFAULT_ITEMS_PER_PAGE,
    hotspots_per_area: int = DEFAULT_HOTSPOTS_PER_AREA,
    sequences_per_hotspot: int = DEFAULT_SEQUENCES_PER_HOTSPOT,
    max_requests: int = DEFAULT_MAX_REQUESTS,
) -> tuple[list[SequencePageRequest], PlanSummary]:
    """Build an input-order-independent, strictly bounded request plan."""

    if not 1 <= items_per_page <= 150:
        raise ValueError("items_per_page must be between 1 and 150")
    if not 1 <= hotspots_per_area <= DEFAULT_HOTSPOTS_PER_AREA:
        raise ValueError("hotspots_per_area must be between 1 and 2")
    if not 1 <= sequences_per_hotspot <= DEFAULT_SEQUENCES_PER_HOTSPOT:
        raise ValueError("sequences_per_hotspot must be between 1 and 3")
    if max_requests < 1:
        raise ValueError("max_requests must be >= 1")

    hotspot_photo_ids: dict[tuple[str, str], set[str]] = defaultdict(set)
    sequence_indices: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    sequence_photo_ids: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    records_with_query = 0
    records_with_sequence = 0

    for record in records:
        metadata = _metadata_object(record)
        raw_query = _first_present(record, metadata, ("_geosnap_acquisition_query",))
        if raw_query in (None, ""):
            continue
        query = str(raw_query)
        area_id = _area_from_query(query)
        if area_id is None:
            continue
        records_with_query += 1
        photo_id_value = _first_present(record, metadata, ("source_image_id", "id", "photoId", "imageId"))
        # Canonically identical anonymous rows count once and remain independent
        # of input ordering.
        if photo_id_value in (None, ""):
            canonical = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            photo_id = f"anonymous:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"
        else:
            photo_id = str(photo_id_value)
        hotspot_photo_ids[(area_id, query)].add(photo_id)

        sequence_value = _first_present(record, metadata, ("sequence_id", "sequenceId"))
        if sequence_value in (None, ""):
            continue
        sequence_id = str(sequence_value)
        sequence_key = (area_id, query, sequence_id)
        if photo_id in sequence_photo_ids[sequence_key]:
            continue
        sequence_photo_ids[sequence_key].add(photo_id)
        records_with_sequence += 1
        index = _non_negative_index(
            _first_present(record, metadata, ("sequence_index", "sequenceIndex"))
        )
        if index is not None:
            sequence_indices[sequence_key].append(index)

    hotspots_by_area: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for (area_id, query), photo_ids in hotspot_photo_ids.items():
        has_sequence = any(key[:2] == (area_id, query) for key in sequence_photo_ids)
        if has_sequence:
            hotspots_by_area[area_id].append((query, len(photo_ids)))

    requests: list[SequencePageRequest] = []
    selected_hotspots: set[tuple[str, str]] = set()
    for area_id in sorted(hotspots_by_area):
        ranked_hotspots = sorted(hotspots_by_area[area_id], key=lambda item: (-item[1], item[0]))
        for query, hotspot_strength in ranked_hotspots[:hotspots_per_area]:
            selected_hotspots.add((area_id, query))
            candidates: list[tuple[str, int, int]] = []
            for (candidate_area, candidate_query, sequence_id), photo_ids in sequence_photo_ids.items():
                if (candidate_area, candidate_query) != (area_id, query):
                    continue
                indices = sorted(sequence_indices[(candidate_area, candidate_query, sequence_id)])
                representative_index = indices[(len(indices) - 1) // 2] if indices else 0
                candidates.append((sequence_id, len(photo_ids), representative_index))
            candidates.sort(key=lambda item: (-item[1], item[0], item[2]))
            for sequence_id, sequence_strength, representative_index in candidates[:sequences_per_hotspot]:
                page = representative_index // items_per_page + 1
                requests.append(
                    SequencePageRequest(
                        key=_stable_request_key(
                            hotspot_query=query,
                            sequence_id=sequence_id,
                            page=page,
                            items_per_page=items_per_page,
                        ),
                        area_id=area_id,
                        hotspot_query=query,
                        hotspot_strength=hotspot_strength,
                        sequence_id=sequence_id,
                        sequence_strength=sequence_strength,
                        representative_sequence_index=representative_index,
                        page=page,
                        items_per_page=items_per_page,
                    )
                )

    requests_before_limit = len(requests)
    requests = requests[:max_requests]
    planned_hotspots = {(request.area_id, request.hotspot_query) for request in requests}
    summary = PlanSummary(
        discovery_records=len(records),
        records_with_query=records_with_query,
        records_with_sequence=records_with_sequence,
        areas_discovered=len(hotspots_by_area),
        hotspots_discovered=sum(len(items) for items in hotspots_by_area.values()),
        areas_planned=len({request.area_id for request in requests}),
        hotspots_planned=len(planned_hotspots),
        requests_before_global_limit=requests_before_limit,
        requests_planned=len(requests),
    )
    return requests, summary


def _extract_page(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise RuntimeError("KartaView response must be a JSON object")
    status = payload.get("status")
    if isinstance(status, dict):
        logical_http = status.get("httpCode")
        try:
            is_error = logical_http is not None and int(logical_http) >= 400
        except (TypeError, ValueError):
            is_error = False
        if is_error:
            message = status.get("apiMessage") or status.get("httpMessage") or "unknown error"
            raise RuntimeError(f"KartaView API error {logical_http}: {message}")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("KartaView response is missing an object result")
    data = result.get("data")
    if not isinstance(data, list):
        raise RuntimeError("KartaView response result is missing a data array")
    if any(not isinstance(item, dict) for item in data):
        raise RuntimeError("KartaView response data contains a non-object item")
    return data


def fetch_sequence_page(
    session: Any,
    request: SequencePageRequest,
    *,
    limiter: RequestRateLimiter,
    retries: int,
    backoff_sec: float,
    timeout_sec: float,
    retry_metrics: RetryMetrics | None = None,
) -> list[dict[str, Any]]:
    """Fetch exactly one planned page; every retry passes through ``limiter``."""

    response = request_with_retry(
        session,
        url=KARTAVIEW_API_URL,
        params={
            "sequenceId": request.sequence_id,
            "page": request.page,
            "itemsPerPage": request.items_per_page,
        },
        retries=retries,
        backoff_sec=backoff_sec,
        timeout_sec=timeout_sec,
        metrics=retry_metrics,
        before_request=limiter.wait,
    )
    try:
        return _extract_page(response.json())
    finally:
        if hasattr(response, "close"):
            response.close()


def _discovery_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _plan_payload(
    *,
    discovery_json: Path,
    discovery_sha256: str,
    requests: list[SequencePageRequest],
    summary: PlanSummary,
    hotspots_per_area: int,
    sequences_per_hotspot: int,
    max_requests: int,
    items_per_page: int,
    min_request_interval_sec: float,
) -> dict[str, Any]:
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "source": "kartaview",
        "endpoint": KARTAVIEW_API_URL,
        "discovery_json": str(discovery_json),
        "discovery_sha256": discovery_sha256,
        "limits": {
            "hotspots_per_area": hotspots_per_area,
            "sequences_per_hotspot": sequences_per_hotspot,
            "max_requests": max_requests,
            "items_per_page": items_per_page,
            "minimum_request_interval_sec": min_request_interval_sec,
        },
        "summary": summary.as_dict(),
        "estimated_minimum_duration_seconds": round(
            max(0, len(requests) - 1) * min_request_interval_sec, 3
        ),
        "requests": [request.as_dict() for request in requests],
    }


def _in_moscow_aoi(lat: Any, lon: Any) -> bool:
    try:
        latitude = float(lat)
        longitude = float(lon)
    except (TypeError, ValueError):
        return False
    min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
    return min_lat <= latitude <= max_lat and min_lon <= longitude <= max_lon


def run(
    discovery_json: Path,
    output_json: Path,
    *,
    plan_json: Path | None = None,
    checkpoint_path: Path | None = None,
    stats_path: Path | None = None,
    items_per_page: int = DEFAULT_ITEMS_PER_PAGE,
    hotspots_per_area: int = DEFAULT_HOTSPOTS_PER_AREA,
    sequences_per_hotspot: int = DEFAULT_SEQUENCES_PER_HOTSPOT,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    min_request_interval_sec: float = DEFAULT_MIN_REQUEST_INTERVAL_SEC,
    request_retries: int = 5,
    backoff_sec: float = 1.5,
    timeout_sec: float = 30.0,
    plan_only: bool = False,
    session: Any | None = None,
    limiter: RequestRateLimiter | None = None,
) -> dict[str, Any]:
    """Build the plan and optionally execute it into normalized raw records."""

    if min_request_interval_sec < 0:
        raise ValueError("min_request_interval_sec must be >= 0")
    if request_retries < 1:
        raise ValueError("request_retries must be >= 1")
    if backoff_sec < 0:
        raise ValueError("backoff_sec must be >= 0")
    if timeout_sec <= 0:
        raise ValueError("timeout_sec must be > 0")

    records = load_discovery_records(discovery_json)
    requests, summary = build_sequence_plan(
        records,
        items_per_page=items_per_page,
        hotspots_per_area=hotspots_per_area,
        sequences_per_hotspot=sequences_per_hotspot,
        max_requests=max_requests,
    )
    discovery_digest = _discovery_sha256(discovery_json)
    plan_path = plan_json or output_json.with_suffix(".plan.json")
    statistics_path = stats_path or output_json.with_suffix(".stats.json")
    payload = _plan_payload(
        discovery_json=discovery_json,
        discovery_sha256=discovery_digest,
        requests=requests,
        summary=summary,
        hotspots_per_area=hotspots_per_area,
        sequences_per_hotspot=sequences_per_hotspot,
        max_requests=max_requests,
        items_per_page=items_per_page,
        min_request_interval_sec=min_request_interval_sec,
    )
    write_json(plan_path, payload)

    if plan_only:
        stats: dict[str, Any] = {
            "source": "kartaview",
            "plan_only": True,
            **summary.as_dict(),
            "minimum_request_interval_sec": min_request_interval_sec,
            "estimated_minimum_duration_seconds": payload["estimated_minimum_duration_seconds"],
            "network_attempts": 0,
            "retry_attempts": 0,
            "throttled_attempts": 0,
            "final_records": 0,
        }
        write_json(statistics_path, stats)
        return stats
    if not requests:
        raise ValueError("discovery JSON produced no sequence requests")

    fingerprint = ingestion_config_fingerprint(
        {
            "source": "kartaview",
            "endpoint": KARTAVIEW_API_URL,
            # The effective requests are the acquisition contract. Reordering
            # semantically identical discovery rows must not invalidate a safe
            # checkpoint merely because the source file byte digest changed.
            "requests": [request.as_dict() for request in requests],
        }
    )
    previous_stats = read_json(statistics_path, default={})
    if not isinstance(previous_stats, dict):
        raise ValueError(f"{statistics_path} must contain a JSON object")
    state = IngestionState.load(
        source="kartaview",
        output_json=output_json,
        checkpoint_path=checkpoint_path,
        stats_path=statistics_path,
        config_fingerprint=fingerprint,
    )
    state.stats.update(summary.as_dict())
    state.stats["plan_only"] = False
    state.stats["minimum_request_interval_sec"] = min_request_interval_sec
    state.stats["estimated_minimum_duration_seconds"] = payload[
        "estimated_minimum_duration_seconds"
    ]
    state.stats["records_outside_moscow_aoi"] = int(
        previous_stats.get("records_outside_moscow_aoi", 0)
    )
    state.stats["throttled_attempts"] = int(previous_stats.get("throttled_attempts", 0))
    state.stats["tiles_total"] = len(requests)

    retry_metrics = RetryMetrics()
    rate_limiter = limiter or RequestRateLimiter(min_request_interval_sec)
    seen_ids = state.seen_ids
    started_at = time.monotonic()
    live_attempts = 0
    live_successes = 0
    owned_session = session is None
    if session is None:
        import requests as requests_library

        session = requests_library.Session()

    try:
        for planned_request in requests:
            if planned_request.key in state.completed_tiles:
                state.stats["tiles_skipped_checkpoint"] = int(
                    state.stats["tiles_skipped_checkpoint"]
                ) + 1
                continue
            live_attempts += 1
            state.stats["tiles_attempted"] = int(state.stats["tiles_attempted"]) + 1
            try:
                page_items = fetch_sequence_page(
                    session,
                    planned_request,
                    limiter=rate_limiter,
                    retries=request_retries,
                    backoff_sec=backoff_sec,
                    timeout_sec=timeout_sec,
                    retry_metrics=retry_metrics,
                )
                live_successes += 1
                state.stats["pages_fetched"] = int(state.stats["pages_fetched"]) + 1
                state.stats["source_records_discovered"] = int(
                    state.stats["source_records_discovered"]
                ) + len(page_items)
                for item in page_items:
                    try:
                        item_with_provenance = dict(item)
                        item_with_provenance["_geosnap_acquisition_query"] = (
                            planned_request.hotspot_query
                        )
                        item_with_provenance["_geosnap_sequence_expansion"] = {
                            "area_id": planned_request.area_id,
                            "hotspot_strength": planned_request.hotspot_strength,
                            "items_per_page": planned_request.items_per_page,
                            "page": planned_request.page,
                            "plan_key": planned_request.key,
                            "representative_sequence_index": (
                                planned_request.representative_sequence_index
                            ),
                            "sequence_id": planned_request.sequence_id,
                            "sequence_strength": planned_request.sequence_strength,
                        }
                        parsed = parse_kartaview_item(item_with_provenance)
                    except Exception as exc:  # noqa: BLE001 - isolate corrupt API records
                        logger.warning(
                            "kartaview_sequence_record_invalid reason=%s",
                            sanitize_error_message(exc, max_length=160),
                        )
                        parsed = None
                    if parsed is None:
                        state.stats["invalid_records"] = int(state.stats["invalid_records"]) + 1
                        continue
                    if not _in_moscow_aoi(parsed["lat"], parsed["lon"]):
                        state.stats["records_outside_moscow_aoi"] = int(
                            state.stats["records_outside_moscow_aoi"]
                        ) + 1
                        continue
                    photo_id = str(parsed["id"])
                    if photo_id in seen_ids:
                        state.stats["duplicate_records"] = int(
                            state.stats["duplicate_records"]
                        ) + 1
                        continue
                    seen_ids.add(photo_id)
                    state.records.append(parsed)
                    state.stats["metadata_normalized"] = int(
                        state.stats["metadata_normalized"]
                    ) + 1
                state.completed_tiles.add(planned_request.key)
                state.failed_tiles.pop(planned_request.key, None)
                state.stats["tiles_succeeded"] = int(state.stats["tiles_succeeded"]) + 1
            except Exception as exc:  # noqa: BLE001 - failed requests remain resumable
                reason = f"{type(exc).__name__}: {sanitize_error_message(exc, max_length=180)}"
                state.failed_tiles[planned_request.key] = reason
                state.stats["tiles_failed"] = int(state.stats["tiles_failed"]) + 1
                logger.error(
                    "kartaview_sequence_request_failed key=%s reason=%s",
                    planned_request.key,
                    reason,
                )
            finally:
                state.save()
    finally:
        if owned_session and hasattr(session, "close"):
            session.close()
        elapsed_seconds = time.monotonic() - started_at
        for name, value in retry_metrics.as_dict().items():
            state.stats[name] = int(state.stats.get(name, 0)) + value
        state.stats["last_run_throttled_attempts"] = retry_metrics.network_attempts
        state.stats["throttled_attempts"] = int(state.stats["throttled_attempts"]) + (
            retry_metrics.network_attempts
        )
        cumulative_elapsed = float(state.stats.get("elapsed_seconds", 0.0)) + elapsed_seconds
        state.stats["last_run_elapsed_seconds"] = round(elapsed_seconds, 6)
        state.stats["elapsed_seconds"] = round(cumulative_elapsed, 6)
        state.stats["records_per_second"] = (
            round(int(state.stats["metadata_normalized"]) / cumulative_elapsed, 6)
            if cumulative_elapsed > 0
            else 0.0
        )
        state.save(compact=True)

    if live_attempts > 0 and live_successes == 0:
        raise RuntimeError(
            f"all {live_attempts} live KartaView sequence requests failed; "
            f"see {state.stats_path} and {state.checkpoint_path}"
        )
    return state.stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Expand sparse KartaView Moscow discovery hits by sequence"
    )
    parser.add_argument("--discovery-json", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--plan-json", type=Path)
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--stats", type=Path)
    parser.add_argument("--items-per-page", type=int, default=DEFAULT_ITEMS_PER_PAGE)
    parser.add_argument(
        "--hotspots-per-area", type=int, default=DEFAULT_HOTSPOTS_PER_AREA
    )
    parser.add_argument(
        "--sequences-per-hotspot", type=int, default=DEFAULT_SEQUENCES_PER_HOTSPOT
    )
    parser.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS)
    parser.add_argument(
        "--min-request-interval-sec",
        type=float,
        default=DEFAULT_MIN_REQUEST_INTERVAL_SEC,
        help="minimum delay shared by initial attempts and retries (default: 45s)",
    )
    parser.add_argument("--request-retries", type=int, default=5)
    parser.add_argument("--backoff-sec", type=float, default=1.5)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    stats = run(
        discovery_json=args.discovery_json,
        output_json=args.output_json,
        plan_json=args.plan_json,
        checkpoint_path=args.checkpoint,
        stats_path=args.stats,
        items_per_page=args.items_per_page,
        hotspots_per_area=args.hotspots_per_area,
        sequences_per_hotspot=args.sequences_per_hotspot,
        max_requests=args.max_requests,
        min_request_interval_sec=args.min_request_interval_sec,
        request_retries=args.request_retries,
        backoff_sec=args.backoff_sec,
        timeout_sec=args.timeout_sec,
        plan_only=args.plan_only,
    )
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
