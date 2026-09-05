"""Expand discovered Mapillary sequences into a bounded reference-frame pool.

The citywide vector-tile loader intentionally discovers at most one image per
sequence.  This module is a separate, downstream gallery-construction stage:
it uses Mapillary's documented ``/image_ids?sequence_id=`` collection, hydrates
the returned IDs with the same Graph metadata contract as citywide ingestion,
and greedily keeps only spatially or directionally novel frames.

No query/test decision is made here.  Provider-sequence holdout remains the
responsibility of the versioned evaluation split, which sees every expanded
frame before assigning a sequence to gallery or query use.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ml.ingestion.common import RequestRateLimiter, RetryMetrics, read_json, request_with_retry, write_json
from ml.ingestion.mapillary_citywide import (
    DEFAULT_MAX_GRAPH_RESPONSE_BYTES,
    _has_usable_source_dimensions,
    fetch_metadata_batch,
    load_aoi_boundary,
)
from ml.ingestion.merge_sources import haversine_meters
from ml.ingestion.parsers import parse_mapillary_item
from ml.ingestion.state import ingestion_config_fingerprint

SEQUENCE_ENDPOINT = "https://graph.mapillary.com/image_ids"
CHECKPOINT_SCHEMA_VERSION = 1
SEQUENCE_CACHE_SCHEMA_VERSION = 1
DEFAULT_MAX_SEQUENCE_RESPONSE_BYTES = 4 * 1024 * 1024


class MapillaryReferenceExpansionError(RuntimeError):
    """The expansion configuration, checkpoint, or source response is invalid."""


@dataclass(frozen=True, slots=True)
class DiverseFrameConfig:
    max_per_sequence: int = 5
    min_spacing_m: float = 35.0
    heading_diversity_deg: float = 45.0
    min_heading_spacing_m: float = 8.0
    phash_distance_threshold: int = 4

    def __post_init__(self) -> None:
        if self.max_per_sequence < 1:
            raise ValueError("max_per_sequence must be >= 1")
        if self.min_spacing_m < 0 or self.min_heading_spacing_m < 0:
            raise ValueError("spacing thresholds must be non-negative")
        if self.min_heading_spacing_m > self.min_spacing_m:
            raise ValueError("min_heading_spacing_m must not exceed min_spacing_m")
        if not 0 <= self.heading_diversity_deg <= 180:
            raise ValueError("heading_diversity_deg must be in [0, 180]")
        if not 0 <= self.phash_distance_threshold <= 64:
            raise ValueError("phash_distance_threshold must be in [0, 64]")


DEFAULT_DIVERSE_FRAME_CONFIG = DiverseFrameConfig()


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _finite_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _heading_delta(left: Any, right: Any) -> float | None:
    first = _finite_float(left)
    second = _finite_float(right)
    if first is None or second is None:
        return None
    delta = abs((first - second) % 360.0)
    return min(delta, 360.0 - delta)


def _phash_int(row: Mapping[str, Any]) -> int | None:
    value = _text(row.get("perceptual_hash") or row.get("phash"))
    if not value:
        return None
    try:
        return int(value, 16)
    except ValueError:
        return None


def _near_phash(left: Mapping[str, Any], right: Mapping[str, Any], threshold: int) -> bool:
    first = _phash_int(left)
    second = _phash_int(right)
    return first is not None and second is not None and (first ^ second).bit_count() <= threshold


def _stable_tie_key(row: Mapping[str, Any]) -> str:
    identity = _text(row.get("source_image_id") or row.get("id"))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _distance(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    return haversine_meters(float(left["lat"]), float(left["lon"]), float(right["lat"]), float(right["lon"]))


def select_diverse_frames(
    rows: Sequence[Mapping[str, Any]],
    *,
    config: DiverseFrameConfig = DEFAULT_DIVERSE_FRAME_CONFIG,
    anchor_image_id: str | None = None,
) -> list[dict[str, Any]]:
    """Deterministically select spatial, heading, and visual diversity.

    Exact source IDs are always deduplicated.  When pHashes are supplied,
    near-duplicates are rejected before spatial scoring.  Metadata-only runs
    still use displacement and heading; the ordinary cleaning pipeline applies
    image-byte and pHash deduplication after download.
    """

    unique: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for value in rows:
        row = dict(value)
        identity = _text(row.get("source_image_id") or row.get("id"))
        lat = _finite_float(row.get("lat"))
        lon = _finite_float(row.get("lon"))
        if not identity or identity in seen_ids or lat is None or lon is None:
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        row["source_image_id"] = identity
        row["lat"] = lat
        row["lon"] = lon
        seen_ids.add(identity)
        unique.append(row)
    if not unique:
        return []

    ordered = sorted(unique, key=lambda row: (_stable_tie_key(row), row["source_image_id"]))
    anchor = next((row for row in ordered if row["source_image_id"] == anchor_image_id), ordered[0])
    selected = [anchor]
    remaining = [row for row in ordered if row is not anchor]
    while remaining and len(selected) < config.max_per_sequence:
        eligible: list[tuple[float, str, dict[str, Any]]] = []
        for row in remaining:
            if any(_near_phash(row, kept, config.phash_distance_threshold) for kept in selected):
                continue
            distances = [_distance(row, kept) for kept in selected]
            min_distance = min(distances)
            heading_deltas = [
                delta
                for kept in selected
                if (delta := _heading_delta(row.get("heading"), kept.get("heading"))) is not None
            ]
            min_heading_delta = min(heading_deltas) if heading_deltas else 0.0
            spatially_novel = min_distance >= config.min_spacing_m
            directionally_novel = (
                min_distance >= config.min_heading_spacing_m
                and min_heading_delta >= config.heading_diversity_deg
            )
            if not (spatially_novel or directionally_novel):
                continue
            spatial_score = min_distance / max(config.min_spacing_m, 1.0)
            heading_score = min_heading_delta / max(config.heading_diversity_deg, 1.0)
            score = spatial_score + 0.5 * heading_score
            eligible.append((score, _stable_tie_key(row), row))
        if not eligible:
            break
        _, _, chosen = max(eligible, key=lambda item: (item[0], item[1]))
        selected.append(chosen)
        remaining = [row for row in remaining if row is not chosen]
    return selected


def _read_bounded_json(response: Any, *, max_bytes: int) -> Any:
    declared = response.headers.get("Content-Length")
    if declared not in (None, "") and int(declared) > max_bytes:
        raise MapillaryReferenceExpansionError("Mapillary sequence response exceeds configured byte limit")
    payload = bytearray()
    for chunk in response.iter_content(chunk_size=64 * 1024):
        if chunk:
            payload.extend(chunk)
            if len(payload) > max_bytes:
                raise MapillaryReferenceExpansionError("Mapillary sequence response exceeds configured byte limit")
    try:
        return json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MapillaryReferenceExpansionError("Mapillary sequence response is not valid JSON") from exc


def fetch_sequence_image_ids(
    session: Any,
    *,
    token: str,
    sequence_id: str,
    retries: int = 5,
    backoff_sec: float = 1.5,
    timeout_sec: float = 30.0,
    max_response_bytes: int = DEFAULT_MAX_SEQUENCE_RESPONSE_BYTES,
    retry_metrics: RetryMetrics | None = None,
    before_request: Callable[[], None] | None = None,
) -> list[str]:
    """Fetch the documented capture-time-ordered image IDs for one sequence."""

    parsed: dict[str, Any] = {}

    def validate(response: Any) -> None:
        payload = _read_bounded_json(response, max_bytes=max_response_bytes)
        if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), list):
            raise MapillaryReferenceExpansionError("Mapillary sequence response must contain a data array")
        if payload.get("paging"):
            raise MapillaryReferenceExpansionError("unexpected paginated sequence response; refusing partial expansion")
        parsed["payload"] = payload

    response = request_with_retry(
        session,
        url=SEQUENCE_ENDPOINT,
        params={"sequence_id": sequence_id, "access_token": token},
        retries=retries,
        backoff_sec=backoff_sec,
        timeout_sec=timeout_sec,
        metrics=retry_metrics,
        before_request=before_request,
        request_kwargs={"stream": True},
        response_validator=validate,
    )
    try:
        result: list[str] = []
        seen: set[str] = set()
        for item in parsed["payload"]["data"]:
            identity = _text(item.get("id") if isinstance(item, Mapping) else None)
            if identity and identity not in seen:
                seen.add(identity)
                result.append(identity)
        return result
    finally:
        response.close()


def _load_env_value(path: Path, name: str) -> str | None:
    if not path.is_file():
        return None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == name:
            return value.strip().strip('"').strip("'")
    return None


def _sequence_cache_path(cache_dir: Path, sequence_id: str) -> Path:
    safe = "".join(character for character in sequence_id if character.isalnum() or character in "-_")
    if not safe or safe != sequence_id:
        safe = hashlib.sha256(sequence_id.encode("utf-8")).hexdigest()
    return cache_dir / "sequences" / f"{safe}.json"


def _load_sequence_cache(cache_dir: Path, sequence_id: str) -> list[str] | None:
    payload = read_json(_sequence_cache_path(cache_dir, sequence_id), default=None)
    if not isinstance(payload, Mapping):
        return None
    ids = payload.get("image_ids")
    if (
        payload.get("schema_version") != SEQUENCE_CACHE_SCHEMA_VERSION
        or payload.get("sequence_id") != sequence_id
        or not isinstance(ids, list)
        or any(not isinstance(value, str) or not value for value in ids)
    ):
        return None
    return list(dict.fromkeys(ids))


def _write_sequence_cache(cache_dir: Path, sequence_id: str, image_ids: Sequence[str]) -> None:
    write_json(
        _sequence_cache_path(cache_dir, sequence_id),
        {
            "schema_version": SEQUENCE_CACHE_SCHEMA_VERSION,
            "sequence_id": sequence_id,
            "endpoint": SEQUENCE_ENDPOINT,
            "ordered_by": "provider_capture_time",
            "fetched_at_unix": time.time(),
            "image_ids": list(image_ids),
        },
    )


def _load_plan_targets(path: Path | None) -> tuple[set[tuple[int, int]], set[str]]:
    if path is None:
        return set(), set()
    payload = read_json(path, default={})
    if not isinstance(payload, Mapping):
        raise MapillaryReferenceExpansionError("acquisition plan must be a JSON object")
    cells: set[tuple[int, int]] = set()
    sequences: set[str] = set()
    for cell in payload.get("tranche", {}).get("cells", []):
        if not isinstance(cell, Mapping):
            continue
        cells.add((int(cell["x"]), int(cell["y"])))
        sequences.update(_text(value) for value in cell.get("mapillary_sequence_ids", []) if _text(value))
    return cells, sequences


def _cell_index(lat: float, lon: float, bounds: Sequence[float], bins: int) -> tuple[int, int]:
    west, south, east, north = (float(value) for value in bounds)
    x = min(bins - 1, max(0, int((lon - west) / (east - west) * bins)))
    y = min(bins - 1, max(0, int((lat - south) / (north - south) * bins)))
    return x, y


def _checkpoint(path: Path, fingerprint: str) -> dict[str, Any]:
    payload = read_json(path, default={})
    if not payload:
        return {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "source": "mapillary_reference_expansion",
            "config_fingerprint": fingerprint,
            "completed_sequences": [],
            "failed_sequences": {},
        }
    if (
        not isinstance(payload, Mapping)
        or payload.get("schema_version") != CHECKPOINT_SCHEMA_VERSION
        or payload.get("source") != "mapillary_reference_expansion"
        or payload.get("config_fingerprint") != fingerprint
        or not isinstance(payload.get("completed_sequences"), list)
        or not isinstance(payload.get("failed_sequences"), Mapping)
    ):
        raise MapillaryReferenceExpansionError("incompatible Mapillary expansion checkpoint")
    return dict(payload)


def run(
    *,
    discovery_jsons: Sequence[Path],
    output_json: Path,
    checkpoint_path: Path,
    cache_dir: Path,
    stats_path: Path,
    aoi_geojson: Path,
    acquisition_plan: Path | None = None,
    max_sequences: int = 200,
    config: DiverseFrameConfig = DEFAULT_DIVERSE_FRAME_CONFIG,
    metadata_batch_size: int = 50,
    request_interval_sec: float = 0.1,
    token: str | None = None,
    session: Any | None = None,
) -> dict[str, Any]:
    if max_sequences < 1 or not 1 <= metadata_batch_size <= 50:
        raise ValueError("invalid sequence or metadata batch limit")
    if not discovery_jsons:
        raise ValueError("at least one discovery_json is required")
    discovery_by_id: dict[str, dict[str, Any]] = {}
    for discovery_json in discovery_jsons:
        payload = read_json(discovery_json, default=[])
        if not isinstance(payload, list):
            raise MapillaryReferenceExpansionError("discovery JSON must be an array")
        for value in payload:
            if not isinstance(value, Mapping):
                continue
            identity = _text(value.get("source_image_id") or value.get("id"))
            if identity:
                discovery_by_id[identity] = dict(value)
    discovery = list(discovery_by_id.values())
    boundary = load_aoi_boundary(aoi_geojson)
    target_cells, target_sequences = _load_plan_targets(acquisition_plan)
    bins = 20
    bounds = boundary.bounds.to_list()
    candidates: list[dict[str, Any]] = []
    seen_sequences: set[str] = set()
    for value in discovery:
        if not isinstance(value, Mapping):
            continue
        sequence_id = _text(value.get("sequence_id"))
        lat = _finite_float(value.get("lat"))
        lon = _finite_float(value.get("lon"))
        if not sequence_id or sequence_id in seen_sequences or lat is None or lon is None:
            continue
        cell = _cell_index(lat, lon, bounds, bins)
        if target_sequences and sequence_id not in target_sequences:
            continue
        if target_cells and cell not in target_cells:
            continue
        seen_sequences.add(sequence_id)
        candidates.append(dict(value) | {"_expansion_cell": list(cell)})
    candidates = sorted(
        candidates,
        key=lambda row: (
            hashlib.sha256(_text(row["sequence_id"]).encode("utf-8")).hexdigest(),
            _text(row["sequence_id"]),
        ),
    )[:max_sequences]
    fingerprint = ingestion_config_fingerprint(
        {
            "source": "mapillary",
            "method": "official_sequence_image_ids_then_graph_batch_diversity_selection",
            "discovery_sha256": {
                str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in discovery_jsons
            },
            "aoi_sha256": boundary.sha256,
            "plan_sha256": hashlib.sha256(acquisition_plan.read_bytes()).hexdigest() if acquisition_plan else None,
            "sequence_ids": [_text(row["sequence_id"]) for row in candidates],
            "diversity": {
                "max_per_sequence": config.max_per_sequence,
                "min_spacing_m": config.min_spacing_m,
                "heading_diversity_deg": config.heading_diversity_deg,
                "min_heading_spacing_m": config.min_heading_spacing_m,
                "phash_distance_threshold": config.phash_distance_threshold,
            },
        }
    )
    progress = _checkpoint(checkpoint_path, fingerprint)
    completed = set(progress["completed_sequences"])
    existing = read_json(output_json, default=[])
    if not isinstance(existing, list):
        raise MapillaryReferenceExpansionError("existing expansion output must be an array")
    records_by_id = {
        _text(row.get("source_image_id") or row.get("id")): dict(row)
        for row in existing
        if isinstance(row, Mapping) and _text(row.get("source_image_id") or row.get("id"))
    }
    active_token = token or os.environ.get("MAPILLARY_ACCESS_TOKEN") or _load_env_value(Path(".env"), "MAPILLARY_ACCESS_TOKEN")
    if not active_token:
        raise MapillaryReferenceExpansionError("MAPILLARY_ACCESS_TOKEN is required")
    owns_session = session is None
    if session is None:
        import requests

        session = requests.Session()
    limiter = RequestRateLimiter(request_interval_sec)
    retry_metrics = RetryMetrics()
    started = time.monotonic()
    sequence_id_total = 0
    metadata_total = 0
    selected_total = 0

    def save() -> None:
        progress["completed_sequences"] = sorted(completed)
        write_json(checkpoint_path, progress)
        write_json(output_json, sorted(records_by_id.values(), key=lambda row: _text(row.get("source_image_id"))))

    try:
        for representative in candidates:
            sequence_id = _text(representative["sequence_id"])
            if sequence_id in completed:
                continue
            try:
                image_ids = _load_sequence_cache(cache_dir, sequence_id)
                if image_ids is None:
                    image_ids = fetch_sequence_image_ids(
                        session,
                        token=active_token,
                        sequence_id=sequence_id,
                        retry_metrics=retry_metrics,
                        before_request=limiter.wait,
                    )
                    _write_sequence_cache(cache_dir, sequence_id, image_ids)
                sequence_id_total += len(image_ids)
                hydrated: dict[str, dict[str, Any]] = {}
                for offset in range(0, len(image_ids), metadata_batch_size):
                    batch = image_ids[offset : offset + metadata_batch_size]
                    hydrated.update(
                        fetch_metadata_batch(
                            session,
                            token=active_token,
                            image_ids=batch,
                            retries=5,
                            backoff_sec=1.5,
                            timeout_sec=30.0,
                            retry_metrics=retry_metrics,
                            before_request=limiter.wait,
                            max_response_bytes=DEFAULT_MAX_GRAPH_RESPONSE_BYTES,
                        )
                    )
                metadata_total += len(hydrated)
                parsed_rows: list[dict[str, Any]] = []
                for image_id in image_ids:
                    item = hydrated.get(image_id)
                    if item is None or not _has_usable_source_dimensions(item):
                        continue
                    enriched = dict(item)
                    enriched["_geosnap_reference_expansion"] = {
                        "method": "official_sequence_image_ids_then_graph_batch",
                        "sequence_endpoint": SEQUENCE_ENDPOINT,
                        "sequence_id": sequence_id,
                        "discovery_image_id": _text(representative.get("source_image_id") or representative.get("id")),
                        "discovery_cell": representative["_expansion_cell"],
                    }
                    row = parse_mapillary_item(enriched)
                    if row is None or _text(row.get("sequence_id")) != sequence_id:
                        continue
                    if not boundary.covers(float(row["lon"]), float(row["lat"])):
                        continue
                    actual_cell = _cell_index(float(row["lat"]), float(row["lon"]), bounds, bins)
                    if target_cells and actual_cell not in target_cells:
                        continue
                    row["_geosnap_expansion_cell"] = list(actual_cell)
                    parsed_rows.append(row)
                selected = select_diverse_frames(
                    parsed_rows,
                    config=config,
                    anchor_image_id=_text(representative.get("source_image_id") or representative.get("id")),
                )
                for rank, row in enumerate(selected, start=1):
                    row["_geosnap_sequence_selection_rank"] = rank
                    records_by_id[_text(row["source_image_id"])] = row
                selected_total += len(selected)
                completed.add(sequence_id)
                progress["failed_sequences"].pop(sequence_id, None)
            except Exception as exc:  # noqa: BLE001 - preserve resumable progress
                progress["failed_sequences"][sequence_id] = f"{type(exc).__name__}: {str(exc)[:200]}"
            save()
        stats: dict[str, Any] = {
            "schema_version": 1,
            "complete": len(completed) == len(candidates) and not progress["failed_sequences"],
            "config_fingerprint": fingerprint,
            "method": "official_sequence_image_ids_then_graph_batch_diversity_selection",
            "sequence_endpoint": SEQUENCE_ENDPOINT,
            "discovered_sequence_candidates": len(candidates),
            "completed_sequences": len(completed),
            "failed_sequences": len(progress["failed_sequences"]),
            "image_ids_observed_this_run": sequence_id_total,
            "metadata_records_hydrated_this_run": metadata_total,
            "frames_selected_this_run": selected_total,
            "output_records": len(records_by_id),
            "elapsed_seconds": round(time.monotonic() - started, 6),
            "diversity_config": {
                "max_per_sequence": config.max_per_sequence,
                "min_spacing_m": config.min_spacing_m,
                "heading_diversity_deg": config.heading_diversity_deg,
                "min_heading_spacing_m": config.min_heading_spacing_m,
                "phash_distance_threshold": config.phash_distance_threshold,
            },
            **retry_metrics.as_dict(),
        }
        write_json(stats_path, stats)
        return stats
    finally:
        if owns_session:
            session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Expand selected Mapillary sequences into diverse gallery frames")
    parser.add_argument("--discovery-json", type=Path, action="append", required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--stats", type=Path, required=True)
    parser.add_argument("--aoi-geojson", type=Path, required=True)
    parser.add_argument("--acquisition-plan", type=Path)
    parser.add_argument("--max-sequences", type=int, default=200)
    parser.add_argument("--max-per-sequence", type=int, default=5)
    parser.add_argument("--min-spacing-m", type=float, default=35.0)
    parser.add_argument("--heading-diversity-deg", type=float, default=45.0)
    parser.add_argument("--min-heading-spacing-m", type=float, default=8.0)
    parser.add_argument("--phash-distance-threshold", type=int, default=4)
    parser.add_argument("--metadata-batch-size", type=int, default=50)
    parser.add_argument("--request-interval-sec", type=float, default=0.1)
    args = parser.parse_args()
    stats = run(
        discovery_jsons=args.discovery_json,
        output_json=args.output_json,
        checkpoint_path=args.checkpoint,
        cache_dir=args.cache_dir,
        stats_path=args.stats,
        aoi_geojson=args.aoi_geojson,
        acquisition_plan=args.acquisition_plan,
        max_sequences=args.max_sequences,
        config=DiverseFrameConfig(
            max_per_sequence=args.max_per_sequence,
            min_spacing_m=args.min_spacing_m,
            heading_diversity_deg=args.heading_diversity_deg,
            min_heading_spacing_m=args.min_heading_spacing_m,
            phash_distance_threshold=args.phash_distance_threshold,
        ),
        metadata_batch_size=args.metadata_batch_size,
        request_interval_sec=args.request_interval_sec,
    )
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
