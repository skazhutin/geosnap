"""Restart-safe Mapillary Graph API ingestion for Moscow tiles."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ml.ingestion.common import RetryMetrics, request_with_retry, sanitize_error_message
from ml.ingestion.grid import BBox, iter_moscow_tiles
from ml.ingestion.kartaview_loader import load_query_points_config
from ml.ingestion.parsers import parse_mapillary_item
from ml.ingestion.state import IngestionState, ingestion_config_fingerprint

logger = logging.getLogger(__name__)

MAPILLARY_ENDPOINT = "https://graph.mapillary.com/images"
MAPILLARY_FIELDS = ",".join(
    [
        "id",
        "captured_at",
        "geometry",
        "computed_geometry",
        "compass_angle",
        "computed_compass_angle",
        "thumb_2048_url",
        "thumb_1024_url",
        "thumb_original_url",
        "sequence",
        "creator",
        "width",
        "height",
        "camera_type",
    ]
)


@dataclass(frozen=True)
class TileFetchResult:
    items: list[dict[str, Any]]
    pages: int
    truncated: bool


def _request_with_retry(
    session: Any,
    *,
    url: str,
    params: dict[str, Any],
    retries: int,
    backoff_sec: float,
    timeout_sec: float = 30.0,
    metrics: RetryMetrics | None = None,
) -> Any:
    return request_with_retry(
        session,
        url=url,
        params=params,
        retries=retries,
        backoff_sec=backoff_sec,
        timeout_sec=timeout_sec,
        metrics=metrics,
    )


def fetch_tile(
    session: Any,
    *,
    token: str,
    bbox: tuple[float, float, float, float],
    limit: int,
    retries: int,
    backoff_sec: float,
    max_pages: int,
    timeout_sec: float = 30.0,
    return_details: bool = False,
    retry_metrics: RetryMetrics | None = None,
) -> list[dict[str, Any]] | TileFetchResult:
    if not 1 <= limit <= 2000:
        raise ValueError("Mapillary limit must be between 1 and 2000")
    if max_pages < 1:
        raise ValueError("max_pages must be >= 1")
    min_lon, min_lat, max_lon, max_lat = bbox
    params = {
        "access_token": token,
        "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "fields": MAPILLARY_FIELDS,
        "limit": limit,
    }
    all_items: list[dict[str, Any]] = []
    next_url: str | None = MAPILLARY_ENDPOINT
    next_params: dict[str, Any] | None = params
    pages = 0
    saturated_without_paging = False

    while next_url and pages < max_pages:
        pages += 1
        response = _request_with_retry(
            session,
            url=next_url,
            params=next_params or {},
            retries=retries,
            backoff_sec=backoff_sec,
            timeout_sec=timeout_sec,
            metrics=retry_metrics,
        )
        try:
            payload = response.json()
        finally:
            if hasattr(response, "close"):
                response.close()
        if not isinstance(payload, dict):
            raise ValueError("Mapillary response must be a JSON object")
        batch = payload.get("data", [])
        if not isinstance(batch, list):
            raise ValueError("Mapillary response data must be a list")
        all_items.extend(item for item in batch if isinstance(item, dict))

        paging = payload.get("paging")
        next_link = paging.get("next") if isinstance(paging, dict) else None
        if isinstance(next_link, str) and next_link:
            next_url = next_link
            next_params = None
        else:
            next_url = None
            # Current spatial endpoints may be capped without pagination.
            # Record this honestly instead of claiming the tile is complete.
            saturated_without_paging = len(batch) >= limit

    truncated = next_url is not None or saturated_without_paging
    if truncated:
        logger.warning("mapillary_tile_truncated pages=%s limit=%s", pages, limit)
    details = TileFetchResult(all_items, pages=pages, truncated=truncated)
    return details if return_details else details.items


def run(
    output_json: Path,
    limit_per_tile: int,
    request_pause_sec: float,
    request_retries: int,
    backoff_sec: float,
    max_pages_per_tile: int,
    *,
    timeout_sec: float = 30.0,
    checkpoint_path: Path | None = None,
    stats_path: Path | None = None,
    checkpoint_every_tiles: int = 1,
    lat_step: float = 0.01,
    lon_step: float = 0.01,
    max_tiles: int | None = None,
    query_points_config: Path | None = None,
    query_radius_override_m: float | None = None,
) -> dict[str, Any]:
    if not 1 <= limit_per_tile <= 2000:
        raise ValueError("limit_per_tile must be between 1 and 2000")
    if request_pause_sec < 0:
        raise ValueError("request_pause_sec must be >= 0")
    if request_retries < 1:
        raise ValueError("request_retries must be >= 1")
    if backoff_sec < 0:
        raise ValueError("backoff_sec must be >= 0")
    if max_pages_per_tile < 1:
        raise ValueError("max_pages_per_tile must be >= 1")
    if timeout_sec <= 0:
        raise ValueError("timeout_sec must be > 0")
    if checkpoint_every_tiles < 1:
        raise ValueError("checkpoint_every_tiles must be >= 1")
    if max_tiles is not None and max_tiles < 1:
        raise ValueError("max_tiles must be >= 1")
    if query_radius_override_m is not None:
        if query_points_config is None:
            raise ValueError("query_radius_override_m requires query_points_config")
        if not 1 <= query_radius_override_m <= 500:
            raise ValueError("query_radius_override_m must be between 1 and 500")

    token = os.getenv("MAPILLARY_ACCESS_TOKEN")
    if not token:
        raise RuntimeError(
            "MAPILLARY_ACCESS_TOKEN is required for live Mapillary ingestion; "
            "fixture-based ingestion tests remain available without it"
        )

    if query_points_config is not None:
        query_tiles: list[tuple[str, BBox]] = []
        for key, lat, lon, configured_radius_m in load_query_points_config(query_points_config):
            radius_m = query_radius_override_m or configured_radius_m
            lat_delta = radius_m / 111_320.0
            lon_delta = radius_m / (111_320.0 * max(math.cos(math.radians(lat)), 1e-6))
            query_tiles.append(
                (
                    key,
                    BBox(
                        min_lat=lat - lat_delta,
                        max_lat=lat + lat_delta,
                        min_lon=lon - lon_delta,
                        max_lon=lon + lon_delta,
                    ),
                )
            )
    else:
        query_tiles = [(tile.key, tile) for tile in iter_moscow_tiles(lat_step=lat_step, lon_step=lon_step)]
    if max_tiles is not None:
        query_tiles = query_tiles[:max_tiles]
    config_fingerprint = ingestion_config_fingerprint(
        {
            "source": "mapillary",
            "endpoint": MAPILLARY_ENDPOINT,
            "fields": MAPILLARY_FIELDS,
            "limit_per_tile": limit_per_tile,
            "tiles": [
                {
                    "key": key,
                    "bbox": [tile.min_lon, tile.min_lat, tile.max_lon, tile.max_lat],
                }
                for key, tile in query_tiles
            ],
        }
    )
    started_at = time.monotonic()
    retry_metrics = RetryMetrics()
    state = IngestionState.load(
        source="mapillary",
        output_json=output_json,
        checkpoint_path=checkpoint_path,
        stats_path=stats_path,
        config_fingerprint=config_fingerprint,
    )
    seen_ids = state.seen_ids
    state.stats["tiles_total"] = len(query_tiles)

    import requests

    processed_since_save = 0
    live_attempts = 0
    live_successes = 0
    with requests.Session() as session:
        for tile_key, tile in query_tiles:
            if tile_key in state.completed_tiles:
                state.stats["tiles_skipped_checkpoint"] = int(state.stats["tiles_skipped_checkpoint"]) + 1
                continue
            live_attempts += 1
            state.stats["tiles_attempted"] = int(state.stats["tiles_attempted"]) + 1
            try:
                details = fetch_tile(
                    session,
                    token=token,
                    bbox=(tile.min_lon, tile.min_lat, tile.max_lon, tile.max_lat),
                    limit=limit_per_tile,
                    retries=request_retries,
                    backoff_sec=backoff_sec,
                    max_pages=max_pages_per_tile,
                    timeout_sec=timeout_sec,
                    return_details=True,
                    retry_metrics=retry_metrics,
                )
                assert isinstance(details, TileFetchResult)
                live_successes += 1
                state.stats["pages_fetched"] = int(state.stats["pages_fetched"]) + details.pages
                state.stats["source_records_discovered"] = int(state.stats["source_records_discovered"]) + len(
                    details.items
                )
                if details.truncated:
                    state.stats["truncated_tiles"] = int(state.stats["truncated_tiles"]) + 1
                    state.stats["tiles_incomplete"] = int(state.stats["tiles_incomplete"]) + 1

                for item in details.items:
                    try:
                        item_with_provenance = dict(item)
                        item_with_provenance["_geosnap_acquisition_query"] = tile_key
                        parsed = parse_mapillary_item(item_with_provenance)
                    except Exception as exc:  # noqa: BLE001 - isolate corrupt source records
                        logger.warning(
                            "mapillary_record_invalid reason=%s", sanitize_error_message(exc, max_length=160)
                        )
                        parsed = None
                    if parsed is None:
                        state.stats["invalid_records"] = int(state.stats["invalid_records"]) + 1
                        continue
                    if parsed["id"] in seen_ids:
                        state.stats["duplicate_records"] = int(state.stats["duplicate_records"]) + 1
                        continue
                    seen_ids.add(parsed["id"])
                    state.records.append(parsed)
                    state.stats["metadata_normalized"] = int(state.stats["metadata_normalized"]) + 1
                if details.truncated:
                    state.completed_tiles.discard(tile_key)
                    state.failed_tiles[tile_key] = (
                        f"truncated:max_pages_per_tile={max_pages_per_tile}; "
                        "rerun with a larger page budget"
                    )
                else:
                    state.stats["tiles_succeeded"] = int(state.stats["tiles_succeeded"]) + 1
                    state.completed_tiles.add(tile_key)
                    state.failed_tiles.pop(tile_key, None)
            except Exception as exc:  # noqa: BLE001 - failed tiles remain resumable
                reason = f"{type(exc).__name__}: {sanitize_error_message(exc, max_length=180)}"
                state.failed_tiles[tile_key] = reason
                state.stats["tiles_failed"] = int(state.stats["tiles_failed"]) + 1
                logger.error("mapillary_tile_failed tile=%s reason=%s", tile_key, reason)
            finally:
                processed_since_save += 1
                if processed_since_save >= checkpoint_every_tiles:
                    state.save()
                    processed_since_save = 0
                if request_pause_sec:
                    time.sleep(request_pause_sec)

    elapsed_seconds = time.monotonic() - started_at
    for name, value in retry_metrics.as_dict().items():
        state.stats[name] = int(state.stats.get(name, 0)) + value
    cumulative_elapsed = float(state.stats.get("elapsed_seconds", 0.0)) + elapsed_seconds
    state.stats["last_run_elapsed_seconds"] = round(elapsed_seconds, 6)
    state.stats["elapsed_seconds"] = round(cumulative_elapsed, 6)
    state.stats["records_per_second"] = (
        round(int(state.stats["metadata_normalized"]) / cumulative_elapsed, 6) if cumulative_elapsed > 0 else 0.0
    )
    state.save(compact=True)
    if live_attempts > 0 and live_successes == 0:
        raise RuntimeError(
            f"all {live_attempts} live Mapillary tile requests failed; "
            f"see {state.stats_path} and {state.checkpoint_path}"
        )
    return state.stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Mapillary metadata for Moscow")
    parser.add_argument("--output-json", default="data/raw/mapillary_raw.json")
    parser.add_argument("--checkpoint")
    parser.add_argument("--stats")
    parser.add_argument("--limit-per-tile", type=int, default=2000)
    parser.add_argument("--request-pause-sec", type=float, default=0.25)
    parser.add_argument("--request-retries", type=int, default=5)
    parser.add_argument("--backoff-sec", type=float, default=1.5)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--max-pages-per-tile", type=int, default=200)
    parser.add_argument("--checkpoint-every-tiles", type=int, default=1)
    parser.add_argument("--lat-step", type=float, default=0.01)
    parser.add_argument("--lon-step", type=float, default=0.01)
    parser.add_argument("--max-tiles", type=int)
    parser.add_argument("--query-points-config", type=Path)
    parser.add_argument("--query-radius-override-m", type=float)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    stats = run(
        output_json=Path(args.output_json),
        limit_per_tile=args.limit_per_tile,
        request_pause_sec=args.request_pause_sec,
        request_retries=args.request_retries,
        backoff_sec=args.backoff_sec,
        max_pages_per_tile=args.max_pages_per_tile,
        timeout_sec=args.timeout_sec,
        checkpoint_path=Path(args.checkpoint) if args.checkpoint else None,
        stats_path=Path(args.stats) if args.stats else None,
        checkpoint_every_tiles=args.checkpoint_every_tiles,
        lat_step=args.lat_step,
        lon_step=args.lon_step,
        max_tiles=args.max_tiles,
        query_points_config=args.query_points_config,
        query_radius_override_m=args.query_radius_override_m,
    )
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
