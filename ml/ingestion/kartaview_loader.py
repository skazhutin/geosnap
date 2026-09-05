"""Restart-safe ingestion from the current public KartaView Photo API."""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ml.ingestion.common import RequestRateLimiter, RetryMetrics, request_with_retry, sanitize_error_message
from ml.ingestion.grid import iter_moscow_tiles
from ml.ingestion.parsers import parse_kartaview_item
from ml.ingestion.state import IngestionState, ingestion_config_fingerprint

logger = logging.getLogger(__name__)

KARTAVIEW_API_URL = "https://api.openstreetcam.org/2.0/photo/"
AREA_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


@dataclass(frozen=True)
class TileFetchResult:
    items: list[dict[str, Any]]
    pages: int
    truncated: bool


def load_query_points_config(path: Path) -> list[tuple[str, float, float, int]]:
    """Load a deterministic, bounded set of coverage-confirmed query points."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid KartaView query-points config: {path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError("KartaView query-points config must use schema_version 1")
    dataset_id = payload.get("dataset_id")
    if not isinstance(dataset_id, str) or not AREA_ID_RE.fullmatch(dataset_id):
        raise ValueError("query-points dataset_id must be a lowercase slug")
    defaults = payload.get("query_defaults", {})
    if not isinstance(defaults, dict):
        raise ValueError("query_defaults must be an object")
    default_radius = defaults.get("radius_m", 200)
    try:
        default_radius = int(default_radius)
    except (TypeError, ValueError) as exc:
        raise ValueError("query_defaults.radius_m must be an integer") from exc
    areas = payload.get("areas")
    if not isinstance(areas, list) or not areas:
        raise ValueError("query-points config must contain at least one area")

    query_points: list[tuple[str, float, float, int]] = []
    seen_area_ids: set[str] = set()
    seen_coordinates: set[tuple[float, float, int]] = set()
    for area in areas:
        if not isinstance(area, dict):
            raise ValueError("every query-points area must be an object")
        area_id = area.get("area_id")
        if not isinstance(area_id, str) or not AREA_ID_RE.fullmatch(area_id):
            raise ValueError("area_id must be a lowercase slug")
        if area_id in seen_area_ids:
            raise ValueError(f"duplicate area_id: {area_id}")
        seen_area_ids.add(area_id)
        points = area.get("points")
        if not isinstance(points, list) or not points:
            raise ValueError(f"area {area_id!r} must contain at least one point")
        for index, point in enumerate(points):
            if not isinstance(point, dict):
                raise ValueError(f"area {area_id!r} point {index} must be an object")
            try:
                lat = float(point["lat"])
                lon = float(point["lon"])
                radius_m = int(point.get("radius_m", default_radius))
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"invalid point {index} in area {area_id!r}") from exc
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                raise ValueError(f"point {index} in area {area_id!r} is outside world bounds")
            if not 1 <= radius_m <= 10_000:
                raise ValueError(f"point {index} in area {area_id!r} radius_m must be in [1, 10000]")
            coordinate_key = (round(lat, 7), round(lon, 7), radius_m)
            if coordinate_key in seen_coordinates:
                raise ValueError(f"duplicate query point in area {area_id!r}")
            seen_coordinates.add(coordinate_key)
            query_points.append(
                (
                    f"config:{dataset_id}:{area_id}:{index:03d}",
                    lat,
                    lon,
                    radius_m,
                )
            )
    return query_points


def _request_with_retry(
    session: Any,
    *,
    url: str,
    params: dict[str, Any],
    retries: int,
    backoff_sec: float,
    timeout_sec: float = 30.0,
    metrics: RetryMetrics | None = None,
    before_request: Any | None = None,
) -> Any:
    return request_with_retry(
        session,
        url=url,
        params=params,
        retries=retries,
        backoff_sec=backoff_sec,
        timeout_sec=timeout_sec,
        metrics=metrics,
        before_request=before_request,
    )


def _extract_page(payload: Any, page_size: int | None = None) -> tuple[list[dict[str, Any]], bool]:
    if not isinstance(payload, dict):
        return [], False
    status = payload.get("status")
    if isinstance(status, dict):
        logical_http = status.get("httpCode")
        if logical_http is not None and int(logical_http) >= 400:
            raise RuntimeError(
                f"KartaView API error {logical_http}: {status.get('apiMessage') or status.get('httpMessage')}"
            )
    result = payload.get("result")
    if not isinstance(result, dict):
        return [], False
    raw_data = result.get("data")
    data = [item for item in raw_data if isinstance(item, dict)] if isinstance(raw_data, list) else []

    for key in ("hasMoreData", "hasMore", "has_more"):
        value = result.get(key)
        if isinstance(value, bool):
            return data, value
        if value in (0, 1, "0", "1"):
            return data, str(value) == "1"
    current_page = result.get("currentPage") or result.get("page")
    total_pages = result.get("totalPages")
    try:
        if current_page is not None and total_pages is not None:
            return data, int(current_page) < int(total_pages)
    except (TypeError, ValueError):
        pass
    return data, bool(data) if page_size is None else len(data) >= page_size


def fetch_tile(
    session: Any,
    bbox: tuple[float, float, float, float] | None = None,
    limit: int = 150,
    retries: int = 5,
    backoff_sec: float = 1.5,
    max_pages: int = 200,
    *,
    lat: float | None = None,
    lon: float | None = None,
    radius_m: int | None = None,
    timeout_sec: float = 30.0,
    return_details: bool = False,
    retry_metrics: RetryMetrics | None = None,
    request_rate_limiter: RequestRateLimiter | None = None,
) -> list[dict[str, Any]] | TileFetchResult:
    """Fetch photos around a point; bbox is converted for API compatibility."""
    if not 1 <= limit <= 150:
        raise ValueError("KartaView itemsPerPage must be between 1 and 150")
    if max_pages < 1:
        raise ValueError("max_pages must be >= 1")
    if bbox is not None:
        min_lon, min_lat, max_lon, max_lat = bbox
        lat = (min_lat + max_lat) / 2.0
        lon = (min_lon + max_lon) / 2.0
        from ml.ingestion.grid import BBox

        radius_m = BBox(min_lat=min_lat, max_lat=max_lat, min_lon=min_lon, max_lon=max_lon).enclosing_radius_m()
    if lat is None or lon is None or radius_m is None:
        raise ValueError("lat, lon and radius_m are required")
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise ValueError("lat/lon outside world bounds")
    if not 1 <= radius_m <= 500:
        raise ValueError("KartaView radius_m must be in [1, 500]")

    page = 1
    all_items: list[dict[str, Any]] = []
    previous_page_ids: tuple[str, ...] | None = None
    truncated = False
    while page <= max_pages:
        params = {
            "lat": lat,
            "lng": lon,
            "radius": int(radius_m),
            "page": page,
            "itemsPerPage": limit,
            "zoomLevel": 18,
            "join": "sequence",
        }
        response = _request_with_retry(
            session,
            url=KARTAVIEW_API_URL,
            params=params,
            retries=retries,
            backoff_sec=backoff_sec,
            timeout_sec=timeout_sec,
            metrics=retry_metrics,
            before_request=request_rate_limiter.wait if request_rate_limiter is not None else None,
        )
        try:
            payload = response.json()
        finally:
            if hasattr(response, "close"):
                response.close()
        page_items, has_more = _extract_page(payload, page_size=limit)
        page_ids = tuple(str(item.get("id")) for item in page_items)
        if page_ids and page_ids == previous_page_ids:
            raise RuntimeError("KartaView pagination repeated the same non-empty page")
        previous_page_ids = page_ids
        all_items.extend(page_items)
        if not has_more:
            break
        if page == max_pages:
            truncated = True
            break
        page += 1

    details = TileFetchResult(items=all_items, pages=page, truncated=truncated)
    if truncated:
        logger.warning("kartaview_tile_truncated pages=%s limit=%s", page, limit)
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
    radius_override_m: int | None = None,
    center_lat: float | None = None,
    center_lon: float | None = None,
    query_points_config: Path | None = None,
    minimum_request_interval_sec: float = 45.0,
) -> dict[str, Any]:
    if not 1 <= limit_per_tile <= 150:
        raise ValueError("limit_per_tile must be between 1 and 150")
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
    if radius_override_m is not None and not 1 <= radius_override_m <= 500:
        raise ValueError("KartaView radius_override_m must be in [1, 500]")
    if minimum_request_interval_sec < 0:
        raise ValueError("minimum_request_interval_sec must be >= 0")
    if (center_lat is None) != (center_lon is None):
        raise ValueError("center_lat and center_lon must be provided together")
    if query_points_config is not None and center_lat is not None:
        raise ValueError("query_points_config cannot be combined with center_lat/center_lon")
    if center_lat is not None and not (-90 <= center_lat <= 90 and -180 <= float(center_lon) <= 180):
        raise ValueError("center_lat/center_lon outside world bounds")

    if query_points_config is not None:
        query_points = load_query_points_config(query_points_config)
        if max_tiles is not None:
            query_points = query_points[:max_tiles]
    elif center_lat is not None and center_lon is not None:
        query_points = [
            (
                f"point:{center_lat:.7f}:{center_lon:.7f}:{radius_override_m or 100}",
                center_lat,
                center_lon,
                radius_override_m or 100,
            )
        ]
    else:
        tiles = list(iter_moscow_tiles(lat_step=lat_step, lon_step=lon_step))
        if max_tiles is not None:
            tiles = tiles[:max_tiles]
        query_points = [
            (tile.key, tile.center[0], tile.center[1], radius_override_m or tile.enclosing_radius_m()) for tile in tiles
        ]
    invalid_query_radii = [radius_m for _, _, _, radius_m in query_points if not 1 <= radius_m <= 500]
    if invalid_query_radii:
        raise ValueError(
            "generated KartaView queries exceed the official 500 m radius; "
            "use --query-points-config, --radius-override-m <= 500, or a finer grid"
        )
    config_fingerprint = ingestion_config_fingerprint(
        {
            "source": "kartaview",
            "endpoint": KARTAVIEW_API_URL,
            "query_contract": {"zoomLevel": 18, "join": "sequence", "max_radius_m": 500},
            "limit_per_tile": limit_per_tile,
            "queries": [
                {"key": key, "lat": lat, "lon": lon, "radius_m": radius_m}
                for key, lat, lon, radius_m in query_points
            ],
        }
    )
    started_at = time.monotonic()
    retry_metrics = RetryMetrics()
    request_rate_limiter = RequestRateLimiter(minimum_request_interval_sec)
    state = IngestionState.load(
        source="kartaview",
        output_json=output_json,
        checkpoint_path=checkpoint_path,
        stats_path=stats_path,
        config_fingerprint=config_fingerprint,
    )
    seen_ids = state.seen_ids
    state.stats["tiles_total"] = len(query_points)

    import requests

    processed_since_save = 0
    live_attempts = 0
    live_successes = 0
    with requests.Session() as session:
        for tile_key, query_lat, query_lon, radius_m in query_points:
            if tile_key in state.completed_tiles:
                state.stats["tiles_skipped_checkpoint"] = int(state.stats["tiles_skipped_checkpoint"]) + 1
                continue
            live_attempts += 1
            state.stats["tiles_attempted"] = int(state.stats["tiles_attempted"]) + 1
            try:
                details = fetch_tile(
                    session,
                    limit=limit_per_tile,
                    retries=request_retries,
                    backoff_sec=backoff_sec,
                    max_pages=max_pages_per_tile,
                    lat=query_lat,
                    lon=query_lon,
                    radius_m=radius_m,
                    timeout_sec=timeout_sec,
                    return_details=True,
                    retry_metrics=retry_metrics,
                    request_rate_limiter=request_rate_limiter,
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
                        parsed = parse_kartaview_item(item_with_provenance)
                    except Exception as exc:  # noqa: BLE001 - isolate corrupt source records
                        logger.warning(
                            "kartaview_record_invalid reason=%s", sanitize_error_message(exc, max_length=160)
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
                logger.error("kartaview_tile_failed tile=%s reason=%s", tile_key, reason)
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
            f"all {live_attempts} live KartaView tile requests failed; "
            f"see {state.stats_path} and {state.checkpoint_path}"
        )
    return state.stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Load KartaView metadata for Moscow")
    parser.add_argument("--output-json", default="data/raw/kartaview_raw.json")
    parser.add_argument("--checkpoint")
    parser.add_argument("--stats")
    parser.add_argument("--limit-per-tile", type=int, default=150)
    parser.add_argument("--request-pause-sec", type=float, default=0.25)
    parser.add_argument("--request-retries", type=int, default=5)
    parser.add_argument("--backoff-sec", type=float, default=1.5)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--max-pages-per-tile", type=int, default=200)
    parser.add_argument("--checkpoint-every-tiles", type=int, default=1)
    parser.add_argument("--lat-step", type=float, default=0.01)
    parser.add_argument("--lon-step", type=float, default=0.01)
    parser.add_argument("--max-tiles", type=int)
    parser.add_argument("--radius-override-m", type=int)
    parser.add_argument("--center-lat", type=float)
    parser.add_argument("--center-lon", type=float)
    parser.add_argument("--query-points-config", type=Path)
    parser.add_argument(
        "--minimum-request-interval-sec",
        type=float,
        default=45.0,
        help="global interval between every KartaView network attempt; 45s stays below the public 100/hour limit",
    )
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
        radius_override_m=args.radius_override_m,
        center_lat=args.center_lat,
        center_lon=args.center_lon,
        query_points_config=args.query_points_config,
        minimum_request_interval_sec=args.minimum_request_interval_sec,
    )
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
