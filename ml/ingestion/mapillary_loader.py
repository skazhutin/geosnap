"""Mapillary ingestion loader for Moscow bbox tiles."""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path
from typing import Any

from ml.ingestion.common import read_json, write_json
from ml.ingestion.grid import iter_moscow_tiles
from ml.ingestion.parsers import parse_mapillary_item

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

MAPILLARY_ENDPOINT = "https://graph.mapillary.com/images"


def _request_with_retry(
    session: Any,
    *,
    url: str,
    params: dict[str, Any],
    retries: int,
    backoff_sec: float,
) -> Any:
    if retries < 1:
        raise ValueError("retries must be >= 1")
    last_exception: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, params=params, timeout=30)
        except Exception as exc:  # noqa: BLE001
            last_exception = exc
            wait = backoff_sec * attempt
            logger.warning("Mapillary request error=%s attempt=%s/%s; sleeping %.1fs", exc, attempt, retries, wait)
            if attempt == retries:
                raise
            time.sleep(wait)
            continue
        if response.status_code in {429, 500, 502, 503, 504}:
            wait = backoff_sec * attempt
            logger.warning(
                "Mapillary rate/server limit status=%s attempt=%s/%s; sleeping %.1fs",
                response.status_code,
                attempt,
                retries,
                wait,
            )
            if hasattr(response, "close"):
                response.close()
            if attempt == retries:
                response.raise_for_status()
            time.sleep(wait)
            continue
        response.raise_for_status()
        return response
    if last_exception is not None:
        raise last_exception
    raise RuntimeError("request retry loop exited without response")


def fetch_tile(
    session: Any,
    *,
    token: str,
    bbox: tuple[float, float, float, float],
    limit: int,
    retries: int,
    backoff_sec: float,
    max_pages: int,
) -> list[dict[str, Any]]:
    min_lon, min_lat, max_lon, max_lat = bbox
    params = {
        "access_token": token,
        "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "fields": "id,captured_at,geometry,computed_geometry,thumb_2048_url,thumb_1024_url,thumb_original_url,sequence",
        "limit": limit,
    }
    all_items: list[dict[str, Any]] = []
    next_url: str | None = MAPILLARY_ENDPOINT
    next_params: dict[str, Any] | None = params

    pages = 0
    while next_url and pages < max_pages:
        pages += 1
        response = _request_with_retry(
            session,
            url=next_url,
            params=next_params or {},
            retries=retries,
            backoff_sec=backoff_sec,
        )
        try:
            payload = response.json()
        finally:
            if hasattr(response, "close"):
                response.close()
        batch = payload.get("data", [])
        if isinstance(batch, list):
            all_items.extend(batch)

        paging = payload.get("paging") if isinstance(payload, dict) else None
        next_link = paging.get("next") if isinstance(paging, dict) else None
        if isinstance(next_link, str) and next_link:
            next_url = next_link
            next_params = None
        else:
            next_url = None

    if next_url is not None:
        logger.warning("Mapillary tile pagination truncated at max_pages=%s", max_pages)
    return all_items


def run(
    output_json: Path,
    limit_per_tile: int,
    request_pause_sec: float,
    request_retries: int,
    backoff_sec: float,
    max_pages_per_tile: int,
) -> None:
    token = os.getenv("MAPILLARY_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("MAPILLARY_ACCESS_TOKEN is required")

    existing: list[dict[str, Any]] = read_json(output_json, default=[])
    seen_ids = {row.get("id") for row in existing}

    import requests

    parsed_total = 0

    with requests.Session() as session:
        for tile in iter_moscow_tiles():
            bbox = (tile.min_lon, tile.min_lat, tile.max_lon, tile.max_lat)
            try:
                items = fetch_tile(
                    session,
                    token=token,
                    bbox=bbox,
                    limit=limit_per_tile,
                    retries=request_retries,
                    backoff_sec=backoff_sec,
                    max_pages=max_pages_per_tile,
                )
                for item in items:
                    parsed = parse_mapillary_item(item)
                    if not parsed:
                        continue
                    if parsed["id"] in seen_ids:
                        continue
                    seen_ids.add(parsed["id"])
                    existing.append(parsed)
                    parsed_total += 1

                if parsed_total and parsed_total % 500 == 0:
                    write_json(output_json, existing)
                    logger.info("Mapillary progress: %s records", parsed_total)
            except Exception as exc:  # noqa: BLE001
                logger.warning("tile request failed for %s: %s", tile, exc)
            finally:
                time.sleep(request_pause_sec)

    write_json(output_json, existing)
    logger.info("Mapillary done. New records: %s, total: %s", parsed_total, len(existing))


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Mapillary metadata/images for Moscow")
    parser.add_argument("--output-json", default="data/raw/mapillary_raw.json")
    parser.add_argument("--limit-per-tile", type=int, default=2000)
    parser.add_argument("--request-pause-sec", type=float, default=0.25)
    parser.add_argument("--request-retries", type=int, default=5)
    parser.add_argument("--backoff-sec", type=float, default=1.5)
    parser.add_argument("--max-pages-per-tile", type=int, default=200)
    args = parser.parse_args()

    run(
        output_json=Path(args.output_json),
        limit_per_tile=args.limit_per_tile,
        request_pause_sec=args.request_pause_sec,
        request_retries=args.request_retries,
        backoff_sec=args.backoff_sec,
        max_pages_per_tile=args.max_pages_per_tile,
    )


if __name__ == "__main__":
    main()
