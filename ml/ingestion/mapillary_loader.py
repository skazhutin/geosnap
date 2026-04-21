"""Mapillary ingestion loader for Moscow bbox tiles."""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path
from typing import Any

import requests

from ml.ingestion.common import read_json, write_json
from ml.ingestion.grid import iter_moscow_tiles
from ml.ingestion.parsers import parse_mapillary_item

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

MAPILLARY_ENDPOINT = "https://graph.mapillary.com/images"


def _request_with_retry(
    session: requests.Session,
    *,
    url: str,
    params: dict[str, Any],
    retries: int,
    backoff_sec: float,
) -> requests.Response:
    for attempt in range(1, retries + 1):
        response = session.get(url, params=params, timeout=30)
        if response.status_code in {429, 500, 502, 503, 504}:
            wait = backoff_sec * attempt
            logger.warning(
                "Mapillary rate/server limit status=%s attempt=%s/%s; sleeping %.1fs",
                response.status_code,
                attempt,
                retries,
                wait,
            )
            time.sleep(wait)
            continue
        response.raise_for_status()
        return response
    response.raise_for_status()
    return response


def fetch_tile(
    session: requests.Session,
    *,
    token: str,
    bbox: tuple[float, float, float, float],
    limit: int,
    retries: int,
    backoff_sec: float,
) -> list[dict[str, Any]]:
    min_lon, min_lat, max_lon, max_lat = bbox
    params = {
        "access_token": token,
        "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "fields": "id,captured_at,geometry,computed_geometry,thumb_2048_url,thumb_1024_url,thumb_original_url,sequence",
        "limit": limit,
    }
    response = _request_with_retry(
        session,
        url=MAPILLARY_ENDPOINT,
        params=params,
        retries=retries,
        backoff_sec=backoff_sec,
    )
    payload = response.json()
    return payload.get("data", [])


def run(
    output_json: Path,
    limit_per_tile: int,
    request_pause_sec: float,
    request_retries: int,
    backoff_sec: float,
) -> None:
    token = os.getenv("MAPILLARY_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("MAPILLARY_ACCESS_TOKEN is required")

    existing: list[dict[str, Any]] = read_json(output_json, default=[])
    seen_ids = {row.get("id") for row in existing}

    session = requests.Session()
    parsed_total = 0

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
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("tile request failed for %s: %s", tile, exc)
            continue

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
    args = parser.parse_args()

    run(
        output_json=Path(args.output_json),
        limit_per_tile=args.limit_per_tile,
        request_pause_sec=args.request_pause_sec,
        request_retries=args.request_retries,
        backoff_sec=args.backoff_sec,
    )


if __name__ == "__main__":
    main()
