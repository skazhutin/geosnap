"""KartaView ingestion loader for Moscow bbox tiles."""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Any

from ml.ingestion.common import read_json, write_json
from ml.ingestion.grid import iter_moscow_tiles
from ml.ingestion.parsers import parse_kartaview_item

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# API shape differs by deployment/version, so keep this configurable.
KARTAVIEW_API_URL = "https://api.openstreetcam.org/2.0/photo/"


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
            logger.warning("KartaView request error=%s attempt=%s/%s; sleeping %.1fs", exc, attempt, retries, wait)
            if attempt == retries:
                raise
            time.sleep(wait)
            continue
        if response.status_code in {429, 500, 502, 503, 504}:
            wait = backoff_sec * attempt
            logger.warning(
                "KartaView rate/server limit status=%s attempt=%s/%s; sleeping %.1fs",
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


def _extract_page(payload: Any) -> tuple[list[dict[str, Any]], bool]:
    if isinstance(payload, dict):
        if isinstance(payload.get("result"), dict):
            result = payload["result"]
            data = result.get("data")
            if not isinstance(data, list):
                data = []
            current_page = result.get("currentPage") or result.get("page")
            total_pages = result.get("totalPages")
            has_more = result.get("hasMore")
            if isinstance(has_more, bool):
                return data, has_more
            if isinstance(current_page, int) and isinstance(total_pages, int):
                return data, current_page < total_pages
            return data, len(data) > 0
        data = payload.get("data")
        if isinstance(data, list):
            return data, len(data) > 0
    if isinstance(payload, list):
        return payload, len(payload) > 0
    return [], False


def fetch_tile(
    session: Any,
    bbox: tuple[float, float, float, float],
    limit: int,
    retries: int,
    backoff_sec: float,
    max_pages: int,
) -> list[dict[str, Any]]:
    min_lon, min_lat, max_lon, max_lat = bbox
    page = 1
    all_items: list[dict[str, Any]] = []

    while page <= max_pages:
        params = {
            "ipp": limit,
            "page": page,
            "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
        }
        response = _request_with_retry(
            session,
            url=KARTAVIEW_API_URL,
            params=params,
            retries=retries,
            backoff_sec=backoff_sec,
        )
        try:
            payload = response.json()
        finally:
            if hasattr(response, "close"):
                response.close()
        page_items, has_more = _extract_page(payload)
        all_items.extend(page_items)
        if not has_more or len(page_items) == 0:
            break
        page += 1

    if page > max_pages:
        logger.warning("KartaView tile pagination truncated at max_pages=%s", max_pages)
    return all_items


def run(
    output_json: Path,
    limit_per_tile: int,
    request_pause_sec: float,
    request_retries: int,
    backoff_sec: float,
    max_pages_per_tile: int,
) -> None:
    if limit_per_tile < 1:
        raise ValueError("limit_per_tile must be >= 1")
    if request_pause_sec < 0:
        raise ValueError("request_pause_sec must be >= 0")
    if max_pages_per_tile < 1:
        raise ValueError("max_pages_per_tile must be >= 1")

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
                    bbox=bbox,
                    limit=limit_per_tile,
                    retries=request_retries,
                    backoff_sec=backoff_sec,
                    max_pages=max_pages_per_tile,
                )

                for item in items:
                    parsed = parse_kartaview_item(item)
                    if not parsed:
                        continue
                    if parsed["id"] in seen_ids:
                        continue
                    seen_ids.add(parsed["id"])
                    existing.append(parsed)
                    parsed_total += 1

                if parsed_total and parsed_total % 500 == 0:
                    write_json(output_json, existing)
                    logger.info("KartaView progress: %s records", parsed_total)
            except Exception as exc:  # noqa: BLE001
                logger.warning("tile request failed for %s: %s", tile, exc)
            finally:
                time.sleep(request_pause_sec)

    write_json(output_json, existing)
    logger.info("KartaView done. New records: %s, total: %s", parsed_total, len(existing))


def main() -> None:
    parser = argparse.ArgumentParser(description="Load KartaView metadata/images for Moscow")
    parser.add_argument("--output-json", default="data/raw/kartaview_raw.json")
    parser.add_argument("--limit-per-tile", type=int, default=1000)
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
