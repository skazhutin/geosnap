"""Pure parsing helpers for ingestion payloads."""

from __future__ import annotations

from typing import Any


def _first_present(item: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in item and item[key] is not None:
            return item[key]
    return None


def parse_mapillary_item(item: dict[str, Any]) -> dict[str, Any] | None:
    image_id = item.get("id")
    geometry = item.get("geometry") or item.get("computed_geometry")
    coords = None
    if isinstance(geometry, dict):
        coords = geometry.get("coordinates")
    if image_id is None or not coords or len(coords) < 2:
        return None

    lon, lat = coords[0], coords[1]
    image_url = (
        item.get("thumb_2048_url")
        or item.get("thumb_1024_url")
        or item.get("thumb_original_url")
        or item.get("url")
    )
    sequence = item.get("sequence")
    if isinstance(sequence, dict):
        sequence_id = sequence.get("id")
    else:
        sequence_id = sequence

    return {
        "id": str(image_id),
        "lat": lat,
        "lon": lon,
        "timestamp": item.get("captured_at"),
        "image_url": image_url,
        "sequence_id": str(sequence_id) if sequence_id else None,
    }


def parse_kartaview_item(item: dict[str, Any]) -> dict[str, Any] | None:
    image_id = _first_present(item, ["id", "photoId", "imageId"])
    lat = _first_present(item, ["lat", "latitude", "gpsLat"])
    lon = _first_present(item, ["lon", "lng", "longitude", "gpsLng"])
    if image_id is None or lat is None or lon is None:
        return None

    image_url = (
        item.get("fileurlProc")
        or item.get("fileurl")
        or item.get("thumbnailUrl")
        or item.get("url")
    )
    timestamp = item.get("shotDate") or item.get("dateAdded") or item.get("timestamp")

    return {
        "id": str(image_id),
        "lat": float(lat),
        "lon": float(lon),
        "timestamp": timestamp,
        "image_url": image_url,
    }
