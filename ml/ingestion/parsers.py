"""Pure parsers for current Mapillary and KartaView response records."""

from __future__ import annotations

from typing import Any

from ml.ingestion.schema import normalize_captured_at, normalize_heading, normalize_metadata_json


def _first_present(item: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in item and item[key] is not None:
            return item[key]
    return None


def _finite_coordinate(value: Any, minimum: float, maximum: float) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not minimum <= number <= maximum:
        return None
    return number


def parse_mapillary_item(item: dict[str, Any]) -> dict[str, Any] | None:
    image_id = item.get("id")
    geometry = item.get("computed_geometry") or item.get("geometry")
    coords = geometry.get("coordinates") if isinstance(geometry, dict) else None
    if image_id is None or not isinstance(coords, (list, tuple)) or len(coords) < 2:
        return None
    lon = _finite_coordinate(coords[0], -180.0, 180.0)
    lat = _finite_coordinate(coords[1], -90.0, 90.0)
    if lat is None or lon is None:
        return None

    image_url = _first_present(item, ["thumb_original_url", "thumb_2048_url", "thumb_1024_url"])
    if not isinstance(image_url, str) or not image_url.startswith(("https://", "http://")):
        return None

    sequence = item.get("sequence")
    sequence_id = sequence.get("id") if isinstance(sequence, dict) else sequence
    creator = item.get("creator")
    creator_name = creator.get("username") if isinstance(creator, dict) else None
    if not isinstance(creator_name, str) or not creator_name.strip():
        return None
    creator_name = creator_name.strip()
    source_url = f"https://www.mapillary.com/app/?focus=photo&pKey={image_id}"
    attribution = f"Mapillary image by {creator_name}"

    captured_at = normalize_captured_at(item.get("captured_at"))
    heading = normalize_heading(_first_present(item, ["computed_compass_angle", "compass_angle"]))
    quality_score = _finite_coordinate(item.get("quality_score"), 0.0, 1.0)
    return {
        # Keep id/timestamp/image_url aliases for backward-compatible raw JSON.
        "id": str(image_id),
        "source_image_id": str(image_id),
        "lat": lat,
        "lon": lon,
        "timestamp": captured_at,
        "captured_at": captured_at,
        "heading": heading,
        "quality_score": quality_score,
        "image_url": image_url,
        "download_url": image_url,
        "sequence_id": str(sequence_id) if sequence_id not in (None, "") else None,
        "license": "CC BY-SA 4.0",
        "attribution": attribution,
        "source_url": source_url,
        "metadata_json": normalize_metadata_json(item),
    }


def parse_kartaview_item(item: dict[str, Any]) -> dict[str, Any] | None:
    image_id = _first_present(item, ["id", "photoId", "imageId"])
    lat = _finite_coordinate(_first_present(item, ["lat", "latitude", "gpsLat"]), -90.0, 90.0)
    lon = _finite_coordinate(_first_present(item, ["lng", "lon", "longitude", "gpsLng"]), -180.0, 180.0)
    if image_id is None or lat is None or lon is None:
        return None

    # Prefer KartaView's public CDN proxy over the backing storage host. Older
    # storage*.openstreetcam.org origins can be temporarily unavailable while
    # the CDN URL returned by the same API record remains healthy.
    image_url = _first_present(item, ["imageProcUrl", "imageLthUrl", "fileurlProc", "fileurlLTh", "fileurl", "url"])
    if not isinstance(image_url, str) or not image_url.startswith(("https://", "http://")):
        return None
    captured_at = normalize_captured_at(_first_present(item, ["shotDate", "dateAdded", "timestamp"]))
    sequence_id = _first_present(item, ["sequenceId", "sequence_id"])
    heading = normalize_heading(_first_present(item, ["heading", "direction", "gpsDirection"]))
    source_url = (
        f"https://kartaview.org/details/{sequence_id}/{item.get('sequenceIndex', 0)}/track-info"
        if sequence_id
        else "https://kartaview.org/"
    )

    return {
        "id": str(image_id),
        "source_image_id": str(image_id),
        "lat": lat,
        "lon": lon,
        "timestamp": captured_at,
        "captured_at": captured_at,
        "heading": heading,
        "image_url": image_url,
        "download_url": image_url,
        "sequence_id": str(sequence_id) if sequence_id not in (None, "") else None,
        "sequence_index": item.get("sequenceIndex"),
        "license": "CC BY-SA 4.0",
        "attribution": "© Grab and KartaView Contributors",
        "source_url": source_url,
        "metadata_json": normalize_metadata_json(item),
    }
