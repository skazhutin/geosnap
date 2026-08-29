"""Bounded, restart-safe Mapillary citywide metadata selection.

The official ``mly1_public`` vector sequence tiles are used only for coverage
discovery and geographically balanced candidate selection.  Complete image
metadata is then fetched in bounded batches from the Graph root ``/?ids=``
endpoint and normalized by the existing ``parse_mapillary_item`` contract.

No image bytes are downloaded here.  The output is the same backward-
compatible raw JSON array consumed by :mod:`ml.ingestion.merge_sources`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import time
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shapely.geometry import Point, box, shape
from shapely.geometry.base import BaseGeometry

from ml.ingestion.common import (
    RequestRateLimiter,
    RetryMetrics,
    read_json,
    request_with_retry,
    sanitize_error_message,
    write_json,
)
from ml.ingestion.mapillary_loader import MAPILLARY_FIELDS
from ml.ingestion.parsers import parse_mapillary_item
from ml.ingestion.schema import MOSCOW_LEGACY_CORE_BOUNDS
from ml.ingestion.state import ingestion_config_fingerprint

logger = logging.getLogger(__name__)

CHECKPOINT_SCHEMA_VERSION = 1
METADATA_CACHE_SCHEMA_VERSION = 2
VECTOR_TILESET = "mly1_public"
VECTOR_TILE_API_VERSION = 2
VECTOR_LAYER = "sequence"
VECTOR_TILE_ENDPOINT = (
    f"https://tiles.mapillary.com/maps/vtp/{VECTOR_TILESET}/{VECTOR_TILE_API_VERSION}/{{z}}/{{x}}/{{y}}"
)
GRAPH_ROOT_ENDPOINT = "https://graph.mapillary.com/"
CITYWIDE_MAPILLARY_FIELDS = ",".join(field for field in MAPILLARY_FIELDS.split(",") if field != "thumb_original_url")
DEFAULT_ZOOM = 12
MIN_SEQUENCE_ZOOM = 6
MAX_SEQUENCE_ZOOM = 14
# Dense Moscow z12 sequence tiles are observed above 5 MiB. Keep a hard,
# streaming-enforced ceiling with enough headroom for those official tiles.
DEFAULT_MAX_TILE_BYTES = 32 * 1024 * 1024
DEFAULT_MAX_GRAPH_RESPONSE_BYTES = 5 * 1024 * 1024
MIN_SOURCE_SHORT_SIDE_PX = 240
MIN_SOURCE_LONG_SIDE_PX = 320
DEFAULT_VECTOR_TILE_CACHE_MAX_AGE_SEC = 24 * 60 * 60
DEFAULT_METADATA_CACHE_MAX_AGE_SEC = 60 * 60
MAX_RECORDS_LIMIT = 100_000
MAX_CANDIDATE_MULTIPLIER = 10
MAX_AOI_GEOJSON_BYTES = 5 * 1024 * 1024


class MapillaryCitywideError(RuntimeError):
    """The citywide selector configuration or source response is invalid."""


class IncompleteMapillarySelectionError(MapillaryCitywideError):
    """The run retained resumable progress but refused to publish partial output."""


@dataclass(frozen=True, slots=True)
class Bounds:
    west: float
    south: float
    east: float
    north: float

    def __post_init__(self) -> None:
        values = (self.west, self.south, self.east, self.north)
        if not all(math.isfinite(value) for value in values):
            raise ValueError("bounds must be finite")
        if not -180.0 <= self.west < self.east <= 180.0:
            raise ValueError("longitude bounds must satisfy -180 <= west < east <= 180")
        if not -85.05112878 <= self.south < self.north <= 85.05112878:
            raise ValueError("latitude bounds exceed Web Mercator limits")

    def contains(self, lon: float, lat: float) -> bool:
        return self.west <= lon <= self.east and self.south <= lat <= self.north

    def to_list(self) -> list[float]:
        return [self.west, self.south, self.east, self.north]


@dataclass(frozen=True, slots=True)
class AoiBoundary:
    geometry: BaseGeometry
    bounds: Bounds
    sha256: str
    path: Path

    def covers(self, lon: float, lat: float) -> bool:
        return bool(self.geometry.covers(Point(lon, lat)))


min_lat, max_lat, min_lon, max_lon = MOSCOW_LEGACY_CORE_BOUNDS
DEFAULT_BOUNDS: Bounds = Bounds(west=min_lon, south=min_lat, east=max_lon, north=max_lat)


def load_aoi_boundary(path: Path) -> AoiBoundary:
    if not path.is_file():
        raise FileNotFoundError(f"AOI GeoJSON does not exist: {path}")
    raw = path.read_bytes()
    if not raw or len(raw) > MAX_AOI_GEOJSON_BYTES:
        raise ValueError(f"AOI GeoJSON must be between 1 byte and {MAX_AOI_GEOJSON_BYTES} bytes")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("AOI GeoJSON is not valid JSON") from exc
    geometry_payload: Any = None
    if isinstance(payload, Mapping) and payload.get("type") == "FeatureCollection":
        features = payload.get("features")
        if not isinstance(features, list) or len(features) != 1 or not isinstance(features[0], Mapping):
            raise ValueError("AOI FeatureCollection must contain exactly one feature")
        geometry_payload = features[0].get("geometry")
    elif isinstance(payload, Mapping) and payload.get("type") == "Feature":
        geometry_payload = payload.get("geometry")
    elif isinstance(payload, Mapping):
        geometry_payload = payload
    if not isinstance(geometry_payload, Mapping):
        raise ValueError("AOI GeoJSON does not contain a geometry")
    try:
        geometry = shape(geometry_payload)
    except Exception as exc:  # noqa: BLE001 - Shapely exposes several parse exceptions
        raise ValueError("AOI GeoJSON geometry cannot be parsed") from exc
    if geometry.geom_type not in {"Polygon", "MultiPolygon"} or geometry.is_empty:
        raise ValueError("AOI geometry must be a non-empty Polygon or MultiPolygon")
    if not geometry.is_valid:
        raise ValueError("AOI geometry must be topologically valid")
    west, south, east, north = geometry.bounds
    return AoiBoundary(
        geometry=geometry,
        bounds=Bounds(west=float(west), south=float(south), east=float(east), north=float(north)),
        sha256=hashlib.sha256(raw).hexdigest(),
        path=path,
    )


@dataclass(frozen=True, slots=True, order=True)
class VectorTile:
    z: int
    x: int
    y: int

    @property
    def key(self) -> str:
        return f"{self.z}/{self.x}/{self.y}"

    @property
    def url(self) -> str:
        return VECTOR_TILE_ENDPOINT.format(z=self.z, x=self.x, y=self.y)


@dataclass(frozen=True, slots=True)
class SequenceCandidate:
    image_id: str
    sequence_id: str
    tile: VectorTile
    subcell_x: int
    subcell_y: int
    representative_lon: float
    representative_lat: float

    @property
    def tile_key(self) -> str:
        return self.tile.key

    @property
    def subcell_key(self) -> str:
        return f"{self.tile.key}:{self.subcell_x}:{self.subcell_y}"


@dataclass(frozen=True, slots=True)
class DecodedTile:
    candidates: tuple[SequenceCandidate, ...]
    feature_count: int
    invalid_feature_count: int


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _stable_score(seed: int, *parts: object) -> str:
    text = "\0".join([str(seed), *(str(part) for part in parts)])
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def enumerate_vector_tiles(
    bounds: Bounds = DEFAULT_BOUNDS,
    *,
    zoom: int = DEFAULT_ZOOM,
) -> list[VectorTile]:
    """Enumerate every Web Mercator tile intersecting ``bounds``."""

    if not MIN_SEQUENCE_ZOOM <= zoom <= MAX_SEQUENCE_ZOOM:
        raise ValueError(f"Mapillary sequence tiles support zoom {MIN_SEQUENCE_ZOOM}..{MAX_SEQUENCE_ZOOM}")
    import mercantile

    tiles = {
        VectorTile(z=int(tile.z), x=int(tile.x), y=int(tile.y))
        for tile in mercantile.tiles(
            bounds.west,
            bounds.south,
            bounds.east,
            bounds.north,
            zooms=zoom,
        )
    }
    return sorted(tiles)


def filter_tiles_to_aoi(tiles: Sequence[VectorTile], boundary: AoiBoundary) -> list[VectorTile]:
    import mercantile

    filtered: list[VectorTile] = []
    for tile in tiles:
        tile_bounds = mercantile.bounds(tile.x, tile.y, tile.z)
        tile_polygon = box(
            tile_bounds.west,
            tile_bounds.south,
            tile_bounds.east,
            tile_bounds.north,
        )
        if boundary.geometry.intersects(tile_polygon):
            filtered.append(tile)
    return filtered


def _flatten_points(value: Any) -> list[tuple[float, float]]:
    if isinstance(value, (list, tuple)):
        if len(value) >= 2 and isinstance(value[0], (int, float)) and isinstance(value[1], (int, float)):
            return [(float(value[0]), float(value[1]))]
        result: list[tuple[float, float]] = []
        for item in value:
            result.extend(_flatten_points(item))
        return result
    return []


def _tile_coordinate_to_lonlat(
    tile: VectorTile,
    tile_x: float,
    tile_y_up: float,
    extent: float,
) -> tuple[float, float]:
    if extent <= 0:
        raise ValueError("vector tile extent must be positive")
    world_size = float(1 << tile.z)
    world_x = (tile.x + tile_x / extent) / world_size
    world_y = (tile.y + (extent - tile_y_up) / extent) / world_size
    lon = world_x * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * world_y))))
    return lon, lat


def decode_sequence_tile(
    payload: bytes,
    tile: VectorTile,
    *,
    subcells_per_axis: int = 4,
) -> DecodedTile:
    """Decode the official sequence layer into representative image candidates."""

    if not 1 <= subcells_per_axis <= 32:
        raise ValueError("subcells_per_axis must be between 1 and 32")
    try:
        import mapbox_vector_tile

        decoded = mapbox_vector_tile.decode(
            payload,
            default_options={"y_coord_down": False},
        )
    except Exception as exc:
        raise MapillaryCitywideError(f"cannot decode vector tile {tile.key}: {exc}") from exc
    if not isinstance(decoded, Mapping):
        raise MapillaryCitywideError(f"decoded vector tile {tile.key} is not a layer mapping")
    layer = decoded.get(VECTOR_LAYER)
    if layer is None:
        return DecodedTile(candidates=(), feature_count=0, invalid_feature_count=0)
    if not isinstance(layer, Mapping) or not isinstance(layer.get("features"), list):
        raise MapillaryCitywideError(f"vector tile {tile.key} has malformed sequence layer")
    try:
        extent = float(layer.get("extent", 4096))
    except (TypeError, ValueError) as exc:
        raise MapillaryCitywideError(f"vector tile {tile.key} has invalid extent") from exc
    if not math.isfinite(extent) or extent <= 0:
        raise MapillaryCitywideError(f"vector tile {tile.key} has invalid extent")

    candidates: list[SequenceCandidate] = []
    invalid = 0
    for feature in layer["features"]:
        if not isinstance(feature, Mapping):
            invalid += 1
            continue
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        if (
            not isinstance(properties, Mapping)
            or not isinstance(geometry, Mapping)
            or geometry.get("type") not in {"LineString", "MultiLineString"}
        ):
            invalid += 1
            continue
        sequence_id = properties.get("id")
        image_id = properties.get("image_id")
        points = _flatten_points(geometry.get("coordinates"))
        if sequence_id in (None, "") or image_id in (None, "") or not points:
            invalid += 1
            continue
        point_x, point_y = points[len(points) // 2]
        if not (math.isfinite(point_x) and math.isfinite(point_y)):
            invalid += 1
            continue
        lon, lat = _tile_coordinate_to_lonlat(tile, point_x, point_y, extent)
        normalized_x = min(max(point_x / extent, 0.0), math.nextafter(1.0, 0.0))
        normalized_y = min(
            max((extent - point_y) / extent, 0.0),
            math.nextafter(1.0, 0.0),
        )
        candidates.append(
            SequenceCandidate(
                image_id=str(image_id),
                sequence_id=str(sequence_id),
                tile=tile,
                subcell_x=int(normalized_x * subcells_per_axis),
                subcell_y=int(normalized_y * subcells_per_axis),
                representative_lon=lon,
                representative_lat=lat,
            )
        )
    return DecodedTile(
        candidates=tuple(candidates),
        feature_count=len(layer["features"]),
        invalid_feature_count=invalid,
    )


def _point_candidate(
    *,
    image_id: str,
    sequence_id: str,
    lon: float,
    lat: float,
    zoom: int,
    subcells_per_axis: int,
) -> SequenceCandidate:
    import mercantile

    tile_value = mercantile.tile(lon, lat, zoom)
    tile = VectorTile(tile_value.z, tile_value.x, tile_value.y)
    world_size = float(1 << zoom)
    world_x = (lon + 180.0) / 360.0 * world_size
    latitude_radians = math.radians(lat)
    world_y = (1.0 - math.asinh(math.tan(latitude_radians)) / math.pi) / 2.0 * world_size
    local_x = min(max(world_x - tile.x, 0.0), math.nextafter(1.0, 0.0))
    local_y = min(max(world_y - tile.y, 0.0), math.nextafter(1.0, 0.0))
    return SequenceCandidate(
        image_id=image_id,
        sequence_id=sequence_id,
        tile=tile,
        subcell_x=int(local_x * subcells_per_axis),
        subcell_y=int(local_y * subcells_per_axis),
        representative_lon=lon,
        representative_lat=lat,
    )


def select_balanced_candidates(
    candidates: Sequence[SequenceCandidate],
    *,
    max_candidates: int,
    max_per_tile: int,
    max_per_subcell: int,
    seed: int = 0,
) -> list[SequenceCandidate]:
    """Round-robin spatial buckets with deterministic caps and identity dedup."""

    if max_candidates < 1 or max_per_tile < 1 or max_per_subcell < 1:
        raise ValueError("candidate and geographic caps must be >= 1")
    unique: list[SequenceCandidate] = []
    seen_images: set[str] = set()
    seen_sequences: set[str] = set()
    ordered = sorted(
        candidates,
        key=lambda item: (
            _stable_score(
                seed,
                "deduplicate",
                item.sequence_id,
                item.image_id,
                item.tile_key,
                item.subcell_key,
            ),
            item.sequence_id,
            item.image_id,
        ),
    )
    for candidate in ordered:
        if candidate.image_id in seen_images or candidate.sequence_id in seen_sequences:
            continue
        seen_images.add(candidate.image_id)
        seen_sequences.add(candidate.sequence_id)
        unique.append(candidate)

    grouped: dict[str, deque[SequenceCandidate]] = defaultdict(deque)
    for candidate in unique:
        grouped[candidate.subcell_key].append(candidate)
    for key, values in list(grouped.items()):
        grouped[key] = deque(
            sorted(
                values,
                key=lambda item: (
                    _stable_score(seed, "within-subcell", key, item.sequence_id, item.image_id),
                    item.sequence_id,
                    item.image_id,
                ),
            )
        )
    bucket_order = sorted(
        grouped,
        key=lambda key: (_stable_score(seed, "subcell", key), key),
    )
    tile_counts: Counter[str] = Counter()
    subcell_counts: Counter[str] = Counter()
    selected: list[SequenceCandidate] = []
    active = list(bucket_order)
    while active and len(selected) < max_candidates:
        next_active: list[str] = []
        made_progress = False
        for key in active:
            queue = grouped[key]
            if not queue:
                continue
            candidate = queue.popleft()
            if tile_counts[candidate.tile_key] < max_per_tile and subcell_counts[key] < max_per_subcell:
                selected.append(candidate)
                tile_counts[candidate.tile_key] += 1
                subcell_counts[key] += 1
                made_progress = True
                if len(selected) >= max_candidates:
                    break
            if queue and tile_counts[candidate.tile_key] < max_per_tile and subcell_counts[key] < max_per_subcell:
                next_active.append(key)
        if not made_progress:
            break
        active = next_active
    return selected


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_bounded_response(response: Any, *, max_bytes: int, label: str) -> bytes:
    headers = getattr(response, "headers", {})
    declared_length = headers.get("Content-Length") if hasattr(headers, "get") else None
    if declared_length not in (None, ""):
        try:
            parsed_length = int(declared_length)
        except (TypeError, ValueError) as exc:
            raise MapillaryCitywideError(f"{label} has invalid Content-Length") from exc
        if parsed_length < 0 or parsed_length > max_bytes:
            raise MapillaryCitywideError(f"{label} exceeds max_bytes={max_bytes}")
    payload = bytearray()
    if hasattr(response, "iter_content"):
        chunks = response.iter_content(chunk_size=64 * 1024)
    else:  # Minimal fixture/client compatibility; requests always streams above.
        chunks = (bytes(response.content),)
    for chunk in chunks:
        if not chunk:
            continue
        payload.extend(chunk)
        if len(payload) > max_bytes:
            raise MapillaryCitywideError(f"{label} exceeds max_bytes={max_bytes}")
    return bytes(payload)


def fetch_vector_tile(
    session: Any,
    *,
    token: str,
    tile: VectorTile,
    retries: int,
    backoff_sec: float,
    timeout_sec: float,
    max_tile_bytes: int = DEFAULT_MAX_TILE_BYTES,
    retry_metrics: RetryMetrics | None = None,
    before_request: Callable[[], None] | None = None,
    payload_validator: Callable[[bytes], Any] | None = None,
) -> bytes:
    if not 1 <= max_tile_bytes <= 50 * 1024 * 1024:
        raise ValueError("max_tile_bytes must be between 1 and 50 MiB")
    validated_payload: dict[str, bytes] = {}

    def validate_response(response: Any) -> None:
        payload = _read_bounded_response(
            response,
            max_bytes=max_tile_bytes,
            label=f"vector tile {tile.key}",
        )
        if payload_validator is not None:
            payload_validator(payload)
        validated_payload["payload"] = payload

    response = request_with_retry(
        session,
        url=tile.url,
        params={"access_token": token},
        retries=retries,
        backoff_sec=backoff_sec,
        timeout_sec=timeout_sec,
        metrics=retry_metrics,
        before_request=before_request,
        request_kwargs={"stream": True},
        response_validator=validate_response,
    )
    try:
        return validated_payload["payload"]
    finally:
        if hasattr(response, "close"):
            response.close()


def _metadata_items(payload: Any, requested_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, Mapping):
        raise MapillaryCitywideError("Mapillary batch metadata response must be a JSON object")
    if "error" in payload:
        error = payload.get("error")
        message = error.get("message") if isinstance(error, Mapping) else "unknown Graph error"
        raise MapillaryCitywideError(f"Mapillary Graph batch error: {message}")
    requested = set(requested_ids)
    result: dict[str, dict[str, Any]] = {}
    data = payload.get("data")
    values: Sequence[Any]
    if isinstance(data, list):
        values = data
    else:
        values = [payload.get(image_id) for image_id in requested_ids]
    for value in values:
        if not isinstance(value, Mapping):
            continue
        item = dict(value)
        # Keep the downstream download bounded even if Graph returns an
        # unrequested original URL alongside the requested 2048px thumbnail.
        item.pop("thumb_original_url", None)
        image_id = item.get("id")
        if image_id is None:
            continue
        normalized_id = str(image_id)
        if normalized_id in requested:
            result[normalized_id] = item
    return result


def _has_usable_source_dimensions(item: Mapping[str, Any]) -> bool:
    """Reject provider records whose best available image is too small for VPR."""
    try:
        width = int(item["width"])
        height = int(item["height"])
    except (KeyError, TypeError, ValueError):
        return False
    if width < 1 or height < 1:
        return False
    return (
        min(width, height) >= MIN_SOURCE_SHORT_SIDE_PX
        and max(width, height) >= MIN_SOURCE_LONG_SIDE_PX
    )


def fetch_metadata_batch(
    session: Any,
    *,
    token: str,
    image_ids: Sequence[str],
    retries: int,
    backoff_sec: float,
    timeout_sec: float,
    retry_metrics: RetryMetrics | None = None,
    before_request: Callable[[], None] | None = None,
    max_response_bytes: int = DEFAULT_MAX_GRAPH_RESPONSE_BYTES,
) -> dict[str, dict[str, Any]]:
    if not image_ids:
        return {}
    if len(image_ids) > 50:
        raise ValueError("Mapillary Graph batch size cannot exceed 50")
    if len(set(image_ids)) != len(image_ids):
        raise ValueError("Mapillary Graph batch image IDs must be unique")
    if max_response_bytes < 1:
        raise ValueError("max_response_bytes must be >= 1")
    validated_items: dict[str, dict[str, Any]] = {}

    def validate_response(response: Any) -> None:
        raw_payload = _read_bounded_response(
            response,
            max_bytes=max_response_bytes,
            label="Mapillary Graph batch response",
        )
        try:
            payload = json.loads(raw_payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MapillaryCitywideError("Mapillary Graph batch response is not valid JSON") from exc
        validated_items.clear()
        validated_items.update(_metadata_items(payload, image_ids))

    response = request_with_retry(
        session,
        url=GRAPH_ROOT_ENDPOINT,
        params={
            "ids": ",".join(image_ids),
            "fields": CITYWIDE_MAPILLARY_FIELDS,
            "access_token": token,
        },
        retries=retries,
        backoff_sec=backoff_sec,
        timeout_sec=timeout_sec,
        metrics=retry_metrics,
        before_request=before_request,
        request_kwargs={"stream": True},
        response_validator=validate_response,
    )
    try:
        return dict(validated_items)
    finally:
        if hasattr(response, "close"):
            response.close()


def _metadata_cache_path(cache_dir: Path, image_id: str) -> Path:
    safe = "".join(character for character in image_id if character.isalnum() or character in "-_")
    if safe != image_id or not safe:
        safe = hashlib.sha256(image_id.encode("utf-8")).hexdigest()
    return cache_dir / "metadata" / f"{safe}.json"


def _load_metadata_cache(
    cache_dir: Path,
    image_id: str,
    *,
    max_age_sec: float,
    now_unix: float | None = None,
) -> dict[str, Any] | None:
    path = _metadata_cache_path(cache_dir, image_id)
    try:
        payload = read_json(path, default=None)
    except (OSError, ValueError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    fetched_at = payload.get("fetched_at_unix")
    if (
        payload.get("schema_version") != METADATA_CACHE_SCHEMA_VERSION
        or payload.get("image_id") != image_id
        or payload.get("fields") != CITYWIDE_MAPILLARY_FIELDS
        or not isinstance(fetched_at, (int, float))
        or not math.isfinite(float(fetched_at))
        or not isinstance(payload.get("item"), Mapping)
        or str(payload["item"].get("id")) != image_id
    ):
        return None
    now = time.time() if now_unix is None else now_unix
    # Signed Mapillary thumbnail URLs are intentionally refreshed.  A small
    # future-clock tolerance avoids a needless refetch after a clock correction.
    if float(fetched_at) > now + 300 or now - float(fetched_at) > max_age_sec:
        return None
    return dict(payload["item"])


def _write_metadata_cache(cache_dir: Path, image_id: str, item: Mapping[str, Any]) -> None:
    write_json(
        _metadata_cache_path(cache_dir, image_id),
        {
            "schema_version": METADATA_CACHE_SCHEMA_VERSION,
            "image_id": image_id,
            "fields": CITYWIDE_MAPILLARY_FIELDS,
            "fetched_at_unix": time.time(),
            "item": dict(item),
        },
    )


def _new_checkpoint(fingerprint: str) -> dict[str, Any]:
    return {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "source": "mapillary_citywide_vector_tiles",
        "config_fingerprint": fingerprint,
        "tiles": {},
        "failed_tiles": {},
        "failed_metadata": {},
    }


def _load_checkpoint(path: Path, fingerprint: str) -> dict[str, Any]:
    payload = read_json(path, default={})
    if not isinstance(payload, Mapping):
        raise MapillaryCitywideError(f"checkpoint must be a JSON object: {path}")
    if not payload:
        return _new_checkpoint(fingerprint)
    if payload.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
        raise MapillaryCitywideError(f"unsupported checkpoint schema in {path}")
    if payload.get("source") != "mapillary_citywide_vector_tiles":
        raise MapillaryCitywideError(f"checkpoint source mismatch in {path}")
    for name in ("tiles", "failed_tiles", "failed_metadata"):
        if not isinstance(payload.get(name), Mapping):
            raise MapillaryCitywideError(f"checkpoint {name} must be an object: {path}")
    has_progress = any(payload.get(name) for name in ("tiles", "failed_tiles", "failed_metadata"))
    if has_progress and payload.get("config_fingerprint") != fingerprint:
        raise MapillaryCitywideError(f"acquisition configuration changed for {path}; use a new checkpoint path")
    return {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "source": "mapillary_citywide_vector_tiles",
        "config_fingerprint": fingerprint,
        "tiles": dict(payload["tiles"]),
        "failed_tiles": dict(payload["failed_tiles"]),
        "failed_metadata": dict(payload["failed_metadata"]),
    }


def _seed_tile_entries(
    checkpoint: dict[str, Any],
    *,
    seed_path: Path,
    allowed_tile_keys: set[str],
) -> tuple[int, int]:
    """Reuse content-addressed tile entries across bbox/AOI checkpoints.

    A vector tile URL depends only on z/x/y.  The destination run still
    verifies the cached bytes against each imported SHA-256 and applies its own
    freshness policy before accepting them.
    """

    if not seed_path.is_file():
        raise FileNotFoundError(f"seed tile checkpoint does not exist: {seed_path}")
    payload = read_json(seed_path, default={})
    if (
        not isinstance(payload, Mapping)
        or payload.get("schema_version") != CHECKPOINT_SCHEMA_VERSION
        or payload.get("source") != "mapillary_citywide_vector_tiles"
        or not isinstance(payload.get("tiles"), Mapping)
    ):
        raise MapillaryCitywideError(f"invalid seed tile checkpoint: {seed_path}")
    eligible = 0
    imported = 0
    for tile_key, value in payload["tiles"].items():
        if tile_key not in allowed_tile_keys:
            continue
        eligible += 1
        if tile_key in checkpoint["tiles"]:
            continue
        if not isinstance(value, Mapping):
            raise MapillaryCitywideError(f"invalid seed tile entry {tile_key}: {seed_path}")
        sha256 = value.get("sha256")
        fetched_at = value.get("fetched_at_unix")
        if (
            not isinstance(sha256, str)
            or len(sha256) != 64
            or any(character not in "0123456789abcdef" for character in sha256.lower())
            or not isinstance(fetched_at, (int, float))
            or not math.isfinite(float(fetched_at))
        ):
            raise MapillaryCitywideError(f"invalid seed tile integrity metadata {tile_key}: {seed_path}")
        checkpoint["tiles"][tile_key] = dict(value)
        imported += 1
    return eligible, imported


def _tile_cache_path(cache_dir: Path, tile: VectorTile) -> Path:
    return cache_dir / "tiles" / str(tile.z) / str(tile.x) / f"{tile.y}.mvt"


def _safe_failure(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {sanitize_error_message(exc, max_length=220)}"


def _http_status_code(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    return int(status) if isinstance(status, int) else None


def _validate_run_parameters(
    *,
    zoom: int,
    max_records: int,
    max_per_tile: int,
    max_per_subcell: int,
    subcells_per_axis: int,
    candidate_multiplier: int,
    metadata_batch_size: int,
    retries: int,
    backoff_sec: float,
    timeout_sec: float,
    request_interval_sec: float,
    vector_tile_cache_max_age_sec: float,
    metadata_cache_max_age_sec: float,
    max_graph_response_bytes: int,
    max_tiles: int | None,
) -> None:
    if not MIN_SEQUENCE_ZOOM <= zoom <= MAX_SEQUENCE_ZOOM:
        raise ValueError(f"Mapillary sequence tiles support zoom {MIN_SEQUENCE_ZOOM}..{MAX_SEQUENCE_ZOOM}")
    if not 1 <= max_records <= MAX_RECORDS_LIMIT:
        raise ValueError(f"max_records must be between 1 and {MAX_RECORDS_LIMIT}")
    if max_per_tile < 1 or max_per_subcell < 1:
        raise ValueError("record and geographic caps must be >= 1")
    if not 1 <= subcells_per_axis <= 32:
        raise ValueError("subcells_per_axis must be between 1 and 32")
    if not 1 <= candidate_multiplier <= MAX_CANDIDATE_MULTIPLIER:
        raise ValueError(f"candidate_multiplier must be between 1 and {MAX_CANDIDATE_MULTIPLIER}")
    if not 1 <= metadata_batch_size <= 50:
        raise ValueError("metadata_batch_size must be between 1 and 50")
    if (
        not 1 <= retries <= 10
        or not 0 <= backoff_sec <= 300
        or not 0 < timeout_sec <= 300
        or not 0 <= request_interval_sec <= 60
        or not 0 <= vector_tile_cache_max_age_sec <= 30 * 24 * 60 * 60
        or not 0 <= metadata_cache_max_age_sec <= 7 * 24 * 60 * 60
    ):
        raise ValueError("invalid request retry/timeout/rate/cache-age parameters")
    if not 1 <= max_graph_response_bytes <= 50 * 1024 * 1024:
        raise ValueError("max_graph_response_bytes must be between 1 and 50 MiB")
    if max_tiles is not None and not 1 <= max_tiles <= 100_000:
        raise ValueError("max_tiles must be between 1 and 100000")


def _acquisition_fingerprint(
    bounds: Bounds,
    zoom: int,
    *,
    aoi_sha256: str | None = None,
) -> str:
    config: dict[str, Any] = {
        "source": "mapillary",
        "method": "official_vector_sequence_tiles_then_graph_batch",
        "vector_endpoint": VECTOR_TILE_ENDPOINT,
        "vector_layer": VECTOR_LAYER,
        "graph_endpoint": GRAPH_ROOT_ENDPOINT,
        "graph_fields": CITYWIDE_MAPILLARY_FIELDS,
        "bounds": bounds.to_list(),
        "zoom": zoom,
    }
    if aoi_sha256 is not None:
        config["aoi_sha256"] = aoi_sha256
    return ingestion_config_fingerprint(config)


def _parse_bounds(value: str) -> Bounds:
    try:
        west, south, east, north = (float(item.strip()) for item in value.split(","))
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("bbox must be west,south,east,north") from exc
    try:
        return Bounds(west=west, south=south, east=east, north=north)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def run(
    *,
    output_json: Path,
    cache_dir: Path,
    checkpoint_path: Path,
    seed_tile_checkpoint_path: Path | None = None,
    stats_path: Path,
    bounds: Bounds = DEFAULT_BOUNDS,
    aoi_geojson: Path | None = None,
    zoom: int = DEFAULT_ZOOM,
    max_records: int = 1200,
    max_per_tile: int = 80,
    max_per_subcell: int = 10,
    subcells_per_axis: int = 4,
    candidate_multiplier: int = 3,
    metadata_batch_size: int = 50,
    request_retries: int = 5,
    backoff_sec: float = 1.5,
    timeout_sec: float = 30.0,
    request_interval_sec: float = 0.10,
    vector_tile_cache_max_age_sec: float = DEFAULT_VECTOR_TILE_CACHE_MAX_AGE_SEC,
    metadata_cache_max_age_sec: float = DEFAULT_METADATA_CACHE_MAX_AGE_SEC,
    max_tile_bytes: int = DEFAULT_MAX_TILE_BYTES,
    max_graph_response_bytes: int = DEFAULT_MAX_GRAPH_RESPONSE_BYTES,
    max_tiles: int | None = None,
    allow_incomplete: bool = False,
    seed: int = 0,
    session: Any | None = None,
    decode_tile: Callable[..., DecodedTile] = decode_sequence_tile,
) -> dict[str, Any]:
    """Discover, balance, hydrate, validate, and atomically publish metadata."""

    _validate_run_parameters(
        zoom=zoom,
        max_records=max_records,
        max_per_tile=max_per_tile,
        max_per_subcell=max_per_subcell,
        subcells_per_axis=subcells_per_axis,
        candidate_multiplier=candidate_multiplier,
        metadata_batch_size=metadata_batch_size,
        retries=request_retries,
        backoff_sec=backoff_sec,
        timeout_sec=timeout_sec,
        request_interval_sec=request_interval_sec,
        vector_tile_cache_max_age_sec=vector_tile_cache_max_age_sec,
        metadata_cache_max_age_sec=metadata_cache_max_age_sec,
        max_graph_response_bytes=max_graph_response_bytes,
        max_tiles=max_tiles,
    )
    if not 1 <= max_tile_bytes <= 50 * 1024 * 1024:
        raise ValueError("max_tile_bytes must be between 1 and 50 MiB")
    boundary = load_aoi_boundary(aoi_geojson) if aoi_geojson is not None else None
    if boundary is None:
        if not (
            DEFAULT_BOUNDS.contains(bounds.west, bounds.south)
            and DEFAULT_BOUNDS.contains(bounds.east, bounds.north)
        ):
            raise ValueError(f"bbox must stay within the configured Moscow bounds {DEFAULT_BOUNDS.to_list()}")
    elif not (
        bounds.west <= boundary.bounds.west
        and bounds.south <= boundary.bounds.south
        and bounds.east >= boundary.bounds.east
        and bounds.north >= boundary.bounds.north
    ):
        raise ValueError("bbox must contain the complete AOI GeoJSON geometry")
    token = os.getenv("MAPILLARY_ACCESS_TOKEN")
    if not token:
        raise MapillaryCitywideError("MAPILLARY_ACCESS_TOKEN is required; no token is stored in checkpoints or caches")

    started = time.monotonic()
    bbox_tiles = enumerate_vector_tiles(bounds, zoom=zoom)
    all_tiles = filter_tiles_to_aoi(bbox_tiles, boundary) if boundary is not None else bbox_tiles
    planned_tiles = all_tiles[:max_tiles] if max_tiles is not None else all_tiles
    enumeration_truncated = len(planned_tiles) != len(all_tiles)
    fingerprint = _acquisition_fingerprint(
        bounds,
        zoom,
        aoi_sha256=boundary.sha256 if boundary is not None else None,
    )
    checkpoint = _load_checkpoint(checkpoint_path, fingerprint)
    seed_tiles_eligible = 0
    seed_tiles_imported = 0
    if seed_tile_checkpoint_path is not None:
        seed_tiles_eligible, seed_tiles_imported = _seed_tile_entries(
            checkpoint,
            seed_path=seed_tile_checkpoint_path,
            allowed_tile_keys={tile.key for tile in planned_tiles},
        )
    retry_metrics = RetryMetrics()
    limiter = RequestRateLimiter(request_interval_sec)
    stats: dict[str, Any] = {
        "source": "mapillary",
        "mode": "official_vector_sequence_tiles_then_graph_batch",
        "config_fingerprint": fingerprint,
        "bounds": bounds.to_list(),
        "aoi_mode": "geojson_polygon" if boundary is not None else "bbox",
        "aoi_geojson": str(boundary.path) if boundary is not None else None,
        "aoi_sha256": boundary.sha256 if boundary is not None else None,
        "aoi_bounds": boundary.bounds.to_list() if boundary is not None else bounds.to_list(),
        "zoom": zoom,
        "bbox_tiles_total": len(bbox_tiles),
        "tiles_total": len(all_tiles),
        "tiles_planned_this_run": len(planned_tiles),
        "seed_tile_checkpoint": str(seed_tile_checkpoint_path) if seed_tile_checkpoint_path is not None else None,
        "seed_tiles_eligible": seed_tiles_eligible,
        "seed_tiles_imported": seed_tiles_imported,
        "tile_enumeration_truncated": enumeration_truncated,
        "tiles_cache_hits": 0,
        "tiles_cache_stale": 0,
        "tiles_cache_invalid": 0,
        "vector_tile_cache_max_age_sec": vector_tile_cache_max_age_sec,
        "max_tile_bytes": max_tile_bytes,
        "tiles_fetched": 0,
        "tiles_failed": 0,
        "vector_features": 0,
        "invalid_vector_features": 0,
        "vector_candidates_outside_aoi": 0,
        "candidates_discovered": 0,
        "candidates_selected_for_metadata": 0,
        "metadata_cache_hits": 0,
        "metadata_cache_stale_or_invalid": 0,
        "metadata_cache_max_age_sec": metadata_cache_max_age_sec,
        "max_graph_response_bytes": max_graph_response_bytes,
        "metadata_batches_attempted": 0,
        "metadata_batches_failed": 0,
        "metadata_batch_splits": 0,
        "metadata_singletons_quarantined": 0,
        "metadata_items_received": 0,
        "metadata_items_missing": 0,
        "metadata_items_invalid": 0,
        "metadata_items_below_min_dimensions": 0,
        "metadata_items_outside_aoi": 0,
        "records_before_final_balance": 0,
        "records_selected": 0,
        "max_records": max_records,
        "max_per_tile": max_per_tile,
        "max_per_subcell": max_per_subcell,
        "subcells_per_axis": subcells_per_axis,
        "candidate_multiplier": candidate_multiplier,
        "allow_incomplete": allow_incomplete,
        "complete": False,
        "output_written": False,
    }

    owns_session = session is None
    if session is None:
        import requests

        session = requests.Session()

    def save_progress() -> None:
        write_json(checkpoint_path, checkpoint)
        for name, value in retry_metrics.as_dict().items():
            stats[name] = value
        stats["last_checkpoint_elapsed_seconds"] = round(time.monotonic() - started, 6)
        stats["failed_tiles_current"] = len(checkpoint["failed_tiles"])
        stats["failed_metadata_current"] = len(checkpoint["failed_metadata"])
        write_json(stats_path, stats)

    discovered: list[SequenceCandidate] = []
    try:
        for tile in planned_tiles:
            cache_path = _tile_cache_path(cache_dir, tile)
            entry = checkpoint["tiles"].get(tile.key)
            payload: bytes | None = None
            decoded: DecodedTile | None = None
            tile_fetched_at: float | None = None
            entry_fetched_at = entry.get("fetched_at_unix") if isinstance(entry, Mapping) else None
            cache_is_fresh = (
                isinstance(entry_fetched_at, (int, float))
                and math.isfinite(float(entry_fetched_at))
                and float(entry_fetched_at) <= time.time() + 300
                and time.time() - float(entry_fetched_at) <= vector_tile_cache_max_age_sec
            )
            if cache_path.is_file() and not cache_is_fresh:
                stats["tiles_cache_stale"] += 1
            elif cache_path.is_file():
                try:
                    cached = cache_path.read_bytes()
                    cached_sha = _sha256_bytes(cached)
                    expected_sha = entry.get("sha256") if isinstance(entry, Mapping) else None
                    if expected_sha is not None and cached_sha != expected_sha:
                        raise MapillaryCitywideError("cached tile SHA-256 disagrees with checkpoint")
                    decoded = decode_tile(cached, tile, subcells_per_axis=subcells_per_axis)
                    payload = cached
                    tile_fetched_at = float(entry_fetched_at)
                    stats["tiles_cache_hits"] += 1
                except Exception as exc:  # noqa: BLE001 - corrupt cache is repaired from source
                    logger.warning("mapillary_vector_cache_invalid tile=%s reason=%s", tile.key, _safe_failure(exc))
                    stats["tiles_cache_invalid"] += 1
                    payload = None
                    decoded = None
            if payload is None:
                try:
                    payload = fetch_vector_tile(
                        session,
                        token=token,
                        tile=tile,
                        retries=request_retries,
                        backoff_sec=backoff_sec,
                        timeout_sec=timeout_sec,
                        max_tile_bytes=max_tile_bytes,
                        retry_metrics=retry_metrics,
                        before_request=limiter.wait,
                        payload_validator=lambda value, current_tile=tile: decode_tile(
                            value,
                            current_tile,
                            subcells_per_axis=subcells_per_axis,
                        ),
                    )
                    decoded = decode_tile(payload, tile, subcells_per_axis=subcells_per_axis)
                    _atomic_bytes(cache_path, payload)
                    tile_fetched_at = time.time()
                    stats["tiles_fetched"] += 1
                except Exception as exc:  # noqa: BLE001 - retain other tile progress
                    reason = _safe_failure(exc)
                    checkpoint["failed_tiles"][tile.key] = reason
                    checkpoint["tiles"].pop(tile.key, None)
                    stats["tiles_failed"] += 1
                    logger.error("mapillary_vector_tile_failed tile=%s reason=%s", tile.key, reason)
                    save_progress()
                    continue
            assert payload is not None and decoded is not None
            checkpoint["tiles"][tile.key] = {
                "sha256": _sha256_bytes(payload),
                "bytes": len(payload),
                "feature_count": decoded.feature_count,
                "candidate_count": len(decoded.candidates),
                "invalid_feature_count": decoded.invalid_feature_count,
                "fetched_at_unix": tile_fetched_at,
            }
            checkpoint["failed_tiles"].pop(tile.key, None)
            stats["vector_features"] += decoded.feature_count
            stats["invalid_vector_features"] += decoded.invalid_feature_count
            if boundary is None:
                discovered.extend(decoded.candidates)
            else:
                inside_candidates = [
                    candidate
                    for candidate in decoded.candidates
                    if boundary.covers(candidate.representative_lon, candidate.representative_lat)
                ]
                stats["vector_candidates_outside_aoi"] += len(decoded.candidates) - len(inside_candidates)
                discovered.extend(inside_candidates)
            save_progress()

        stats["candidates_discovered"] = len(discovered)
        candidate_cap = max_records * candidate_multiplier
        metadata_candidates = select_balanced_candidates(
            discovered,
            max_candidates=candidate_cap,
            max_per_tile=max_per_tile * candidate_multiplier,
            max_per_subcell=max_per_subcell * candidate_multiplier,
            seed=seed,
        )
        stats["candidates_selected_for_metadata"] = len(metadata_candidates)
        selected_image_ids = {candidate.image_id for candidate in metadata_candidates}
        checkpoint["failed_metadata"] = {
            image_id: reason
            for image_id, reason in checkpoint["failed_metadata"].items()
            if image_id in selected_image_ids
        }
        metadata: dict[str, dict[str, Any]] = {}
        missing_from_cache: list[str] = []
        for candidate in metadata_candidates:
            cache_path = _metadata_cache_path(cache_dir, candidate.image_id)
            cached = (
                None
                if candidate.image_id in checkpoint["failed_metadata"]
                else _load_metadata_cache(
                    cache_dir,
                    candidate.image_id,
                    max_age_sec=metadata_cache_max_age_sec,
                )
            )
            if cached is not None:
                metadata[candidate.image_id] = cached
                stats["metadata_cache_hits"] += 1
            else:
                if cache_path.exists():
                    stats["metadata_cache_stale_or_invalid"] += 1
                missing_from_cache.append(candidate.image_id)

        pending_batches: deque[list[str]] = deque(
            missing_from_cache[offset : offset + metadata_batch_size]
            for offset in range(0, len(missing_from_cache), metadata_batch_size)
        )
        while pending_batches:
            batch = pending_batches.popleft()
            stats["metadata_batches_attempted"] += 1
            try:
                received = fetch_metadata_batch(
                    session,
                    token=token,
                    image_ids=batch,
                    retries=request_retries,
                    backoff_sec=backoff_sec,
                    timeout_sec=timeout_sec,
                    retry_metrics=retry_metrics,
                    before_request=limiter.wait,
                    max_response_bytes=max_graph_response_bytes,
                )
            except Exception as exc:  # noqa: BLE001 - other batches remain useful/resumable
                reason = _safe_failure(exc)
                status_code = _http_status_code(exc)
                if status_code in {400, 404} and len(batch) > 1:
                    middle = len(batch) // 2
                    pending_batches.appendleft(batch[middle:])
                    pending_batches.appendleft(batch[:middle])
                    stats["metadata_batch_splits"] += 1
                    logger.warning(
                        "mapillary_metadata_batch_split status=%s size=%s",
                        status_code,
                        len(batch),
                    )
                    save_progress()
                    continue
                if status_code in {400, 404}:
                    checkpoint["failed_metadata"][batch[0]] = reason
                    stats["metadata_singletons_quarantined"] += 1
                    logger.warning(
                        "mapillary_metadata_singleton_quarantined status=%s image_id=%s",
                        status_code,
                        batch[0],
                    )
                    save_progress()
                    continue
                stats["metadata_batches_failed"] += 1
                for image_id in batch:
                    checkpoint["failed_metadata"][image_id] = reason
                logger.error("mapillary_metadata_batch_failed size=%s reason=%s", len(batch), reason)
                save_progress()
                continue
            stats["metadata_items_received"] += len(received)
            for image_id in batch:
                item = received.get(image_id)
                if item is None:
                    checkpoint["failed_metadata"][image_id] = "missing_from_graph_batch_response"
                    stats["metadata_items_missing"] += 1
                    continue
                metadata[image_id] = item
                _write_metadata_cache(cache_dir, image_id, item)
                checkpoint["failed_metadata"].pop(image_id, None)
            save_progress()

        discovery_by_image = {candidate.image_id: candidate for candidate in metadata_candidates}
        parsed_by_image: dict[str, dict[str, Any]] = {}
        exact_candidates: list[SequenceCandidate] = []
        invalid_metadata_ids: list[str] = []
        for candidate in metadata_candidates:
            item = metadata.get(candidate.image_id)
            if item is None:
                continue
            if not _has_usable_source_dimensions(item):
                invalid_metadata_ids.append(candidate.image_id)
                stats["metadata_items_invalid"] += 1
                stats["metadata_items_below_min_dimensions"] += 1
                continue
            enriched = dict(item)
            sequence_value = enriched.get("sequence")
            effective_sequence_id = sequence_value.get("id") if isinstance(sequence_value, Mapping) else sequence_value
            if effective_sequence_id in (None, ""):
                enriched["sequence"] = candidate.sequence_id
            enriched["_geosnap_acquisition"] = {
                "method": "official_vector_sequence_tile_plus_graph_batch",
                "vector_tileset": VECTOR_TILESET,
                "vector_layer": VECTOR_LAYER,
                "vector_tile_url": candidate.tile.url,
                "discovery_tile": candidate.tile_key,
                "discovery_sequence_id": candidate.sequence_id,
                "graph_endpoint": GRAPH_ROOT_ENDPOINT,
                "graph_fields": CITYWIDE_MAPILLARY_FIELDS,
            }
            if boundary is not None:
                enriched["_geosnap_acquisition"]["aoi_sha256"] = boundary.sha256
            try:
                parsed = parse_mapillary_item(enriched)
            except Exception as exc:  # noqa: BLE001 - source schema drift is incomplete
                logger.warning(
                    "mapillary_metadata_invalid image_id=%s reason=%s",
                    candidate.image_id,
                    _safe_failure(exc),
                )
                parsed = None
            if parsed is None or parsed.get("sequence_id") in (None, ""):
                invalid_metadata_ids.append(candidate.image_id)
                stats["metadata_items_invalid"] += 1
                continue
            lon = float(parsed["lon"])
            lat = float(parsed["lat"])
            if not bounds.contains(lon, lat) or (boundary is not None and not boundary.covers(lon, lat)):
                stats["metadata_items_outside_aoi"] += 1
                continue
            if (
                parsed.get("license") != "CC BY-SA 4.0"
                or not str(parsed.get("attribution", "")).startswith("Mapillary image by ")
                or not str(parsed.get("source_url", "")).startswith("https://www.mapillary.com/app/")
            ):
                invalid_metadata_ids.append(candidate.image_id)
                stats["metadata_items_invalid"] += 1
                continue
            exact = _point_candidate(
                image_id=candidate.image_id,
                sequence_id=str(parsed["sequence_id"]),
                lon=lon,
                lat=lat,
                zoom=zoom,
                subcells_per_axis=subcells_per_axis,
            )
            enriched["_geosnap_acquisition"]["selection_tile"] = exact.tile_key
            enriched["_geosnap_acquisition"]["selection_subcell"] = [
                exact.subcell_x,
                exact.subcell_y,
            ]
            parsed_with_selection = parse_mapillary_item(enriched)
            if parsed_with_selection is None:
                invalid_metadata_ids.append(candidate.image_id)
                stats["metadata_items_invalid"] += 1
                continue
            parsed_by_image[candidate.image_id] = parsed_with_selection
            exact_candidates.append(exact)

        for image_id in invalid_metadata_ids:
            checkpoint["failed_metadata"][image_id] = "metadata_invalid_or_missing_legal_provenance"
        stats["records_before_final_balance"] = len(exact_candidates)
        final_candidates = select_balanced_candidates(
            exact_candidates,
            max_candidates=max_records,
            max_per_tile=max_per_tile,
            max_per_subcell=max_per_subcell,
            seed=seed,
        )
        records: list[dict[str, Any]] = []
        for rank, candidate in enumerate(final_candidates, start=1):
            record = dict(parsed_by_image[candidate.image_id])
            discovery = discovery_by_image[candidate.image_id]
            record["_geosnap_selection_rank"] = rank
            record["_geosnap_discovery_tile"] = discovery.tile_key
            record["_geosnap_selection_tile"] = candidate.tile_key
            record["_geosnap_selection_subcell"] = [
                candidate.subcell_x,
                candidate.subcell_y,
            ]
            records.append(record)
        stats["records_selected"] = len(records)
        stats["selected_tile_counts"] = dict(
            sorted(Counter(item["_geosnap_selection_tile"] for item in records).items())
        )
        stats["selected_subcell_counts"] = dict(
            sorted(
                Counter(
                    f"{item['_geosnap_selection_tile']}:"
                    f"{item['_geosnap_selection_subcell'][0]}:"
                    f"{item['_geosnap_selection_subcell'][1]}"
                    for item in records
                ).items()
            )
        )

        incomplete_reasons: list[str] = []
        if enumeration_truncated:
            incomplete_reasons.append("tile_enumeration_truncated_by_max_tiles")
        if checkpoint["failed_tiles"]:
            incomplete_reasons.append("failed_vector_tiles")
        if stats["invalid_vector_features"]:
            incomplete_reasons.append("invalid_vector_features")
        # Vector coverage can legitimately retain representatives whose image
        # was later deleted from Graph. Quarantine such IDs when enough fully
        # validated records remain, but never hide a failed batch request or a
        # shortfall against the requested final gallery size.
        blocking_failed_metadata = bool(stats["metadata_batches_failed"]) or (
            bool(checkpoint["failed_metadata"]) and len(records) < max_records
        )
        stats["quarantined_metadata_current"] = (
            len(checkpoint["failed_metadata"]) if not blocking_failed_metadata else 0
        )
        stats["blocking_failed_metadata_current"] = (
            len(checkpoint["failed_metadata"]) if blocking_failed_metadata else 0
        )
        if blocking_failed_metadata:
            incomplete_reasons.append("failed_or_invalid_graph_metadata")
        if not discovered:
            incomplete_reasons.append("no_vector_candidates_discovered")
        if not records:
            incomplete_reasons.append("no_valid_in_aoi_records_selected")
        stats["incomplete_reasons"] = sorted(set(incomplete_reasons))
        stats["complete"] = not incomplete_reasons
        stats["elapsed_seconds"] = round(time.monotonic() - started, 6)
        stats["records_per_second"] = (
            round(len(records) / stats["elapsed_seconds"], 6) if stats["elapsed_seconds"] > 0 else 0.0
        )
        for name, value in retry_metrics.as_dict().items():
            stats[name] = value
        save_progress()
        if incomplete_reasons and not allow_incomplete:
            raise IncompleteMapillarySelectionError(
                "Mapillary citywide selection is incomplete; partial output was not published. "
                f"Reasons: {', '.join(stats['incomplete_reasons'])}. "
                f"Resume with the same checkpoint or pass --allow-incomplete explicitly."
            )
        write_json(output_json, records)
        stats["output_written"] = True
        write_json(stats_path, stats)
        return stats
    finally:
        if owns_session and session is not None and hasattr(session, "close"):
            session.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Select geographically balanced Mapillary metadata from official vector sequence tiles"
    )
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument(
        "--seed-tile-checkpoint",
        type=Path,
        help="reuse verified z/x/y tile cache entries from another AOI checkpoint",
    )
    parser.add_argument("--stats", type=Path, required=True)
    parser.add_argument(
        "--bbox",
        type=_parse_bounds,
        help=(
            "west,south,east,north; defaults to the configured Moscow bbox, "
            "or to the complete --aoi-geojson bounds when a polygon is supplied"
        ),
    )
    parser.add_argument(
        "--aoi-geojson",
        type=Path,
        help="exact Polygon/MultiPolygon AOI; vector representatives and Graph coordinates are filtered to it",
    )
    parser.add_argument("--zoom", type=int, default=DEFAULT_ZOOM)
    parser.add_argument("--max-records", type=int, default=1200)
    parser.add_argument("--max-per-tile", type=int, default=80)
    parser.add_argument("--max-per-subcell", type=int, default=10)
    parser.add_argument("--subcells-per-axis", type=int, default=4)
    parser.add_argument("--candidate-multiplier", type=int, default=3)
    parser.add_argument("--metadata-batch-size", type=int, default=50)
    parser.add_argument("--request-retries", type=int, default=5)
    parser.add_argument("--backoff-sec", type=float, default=1.5)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--request-interval-sec", type=float, default=0.10)
    parser.add_argument(
        "--vector-tile-cache-max-age-sec",
        type=float,
        default=DEFAULT_VECTOR_TILE_CACHE_MAX_AGE_SEC,
    )
    parser.add_argument(
        "--metadata-cache-max-age-sec",
        type=float,
        default=DEFAULT_METADATA_CACHE_MAX_AGE_SEC,
    )
    parser.add_argument("--max-tile-bytes", type=int, default=DEFAULT_MAX_TILE_BYTES)
    parser.add_argument(
        "--max-graph-response-bytes",
        type=int,
        default=DEFAULT_MAX_GRAPH_RESPONSE_BYTES,
    )
    parser.add_argument("--max-tiles", type=int)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    if args.bbox is not None:
        bounds = args.bbox
    elif args.aoi_geojson is not None:
        bounds = load_aoi_boundary(args.aoi_geojson).bounds
    else:
        bounds = DEFAULT_BOUNDS
    stats = run(
        output_json=args.output_json,
        cache_dir=args.cache_dir,
        checkpoint_path=args.checkpoint,
        seed_tile_checkpoint_path=args.seed_tile_checkpoint,
        stats_path=args.stats,
        bounds=bounds,
        aoi_geojson=args.aoi_geojson,
        zoom=args.zoom,
        max_records=args.max_records,
        max_per_tile=args.max_per_tile,
        max_per_subcell=args.max_per_subcell,
        subcells_per_axis=args.subcells_per_axis,
        candidate_multiplier=args.candidate_multiplier,
        metadata_batch_size=args.metadata_batch_size,
        request_retries=args.request_retries,
        backoff_sec=args.backoff_sec,
        timeout_sec=args.timeout_sec,
        request_interval_sec=args.request_interval_sec,
        vector_tile_cache_max_age_sec=args.vector_tile_cache_max_age_sec,
        metadata_cache_max_age_sec=args.metadata_cache_max_age_sec,
        max_tile_bytes=args.max_tile_bytes,
        max_graph_response_bytes=args.max_graph_response_bytes,
        max_tiles=args.max_tiles,
        allow_incomplete=args.allow_incomplete,
        seed=args.seed,
    )
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
