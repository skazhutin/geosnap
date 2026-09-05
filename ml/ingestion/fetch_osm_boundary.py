"""Fetch and validate an exact administrative boundary from OpenStreetMap.

The boundary is metadata, not imagery.  GeoSnap uses the official Nominatim
lookup endpoint once to materialize a reproducible Polygon/MultiPolygon AOI,
then pins the response content with SHA-256 in a sidecar stats file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from shapely.geometry import shape

from ml.ingestion.common import RetryMetrics, request_with_retry, write_json

logger = logging.getLogger(__name__)

NOMINATIM_LOOKUP_ENDPOINT = "https://nominatim.openstreetmap.org/lookup"
DEFAULT_OSM_RELATION_ID = 102269
DEFAULT_MAX_RESPONSE_BYTES = 5 * 1024 * 1024
DEFAULT_USER_AGENT = "GeoSnap/0.1 (noncommercial Moscow visual-geolocation research)"


class OsmBoundaryError(RuntimeError):
    """The boundary request or returned geometry failed closed."""


def _read_bounded_response(response: Any, *, max_bytes: int) -> bytes:
    headers = getattr(response, "headers", {})
    declared = headers.get("Content-Length") if hasattr(headers, "get") else None
    if declared not in (None, ""):
        try:
            declared_bytes = int(declared)
        except (TypeError, ValueError) as exc:
            raise OsmBoundaryError("Nominatim returned an invalid Content-Length") from exc
        if declared_bytes < 0 or declared_bytes > max_bytes:
            raise OsmBoundaryError(f"Nominatim response exceeds max_bytes={max_bytes}")

    payload = bytearray()
    chunks = (
        response.iter_content(chunk_size=64 * 1024)
        if hasattr(response, "iter_content")
        else (bytes(response.content),)
    )
    for chunk in chunks:
        if not chunk:
            continue
        payload.extend(chunk)
        if len(payload) > max_bytes:
            raise OsmBoundaryError(f"Nominatim response exceeds max_bytes={max_bytes}")
    return bytes(payload)


def validate_boundary_payload(payload: Any, *, relation_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(payload, Mapping) or payload.get("type") != "FeatureCollection":
        raise OsmBoundaryError("Nominatim boundary response must be a GeoJSON FeatureCollection")
    features = payload.get("features")
    if not isinstance(features, list) or len(features) != 1 or not isinstance(features[0], Mapping):
        raise OsmBoundaryError("Nominatim boundary response must contain exactly one feature")
    feature = features[0]
    properties = feature.get("properties")
    geometry_payload = feature.get("geometry")
    if not isinstance(properties, Mapping) or not isinstance(geometry_payload, Mapping):
        raise OsmBoundaryError("Nominatim boundary feature is missing properties or geometry")
    if (
        properties.get("osm_type") != "relation"
        or str(properties.get("osm_id")) != str(relation_id)
        or properties.get("category") != "boundary"
        or properties.get("type") != "administrative"
    ):
        raise OsmBoundaryError("Nominatim returned a different object than the requested administrative relation")
    licence = payload.get("licence")
    if not isinstance(licence, str) or "OpenStreetMap contributors" not in licence or "ODbL" not in licence:
        raise OsmBoundaryError("Nominatim response is missing the OpenStreetMap/ODbL licence notice")
    try:
        geometry = shape(geometry_payload)
    except Exception as exc:  # noqa: BLE001 - Shapely exposes multiple parse errors
        raise OsmBoundaryError("Nominatim boundary geometry cannot be parsed") from exc
    if geometry.geom_type not in {"Polygon", "MultiPolygon"} or geometry.is_empty:
        raise OsmBoundaryError("Nominatim boundary must be a non-empty Polygon or MultiPolygon")
    if not geometry.is_valid:
        raise OsmBoundaryError("Nominatim boundary geometry is topologically invalid")
    bounds = [float(value) for value in geometry.bounds]
    if len(bounds) != 4 or not all(math.isfinite(value) for value in bounds):
        raise OsmBoundaryError("Nominatim boundary bounds are invalid")
    normalized = dict(payload)
    normalized["features"] = [dict(feature)]
    details = {
        "geometry_type": geometry.geom_type,
        "geometry_components": len(geometry.geoms) if geometry.geom_type == "MultiPolygon" else 1,
        "bounds_west_south_east_north": bounds,
        "licence": licence,
        "display_name": properties.get("display_name"),
    }
    return normalized, details


def run(
    *,
    output_geojson: Path,
    stats_path: Path,
    relation_id: int = DEFAULT_OSM_RELATION_ID,
    request_retries: int = 3,
    backoff_sec: float = 1.5,
    timeout_sec: float = 30.0,
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    user_agent: str = DEFAULT_USER_AGENT,
    session: Any | None = None,
) -> dict[str, Any]:
    if relation_id < 1:
        raise ValueError("relation_id must be positive")
    if not 1 <= request_retries <= 10 or not 0 <= backoff_sec <= 300 or not 0 < timeout_sec <= 300:
        raise ValueError("invalid request retry/timeout parameters")
    if not 1 <= max_response_bytes <= 20 * 1024 * 1024:
        raise ValueError("max_response_bytes must be between 1 byte and 20 MiB")
    if not user_agent.strip():
        raise ValueError("user_agent must not be empty")

    owns_session = session is None
    if session is None:
        import requests

        session = requests.Session()
    retry_metrics = RetryMetrics()
    validated: dict[str, Any] = {}
    started = time.monotonic()

    def validate_response(response: Any) -> None:
        raw = _read_bounded_response(response, max_bytes=max_response_bytes)
        try:
            parsed = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise OsmBoundaryError("Nominatim returned invalid JSON") from exc
        payload, details = validate_boundary_payload(parsed, relation_id=relation_id)
        validated.update({"raw": raw, "payload": payload, "details": details})

    try:
        response = request_with_retry(
            session,
            url=NOMINATIM_LOOKUP_ENDPOINT,
            params={
                "osm_ids": f"R{relation_id}",
                "format": "geojson",
                "polygon_geojson": 1,
                "addressdetails": 0,
                "extratags": 0,
            },
            timeout_sec=timeout_sec,
            retries=request_retries,
            backoff_sec=backoff_sec,
            metrics=retry_metrics,
            request_kwargs={"headers": {"User-Agent": user_agent}, "stream": True},
            response_validator=validate_response,
        )
        try:
            payload = validated["payload"]
            raw = validated["raw"]
            details = validated["details"]
            write_json(output_geojson, payload)
            output_bytes = output_geojson.read_bytes()
            stats: dict[str, Any] = {
                "source": "OpenStreetMap via Nominatim lookup",
                "endpoint": NOMINATIM_LOOKUP_ENDPOINT,
                "osm_type": "relation",
                "osm_relation_id": relation_id,
                "source_url": f"https://www.openstreetmap.org/relation/{relation_id}",
                "response_bytes": len(raw),
                "response_sha256": hashlib.sha256(raw).hexdigest(),
                "output_bytes": len(output_bytes),
                "output_sha256": hashlib.sha256(output_bytes).hexdigest(),
                "output_geojson": str(output_geojson),
                "elapsed_seconds": round(time.monotonic() - started, 6),
                "complete": True,
                **details,
                **retry_metrics.as_dict(),
            }
            write_json(stats_path, stats)
            return stats
        finally:
            if hasattr(response, "close"):
                response.close()
    finally:
        if owns_session and session is not None and hasattr(session, "close"):
            session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and validate an exact OSM administrative relation boundary")
    parser.add_argument("--output-geojson", type=Path, required=True)
    parser.add_argument("--stats", type=Path, required=True)
    parser.add_argument("--relation-id", type=int, default=DEFAULT_OSM_RELATION_ID)
    parser.add_argument("--request-retries", type=int, default=3)
    parser.add_argument("--backoff-sec", type=float, default=1.5)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--max-response-bytes", type=int, default=DEFAULT_MAX_RESPONSE_BYTES)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    stats = run(
        output_geojson=args.output_geojson,
        stats_path=args.stats,
        relation_id=args.relation_id,
        request_retries=args.request_retries,
        backoff_sec=args.backoff_sec,
        timeout_sec=args.timeout_sec,
        max_response_bytes=args.max_response_bytes,
    )
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
