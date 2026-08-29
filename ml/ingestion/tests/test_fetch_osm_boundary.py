from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ml.ingestion.common import read_json
from ml.ingestion.fetch_osm_boundary import OsmBoundaryError, run, validate_boundary_payload


def _payload(*, relation_id: int = 102269, geometry_type: str = "Polygon") -> dict[str, Any]:
    geometry: dict[str, Any]
    ring = [
        [36.80, 55.14],
        [37.97, 55.14],
        [37.97, 56.02],
        [36.80, 56.02],
        [36.80, 55.14],
    ]
    if geometry_type == "Polygon":
        geometry = {"type": "Polygon", "coordinates": [ring]}
    else:
        geometry = {"type": geometry_type, "coordinates": [37.61, 55.75]}
    return {
        "type": "FeatureCollection",
        "licence": "Data © OpenStreetMap contributors, ODbL 1.0.",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "osm_type": "relation",
                    "osm_id": relation_id,
                    "category": "boundary",
                    "type": "administrative",
                    "display_name": "Москва, Россия",
                },
                "geometry": geometry,
            }
        ],
    }


class _Response:
    status_code = 200

    def __init__(self, payload: dict[str, Any], *, declared_bytes: int | None = None) -> None:
        self.content = json.dumps(payload).encode("utf-8")
        self.headers = {} if declared_bytes is None else {"Content-Length": str(declared_bytes)}
        self.closed = False

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        del chunk_size
        yield self.content

    def close(self) -> None:
        self.closed = True


class _Session:
    def __init__(self, response: _Response) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any], timeout: float, **kwargs: Any) -> _Response:
        del timeout
        self.calls.append((url, dict(params), dict(kwargs)))
        return self.response


def test_fetches_validated_relation_and_pins_both_hashes(tmp_path: Path) -> None:
    response = _Response(_payload())
    session = _Session(response)
    output = tmp_path / "moscow.geojson"
    stats_path = tmp_path / "moscow.stats.json"
    stats = run(
        output_geojson=output,
        stats_path=stats_path,
        request_retries=1,
        backoff_sec=0,
        timeout_sec=1,
        session=session,
    )
    assert stats["complete"] is True
    assert stats["osm_relation_id"] == 102269
    assert stats["geometry_type"] == "Polygon"
    assert stats["bounds_west_south_east_north"] == [36.8, 55.14, 37.97, 56.02]
    assert len(stats["response_sha256"]) == 64
    assert len(stats["output_sha256"]) == 64
    assert read_json(output, default={})["features"][0]["properties"]["osm_id"] == 102269
    assert read_json(stats_path, default={}) == stats
    assert response.closed is True
    _, params, kwargs = session.calls[0]
    assert params["osm_ids"] == "R102269"
    assert params["polygon_geojson"] == 1
    assert kwargs["stream"] is True
    assert kwargs["headers"]["User-Agent"].startswith("GeoSnap/")


def test_rejects_wrong_relation_or_non_polygon_without_publishing(tmp_path: Path) -> None:
    wrong_relation = _payload(relation_id=2555133)
    with pytest.raises(OsmBoundaryError, match="different object"):
        validate_boundary_payload(wrong_relation, relation_id=102269)

    output = tmp_path / "boundary.geojson"
    stats_path = tmp_path / "stats.json"
    with pytest.raises(OsmBoundaryError, match="Polygon or MultiPolygon"):
        run(
            output_geojson=output,
            stats_path=stats_path,
            request_retries=1,
            backoff_sec=0,
            timeout_sec=1,
            session=_Session(_Response(_payload(geometry_type="Point"))),
        )
    assert not output.exists()
    assert not stats_path.exists()


def test_declared_oversize_response_fails_before_publication(tmp_path: Path) -> None:
    with pytest.raises(OsmBoundaryError, match="exceeds max_bytes=100"):
        run(
            output_geojson=tmp_path / "boundary.geojson",
            stats_path=tmp_path / "stats.json",
            request_retries=1,
            backoff_sec=0,
            timeout_sec=1,
            max_response_bytes=100,
            session=_Session(_Response(_payload(), declared_bytes=101)),
        )
