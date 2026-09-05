from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any
from unittest.mock import patch

import mapbox_vector_tile
import pytest
import requests

from ml.ingestion.common import read_json, write_json
from ml.ingestion.mapillary_citywide import (
    DEFAULT_BOUNDS,
    Bounds,
    IncompleteMapillarySelectionError,
    MapillaryCitywideError,
    SequenceCandidate,
    VectorTile,
    _acquisition_fingerprint,
    _seed_tile_entries,
    decode_sequence_tile,
    enumerate_vector_tiles,
    fetch_metadata_batch,
    fetch_vector_tile,
    filter_tiles_to_aoi,
    load_aoi_boundary,
    run,
    select_balanced_candidates,
)
from ml.ingestion.merge_sources import normalize_record


class _Response:
    def __init__(
        self,
        *,
        payload: Any | None = None,
        content: bytes = b"",
        status_code: int = 200,
    ) -> None:
        self._payload = payload
        self.content = json.dumps(payload).encode("utf-8") if payload is not None and not content else content
        self.status_code = status_code
        self.headers: dict[str, str] = {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> Any:
        return self._payload

    def iter_content(self, chunk_size: int):
        del chunk_size
        yield self.content

    def close(self) -> None:
        return None


class _FixtureSession:
    def __init__(self, tile_payload: bytes, metadata: dict[str, dict[str, Any]]) -> None:
        self.tile_payload = tile_payload
        self.metadata = metadata
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any], timeout: float, **kwargs: Any) -> _Response:
        del timeout, kwargs
        self.calls.append((url, dict(params)))
        if "tiles.mapillary.com" in url:
            return _Response(content=self.tile_payload)
        requested = str(params["ids"]).split(",")
        return _Response(
            payload={image_id: self.metadata[image_id] for image_id in requested if image_id in self.metadata}
        )


class _BadIdBatchSession(_FixtureSession):
    def __init__(self, tile_payload: bytes, metadata: dict[str, dict[str, Any]], bad_id: str) -> None:
        super().__init__(tile_payload, metadata)
        self.bad_id = bad_id

    def get(self, url: str, params: dict[str, Any], timeout: float, **kwargs: Any) -> _Response:
        if "tiles.mapillary.com" in url:
            return super().get(url, params, timeout, **kwargs)
        requested = str(params["ids"]).split(",")
        if self.bad_id in requested:
            self.calls.append((url, dict(params)))
            response = _Response(payload={"error": "invalid id"}, status_code=400)

            def raise_for_status() -> None:
                raise requests.HTTPError("400 Client Error", response=response)

            response.raise_for_status = raise_for_status  # type: ignore[method-assign]
            return response
        return super().get(url, params, timeout, **kwargs)


class _FailingSession:
    def __init__(self) -> None:
        self.calls = 0

    def get(self, url: str, params: dict[str, Any], timeout: float, **kwargs: Any) -> _Response:
        del url, params, timeout, kwargs
        self.calls += 1
        raise RuntimeError("offline token=must-not-leak")


class _NoNetworkSession:
    def get(self, url: str, params: dict[str, Any], timeout: float, **kwargs: Any) -> _Response:
        del url, params, timeout, kwargs
        raise AssertionError("cache-resume unexpectedly touched the network")


class _ResponseSequenceSession:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = responses
        self.calls = 0

    def get(self, url: str, params: dict[str, Any], timeout: float, **kwargs: Any) -> _Response:
        del url, params, timeout, kwargs
        response = self.responses[min(self.calls, len(self.responses) - 1)]
        self.calls += 1
        return response


def _tile_payload(image_ids: list[str], *, invalid_feature: bool = False) -> bytes:
    features: list[dict[str, Any]] = []
    for index, image_id in enumerate(image_ids):
        offset = 300 + index * 650
        features.append(
            {
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[offset, 700], [offset + 200, 1100]],
                },
                "properties": {
                    "id": f"sequence-{image_id}",
                    "image_id": int(image_id),
                },
            }
        )
    if invalid_feature:
        features.append(
            {
                "geometry": {"type": "LineString", "coordinates": [[100, 100], [200, 200]]},
                "properties": {"id": "missing-image-id"},
            }
        )
    return mapbox_vector_tile.encode(
        {"name": "sequence", "features": features},
        default_options={"y_coord_down": False},
    )


def _metadata(image_id: str, *, lon: float, lat: float, creator: bool = True) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": image_id,
        "captured_at": 1_700_000_000_000 + int(image_id),
        "computed_geometry": {"type": "Point", "coordinates": [lon, lat]},
        "computed_compass_angle": 123.4,
        "thumb_2048_url": f"https://scontent.example/{image_id}.jpg?signature=fixture",
        "thumb_original_url": f"https://scontent.example/{image_id}-original.jpg?signature=fixture",
        "sequence": {"id": f"sequence-{image_id}"},
        "width": 2048,
        "height": 1536,
    }
    if creator:
        item["creator"] = {"id": f"creator-{image_id}", "username": f"author-{image_id}"}
    return item


def _run_paths(tmp_path: Path) -> dict[str, Path]:
    return {
        "output_json": tmp_path / "mapillary_raw.json",
        "cache_dir": tmp_path / "cache",
        "checkpoint_path": tmp_path / "checkpoint.json",
        "stats_path": tmp_path / "stats.json",
    }


def _write_polygon_geojson(path: Path, coordinates: list[list[list[float]]]) -> Path:
    write_json(
        path,
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "fixture-aoi"},
                    "geometry": {"type": "Polygon", "coordinates": coordinates},
                }
            ],
        },
    )
    return path


def test_enumerates_all_intersecting_tiles_at_configurable_supported_zoom() -> None:
    bounds = Bounds(west=37.60, south=55.74, east=37.64, north=55.78)
    tiles = enumerate_vector_tiles(bounds, zoom=12)
    assert tiles
    assert tiles == sorted(set(tiles))
    assert {tile.z for tile in tiles} == {12}
    with pytest.raises(ValueError, match="support zoom"):
        enumerate_vector_tiles(bounds, zoom=15)


def test_loads_exact_polygon_aoi_filters_tiles_and_fingerprints_content(tmp_path: Path) -> None:
    aoi_path = _write_polygon_geojson(
        tmp_path / "aoi.geojson",
        [
            [
                [37.60, 55.74],
                [37.62, 55.74],
                [37.62, 55.76],
                [37.60, 55.76],
                [37.60, 55.74],
            ]
        ],
    )
    boundary = load_aoi_boundary(aoi_path)
    assert boundary.bounds == Bounds(37.60, 55.74, 37.62, 55.76)
    assert boundary.covers(37.61, 55.75)
    assert not boundary.covers(37.80, 55.75)

    bbox_tiles = enumerate_vector_tiles(DEFAULT_BOUNDS, zoom=12)
    filtered = filter_tiles_to_aoi(bbox_tiles, boundary)
    assert filtered
    assert len(filtered) < len(bbox_tiles)
    assert set(filtered).issubset(bbox_tiles)

    bbox_fingerprint = _acquisition_fingerprint(DEFAULT_BOUNDS, 12)
    polygon_fingerprint = _acquisition_fingerprint(
        DEFAULT_BOUNDS,
        12,
        aoi_sha256=boundary.sha256,
    )
    assert bbox_fingerprint != polygon_fingerprint
    assert bbox_fingerprint == _acquisition_fingerprint(DEFAULT_BOUNDS, 12)


def test_rejects_ambiguous_or_non_polygon_aoi(tmp_path: Path) -> None:
    ambiguous = tmp_path / "ambiguous.geojson"
    write_json(ambiguous, {"type": "FeatureCollection", "features": []})
    with pytest.raises(ValueError, match="exactly one feature"):
        load_aoi_boundary(ambiguous)

    point = tmp_path / "point.geojson"
    write_json(point, {"type": "Point", "coordinates": [37.61, 55.75]})
    with pytest.raises(ValueError, match="Polygon or MultiPolygon"):
        load_aoi_boundary(point)


def test_seed_checkpoint_imports_only_allowed_integrity_metadata(tmp_path: Path) -> None:
    seed_path = tmp_path / "seed.json"
    entry = {
        "sha256": "a" * 64,
        "bytes": 123,
        "feature_count": 2,
        "candidate_count": 2,
        "invalid_feature_count": 0,
        "fetched_at_unix": 1_800_000_000.0,
    }
    write_json(
        seed_path,
        {
            "schema_version": 1,
            "source": "mapillary_citywide_vector_tiles",
            "config_fingerprint": "different-aoi-is-safe-for-zxy-tiles",
            "tiles": {"12/1/2": entry, "12/9/9": entry},
            "failed_tiles": {},
            "failed_metadata": {},
        },
    )
    checkpoint: dict[str, Any] = {
        "schema_version": 1,
        "source": "mapillary_citywide_vector_tiles",
        "config_fingerprint": "destination",
        "tiles": {},
        "failed_tiles": {},
        "failed_metadata": {},
    }
    eligible, imported = _seed_tile_entries(
        checkpoint,
        seed_path=seed_path,
        allowed_tile_keys={"12/1/2", "12/3/4"},
    )
    assert (eligible, imported) == (1, 1)
    assert checkpoint["tiles"] == {"12/1/2": entry}

    malformed = tmp_path / "malformed.json"
    write_json(
        malformed,
        {
            "schema_version": 1,
            "source": "mapillary_citywide_vector_tiles",
            "tiles": {"12/1/2": {**entry, "sha256": "not-a-sha"}},
        },
    )
    with pytest.raises(MapillaryCitywideError, match="integrity metadata"):
        _seed_tile_entries(
            checkpoint | {"tiles": {}},
            seed_path=malformed,
            allowed_tile_keys={"12/1/2"},
        )


def test_decodes_official_sequence_layer_and_tracks_invalid_features() -> None:
    tile = VectorTile(12, 2475, 1280)
    decoded = decode_sequence_tile(
        _tile_payload(["101", "102"], invalid_feature=True),
        tile,
        subcells_per_axis=4,
    )
    assert decoded.feature_count == 3
    assert decoded.invalid_feature_count == 1
    assert [candidate.image_id for candidate in decoded.candidates] == ["101", "102"]
    assert [candidate.sequence_id for candidate in decoded.candidates] == [
        "sequence-101",
        "sequence-102",
    ]
    assert all(0 <= candidate.subcell_x < 4 for candidate in decoded.candidates)
    assert all(0 <= candidate.subcell_y < 4 for candidate in decoded.candidates)
    assert all(-180 <= candidate.representative_lon <= 180 for candidate in decoded.candidates)
    assert all(-85.05112878 <= candidate.representative_lat <= 85.05112878 for candidate in decoded.candidates)
    empty = decode_sequence_tile(b"", tile)
    assert empty == type(empty)(candidates=(), feature_count=0, invalid_feature_count=0)


def test_balanced_selection_is_deterministic_and_enforces_both_caps() -> None:
    candidates: list[SequenceCandidate] = []
    for tile_index in range(3):
        tile = VectorTile(12, 2400 + tile_index, 1300)
        for index in range(12):
            candidates.append(
                SequenceCandidate(
                    image_id=f"{tile_index}-{index}",
                    sequence_id=f"sequence-{tile_index}-{index}",
                    tile=tile,
                    subcell_x=index % 2,
                    subcell_y=(index // 2) % 2,
                    representative_lon=37.6,
                    representative_lat=55.75,
                )
            )
    # Duplicate sequence and image identities cannot consume extra quota.
    candidates.extend([candidates[0], candidates[1]])
    first = select_balanced_candidates(
        candidates,
        max_candidates=15,
        max_per_tile=6,
        max_per_subcell=2,
        seed=41,
    )
    second = select_balanced_candidates(
        list(reversed(candidates)),
        max_candidates=15,
        max_per_tile=6,
        max_per_subcell=2,
        seed=41,
    )
    assert first == second
    assert len({item.image_id for item in first}) == len(first)
    assert len({item.sequence_id for item in first}) == len(first)
    assert max(Counter(item.tile_key for item in first).values()) <= 6
    assert max(Counter(item.subcell_key for item in first).values()) <= 2


def test_graph_root_batch_request_uses_ids_and_fields() -> None:
    session = _FixtureSession(b"unused", {"1": _metadata("1", lon=37.61, lat=55.75)})
    result = fetch_metadata_batch(
        session,
        token="secret-fixture-token",
        image_ids=["1"],
        retries=1,
        backoff_sec=0,
        timeout_sec=1,
    )
    assert result["1"]["creator"]["username"] == "author-1"
    url, params = session.calls[0]
    assert url == "https://graph.mapillary.com/"
    assert params["ids"] == "1"
    assert "computed_geometry" in params["fields"]
    assert "thumb_original_url" not in params["fields"]
    assert params["access_token"] == "secret-fixture-token"


def test_bounded_streams_and_content_validation_are_retried() -> None:
    tile = VectorTile(12, 2475, 1280)
    valid_tile = _tile_payload(["1"])
    tile_session = _ResponseSequenceSession([_Response(content=b"not-a-vector-tile"), _Response(content=valid_tile)])
    payload = fetch_vector_tile(
        tile_session,
        token="token",
        tile=tile,
        retries=2,
        backoff_sec=0,
        timeout_sec=1,
        payload_validator=lambda value: decode_sequence_tile(value, tile),
    )
    assert payload == valid_tile
    assert tile_session.calls == 2

    oversized_session = _ResponseSequenceSession([_Response(content=b"x" * 11)])
    with pytest.raises(MapillaryCitywideError, match="exceeds max_bytes=10"):
        fetch_vector_tile(
            oversized_session,
            token="token",
            tile=tile,
            retries=1,
            backoff_sec=0,
            timeout_sec=1,
            max_tile_bytes=10,
        )

    metadata_session = _ResponseSequenceSession(
        [
            _Response(content=b"{"),
            _Response(payload={"1": _metadata("1", lon=37.61, lat=55.75)}),
        ]
    )
    received = fetch_metadata_batch(
        metadata_session,
        token="token",
        image_ids=["1"],
        retries=2,
        backoff_sec=0,
        timeout_sec=1,
    )
    assert received["1"]["id"] == "1"
    assert metadata_session.calls == 2


def test_pipeline_publishes_compatible_balanced_json_and_resumes_entirely_from_cache(
    tmp_path: Path,
) -> None:
    tile = VectorTile(12, 2475, 1280)
    tile_payload = _tile_payload(["1", "2", "3", "4"])
    metadata = {
        image_id: _metadata(
            image_id,
            lon=37.605 + int(image_id) * 0.004,
            lat=55.745 + int(image_id) * 0.004,
        )
        for image_id in ("1", "2", "3", "4")
    }
    metadata["1"]["sequence"] = {"id": None}
    paths = _run_paths(tmp_path)
    session = _FixtureSession(tile_payload, metadata)
    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "secret-fixture-token"}),
        patch(
            "ml.ingestion.mapillary_citywide.enumerate_vector_tiles",
            return_value=[tile],
        ),
    ):
        first = run(
            **paths,
            bounds=Bounds(37.60, 55.74, 37.63, 55.78),
            max_records=3,
            max_per_tile=3,
            max_per_subcell=2,
            candidate_multiplier=2,
            metadata_batch_size=2,
            request_retries=1,
            backoff_sec=0,
            timeout_sec=1,
            request_interval_sec=0,
            session=session,
        )
    assert first["complete"] is True
    assert first["records_selected"] == 3
    records = read_json(paths["output_json"], default=[])
    assert len(records) == 3
    assert all(record["license"] == "CC BY-SA 4.0" for record in records)
    assert all(record["attribution"].startswith("Mapillary image by ") for record in records)
    assert all("pKey=" in record["source_url"] for record in records)
    assert all(record["sequence_id"] for record in records)
    assert all("-original.jpg" not in record["image_url"] for record in records)
    assert all(normalize_record("mapillary", record) is not None for record in records)
    assert max(Counter(record["_geosnap_selection_tile"] for record in records).values()) <= 3
    assert any("tiles.mapillary.com" in url for url, _ in session.calls)
    assert any(url == "https://graph.mapillary.com/" for url, _ in session.calls)
    for path in tmp_path.rglob("*"):
        if path.is_file():
            assert b"secret-fixture-token" not in path.read_bytes()

    # The second run must verify and reuse both MVT and per-image Graph caches.
    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "different-runtime-token"}),
        patch(
            "ml.ingestion.mapillary_citywide.enumerate_vector_tiles",
            return_value=[tile],
        ),
    ):
        second = run(
            **paths,
            bounds=Bounds(37.60, 55.74, 37.63, 55.78),
            max_records=3,
            max_per_tile=3,
            max_per_subcell=2,
            candidate_multiplier=2,
            metadata_batch_size=2,
            request_retries=1,
            backoff_sec=0,
            timeout_sec=1,
            request_interval_sec=0,
            session=_NoNetworkSession(),
        )
    assert second["tiles_cache_hits"] == 1
    assert second["metadata_cache_hits"] == 4
    assert read_json(paths["output_json"], default=[]) == records

    # Signed thumbnail URLs are refreshed after the bounded cache lifetime.
    metadata_cache_files = sorted((paths["cache_dir"] / "metadata").glob("*.json"))
    for cache_file in metadata_cache_files:
        payload = read_json(cache_file, default={})
        payload["fetched_at_unix"] = 0
        write_json(cache_file, payload)
    checkpoint = read_json(paths["checkpoint_path"], default={})
    checkpoint["tiles"][tile.key]["fetched_at_unix"] = 0
    write_json(paths["checkpoint_path"], checkpoint)
    refresh_session = _FixtureSession(tile_payload, metadata)
    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "refresh-runtime-token"}),
        patch("ml.ingestion.mapillary_citywide.enumerate_vector_tiles", return_value=[tile]),
    ):
        refreshed = run(
            **paths,
            bounds=Bounds(37.60, 55.74, 37.63, 55.78),
            max_records=3,
            max_per_tile=3,
            max_per_subcell=2,
            candidate_multiplier=2,
            metadata_batch_size=2,
            metadata_cache_max_age_sec=1,
            request_retries=1,
            backoff_sec=0,
            timeout_sec=1,
            request_interval_sec=0,
            session=refresh_session,
        )
    assert refreshed["metadata_cache_stale_or_invalid"] == 4
    assert refreshed["tiles_cache_stale"] == 1
    assert sum("tiles.mapillary.com" in url for url, _ in refresh_session.calls) == 1
    assert sum(url == "https://graph.mapillary.com/" for url, _ in refresh_session.calls) == 2


def test_exact_graph_coordinates_filter_out_tile_candidates_outside_aoi(tmp_path: Path) -> None:
    tile = VectorTile(12, 2475, 1280)
    paths = _run_paths(tmp_path)
    session = _FixtureSession(
        _tile_payload(["1", "2"]),
        {
            "1": _metadata("1", lon=37.61, lat=55.75),
            "2": _metadata("2", lon=38.20, lat=55.75),
        },
    )
    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}),
        patch("ml.ingestion.mapillary_citywide.enumerate_vector_tiles", return_value=[tile]),
    ):
        stats = run(
            **paths,
            bounds=Bounds(37.60, 55.74, 37.63, 55.78),
            max_records=2,
            max_per_tile=2,
            max_per_subcell=2,
            metadata_batch_size=2,
            request_retries=1,
            backoff_sec=0,
            timeout_sec=1,
            request_interval_sec=0,
            session=session,
        )
    records = read_json(paths["output_json"], default=[])
    assert stats["metadata_items_outside_aoi"] == 1
    assert [record["id"] for record in records] == ["1"]


def test_polygon_aoi_rechecks_exact_graph_coordinates_and_records_hash(tmp_path: Path) -> None:
    import mercantile

    tile_value = mercantile.tile(37.61, 55.75, 12)
    tile = VectorTile(tile_value.z, tile_value.x, tile_value.y)
    tile_payload = _tile_payload(["1", "2"])
    representatives = decode_sequence_tile(tile_payload, tile).candidates
    representative_lons = [candidate.representative_lon for candidate in representatives]
    representative_lats = [candidate.representative_lat for candidate in representatives]
    west = min(representative_lons) - 0.001
    east = max(representative_lons) + 0.001
    south = min(representative_lats) - 0.001
    north = max(representative_lats) + 0.001
    aoi_path = _write_polygon_geojson(
        tmp_path / "exact-aoi.geojson",
        [
            [
                [west, south],
                [east, south],
                [east, north],
                [west, north],
                [west, south],
            ]
        ],
    )
    boundary = load_aoi_boundary(aoi_path)
    paths = _run_paths(tmp_path)
    session = _FixtureSession(
        tile_payload,
        {
            "1": _metadata(
                "1",
                lon=representatives[0].representative_lon,
                lat=representatives[0].representative_lat,
            ),
            "2": _metadata("2", lon=37.85, lat=55.75),
        },
    )
    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}),
        patch("ml.ingestion.mapillary_citywide.enumerate_vector_tiles", return_value=[tile]),
    ):
        stats = run(
            **paths,
            bounds=DEFAULT_BOUNDS,
            aoi_geojson=aoi_path,
            max_records=1,
            max_per_tile=2,
            max_per_subcell=2,
            candidate_multiplier=2,
            metadata_batch_size=2,
            request_retries=1,
            backoff_sec=0,
            timeout_sec=1,
            request_interval_sec=0,
            session=session,
        )
    records = read_json(paths["output_json"], default=[])
    assert stats["complete"] is True
    assert stats["aoi_mode"] == "geojson_polygon"
    assert stats["aoi_sha256"] == boundary.sha256
    assert stats["metadata_items_outside_aoi"] == 1
    assert len(records) == 1
    assert records[0]["source_image_id"] == "1"
    source_metadata = json.loads(records[0]["metadata_json"])
    assert source_metadata["_geosnap_acquisition"]["aoi_sha256"] == boundary.sha256


def test_failed_tile_is_checkpointed_and_partial_output_requires_explicit_override(tmp_path: Path) -> None:
    tile = VectorTile(12, 2475, 1280)
    paths = _run_paths(tmp_path)
    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "must-not-leak"}),
        patch("ml.ingestion.mapillary_citywide.enumerate_vector_tiles", return_value=[tile]),
    ):
        with pytest.raises(IncompleteMapillarySelectionError, match="partial output was not published"):
            run(
                **paths,
                bounds=Bounds(37.60, 55.74, 37.63, 55.78),
                max_records=2,
                max_per_tile=2,
                max_per_subcell=2,
                request_retries=1,
                backoff_sec=0,
                timeout_sec=1,
                request_interval_sec=0,
                session=_FailingSession(),
            )
    assert not paths["output_json"].exists()
    checkpoint = read_json(paths["checkpoint_path"], default={})
    stats = read_json(paths["stats_path"], default={})
    assert tile.key in checkpoint["failed_tiles"]
    assert stats["complete"] is False
    assert "failed_vector_tiles" in stats["incomplete_reasons"]
    assert "must-not-leak" not in json.dumps(checkpoint)

    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "must-not-leak"}),
        patch("ml.ingestion.mapillary_citywide.enumerate_vector_tiles", return_value=[tile]),
    ):
        override = run(
            **paths,
            bounds=Bounds(37.60, 55.74, 37.63, 55.78),
            max_records=2,
            max_per_tile=2,
            max_per_subcell=2,
            request_retries=1,
            backoff_sec=0,
            timeout_sec=1,
            request_interval_sec=0,
            allow_incomplete=True,
            session=_FailingSession(),
        )
    assert override["output_written"] is True
    assert read_json(paths["output_json"], default=None) == []


def test_missing_or_legally_incomplete_graph_metadata_fails_closed(tmp_path: Path) -> None:
    tile = VectorTile(12, 2475, 1280)
    paths = _run_paths(tmp_path)
    session = _FixtureSession(
        _tile_payload(["1", "2"]),
        {"1": _metadata("1", lon=37.61, lat=55.75, creator=False)},
    )
    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}),
        patch("ml.ingestion.mapillary_citywide.enumerate_vector_tiles", return_value=[tile]),
    ):
        with pytest.raises(IncompleteMapillarySelectionError, match="failed_or_invalid_graph_metadata"):
            run(
                **paths,
                bounds=Bounds(37.60, 55.74, 37.63, 55.78),
                max_records=2,
                max_per_tile=2,
                max_per_subcell=2,
                metadata_batch_size=2,
                request_retries=1,
                backoff_sec=0,
                timeout_sec=1,
                request_interval_sec=0,
                session=session,
            )
    assert not paths["output_json"].exists()
    stats = read_json(paths["stats_path"], default={})
    assert stats["metadata_items_missing"] == 1
    assert stats["metadata_items_invalid"] == 1


def test_stale_missing_graph_id_is_quarantined_when_valid_target_is_met(tmp_path: Path) -> None:
    tile = VectorTile(12, 2475, 1280)
    paths = _run_paths(tmp_path)
    session = _FixtureSession(
        _tile_payload(["1", "2", "3"]),
        {
            "1": _metadata("1", lon=37.61, lat=55.75),
            "2": _metadata("2", lon=37.611, lat=55.751),
        },
    )
    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}),
        patch("ml.ingestion.mapillary_citywide.enumerate_vector_tiles", return_value=[tile]),
    ):
        stats = run(
            **paths,
            bounds=Bounds(37.60, 55.74, 37.63, 55.78),
            max_records=2,
            max_per_tile=3,
            max_per_subcell=3,
            candidate_multiplier=2,
            metadata_batch_size=3,
            request_retries=1,
            backoff_sec=0,
            timeout_sec=1,
            request_interval_sec=0,
            session=session,
        )
    assert stats["complete"] is True
    assert stats["records_selected"] == 2
    assert stats["metadata_items_missing"] == 1
    assert stats["quarantined_metadata_current"] == 1
    assert stats["blocking_failed_metadata_current"] == 0


def test_low_resolution_graph_image_is_quarantined_and_target_is_backfilled(
    tmp_path: Path,
) -> None:
    tile = VectorTile(12, 2475, 1280)
    paths = _run_paths(tmp_path)
    low_resolution = _metadata("1", lon=37.61, lat=55.75)
    low_resolution.update({"width": 176, "height": 144})
    session = _FixtureSession(
        _tile_payload(["1", "2", "3"]),
        {
            "1": low_resolution,
            "2": _metadata("2", lon=37.611, lat=55.751),
            "3": _metadata("3", lon=37.612, lat=55.752),
        },
    )
    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}),
        patch("ml.ingestion.mapillary_citywide.enumerate_vector_tiles", return_value=[tile]),
    ):
        stats = run(
            **paths,
            bounds=Bounds(37.60, 55.74, 37.63, 55.78),
            max_records=2,
            max_per_tile=3,
            max_per_subcell=3,
            candidate_multiplier=2,
            metadata_batch_size=3,
            request_retries=1,
            backoff_sec=0,
            timeout_sec=1,
            request_interval_sec=0,
            session=session,
        )
    records = read_json(paths["output_json"], default=[])
    assert stats["complete"] is True
    assert stats["records_selected"] == 2
    assert stats["metadata_items_invalid"] == 1
    assert stats["metadata_items_below_min_dimensions"] == 1
    assert stats["quarantined_metadata_current"] == 1
    assert {record["source_image_id"] for record in records} == {"2", "3"}


def test_bad_graph_id_isolated_by_batch_bisection(tmp_path: Path) -> None:
    tile = VectorTile(12, 2475, 1280)
    paths = _run_paths(tmp_path)
    session = _BadIdBatchSession(
        _tile_payload(["1", "2", "3"]),
        {
            "1": _metadata("1", lon=37.61, lat=55.75),
            "2": _metadata("2", lon=37.611, lat=55.751),
        },
        bad_id="3",
    )
    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}),
        patch("ml.ingestion.mapillary_citywide.enumerate_vector_tiles", return_value=[tile]),
    ):
        stats = run(
            **paths,
            bounds=Bounds(37.60, 55.74, 37.63, 55.78),
            max_records=2,
            max_per_tile=3,
            max_per_subcell=3,
            candidate_multiplier=2,
            metadata_batch_size=3,
            request_retries=1,
            backoff_sec=0,
            timeout_sec=1,
            request_interval_sec=0,
            session=session,
        )
    assert stats["complete"] is True
    assert stats["records_selected"] == 2
    assert stats["metadata_batch_splits"] == 2
    assert stats["metadata_singletons_quarantined"] == 1
    assert stats["metadata_batches_failed"] == 0


def test_max_tiles_is_explicitly_incomplete_without_override(tmp_path: Path) -> None:
    tile_a = VectorTile(12, 2475, 1280)
    tile_b = VectorTile(12, 2476, 1280)
    paths = _run_paths(tmp_path)
    session = _FixtureSession(
        _tile_payload(["1"]),
        {"1": _metadata("1", lon=37.61, lat=55.75)},
    )
    with (
        patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}),
        patch(
            "ml.ingestion.mapillary_citywide.enumerate_vector_tiles",
            return_value=[tile_a, tile_b],
        ),
    ):
        with pytest.raises(IncompleteMapillarySelectionError, match="tile_enumeration_truncated"):
            run(
                **paths,
                bounds=Bounds(37.60, 55.74, 37.63, 55.78),
                max_records=1,
                max_per_tile=1,
                max_per_subcell=1,
                max_tiles=1,
                request_retries=1,
                backoff_sec=0,
                timeout_sec=1,
                request_interval_sec=0,
                session=session,
            )
