from __future__ import annotations

import io
import tempfile
from pathlib import Path

import pandas as pd
import pytest
from PIL import Image

import ml.ingestion.combine_reference_gallery as combine_gallery
from ml.ingestion.combine_reference_gallery import GalleryCombineError, run
from ml.ingestion.common import write_json
from ml.ingestion.msls_moscow import MSLS_ATTRIBUTION, MSLS_DATASET_URL, MSLS_LICENSE
from ml.ingestion.schema import canonical_record, manifest_dataframe, read_manifest, write_manifest


def _record(
    root: Path,
    source: str,
    source_id: str,
    *,
    lat: float,
    lon: float,
    sequence: str = "shared-sequence",
    captured_at: str = "2019-06-01T00:00:00Z",
    heading: float = 45.0,
) -> dict[str, object]:
    if source == "mapillary":
        license_name = "CC BY-SA 4.0"
        attribution = "Mapillary image by fixture"
        source_url = f"https://www.mapillary.com/app/?focus=photo&pKey={source_id}"
    elif source == "kartaview":
        license_name = "CC BY-SA 4.0"
        attribution = "© Grab and KartaView Contributors"
        source_url = f"https://kartaview.org/details/{sequence}/0/track-info"
    else:
        license_name = MSLS_LICENSE
        attribution = MSLS_ATTRIBUTION
        source_url = MSLS_DATASET_URL
    record = canonical_record(
        source=source,
        source_image_id=source_id,
        lat=lat,
        lon=lon,
        image_path=str(root / "images" / f"{source}-{source_id}.jpg"),
        sequence_id=f"msls:{sequence}" if source == "msls" else sequence,
        captured_at=captured_at,
        heading=heading,
        quality_score=None if source == "msls" else 0.8,
        license_name=license_name,
        attribution=attribution,
        source_url=source_url,
        metadata={"source_fixture": source},
    )
    if source == "mapillary":
        record["mapillary_creator"] = "fixture"
    elif source == "kartaview":
        record["kartaview_sequence_index"] = 0
    else:
        record.update(
            {
                "msls_original_split": "database",
                "msls_view_direction": "Forward",
                "msls_night": False,
                "msls_pano": False,
            }
        )
    return record


def _write_source(path: Path, records: list[dict[str, object]]) -> Path:
    write_manifest(manifest_dataframe(records), path, allow_empty=False)
    return path


def _write_aoi(
    path: Path,
    *,
    west: float,
    south: float,
    east: float,
    north: float,
) -> Path:
    write_json(
        path,
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "fixture-aoi"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [west, south],
                                [east, south],
                                [east, north],
                                [west, north],
                                [west, south],
                            ]
                        ],
                    },
                }
            ],
        },
    )
    return path


def _fixture_inputs(root: Path, *, create_images: bool = False) -> list[Path]:
    records = {
        "mapillary": [
            _record(root, "mapillary", "m1", lat=55.75, lon=37.61, captured_at="2019-01-01"),
            _record(root, "mapillary", "m2", lat=55.76, lon=37.62, captured_at="2020-07-01"),
        ],
        "kartaview": [
            _record(root, "kartaview", "k1", lat=55.74, lon=37.60, captured_at="2021-04-01"),
            _record(root, "kartaview", "k2", lat=55.77, lon=37.63, captured_at="2022-10-01"),
        ],
        "msls": [
            _record(root, "msls", "s1", lat=55.73, lon=37.59, captured_at="2018-02-01"),
            _record(root, "msls", "s2", lat=55.82, lon=37.75, captured_at="2019-08-01"),
        ],
    }
    if create_images:
        for record in (record for values in records.values() for record in values):
            image_path = Path(str(record["image_path"]))
            image_path.parent.mkdir(parents=True, exist_ok=True)
            buffer = io.BytesIO()
            Image.new("RGB", (64, 48), color=(20, 80, 140)).save(buffer, format="JPEG")
            image_path.write_bytes(buffer.getvalue())
    return [_write_source(root / f"{source}.parquet", values) for source, values in records.items()]


def _run(root: Path, inputs: list[Path], **kwargs: object) -> dict[str, object]:
    kwargs.setdefault("image_base", root)
    # Most fixture coverage intentionally exercises the optional MSLS-aware
    # diagnostics too. Production defaults are tested separately below.
    kwargs.setdefault("required_sources", ("mapillary", "kartaview", "msls"))
    return run(
        input_manifests=inputs,
        output_manifest=root / "combined.parquet",
        report_json=root / "report.json",
        report_markdown=root / "report.md",
        maps_dir=root / "maps",
        **kwargs,
    )


def _published_artifact_paths(root: Path) -> list[Path]:
    return [
        root / "combined.parquet",
        root / "report.json",
        root / "report.md",
        root / "maps" / "mapillary_coverage.png",
        root / "maps" / "kartaview_coverage.png",
        root / "maps" / "msls_coverage.png",
        root / "maps" / "combined_coverage_sources.png",
        root / "maps" / "combined_coverage_density.png",
    ]


def _published_bytes(root: Path) -> dict[Path, bytes]:
    return {path: path.read_bytes() for path in _published_artifact_paths(root)}


def _bundle_root(root: Path) -> Path:
    matches = list(root.glob(".report.bundle-*"))
    assert len(matches) == 1
    return matches[0]


def _generation_names(root: Path) -> set[str]:
    return {path.name for path in (_bundle_root(root) / "generations").iterdir()}


def _staging_names(root: Path) -> set[str]:
    staging = _bundle_root(root) / "staging"
    return {path.name for path in staging.iterdir()} if staging.is_dir() else set()


def test_combines_union_schema_validates_images_and_generates_all_maps() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        report = _run(root, _fixture_inputs(root, create_images=True), require_images=True)

        combined = read_manifest(root / "combined.parquet", allow_empty=False)
        assert len(combined) == 6
        assert set(combined["source"]) == {"mapillary", "kartaview", "msls"}
        assert {
            "mapillary_creator",
            "kartaview_sequence_index",
            "msls_original_split",
            "msls_view_direction",
            "msls_night",
            "msls_pano",
        }.issubset(combined.columns)
        assert report["complete"] is True
        assert report["validation"]["image_files_checked"] == 6
        assert report["coverage"]["combined"]["sequence_diversity"]["unique_provider_sequences"] == 3
        assert report["coverage"]["combined"]["temporal_seasonal_diversity"]["seasons"] == {
            "autumn": 1,
            "spring": 1,
            "summer": 2,
            "winter": 2,
        }
        assert set(report["maps"]) == {
            "mapillary",
            "kartaview",
            "msls",
            "combined_sources",
            "combined_density",
        }
        for value in report["maps"].values():
            with Image.open(Path(value)) as image:
                assert image.format == "PNG"
                assert image.width > 100 and image.height > 100
        assert (root / "report.md").read_text().startswith("# Canonical Moscow reference gallery")
        schema_names = {field["name"] for field in report["output_schema"]}
        assert set(combined.columns) == schema_names
        schema_types = {field["name"]: field["type"] for field in report["output_schema"]}
        assert schema_types["kartaview_sequence_index"] == "int64"
        assert schema_types["msls_night"] == "bool"
        assert report["source_provenance"]["msls"]["license_counts"] == {MSLS_LICENSE: 2}


def test_default_publication_scope_is_deployable_mapillary_and_kartaview() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root, create_images=True)[:2]
        report = run(
            input_manifests=inputs,
            output_manifest=root / "combined.parquet",
            report_json=root / "report.json",
            report_markdown=root / "report.md",
            maps_dir=root / "maps",
            require_images=True,
            image_base=root,
        )

        assert report["validation"]["required_sources"] == ["mapillary", "kartaview"]
        assert set(report["source_counts"]) == {"mapillary", "kartaview"}
        assert "msls" not in report["maps"]


def test_default_publication_scope_rejects_accidental_msls_input() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root, create_images=True)
        with pytest.raises(GalleryCombineError, match="unexpected_sources:msls"):
            run(
                input_manifests=inputs,
                output_manifest=root / "combined.parquet",
                report_json=root / "report.json",
                report_markdown=root / "report.md",
                maps_dir=root / "maps",
                require_images=True,
                image_base=root,
            )


def test_exact_aoi_supports_administrative_moscow_beyond_legacy_bbox() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        mapillary.at[0, "lat"] = 55.99
        mapillary.at[0, "lon"] = 37.80
        write_manifest(mapillary, inputs[0], allow_empty=False)
        aoi = _write_aoi(
            root / "moscow-admin.geojson",
            west=37.50,
            south=55.60,
            east=38.00,
            north=56.10,
        )

        report = _run(root, inputs, aoi_geojson=aoi)

        assert report["rows"] == 6
        assert report["aoi"]["mode"] == "geojson_polygon"
        assert report["aoi"]["rows_excluded"] == 0
        assert report["validation"]["moscow_aoi_mode"] == "geojson_polygon"
        assert report["coverage"]["combined"]["geographic_coverage"]["bounds"]["max_lat"] == 55.99


def test_exact_aoi_requires_explicit_filter_and_audits_excluded_sources() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        mapillary.at[0, "lon"] = 37.40
        write_manifest(mapillary, inputs[0], allow_empty=False)
        aoi = _write_aoi(
            root / "moscow-admin.geojson",
            west=37.58,
            south=55.72,
            east=37.80,
            north=55.85,
        )

        with pytest.raises(GalleryCombineError, match="coordinate_outside_moscow_aoi:1"):
            _run(root, inputs, aoi_geojson=aoi)

        report = _run(
            root,
            inputs,
            aoi_geojson=aoi,
            filter_outside_aoi=True,
        )
        combined = read_manifest(root / "combined.parquet", allow_empty=False)
        assert len(combined) == 5
        assert report["aoi"]["rows_before_filter"] == 6
        assert report["aoi"]["rows_excluded"] == 1
        assert report["aoi"]["rows_excluded_by_source"] == {"mapillary": 1}
        assert report["aoi"]["rows_after_filter"] == 5


def test_missing_required_source_fails_before_overwriting_existing_output() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)[:2]
        output = root / "combined.parquet"
        output.write_bytes(b"sentinel")
        with pytest.raises(GalleryCombineError, match="missing_required_sources:msls"):
            _run(root, inputs)
        assert output.read_bytes() == b"sentinel"
        assert not (root / "maps").exists()


def test_empty_input_is_rejected() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        empty = root / "empty.parquet"
        write_manifest(manifest_dataframe([]), empty)
        with pytest.raises(GalleryCombineError, match="invalid_input_manifest.*empty_manifest"):
            _run(root, [*inputs, empty])


def test_duplicate_uuid_source_identity_and_path_are_all_reported() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        duplicate = read_manifest(inputs[0], allow_empty=False).iloc[[0]].copy()
        duplicate_path = root / "duplicate-mapillary.parquet"
        write_manifest(duplicate, duplicate_path, allow_empty=False)
        with pytest.raises(GalleryCombineError) as captured:
            _run(root, [*inputs, duplicate_path])
        message = str(captured.value)
        assert "duplicate_uuid" in message
        assert "duplicate_source_identity" in message
        assert "duplicate_image_path" in message


def test_duplicate_image_path_across_sources_is_rejected() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        msls = read_manifest(inputs[2], allow_empty=False)
        msls.at[0, "image_path"] = mapillary.at[0, "image_path"]
        write_manifest(msls, inputs[2], allow_empty=False)
        with pytest.raises(GalleryCombineError, match="duplicate_image_path"):
            _run(root, inputs)


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("unstable_id", "unstable_reference_id"),
        ("bad_license", "invalid_source_provenance"),
        ("bad_url", "invalid_source_provenance"),
        ("outside_aoi", "coordinate_outside_moscow_bounds"),
        ("missing_sequence", "missing_sequence_id"),
    ],
)
def test_fail_closed_identity_scope_and_provenance_gates(mutation: str, expected: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        if mutation == "unstable_id":
            mapillary.at[0, "id"] = "00000000-0000-4000-8000-000000000000"
        elif mutation == "bad_license":
            mapillary.at[0, "license"] = "unknown"
        elif mutation == "bad_url":
            mapillary.at[0, "source_url"] = "https://example.test/app/?pKey=m1"
        elif mutation == "outside_aoi":
            mapillary.at[0, "lat"] = 56.1
        else:
            mapillary.at[0, "sequence_id"] = pd.NA
        write_manifest(mapillary, inputs[0], allow_empty=False)
        with pytest.raises(GalleryCombineError, match=expected):
            _run(root, inputs)


@pytest.mark.parametrize(
    ("source_index", "column", "value"),
    [
        (0, "source_url", "https://www.mapillary.com/app/?focus=photo&pKey=m1&token=secret"),
        (1, "source_url", "https://kartaview.org/"),
        (2, "attribution", "MSLS contributors"),
        (2, "source_url", f"{MSLS_DATASET_URL}?token=secret"),
    ],
)
def test_every_source_has_strict_url_and_attribution_policy(source_index: int, column: str, value: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        frame = read_manifest(inputs[source_index], allow_empty=False)
        frame.at[0, column] = value
        write_manifest(frame, inputs[source_index], allow_empty=False)
        with pytest.raises(GalleryCombineError, match="invalid_source_provenance"):
            _run(root, inputs)


def test_require_images_rejects_missing_files() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        with pytest.raises(GalleryCombineError, match="missing_image_files:6"):
            _run(root, _fixture_inputs(root), require_images=True)


def test_conflicting_extra_column_types_are_rejected() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        kartaview = read_manifest(inputs[1], allow_empty=False)
        mapillary["shared_extra"] = pd.Series([1, 2], dtype="Int64")
        kartaview["shared_extra"] = pd.Series(["a", "b"], dtype="string")
        write_manifest(mapillary, inputs[0], allow_empty=False)
        write_manifest(kartaview, inputs[1], allow_empty=False)
        with pytest.raises(GalleryCombineError, match="extra_column_type_conflict:shared_extra"):
            _run(root, inputs)


def test_all_null_extra_column_does_not_create_false_type_conflict() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        kartaview = read_manifest(inputs[1], allow_empty=False)
        mapillary["shared_extra"] = pd.Series([1, 2], dtype="Int64")
        kartaview["shared_extra"] = pd.Series([pd.NA, pd.NA], dtype="string")
        write_manifest(mapillary, inputs[0], allow_empty=False)
        write_manifest(kartaview, inputs[1], allow_empty=False)

        _run(root, inputs)

        combined = read_manifest(root / "combined.parquet", allow_empty=False)
        assert str(combined["shared_extra"].dtype) == "Int64"
        assert combined["shared_extra"].dropna().tolist() == [1, 2]


@pytest.mark.parametrize(
    ("image_path", "reason"),
    [
        ("../outside.jpg", "parent_traversal"),
        ("images/reference.txt", "unsupported_suffix"),
        ("images/bad\x00name.jpg", "blank_or_nul"),
    ],
)
def test_unsafe_or_non_image_paths_fail_closed(image_path: str, reason: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        mapillary.at[0, "image_path"] = image_path
        write_manifest(mapillary, inputs[0], allow_empty=False)

        with pytest.raises(GalleryCombineError, match=rf"invalid_image_path:1:{reason}=1"):
            _run(root, inputs)


def test_mixed_iso_timestamp_precision_is_not_silently_lost() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        mapillary.at[0, "captured_at"] = "2019-01-01T00:00:00.123Z"
        write_manifest(mapillary, inputs[0], allow_empty=False)

        report = _run(root, inputs)

        temporal = report["coverage"]["combined"]["temporal_seasonal_diversity"]
        assert temporal["captured_at_present"] == 6
        assert temporal["captured_at_missing"] == 0


def test_kartaview_source_url_must_identify_the_row_sequence() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        kartaview = read_manifest(inputs[1], allow_empty=False)
        kartaview.at[0, "source_url"] = "https://kartaview.org/details/a-different-sequence/0/track-info"
        write_manifest(kartaview, inputs[1], allow_empty=False)

        with pytest.raises(GalleryCombineError, match="invalid_source_provenance"):
            _run(root, inputs)


def test_require_images_rejects_absolute_path_outside_image_base() -> None:
    with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as outside_tmp:
        root = Path(tmp)
        outside = Path(outside_tmp) / "outside.jpg"
        Image.new("RGB", (64, 48)).save(outside, format="JPEG")
        inputs = _fixture_inputs(root, create_images=True)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        mapillary.at[0, "image_path"] = str(outside)
        write_manifest(mapillary, inputs[0], allow_empty=False)

        with pytest.raises(GalleryCombineError, match="image_path_outside_image_base:1"):
            _run(root, inputs, require_images=True)


def test_require_images_rejects_symlink_escape_from_image_base() -> None:
    with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as outside_tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root, create_images=True)
        outside = Path(outside_tmp) / "outside.jpg"
        Image.new("RGB", (64, 48)).save(outside, format="JPEG")
        escaped = root / "images" / "escaped.jpg"
        escaped.symlink_to(outside)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        mapillary.at[0, "image_path"] = str(escaped)
        write_manifest(mapillary, inputs[0], allow_empty=False)

        with pytest.raises(GalleryCombineError, match="image_path_outside_image_base:1"):
            _run(root, inputs, require_images=True)


def test_staging_fault_keeps_previous_complete_bundle_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        _run(root, inputs)
        before = _published_bytes(root)
        generations_before = _generation_names(root)
        original = combine_gallery._atomic_figure
        calls = 0

        def fail_during_second_map(figure: object, path: Path) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("injected staging fault")
            original(figure, path)

        monkeypatch.setattr(combine_gallery, "_atomic_figure", fail_during_second_map)
        with pytest.raises(OSError, match="injected staging fault"):
            _run(root, inputs)

        assert _published_bytes(root) == before
        assert _generation_names(root) == generations_before
        assert _staging_names(root) == set()


def test_atomic_pointer_fault_keeps_previous_complete_bundle_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        _run(root, inputs)
        before = _published_bytes(root)
        generations_before = _generation_names(root)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        mapillary.at[0, "lat"] = float(mapillary.at[0, "lat"]) + 0.001
        write_manifest(mapillary, inputs[0], allow_empty=False)
        original_replace = combine_gallery.os.replace
        injected = False

        def fail_generation_pointer(source: object, destination: object) -> None:
            nonlocal injected
            if not injected and Path(destination).name == "current":
                injected = True
                raise OSError("injected pointer fault")
            original_replace(source, destination)

        monkeypatch.setattr(combine_gallery.os, "replace", fail_generation_pointer)
        with pytest.raises(OSError, match="injected pointer fault"):
            _run(root, inputs)

        assert injected is True
        assert _published_bytes(root) == before
        assert _generation_names(root) == generations_before
        assert _staging_names(root) == set()


def test_legacy_endpoint_migration_fault_rolls_back_every_file(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        _run(root, inputs)
        before = _published_bytes(root)
        generations_before = _generation_names(root)
        for path, content in before.items():
            path.unlink()
            path.write_bytes(content)
        mapillary = read_manifest(inputs[0], allow_empty=False)
        mapillary.at[0, "lon"] = float(mapillary.at[0, "lon"]) + 0.001
        write_manifest(mapillary, inputs[0], allow_empty=False)
        original_replace_symlink = combine_gallery._replace_symlink
        injected = False

        def fail_one_endpoint(path: Path, target: str) -> None:
            nonlocal injected
            if not injected and path == root / "report.json":
                injected = True
                raise OSError("injected endpoint fault")
            original_replace_symlink(path, target)

        monkeypatch.setattr(combine_gallery, "_replace_symlink", fail_one_endpoint)
        with pytest.raises(OSError, match="injected endpoint fault"):
            _run(root, inputs)

        assert injected is True
        assert _published_bytes(root) == before
        assert all(not path.is_symlink() for path in _published_artifact_paths(root))
        assert _generation_names(root) == generations_before
        assert _staging_names(root) == set()


def test_generation_retention_is_bounded_to_current_and_one_previous() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)

        _run(root, inputs)
        assert len(_generation_names(root)) == 1
        for iteration in range(1, 5):
            mapillary = read_manifest(inputs[0], allow_empty=False)
            mapillary.at[0, "lat"] = 55.75 + iteration * 0.001
            write_manifest(mapillary, inputs[0], allow_empty=False)
            _run(root, inputs)
            generations = _generation_names(root)
            current_target = (_bundle_root(root) / "current").readlink()
            assert len(generations) == 2
            assert current_target.parts[0] == "generations"
            assert current_target.parts[1] in generations
            assert _staging_names(root) == set()


def test_successful_legacy_migration_keeps_only_legacy_previous() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        inputs = _fixture_inputs(root)
        _run(root, inputs)
        original_generation = next(iter(_generation_names(root)))
        materialized = _published_bytes(root)
        for path, content in materialized.items():
            path.unlink()
            path.write_bytes(content)

        _run(root, inputs)

        generations = _generation_names(root)
        current_name = (_bundle_root(root) / "current").readlink().parts[1]
        assert len(generations) == 2
        assert current_name.startswith("generation-")
        assert current_name in generations
        assert original_generation not in generations
        assert len([name for name in generations if name.startswith("legacy-")]) == 1
        assert _staging_names(root) == set()
