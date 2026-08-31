from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
from PIL import Image

from ml.evaluation.moscow_split import MoscowSplitError, run
from ml.ingestion.schema import canonical_record, manifest_dataframe, read_manifest, write_manifest
from ml.localization.geo import haversine_m


def _image(path: Path, seed: int, *, quality: int = 91) -> None:
    generator = np.random.default_rng(seed)
    pixels = generator.integers(0, 256, size=(96, 128, 3), dtype=np.uint8)
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(pixels, mode="RGB").save(path, format="JPEG", quality=quality)


def _record(
    root: Path,
    *,
    source_id: str,
    sequence: str | None,
    lat: float,
    lon: float,
    image_seed: int,
    image_path: Path | None = None,
    source: str = "kartaview",
) -> dict[str, object]:
    path = image_path or root / "images" / f"{source_id}.jpg"
    if image_path is None:
        _image(path, image_seed)
    return canonical_record(
        source=source,
        source_image_id=source_id,
        sequence_id=sequence,
        image_path=str(path),
        lat=lat,
        lon=lon,
        heading=90.0,
        quality_score=0.9,
        license_name="CC BY-SA 4.0",
        attribution="Fixture author",
        source_url=f"https://example.test/{source}/{source_id}",
        metadata={"fixture": source_id},
    )


def _write_manifest(path: Path, rows: list[dict[str, object]]) -> None:
    write_manifest(manifest_dataframe(rows), path, allow_empty=False)


def _single_split_gates() -> dict[str, int]:
    return {
        "minimum_queries_per_split": 0,
        "minimum_sequences_per_split": 0,
        "minimum_areas_per_split": 0,
    }


def _diverse_fixture(tmp_path: Path) -> Path:
    rows: list[dict[str, object]] = []
    image_seed = 1
    # Two separated H3 neighborhoods. Each contains three traversals, so one
    # sequence per neighborhood can be held out while a gallery positive stays.
    for area, (base_lat, base_lon) in {
        "a": (55.7500, 37.6100),
        "b": (55.7650, 37.6400),
    }.items():
        for sequence_number in range(3):
            for frame_number in range(2):
                rows.append(
                    _record(
                        tmp_path,
                        source_id=f"{area}-{sequence_number}-{frame_number}",
                        sequence=f"{area}-sequence-{sequence_number}",
                        lat=base_lat + frame_number * 0.00030 + sequence_number * 0.00001,
                        lon=base_lon + sequence_number * 0.00001,
                        image_seed=image_seed,
                    )
                )
                image_seed += 1
    manifest = tmp_path / "canonical.parquet"
    _write_manifest(manifest, rows)
    return manifest


def _query_union_duplicate_fixture(tmp_path: Path) -> Path:
    rows: list[dict[str, object]] = []
    for sequence_number in range(2):
        sequence = f"sequence-{sequence_number}"
        exact = tmp_path / "images" / f"exact-{sequence_number}.jpg"
        _image(exact, 200 + sequence_number)
        pixels = np.random.default_rng(300 + sequence_number).integers(
            0,
            256,
            size=(96, 128, 3),
            dtype=np.uint8,
        )
        near_a = tmp_path / "images" / f"near-{sequence_number}-a.jpg"
        near_b = tmp_path / "images" / f"near-{sequence_number}-b.jpg"
        Image.fromarray(pixels, mode="RGB").save(near_a, format="JPEG", quality=95)
        Image.fromarray(pixels, mode="RGB").save(near_b, format="JPEG", quality=75)
        paths = (exact, exact, near_a, near_b)
        for frame_number, image_path in enumerate(paths):
            rows.append(
                _record(
                    tmp_path,
                    source_id=f"{sequence_number}-{frame_number}",
                    sequence=sequence,
                    lat=55.75 + frame_number * 0.0001,
                    lon=37.61 + sequence_number * 0.00001,
                    image_seed=0,
                    image_path=image_path,
                )
            )
    manifest = tmp_path / "query-union-duplicates.parquet"
    _write_manifest(manifest, rows)
    return manifest


def test_builds_deterministic_sequence_held_out_split_and_complete_audit(tmp_path: Path) -> None:
    manifest = _diverse_fixture(tmp_path)
    first_dir = tmp_path / "split-a"
    second_dir = tmp_path / "split-b"
    first = run(
        manifest,
        first_dir,
        seed=77,
        max_queries=4,
        min_query_spacing_m=20.0,
        positive_distance_m=100.0,
    )
    second = run(
        manifest,
        second_dir,
        seed=77,
        max_queries=4,
        min_query_spacing_m=20.0,
        positive_distance_m=100.0,
    )

    gallery = read_manifest(first_dir / "gallery.parquet", allow_empty=False)
    calibration = read_manifest(first_dir / "calibration_queries.parquet", allow_empty=True)
    test = read_manifest(first_dir / "test_queries.parquet", allow_empty=True)
    query = pd.concat([calibration, test], ignore_index=True)
    assert len(query) == 4
    assert len(calibration) > 0
    assert len(test) > 0
    assert set(gallery["sequence_id"]).isdisjoint(set(query["sequence_id"]))
    assert set(calibration["sequence_id"]).isdisjoint(set(test["sequence_id"]))
    assert query["nearest_gallery_distance_m"].max() <= 100.0

    coordinates = list(zip(query["lat"], query["lon"], strict=True))
    for left, first_coordinate in enumerate(coordinates):
        for second_coordinate in coordinates[left + 1 :]:
            assert haversine_m(*first_coordinate, *second_coordinate) >= 20.0
    assert set(calibration["evaluation_area_h3"]).isdisjoint(set(test["evaluation_area_h3"]))
    assert set(calibration["evaluation_geo_group_id"]).isdisjoint(set(test["evaluation_geo_group_id"]))

    assert all(all(value == 0 for value in pair.values()) for pair in first["overlap_audit"].values())
    assert len(first["file_phash_ledger"]) == first["input"]["rows"]
    assert first["sample_counts"]["calibration"]["areas"] >= 1
    assert first["sample_counts"]["test"]["areas"] >= 1
    assert first["coverage_gate"]["passed"] is True
    assert not any(
        first["query_union_independence_audit"][key]
        for key in (
            "exact_file_duplicate_pairs",
            "phash_near_duplicate_pairs",
            "query_spacing_violation_pairs",
            "calibration_test_area_h3_overlap",
            "calibration_test_geo_group_overlap",
            "calibration_test_embargo_violation_pairs",
        )
    )
    assert first["hashes"]["file_phash_ledger_sha256"] == second["hashes"]["file_phash_ledger_sha256"]
    assert first["eligibility"]["calibration_sequence_ids"] == second["eligibility"]["calibration_sequence_ids"]
    assert first["eligibility"]["test_sequence_ids"] == second["eligibility"]["test_sequence_ids"]
    assert (first_dir / "split_audit.md").read_text(encoding="utf-8").startswith("# Moscow evaluation split audit")
    assert (first_dir / "bundle_manifest.json").is_file()


def test_exact_and_perceptual_gallery_leaks_are_rejected(tmp_path: Path) -> None:
    exact_path = tmp_path / "images" / "exact.jpg"
    _image(exact_path, 100)
    near_a = tmp_path / "images" / "near-a.jpg"
    near_b = tmp_path / "images" / "near-b.jpg"
    # Re-encoding identical pixels changes the byte SHA-256 while preserving
    # the pHash, exercising the perceptual rather than exact rejection path.
    pixels = np.random.default_rng(101).integers(0, 256, size=(96, 128, 3), dtype=np.uint8)
    Image.fromarray(pixels, mode="RGB").save(near_a, format="JPEG", quality=95)
    Image.fromarray(pixels, mode="RGB").save(near_b, format="JPEG", quality=75)
    unique_a = tmp_path / "images" / "unique-a.jpg"
    unique_b = tmp_path / "images" / "unique-b.jpg"
    _image(unique_a, 102)
    _image(unique_b, 103)

    rows = [
        _record(
            tmp_path,
            source_id="a-exact",
            sequence="sequence-a",
            lat=55.75,
            lon=37.61,
            image_seed=0,
            image_path=exact_path,
        ),
        _record(
            tmp_path,
            source_id="a-near",
            sequence="sequence-a",
            lat=55.7502,
            lon=37.61,
            image_seed=0,
            image_path=near_a,
        ),
        _record(
            tmp_path,
            source_id="a-unique",
            sequence="sequence-a",
            lat=55.7504,
            lon=37.61,
            image_seed=0,
            image_path=unique_a,
        ),
        _record(
            tmp_path,
            source_id="b-exact",
            sequence="sequence-b",
            lat=55.75001,
            lon=37.61001,
            image_seed=0,
            image_path=exact_path,
        ),
        _record(
            tmp_path,
            source_id="b-near",
            sequence="sequence-b",
            lat=55.75021,
            lon=37.61001,
            image_seed=0,
            image_path=near_b,
        ),
        _record(
            tmp_path,
            source_id="b-unique",
            sequence="sequence-b",
            lat=55.75041,
            lon=37.61001,
            image_seed=0,
            image_path=unique_b,
        ),
    ]
    manifest = tmp_path / "leaky.parquet"
    _write_manifest(manifest, rows)
    audit = run(
        manifest,
        tmp_path / "split",
        seed=3,
        max_queries=10,
        min_query_spacing_m=0,
        positive_distance_m=100,
        **_single_split_gates(),
    )
    excluded = audit["excluded_counts"]
    assert excluded["query_frames_rejected_exact_file_match_with_gallery"] >= 1
    assert excluded["query_frames_rejected_phash_near_duplicate_with_gallery"] >= 1
    assert all(all(value == 0 for value in pair.values()) for pair in audit["overlap_audit"].values())
    assert audit["sample_counts"]["calibration"]["rows"] + audit["sample_counts"]["test"]["rows"] >= 1


def test_max_one_query_writes_empty_counterpart_with_identical_schema(tmp_path: Path) -> None:
    manifest = _diverse_fixture(tmp_path)
    output = tmp_path / "one"
    audit = run(
        manifest,
        output,
        seed=10,
        max_queries=1,
        min_query_spacing_m=0,
        positive_distance_m=100,
        **_single_split_gates(),
    )
    calibration = pd.read_parquet(output / "calibration_queries.parquet")
    test = pd.read_parquet(output / "test_queries.parquet")
    assert len(calibration) + len(test) == 1
    assert list(calibration.columns) == list(test.columns)
    assert pq.read_schema(output / "calibration_queries.parquet").equals(
        pq.read_schema(output / "test_queries.parquet")
    )
    assert audit["sample_counts"]["calibration"]["rows"] + audit["sample_counts"]["test"]["rows"] == 1


def test_fails_closed_for_unstable_ids_and_missing_images(tmp_path: Path) -> None:
    image = tmp_path / "image.jpg"
    _image(image, 1)
    rows = [
        _record(
            tmp_path,
            source_id="a",
            sequence="sequence-a",
            lat=55.75,
            lon=37.61,
            image_seed=0,
            image_path=image,
        ),
        _record(
            tmp_path,
            source_id="b",
            sequence="sequence-b",
            lat=55.75001,
            lon=37.61001,
            image_seed=2,
        ),
    ]
    unstable = [dict(row) for row in rows]
    unstable[0]["id"] = "manual-id"
    unstable_path = tmp_path / "unstable.parquet"
    _write_manifest(unstable_path, unstable)
    with pytest.raises(MoscowSplitError, match="non-stable reference IDs"):
        run(unstable_path, tmp_path / "unstable-output")

    missing = [dict(row) for row in rows]
    missing[0]["image_path"] = str(tmp_path / "does-not-exist.jpg")
    missing_path = tmp_path / "missing.parquet"
    _write_manifest(missing_path, missing)
    with pytest.raises(MoscowSplitError, match="cannot fingerprint"):
        run(missing_path, tmp_path / "missing-output")


def test_fails_when_no_multi_sequence_neighborhood_has_a_positive(tmp_path: Path) -> None:
    rows = [
        _record(
            tmp_path,
            source_id="a",
            sequence="only-sequence",
            lat=55.75,
            lon=37.61,
            image_seed=1,
        ),
        _record(
            tmp_path,
            source_id="b",
            sequence=None,
            lat=55.75001,
            lon=37.61001,
            image_seed=2,
        ),
    ]
    manifest = tmp_path / "undefensible.parquet"
    _write_manifest(manifest, rows)
    with pytest.raises(MoscowSplitError, match="no query candidates"):
        run(manifest, tmp_path / "output")
    assert not (tmp_path / "output" / "gallery.parquet").exists()


def test_json_audit_records_artifact_hashes_and_ledger(tmp_path: Path) -> None:
    manifest = _diverse_fixture(tmp_path)
    output = tmp_path / "audited"
    run(manifest, output, seed=42, max_queries=3, min_query_spacing_m=0, positive_distance_m=100)
    audit = json.loads((output / "split_audit.json").read_text(encoding="utf-8"))
    assert set(audit["hashes"]) == {
        "input_manifest_sha256",
        "gallery_parquet_sha256",
        "calibration_queries_parquet_sha256",
        "test_queries_parquet_sha256",
        "file_phash_ledger_sha256",
    }
    assert all(len(value) == 64 for value in audit["hashes"].values())
    assert all(
        len(row["file_sha256"]) == 64 and len(row["perceptual_hash"]) == 16 for row in audit["file_phash_ledger"]
    )


def test_sequence_identity_is_namespaced_by_provider(tmp_path: Path) -> None:
    rows = [
        _record(
            tmp_path,
            source="kartaview",
            source_id="shared-karta",
            sequence="shared",
            lat=55.75,
            lon=37.61,
            image_seed=11,
        ),
        _record(
            tmp_path,
            source="mapillary",
            source_id="shared-mapillary",
            sequence="shared",
            lat=55.75001,
            lon=37.61001,
            image_seed=12,
        ),
    ]
    manifest = tmp_path / "provider-sequences.parquet"
    _write_manifest(manifest, rows)
    output = tmp_path / "provider-split"
    audit = run(
        manifest,
        output,
        max_queries=1,
        min_query_spacing_m=0,
        positive_distance_m=100,
        **_single_split_gates(),
    )

    gallery = pd.read_parquet(output / "gallery.parquet")
    query = pd.concat(
        [
            pd.read_parquet(output / "calibration_queries.parquet"),
            pd.read_parquet(output / "test_queries.parquet"),
        ],
        ignore_index=True,
    )
    assert len(gallery) == len(query) == 1
    assert set(gallery["sequence_id"]) == set(query["sequence_id"]) == {"shared"}
    gallery_keys = set(zip(gallery["source"], gallery["sequence_id"], strict=True))
    query_keys = set(zip(query["source"], query["sequence_id"], strict=True))
    assert gallery_keys.isdisjoint(query_keys)
    assert audit["eligibility"]["eligible_sequences_before_holdout"] == 2
    assert audit["overlap_audit"]["gallery__calibration"]["sequence_id_overlap"] == 0


def test_initial_holdout_pool_is_bounded_and_builds_positive_graph_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Large sequence counts must not trigger one gallery rebuild per sequence."""

    import ml.evaluation.moscow_split as split_module

    sequence_count = 40
    frame = pd.DataFrame(
        {
            "source": ["mapillary"] * sequence_count,
            "sequence_id": [f"sequence-{number:03d}" for number in range(sequence_count)],
            # Every sequence has a cross-sequence positive within 100 m.
            "lat": [55.75] * sequence_count,
            "lon": [37.61] * sequence_count,
            "evaluation_area_h3": ["fixture-area"] * sequence_count,
        }
    )
    original_spatial_index = split_module._SpatialIndex
    spatial_index_builds = 0

    class CountingSpatialIndex(original_spatial_index):
        def __init__(self, *args: object, **kwargs: object) -> None:
            nonlocal spatial_index_builds
            spatial_index_builds += 1
            super().__init__(*args, **kwargs)

    def fail_if_legacy_gallery_rebuild_is_used(*args: object, **kwargs: object) -> None:
        pytest.fail("held-out selection must not call _query_distances for every sequence")

    monkeypatch.setattr(split_module, "_SpatialIndex", CountingSpatialIndex)
    monkeypatch.setattr(split_module, "_query_distances", fail_if_legacy_gallery_rebuild_is_used)
    chosen, diagnostics = split_module._choose_held_out_sequences(
        frame,
        eligible_indices=set(range(sequence_count)),
        usable_indices=list(range(sequence_count)),
        seed=11,
        positive_distance_m=100.0,
        max_queries=3,
        minimum_gallery_rows=1,
        minimum_gallery_sequences=1,
        minimum_gallery_areas=1,
        minimum_gallery_fraction=0.5,
        holdout_candidate_multiplier=2,
    )

    assert spatial_index_builds == 1
    assert len(chosen) == 6
    assert len(set(chosen)) == 6
    assert diagnostics == {
        "ordered_eligible_sequence_count": sequence_count,
        "candidate_multiplier": 2,
        "candidate_sequence_cap": 6,
        "candidate_sequences_selected": 6,
        "candidate_rows_selected": 6,
        "candidate_cap_reached": True,
        "candidate_sequences_rejected_no_gallery_positive": 0,
        "candidate_sequences_rejected_gallery_reserve": 0,
    }


def test_global_spacing_and_geographic_embargo_apply_to_query_union(tmp_path: Path) -> None:
    manifest = _diverse_fixture(tmp_path)
    output = tmp_path / "independent"
    audit = run(
        manifest,
        output,
        seed=77,
        max_queries=8,
        min_query_spacing_m=20,
        positive_distance_m=100,
    )
    calibration = pd.read_parquet(output / "calibration_queries.parquet")
    test = pd.read_parquet(output / "test_queries.parquet")
    query = pd.concat([calibration, test], ignore_index=True)
    for left in range(len(query)):
        for right in range(left + 1, len(query)):
            assert (
                haversine_m(
                    float(query.at[left, "lat"]),
                    float(query.at[left, "lon"]),
                    float(query.at[right, "lat"]),
                    float(query.at[right, "lon"]),
                )
                >= 20
            )
    assert set(calibration["evaluation_area_h3"]).isdisjoint(set(test["evaluation_area_h3"]))
    assert audit["query_union_independence_audit"]["minimum_calibration_test_distance_m"] >= 100
    assert audit["excluded_counts"]["query_frames_removed_by_min_spacing"] >= 1


def test_exact_and_phash_duplicates_are_removed_from_entire_query_union(tmp_path: Path) -> None:
    manifest = _query_union_duplicate_fixture(tmp_path)
    output = tmp_path / "deduplicated-query"
    audit = run(
        manifest,
        output,
        seed=3,
        max_queries=10,
        min_query_spacing_m=0,
        positive_distance_m=100,
        **_single_split_gates(),
    )
    query = pd.concat(
        [
            pd.read_parquet(output / "calibration_queries.parquet"),
            pd.read_parquet(output / "test_queries.parquet"),
        ],
        ignore_index=True,
    )
    assert query["file_sha256"].nunique() == len(query)
    for left in range(len(query)):
        for right in range(left + 1, len(query)):
            distance = (
                int(str(query.at[left, "perceptual_hash"]), 16) ^ int(str(query.at[right, "perceptual_hash"]), 16)
            ).bit_count()
            assert distance > 4
    excluded = audit["excluded_counts"]
    assert excluded["query_frames_rejected_exact_file_match_within_query_split"] >= 1
    assert excluded["query_frames_rejected_phash_near_duplicate_within_query_split"] >= 1
    assert audit["query_union_independence_audit"]["exact_file_duplicate_pairs"] == 0
    assert audit["query_union_independence_audit"]["phash_near_duplicate_pairs"] == 0


def test_coverage_gates_fail_before_publication(tmp_path: Path) -> None:
    manifest = _diverse_fixture(tmp_path)
    output = tmp_path / "insufficient"
    with pytest.raises(MoscowSplitError, match="insufficient coverage.*calibration_rows"):
        run(
            manifest,
            output,
            seed=77,
            max_queries=8,
            min_query_spacing_m=20,
            positive_distance_m=100,
            minimum_queries_per_split=4,
        )
    assert not output.exists()

    reserve_output = tmp_path / "reserve-insufficient"
    with pytest.raises(MoscowSplitError, match="configured gallery reserve"):
        run(
            manifest,
            reserve_output,
            max_queries=2,
            minimum_gallery_rows=12,
        )
    assert not reserve_output.exists()


def test_missing_sequence_rows_are_excluded_and_audited(tmp_path: Path) -> None:
    source_manifest = _diverse_fixture(tmp_path)
    rows = pd.read_parquet(source_manifest).to_dict(orient="records")
    rows.append(
        _record(
            tmp_path,
            source_id="missing-sequence",
            sequence=None,
            lat=55.75,
            lon=37.61,
            image_seed=999,
        )
    )
    manifest = tmp_path / "with-missing-sequence.parquet"
    _write_manifest(manifest, rows)
    output = tmp_path / "missing-sequence-split"
    audit = run(manifest, output, seed=77, max_queries=4)

    assert audit["excluded_counts"]["rows_missing_sequence_id"] == 1
    missing = [row for row in audit["file_phash_ledger"] if row["disposition"] == "excluded_missing_sequence_id"]
    assert [row["source_image_id"] for row in missing] == ["missing-sequence"]


def test_failed_rebuild_preserves_published_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import ml.evaluation.moscow_split as split_module

    manifest = _diverse_fixture(tmp_path)
    output = tmp_path / "atomic"
    first = run(manifest, output, seed=77, max_queries=4)
    artifact_names = {
        "gallery.parquet",
        "calibration_queries.parquet",
        "test_queries.parquet",
        "split_audit.json",
        "split_audit.md",
        "bundle_manifest.json",
    }
    before = {name: (output / name).read_bytes() for name in artifact_names}
    reused = run(manifest, output, seed=77, max_queries=4)
    assert reused["bundle"]["fingerprint"] == first["bundle"]["fingerprint"]
    assert {name: (output / name).read_bytes() for name in artifact_names} == before
    with pytest.raises(MoscowSplitError, match="different or unverifiable split bundle"):
        run(manifest, output, seed=78, max_queries=4)
    assert {name: (output / name).read_bytes() for name in artifact_names} == before

    def fail_markdown(_path: Path, _audit: object) -> None:
        raise RuntimeError("injected markdown failure")

    monkeypatch.setattr(split_module, "_write_markdown", fail_markdown)
    with pytest.raises(RuntimeError, match="injected markdown failure"):
        run(manifest, output, seed=78, max_queries=4, replace_existing=True)

    assert {name: (output / name).read_bytes() for name in artifact_names} == before
    assert not list(tmp_path.glob(".atomic.*"))
    persisted = json.loads((output / "split_audit.json").read_text(encoding="utf-8"))
    assert persisted["bundle"]["fingerprint"] == first["bundle"]["fingerprint"]
