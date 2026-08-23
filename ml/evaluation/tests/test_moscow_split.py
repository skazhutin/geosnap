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
) -> dict[str, object]:
    path = image_path or root / "images" / f"{source_id}.jpg"
    if image_path is None:
        _image(path, image_seed)
    return canonical_record(
        source="kartaview",
        source_image_id=source_id,
        sequence_id=sequence,
        image_path=str(path),
        lat=lat,
        lon=lon,
        heading=90.0,
        quality_score=0.9,
        license_name="CC BY-SA 4.0",
        attribution="Fixture author",
        source_url=f"https://kartaview.org/details/{source_id}",
        metadata={"fixture": source_id},
    )


def _write_manifest(path: Path, rows: list[dict[str, object]]) -> None:
    write_manifest(manifest_dataframe(rows), path, allow_empty=False)


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

    for sequence_frame in (calibration, test):
        for _, group in sequence_frame.groupby("sequence_id"):
            coordinates = list(zip(group["lat"], group["lon"], strict=True))
            for left, first_coordinate in enumerate(coordinates):
                for second_coordinate in coordinates[left + 1 :]:
                    assert haversine_m(*first_coordinate, *second_coordinate) >= 20.0

    assert all(all(value == 0 for value in pair.values()) for pair in first["overlap_audit"].values())
    assert len(first["file_phash_ledger"]) == first["input"]["rows"]
    assert first["sample_counts"]["calibration"]["areas"] >= 1
    assert first["sample_counts"]["test"]["areas"] >= 1
    assert first["hashes"]["file_phash_ledger_sha256"] == second["hashes"]["file_phash_ledger_sha256"]
    assert first["eligibility"]["calibration_sequence_ids"] == second["eligibility"]["calibration_sequence_ids"]
    assert first["eligibility"]["test_sequence_ids"] == second["eligibility"]["test_sequence_ids"]
    assert (first_dir / "split_audit.md").read_text(encoding="utf-8").startswith("# Moscow evaluation split audit")


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
    )
    excluded = audit["excluded_counts"]
    assert excluded["query_frames_rejected_exact_file_match_with_gallery"] >= 1
    assert excluded["query_frames_rejected_phash_near_duplicate_with_gallery"] >= 1
    assert all(all(value == 0 for value in pair.values()) for pair in audit["overlap_audit"].values())
    assert audit["sample_counts"]["calibration"]["rows"] + audit["sample_counts"]["test"]["rows"] >= 1


def test_max_one_query_writes_empty_counterpart_with_identical_schema(tmp_path: Path) -> None:
    manifest = _diverse_fixture(tmp_path)
    output = tmp_path / "one"
    audit = run(manifest, output, seed=10, max_queries=1, min_query_spacing_m=0, positive_distance_m=100)
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
