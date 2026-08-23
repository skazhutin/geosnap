from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from PIL import Image, ImageDraw

from ml.cleaning.clean_images import run as clean_run
from ml.cleaning.deduplicate import _cluster_geo
from ml.cleaning.deduplicate import run as dedup_run
from ml.cleaning.quality_filter import run as quality_run
from ml.ingestion.schema import CANONICAL_COLUMNS, canonical_record, manifest_dataframe, read_manifest, write_manifest


def _pattern_image(path: Path, seed: int, size: tuple[int, int] = (256, 256)) -> None:
    image = Image.new("RGB", size, color=(40 + seed, 80, 120))
    draw = ImageDraw.Draw(image)
    for value in range(0, size[0], 16):
        draw.line((value, 0, size[0] - value - 1, size[1] - 1), fill=(200, (value + seed) % 255, 30), width=3)
    image.save(path, quality=90 + seed % 5)


def _record(
    source_id: str,
    path: Path,
    *,
    lat: float = 55.75,
    lon: float = 37.61,
    quality: float = 0.5,
    heading: float | None = 0,
    sequence: str | None = "seq",
    captured_at: str | None = "2024-01-01T00:00:00Z",
) -> dict:
    return canonical_record(
        source="mapillary",
        source_image_id=source_id,
        lat=lat,
        lon=lon,
        image_path=str(path),
        quality_score=quality,
        heading=heading,
        sequence_id=sequence,
        captured_at=captured_at,
        license_name="CC BY-SA 4.0",
        attribution="Mapillary image by test",
        source_url=f"https://www.mapillary.com/app/?pKey={source_id}",
        metadata={"source_id": source_id},
        download_url=f"https://images.test/{source_id}.jpg",
    )


def _run_dedup(root: Path, records: list[dict], *, threshold: int = 0, heading_threshold: float = 45.0):
    input_path = root / "input.parquet"
    output_path = root / "output.parquet"
    write_manifest(manifest_dataframe(records), input_path)
    summary = dedup_run(
        input_manifest=input_path,
        output_manifest=output_path,
        report_path=root / "dedup.json",
        dedup_radius_m=15.0,
        max_per_geo_point=None,
        hash_distance_threshold=threshold,
        heading_threshold_deg=heading_threshold,
        pipeline_report_path=root / "pipeline.json",
    )
    return read_manifest(output_path), summary


class CleaningRegressionTests(unittest.TestCase):
    def test_a_single_link_chain_is_not_one_group(self) -> None:
        # Adjacent points are 7 m apart; endpoints are 21 m apart (> 15 m).
        frame = pd.DataFrame(
            [
                {
                    "id": chr(65 + index),
                    "lat": 55.75 + index * 7.0 / 111_320.0,
                    "lon": 37.61,
                    "quality_score": 1 - index / 10,
                }
                for index in range(4)
            ]
        )
        groups = _cluster_geo(frame, 15.0)
        self.assertGreater(len(groups), 1)
        self.assertTrue(all(len(group) < 4 for group in groups))

    def test_b_low_quality_first_high_quality_visual_duplicate_survives(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            low = root / "low.jpg"
            high = root / "high.jpg"
            _pattern_image(low, 1)
            _pattern_image(high, 2)
            records = [_record("low", low, quality=0.1), _record("high", high, quality=0.95)]
            with patch("ml.cleaning.deduplicate.compute_visual_hash", return_value=123):
                result, _ = _run_dedup(root, records, threshold=0)
            self.assertEqual(result["source_image_id"].tolist(), ["high"])

    def test_c_empty_clean_and_quality_outputs_preserve_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            missing = root / "missing.jpg"
            input_path = root / "input.parquet"
            clean_path = root / "clean.parquet"
            write_manifest(manifest_dataframe([_record("missing", missing)]), input_path)
            clean_run(input_path, clean_path, root / "clean.json", 1, root / "pipeline.json")
            empty_clean = pd.read_parquet(clean_path)
            self.assertEqual(len(empty_clean), 0)
            self.assertTrue(set(CANONICAL_COLUMNS).issubset(empty_clean.columns))

            small = root / "small.jpg"
            _pattern_image(small, 3, size=(64, 64))
            write_manifest(manifest_dataframe([_record("small", small)]), input_path)
            quality_path = root / "quality.parquet"
            quality_run(input_path, quality_path, root / "quality.json", 224, 224, 0, root / "pipeline.json")
            empty_quality = pd.read_parquet(quality_path)
            self.assertEqual(len(empty_quality), 0)
            self.assertTrue(
                {*CANONICAL_COLUMNS, "blur_score", "brightness", "exposure_score"}.issubset(empty_quality.columns)
            )

    def test_d_metadata_json_remains_deterministic_string(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            image = root / "image.jpg"
            _pattern_image(image, 4)
            input_path = root / "input.parquet"
            output_path = root / "quality.parquet"
            record = _record("meta", image)
            record["metadata_json"] = '{"z": 1, "a": 2}'
            write_manifest(manifest_dataframe([record]), input_path)
            quality_run(input_path, output_path, root / "quality.json", 64, 64, 0, root / "pipeline.json")
            loaded = read_manifest(output_path)
            self.assertEqual(loaded.at[0, "metadata_json"], '{"a":2,"z":1}')
            self.assertIsInstance(loaded.at[0, "metadata_json"], str)

    def test_e_invalid_dedup_and_quality_configuration_rejected(self) -> None:
        dummy = Path("unused.parquet")
        with self.assertRaises(ValueError):
            dedup_run(dummy, dummy, dummy, 0, None, 0, 45, dummy)
        with self.assertRaises(ValueError):
            dedup_run(dummy, dummy, dummy, 15, 0, 0, 45, dummy)
        with self.assertRaises(ValueError):
            dedup_run(dummy, dummy, dummy, 15, None, -1, 45, dummy)
        with self.assertRaises(ValueError):
            dedup_run(dummy, dummy, dummy, 15, None, 0, 181, dummy)
        with self.assertRaises(ValueError):
            quality_run(dummy, dummy, dummy, 64, 64, -1, dummy)

    def test_f_threshold_zero_removes_equal_phash_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = [root / f"{name}.jpg" for name in ("a", "b", "c")]
            for seed, path in enumerate(paths, start=1):
                _pattern_image(path, seed)
            records = [
                _record(name, path, quality=0.9 - index / 10)
                for index, (name, path) in enumerate(zip("abc", paths, strict=True))
            ]
            hashes = {"a.jpg": 0, "b.jpg": 1, "c.jpg": 0}
            with patch("ml.cleaning.deduplicate.compute_visual_hash", side_effect=lambda path: hashes[path.name]):
                result, summary = _run_dedup(root, records, threshold=0)
            self.assertEqual(set(result["source_image_id"]), {"a", "b"})
            self.assertEqual(summary["perceptual_duplicates_removed"], 1)

    def test_materially_different_headings_are_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first, second = root / "first.jpg", root / "second.jpg"
            _pattern_image(first, 5)
            _pattern_image(second, 6)
            records = [_record("north", first, heading=0), _record("south", second, heading=180)]
            with patch("ml.cleaning.deduplicate.compute_visual_hash", return_value=55):
                result, _ = _run_dedup(root, records, threshold=0)
            self.assertEqual(set(result["source_image_id"]), {"north", "south"})

    def test_same_sequence_near_identical_frame_is_thinned(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first, second = root / "first.jpg", root / "second.jpg"
            _pattern_image(first, 7)
            _pattern_image(second, 8)
            records = [
                _record("first", first, quality=0.9, captured_at="2024-01-01T00:00:00Z"),
                _record("second", second, quality=0.8, captured_at="2024-01-01T00:00:10Z"),
            ]
            with patch("ml.cleaning.deduplicate.compute_visual_hash", return_value=77):
                result, _ = _run_dedup(root, records, threshold=0)
            self.assertEqual(result["source_image_id"].tolist(), ["first"])

    def test_far_lookalikes_are_not_compared_globally(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first, second = root / "first.jpg", root / "second.jpg"
            _pattern_image(first, 9)
            _pattern_image(second, 10)
            records = [_record("first", first), _record("second", second, lat=55.80, lon=37.70)]
            with patch("ml.cleaning.deduplicate.compute_visual_hash", return_value=88):
                result, summary = _run_dedup(root, records, threshold=0)
            self.assertEqual(len(result), 2)
            self.assertEqual(summary["visual_comparisons"], 0)


if __name__ == "__main__":
    unittest.main()
