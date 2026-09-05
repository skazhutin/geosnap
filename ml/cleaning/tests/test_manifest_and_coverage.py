from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from ml.cleaning.build_final_manifest import run as final_run
from ml.cleaning.check_dataset import nearest_reference_distances
from ml.cleaning.check_dataset import run as coverage_run
from ml.enrichment.h3_assign import run as h3_run
from ml.ingestion.schema import CANONICAL_COLUMNS, canonical_record, manifest_dataframe, read_manifest, write_manifest


def _record(source_id: str, lat: float, lon: float) -> dict:
    return canonical_record(
        source="kartaview",
        source_image_id=source_id,
        lat=lat,
        lon=lon,
        image_path=f"missing/{source_id}.jpg",
        sequence_id="seq",
        captured_at="2024-01-01T00:00:00Z",
        heading=90,
        quality_score=0.8,
        license_name="CC BY-SA 4.0",
        attribution="© Grab and KartaView Contributors",
        source_url="https://kartaview.org/",
        metadata={"id": source_id},
    )


class ManifestAndCoverageTests(unittest.TestCase):
    def test_h3_validates_resolutions_and_preserves_canonical_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "input.parquet"
            output_path = root / "h3.parquet"
            write_manifest(manifest_dataframe([_record("a", 55.75, 37.61)]), input_path)
            h3_run(input_path, output_path, 6, 9)
            result = read_manifest(output_path)
            self.assertTrue(set(CANONICAL_COLUMNS).issubset(result.columns))
            self.assertTrue(result.at[0, "h3_coarse"])
            self.assertTrue(result.at[0, "h3_fine"])
            with self.assertRaises(ValueError):
                h3_run(input_path, output_path, -1, 9)
            with self.assertRaises(ValueError):
                h3_run(input_path, output_path, 9, 6)

    def test_final_manifest_retains_provenance_and_enrichment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "input.parquet"
            output_path = root / "final.parquet"
            frame = manifest_dataframe([_record("a", 55.75, 37.61)])
            frame["h3_coarse"] = "8611aa"
            frame["h3_fine"] = "8911aa"
            frame["blur_score"] = 123.0
            write_manifest(frame, input_path)
            final_run(input_path, output_path)
            result = read_manifest(output_path)
            self.assertTrue(set(CANONICAL_COLUMNS).issubset(result.columns))
            for column in (
                "source_image_id",
                "sequence_id",
                "license",
                "attribution",
                "source_url",
                "h3_fine",
                "blur_score",
            ):
                self.assertIn(column, result.columns)

    def test_kd_tree_nearest_distances_use_full_data(self) -> None:
        frame = pd.DataFrame(
            [
                {"lat": 55.75, "lon": 37.61},
                {"lat": 55.75 + 10 / 111_320, "lon": 37.61},
                {"lat": 55.75 + 30 / 111_320, "lon": 37.61},
            ]
        )
        distances = nearest_reference_distances(frame)
        self.assertEqual(len(distances), 3)
        self.assertAlmostEqual(float(distances[0]), 10, delta=0.1)
        self.assertAlmostEqual(float(distances[1]), 10, delta=0.1)
        self.assertAlmostEqual(float(distances[2]), 20, delta=0.1)

    def test_empty_coverage_report_is_honest_and_safe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "empty.parquet"
            write_manifest(manifest_dataframe([]), manifest)
            with patch("ml.cleaning.check_dataset.build_visualizations", return_value="ok"):
                summary = coverage_run(
                    manifest,
                    root / "report.json",
                    root / "scatter.png",
                    root / "preview.jpg",
                )
            self.assertTrue(summary["empty_gallery"])
            self.assertEqual(summary["total_references"], 0)
            self.assertIsNone(summary["nearest_reference_distance_m"]["p50"])
            self.assertEqual(summary["fixed_moscow_grid"]["occupied_grid_cells"], 0)
            self.assertTrue((root / "report.md").exists())
            self.assertTrue((root / "preview.jpg").exists())


if __name__ == "__main__":
    unittest.main()
