from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd
from PIL import Image, ImageFilter

from ml.cleaning.deduplicate import run as dedup_run
from ml.cleaning.quality_filter import run as quality_run
from ml.cleaning.reporting import update_cleaning_report


def _create_image(path: Path, size: tuple[int, int], color: tuple[int, int, int]) -> None:
    img = Image.new("RGB", size, color=color)
    img.save(path)


def _create_sharp_image(path: Path, size: tuple[int, int]) -> None:
    img = Image.new("RGB", size, color=(128, 128, 128))
    for x in range(0, size[0], 8):
        for y in range(0, size[1], 8):
            if (x + y) % 16 == 0:
                img.putpixel((x, y), (255, 255, 255))
    img.save(path)


class TestStage3Pipeline(unittest.TestCase):
    def test_reporting_aggregation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            report = Path(tmp_dir) / "cleaning_report.json"
            update_cleaning_report(report_path=report, before_clean=100, after_clean=90)
            update_cleaning_report(report_path=report, after_quality=70)
            payload = update_cleaning_report(report_path=report, after_dedup=60)

            self.assertEqual(payload["before_clean"], 100)
            self.assertEqual(payload["after_clean"], 90)
            self.assertEqual(payload["after_quality"], 70)
            self.assertEqual(payload["after_dedup"], 60)
            self.assertEqual(payload["removed_clean"], 10)
            self.assertEqual(payload["removed_quality"], 20)
            self.assertEqual(payload["removed_dedup"], 10)

    def test_quality_filter_keeps_schema_when_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            img_small = tmp_path / "small.jpg"
            _create_image(img_small, (64, 64), (10, 10, 10))

            df = pd.DataFrame(
                [{"id": "s", "source": "x", "lat": 55.75, "lon": 37.61, "image_path": str(img_small)}]
            )
            in_manifest = tmp_path / "in.parquet"
            out_manifest = tmp_path / "out.parquet"
            report = tmp_path / "quality.json"
            pipeline_report = tmp_path / "cleaning_report.json"
            df.to_parquet(in_manifest, index=False)

            quality_run(
                input_manifest=in_manifest,
                output_manifest=out_manifest,
                report_path=report,
                min_width=224,
                min_height=224,
                min_blur_score=75.0,
                pipeline_report_path=pipeline_report,
            )

            out_df = pd.read_parquet(out_manifest)
            self.assertEqual(len(out_df), 0)
            self.assertTrue({"id", "image_path", "quality_score", "blur_score", "brightness"}.issubset(out_df.columns))

    def test_dedup_heading_exception_and_hash_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            img1 = tmp_path / "i1.jpg"
            img2 = tmp_path / "i2.jpg"
            img3 = tmp_path / "i3.jpg"

            base = Image.new("RGB", (256, 256), color=(120, 120, 120))
            base.save(img1)
            base.rotate(2).save(img2)
            base.filter(ImageFilter.GaussianBlur(radius=3)).save(img3)

            df = pd.DataFrame(
                [
                    {"id": "a", "source": "x", "lat": 55.75, "lon": 37.61, "image_path": str(img1), "quality_score": 0.95, "heading": 0},
                    {"id": "b", "source": "x", "lat": 55.75001, "lon": 37.61001, "image_path": str(img2), "quality_score": 0.90, "heading": 200},
                    {"id": "c", "source": "x", "lat": 55.75002, "lon": 37.61002, "image_path": str(img3), "quality_score": 0.85, "heading": 10},
                ]
            )

            in_manifest = tmp_path / "in_dedup.parquet"
            out_manifest = tmp_path / "out_dedup.parquet"
            report = tmp_path / "dedup.json"
            pipeline_report = tmp_path / "cleaning_report.json"
            df.to_parquet(in_manifest, index=False)

            dedup_run(
                input_manifest=in_manifest,
                output_manifest=out_manifest,
                report_path=report,
                dedup_radius_m=20.0,
                max_per_geo_point=1,
                hash_distance_threshold=0,
                heading_threshold_deg=60.0,
                pipeline_report_path=pipeline_report,
            )

            out_df = pd.read_parquet(out_manifest)
            kept_ids = set(out_df["id"].tolist())
            self.assertIn("a", kept_ids)
            self.assertIn("b", kept_ids)  # kept by heading-aware exception
            self.assertGreaterEqual(len(kept_ids), 2)

            payload = json.loads(pipeline_report.read_text(encoding="utf-8"))
            self.assertEqual(payload["after_dedup"], len(out_df))


if __name__ == "__main__":
    unittest.main()
