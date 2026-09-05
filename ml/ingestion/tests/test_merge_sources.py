import json
import tempfile
import unittest
import warnings
from pathlib import Path

import pandas as pd

from ml.ingestion.common import write_json
from ml.ingestion.merge_sources import deduplicate_spatial, haversine_meters, normalize_record, run, safe_filename
from ml.ingestion.schema import CANONICAL_COLUMNS, validate_manifest_schema


class MergeSourcesTests(unittest.TestCase):
    def test_normalize_record_is_canonical_and_deterministic(self) -> None:
        source = {
            "id": "m1",
            "lat": 55.75,
            "lon": 37.61,
            "timestamp": 1_704_067_200_000,
            "image_url": "https://x/y.jpg",
            "sequence_id": "seq",
            "heading": 42,
            "metadata_json": {"z": 1, "a": 2},
            "attribution": "Mapillary image by fixture",
            "source_url": "https://www.mapillary.com/app/?focus=photo&pKey=m1",
        }
        row_a = normalize_record("mapillary", source)
        row_b = normalize_record("mapillary", source)
        self.assertIsNotNone(row_a)
        self.assertEqual(row_a, row_b)
        assert row_a is not None
        self.assertTrue(set(CANONICAL_COLUMNS).issubset(row_a))
        self.assertEqual(row_a["captured_at"], "2024-01-01T00:00:00Z")
        self.assertEqual(row_a["metadata_json"], '{"a":2,"z":1}')
        self.assertEqual(row_a["sequence_id"], "seq")
        self.assertTrue(row_a["image_path"].startswith("data/raw/images/mapillary/"))

    def test_stable_id_differs_by_source(self) -> None:
        record = {"id": "same", "lat": 55.75, "lon": 37.61, "image_url": "https://x/y.jpg"}
        mapillary = normalize_record(
            "mapillary",
            record
            | {
                "attribution": "Mapillary image by fixture",
                "source_url": "https://www.mapillary.com/app/?focus=photo&pKey=same",
            },
        )
        kartaview = normalize_record(
            "kartaview",
            record
            | {
                "attribution": "© Grab and KartaView Contributors",
                "source_url": "https://kartaview.org/details/sequence/1/track-info",
            },
        )
        assert mapillary is not None and kartaview is not None
        self.assertNotEqual(mapillary["id"], kartaview["id"])

    def test_safe_filename_strips_path_chars(self) -> None:
        value = safe_filename("../unsafe\\id")
        self.assertNotIn("/", value)
        self.assertNotIn("\\", value)

    def test_haversine_small_distance(self) -> None:
        self.assertTrue(0 < haversine_meters(55.75, 37.61, 55.75001, 37.61001) < 5)

    def test_merge_does_not_delete_nearby_views_before_quality(self) -> None:
        rows = [
            {"id": "1", "lat": 55.75, "lon": 37.61},
            {"id": "2", "lat": 55.75001, "lon": 37.61001},
            {"id": "3", "lat": 55.750015, "lon": 37.610015},
        ]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            output = deduplicate_spatial(rows, radius_m=7.0, max_per_cluster=1)
        self.assertEqual(output, rows)

    def test_empty_merge_writes_schema_preserving_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mapillary = root / "mapillary.json"
            kartaview = root / "kartaview.json"
            output = root / "manifest.parquet"
            write_json(mapillary, [])
            write_json(kartaview, [])
            summary = run(mapillary, kartaview, output)
            df = pd.read_parquet(output)
            self.assertEqual(len(df), 0)
            self.assertTrue(set(CANONICAL_COLUMNS).issubset(df.columns))
            self.assertEqual(validate_manifest_schema(df), [])
            self.assertEqual(summary["spatial_pre_quality_removals"], 0)

    def test_mixed_source_timestamps_and_metadata_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(
                root / "m.json",
                [
                    {
                        "id": "m",
                        "lat": 55.75,
                        "lon": 37.61,
                        "timestamp": 1_704_067_200_000,
                        "image_url": "https://x/m",
                        "attribution": "Mapillary image by fixture",
                        "source_url": "https://www.mapillary.com/app/?focus=photo&pKey=m",
                    }
                ],
            )
            write_json(
                root / "k.json",
                [
                    {
                        "id": "k",
                        "lat": 55.76,
                        "lon": 37.62,
                        "timestamp": "2024-01-02 00:00:00",
                        "image_url": "https://x/k",
                        "attribution": "© Grab and KartaView Contributors",
                        "source_url": "https://kartaview.org/details/sequence/1/track-info",
                    }
                ],
            )
            output = root / "manifest.parquet"
            run(root / "m.json", root / "k.json", output)
            df = pd.read_parquet(output)
            self.assertEqual(set(df["captured_at"]), {"2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"})
            self.assertTrue(all(isinstance(value, str) for value in df["metadata_json"]))
            for value in df["metadata_json"]:
                json.loads(value)

    def test_multiple_live_metadata_files_are_unioned_and_deduplicated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            common = {
                "lat": 55.75,
                "lon": 37.61,
                "image_url": "https://x/image.jpg",
                "attribution": "Mapillary image by fixture",
            }
            first = root / "m1.json"
            second = root / "m2.json"
            kartaview = root / "k.json"
            write_json(
                first,
                [common | {"id": "a", "source_url": "https://www.mapillary.com/app/?pKey=a"}],
            )
            write_json(
                second,
                [
                    common | {"id": "a", "source_url": "https://www.mapillary.com/app/?pKey=a"},
                    common | {"id": "b", "source_url": "https://www.mapillary.com/app/?pKey=b"},
                ],
            )
            write_json(kartaview, [])
            summary = run([first, second], kartaview, root / "manifest.parquet")
            self.assertEqual(summary["normalized_rows"], 2)
            self.assertEqual(summary["duplicate_source_ids"], 1)
            self.assertEqual(summary["source_files"]["mapillary"], [str(first), str(second)])

    def test_missing_live_metadata_file_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            kartaview = root / "k.json"
            write_json(kartaview, [])
            with self.assertRaisesRegex(FileNotFoundError, "missing mapillary input"):
                run(root / "missing.json", kartaview, root / "manifest.parquet")

    def test_incomplete_source_attribution_is_quarantined(self) -> None:
        base = {"id": "x", "lat": 55.75, "lon": 37.61, "image_url": "https://x/y.jpg"}
        self.assertIsNone(normalize_record("mapillary", base))
        self.assertIsNone(
            normalize_record(
                "kartaview",
                base
                | {
                    "attribution": "© Grab and KartaView Contributors",
                    "source_url": "https://kartaview.org/",
                },
            )
        )


if __name__ == "__main__":
    unittest.main()
