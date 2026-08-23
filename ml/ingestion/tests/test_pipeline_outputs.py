import json
import unittest

from ml.ingestion.merge_sources import normalize_record
from ml.ingestion.schema import CANONICAL_COLUMNS


class PipelineOutputTests(unittest.TestCase):
    def test_normalize_record_has_full_canonical_contract(self) -> None:
        row = normalize_record(
            "mapillary",
            {
                "id": "img-123",
                "lat": 55.75,
                "lon": 37.61,
                "timestamp": "2024-01-01T00:00:00Z",
                "image_url": "https://example.test/img-123.jpg",
                "sequence_id": "seq-123",
                "heading": 180,
            },
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertTrue(set(CANONICAL_COLUMNS).issubset(row))
        self.assertEqual(row["city_id"], "moscow")
        self.assertEqual(row["source"], "mapillary")
        self.assertEqual(row["sequence_id"], "seq-123")
        self.assertEqual(row["heading"], 180)
        self.assertEqual(row["download_url"], "https://example.test/img-123.jpg")
        self.assertIsInstance(row["metadata_json"], str)
        json.loads(row["metadata_json"])

    def test_missing_url_is_rejected_without_crashing_batch(self) -> None:
        self.assertIsNone(normalize_record("mapillary", {"id": "m1", "lat": 55.75, "lon": 37.61}))


if __name__ == "__main__":
    unittest.main()
