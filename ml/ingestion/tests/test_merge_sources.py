import unittest

from ml.ingestion.merge_sources import deduplicate_spatial, haversine_meters, normalize_record


class MergeSourcesTests(unittest.TestCase):
    def test_normalize_record_contains_download_url(self) -> None:
        row = normalize_record(
            "mapillary",
            {"id": "m1", "lat": 55.75, "lon": 37.61, "timestamp": "2024-01-01", "image_url": "https://x/y.jpg"},
        )
        assert row is not None
        self.assertEqual(row["download_url"], "https://x/y.jpg")

    def test_haversine_small_distance(self) -> None:
        dist = haversine_meters(55.75, 37.61, 55.75001, 37.61001)
        self.assertTrue(0 < dist < 5)

    def test_deduplicate_spatial_keeps_max_two_per_cluster(self) -> None:
        rows = [
            {"id": "1", "lat": 55.75, "lon": 37.61},
            {"id": "2", "lat": 55.75001, "lon": 37.61001},
            {"id": "3", "lat": 55.750015, "lon": 37.610015},
            {"id": "4", "lat": 55.7600, "lon": 37.6200},
        ]
        out = deduplicate_spatial(rows, radius_m=7.0, max_per_cluster=2)
        kept_ids = {r["id"] for r in out}
        self.assertIn("4", kept_ids)
        self.assertEqual(len(out), 3)


if __name__ == "__main__":
    unittest.main()
