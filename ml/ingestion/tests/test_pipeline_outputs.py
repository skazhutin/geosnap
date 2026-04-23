import unittest

from ml.ingestion.merge_sources import deduplicate_spatial, normalize_record


REQUIRED_MANIFEST_KEYS = {
    "id",
    "source",
    "source_image_id",
    "lat",
    "lon",
    "captured_at",
    "heading",
    "image_path",
    "thumb_path",
    "license",
    "attribution",
    "metadata_json",
    "download_url",
}


class PipelineOutputTests(unittest.TestCase):
    def test_normalize_record_has_required_output_schema(self) -> None:
        row = normalize_record(
            "mapillary",
            {
                "id": "img-123",
                "lat": 55.75,
                "lon": 37.61,
                "timestamp": "2024-01-01T00:00:00Z",
                "image_url": "https://example.com/img-123.jpg",
            },
        )
        self.assertIsNotNone(row)
        self.assertTrue(REQUIRED_MANIFEST_KEYS.issubset(set(row.keys())))
        self.assertEqual(row["source"], "mapillary")
        self.assertTrue(str(row["image_path"]).startswith("data/raw/images/mapillary/"))
        self.assertEqual(row["download_url"], "https://example.com/img-123.jpg")

    def test_cross_source_dedup_keeps_limited_nearby_records(self) -> None:
        m1 = normalize_record(
            "mapillary",
            {"id": "m1", "lat": 55.750000, "lon": 37.610000, "timestamp": "2024-01-01", "image_url": "https://m1"},
        )
        k1 = normalize_record(
            "kartaview",
            {"id": "k1", "lat": 55.750010, "lon": 37.610010, "timestamp": "2024-01-01", "image_url": "https://k1"},
        )
        k2 = normalize_record(
            "kartaview",
            {"id": "k2", "lat": 55.750015, "lon": 37.610015, "timestamp": "2024-01-01", "image_url": "https://k2"},
        )
        self.assertIsNotNone(m1)
        self.assertIsNotNone(k1)
        self.assertIsNotNone(k2)

        deduped = deduplicate_spatial([m1, k1, k2], radius_m=7.0, max_per_cluster=2)
        self.assertEqual(len(deduped), 2)


if __name__ == "__main__":
    unittest.main()
