import json
import unittest

from ml.ingestion.parsers import parse_kartaview_item, parse_mapillary_item


class ParserTests(unittest.TestCase):
    def test_parse_current_mapillary_fields(self) -> None:
        parsed = parse_mapillary_item(
            {
                "id": "123",
                "captured_at": 1_735_689_600_000,
                "computed_geometry": {"coordinates": [37.61, 55.75]},
                "thumb_original_url": "https://example.test/123.jpg",
                "sequence": {"id": "seq-1"},
                "computed_compass_angle": 361.5,
                "creator": {"username": "contributor"},
            }
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["id"], "123")
        self.assertEqual(parsed["lat"], 55.75)
        self.assertEqual(parsed["lon"], 37.61)
        self.assertEqual(parsed["sequence_id"], "seq-1")
        self.assertEqual(parsed["heading"], 1.5)
        self.assertEqual(parsed["captured_at"], "2025-01-01T00:00:00Z")
        self.assertIn("contributor", parsed["attribution"])
        self.assertIsInstance(parsed["metadata_json"], str)
        json.loads(parsed["metadata_json"])

    def test_parse_live_shape_kartaview_fields(self) -> None:
        parsed = parse_kartaview_item(
            {
                "id": "kv-1",
                "lat": 55.76,
                "lng": 37.62,
                "shotDate": "2025-01-01 12:30:00",
                "fileurlProc": "https://example.test/kv-1.jpg",
                "heading": 275.0,
                "sequenceId": "sequence-7",
                "sequenceIndex": 4,
            }
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["sequence_id"], "sequence-7")
        self.assertEqual(parsed["heading"], 275.0)
        self.assertEqual(parsed["captured_at"], "2025-01-01T12:30:00Z")
        self.assertIn("Grab", parsed["attribution"])
        self.assertIn("sequence-7", parsed["source_url"])

    def test_parse_kartaview_prefers_official_cdn_proxy(self) -> None:
        parsed = parse_kartaview_item(
            {
                "id": "kv-1",
                "lat": 55.76,
                "lng": 37.62,
                "fileurlProc": "https://storage7.openstreetcam.org/unavailable.jpg",
                "imageProcUrl": "https://cdn.kartaview.org/proxy-token",
            }
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["download_url"], "https://cdn.kartaview.org/proxy-token")

    def test_parse_kartaview_keeps_zero_values(self) -> None:
        parsed = parse_kartaview_item(
            {"id": "0", "lat": 0.0, "lng": 0.0, "timestamp": "2025-01-01", "url": "https://example.test/zero.jpg"}
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["lat"], 0.0)
        self.assertEqual(parsed["lon"], 0.0)

    def test_invalid_coordinates_and_missing_urls_are_rejected(self) -> None:
        self.assertIsNone(parse_mapillary_item({"id": "x", "geometry": {"coordinates": [999, 55]}}))
        self.assertIsNone(parse_kartaview_item({"id": "x", "lat": "bad", "lng": 37.6, "fileurl": "https://x"}))
        self.assertIsNone(parse_kartaview_item({"id": "x", "lat": 55.7, "lng": 37.6}))

    def test_mapillary_without_creator_username_is_quarantined(self) -> None:
        base = {
            "id": "123",
            "geometry": {"coordinates": [37.61, 55.75]},
            "thumb_1024_url": "https://example.test/123.jpg",
        }
        self.assertIsNone(parse_mapillary_item(base))
        self.assertIsNone(parse_mapillary_item(base | {"creator": {"id": "42"}}))


if __name__ == "__main__":
    unittest.main()
