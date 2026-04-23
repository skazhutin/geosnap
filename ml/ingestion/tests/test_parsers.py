import unittest

from ml.ingestion.parsers import parse_kartaview_item, parse_mapillary_item


class ParserTests(unittest.TestCase):
    def test_parse_mapillary_item(self) -> None:
        parsed = parse_mapillary_item(
            {
                "id": "123",
                "captured_at": "2025-01-01T00:00:00Z",
                "geometry": {"coordinates": [37.61, 55.75]},
                "thumb_2048_url": "https://example.com/123.jpg",
                "sequence": {"id": "seq-1"},
            }
        )
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["id"], "123")
        self.assertEqual(parsed["lat"], 55.75)
        self.assertEqual(parsed["lon"], 37.61)
        self.assertEqual(parsed["sequence_id"], "seq-1")

    def test_parse_kartaview_item_fallback_fields(self) -> None:
        parsed = parse_kartaview_item(
            {
                "photoId": "kv-1",
                "latitude": 55.76,
                "longitude": 37.62,
                "shotDate": "2025-01-01",
                "fileurlProc": "https://example.com/kv-1.jpg",
            }
        )
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["id"], "kv-1")
        self.assertEqual(parsed["lat"], 55.76)
        self.assertEqual(parsed["lon"], 37.62)

    def test_parse_kartaview_item_keeps_zero_values(self) -> None:
        parsed = parse_kartaview_item(
            {
                "id": "0",
                "lat": 0.0,
                "lon": 0.0,
                "timestamp": "2025-01-01",
                "url": "https://example.com/zero.jpg",
            }
        )
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["lat"], 0.0)
        self.assertEqual(parsed["lon"], 0.0)


if __name__ == "__main__":
    unittest.main()
