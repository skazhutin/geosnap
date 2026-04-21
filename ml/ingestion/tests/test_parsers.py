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
        assert parsed is not None
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
        assert parsed is not None
        self.assertEqual(parsed["id"], "kv-1")
        self.assertEqual(parsed["lat"], 55.76)
        self.assertEqual(parsed["lon"], 37.62)


if __name__ == "__main__":
    unittest.main()
