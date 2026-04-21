import tempfile
import unittest
from pathlib import Path

from ml.ingestion.common import download_file, write_json, read_json


class CommonTests(unittest.TestCase):
    def test_json_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.json"
            payload = {"a": 1, "b": [1, 2, 3]}
            write_json(path, payload)
            loaded = read_json(path, default={})
            self.assertEqual(loaded, payload)

    def test_download_file_skips_existing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "file.jpg"
            destination.write_bytes(b"x" * 10_000)
            ok = download_file("https://example.com/will-not-be-called.jpg", destination)
            self.assertTrue(ok)

    def test_download_file_retries_guard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "file.jpg"
            with self.assertRaises(ValueError):
                download_file("https://example.com/file.jpg", destination, retries=0)


if __name__ == "__main__":
    unittest.main()
