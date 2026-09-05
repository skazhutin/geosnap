import tempfile
import unittest
from pathlib import Path

from ml.ingestion.common import read_json, redact_url
from ml.ingestion.download_images import _download_urls, run
from ml.ingestion.schema import canonical_record, manifest_dataframe, write_manifest


class DownloadImagesTests(unittest.TestCase):
    def test_missing_url_is_reported_without_secret_leak(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            record = canonical_record(
                source="mapillary",
                source_image_id="1",
                lat=55.75,
                lon=37.61,
                image_path=str(root / "image.jpg"),
                source_url="https://mapillary.test/image/1?token=secret",
                download_url="",
                metadata={},
            )
            manifest = root / "manifest.parquet"
            errors = root / "errors.json"
            write_manifest(manifest_dataframe([record]), manifest)
            stats = run(manifest, errors, retries=1, min_valid_size_bytes=1, workers=1)
            self.assertEqual(stats["failed_downloads"], 1)
            payload = read_json(errors, default=[])
            self.assertEqual(payload[0]["reason"], "missing_download_url")
            self.assertNotIn("secret", errors.read_text(encoding="utf-8"))

    def test_source_host_allowlist_rejects_manifest_ssrf_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            record = canonical_record(
                source="mapillary",
                source_image_id="unsafe",
                lat=55.75,
                lon=37.61,
                image_path=str(root / "image.jpg"),
                source_url="https://www.mapillary.com/app/?pKey=unsafe",
                download_url="https://127.0.0.1/private.jpg",
                metadata={},
            )
            manifest = root / "manifest.parquet"
            errors = root / "errors.json"
            write_manifest(manifest_dataframe([record]), manifest)

            stats = run(manifest, errors, retries=1, min_valid_size_bytes=1, workers=1)

            self.assertEqual(stats["failed_downloads"], 1)
            self.assertEqual(stats["network_attempts"], 0)
            self.assertEqual(stats["max_pending_downloads"], 1)
            self.assertEqual(
                read_json(errors, default=[])[0]["reason"],
                "unsafe_download_url:non_public_address",
            )

    def test_worker_bound_is_validated(self) -> None:
        with self.assertRaisesRegex(ValueError, "between 1 and 64"):
            run(Path("missing"), Path("errors"), retries=1, min_valid_size_bytes=1, workers=0)

    def test_kartaview_download_candidates_include_cdn_fallbacks(self) -> None:
        row = {
            "download_url": "https://cdn.kartaview.org/processed",
            "metadata_json": (
                '{"imageProcUrl":"https://cdn.kartaview.org/processed",'
                '"imageLthUrl":"https://cdn.kartaview.org/large-thumbnail",'
                '"fileurlProc":"https://storage7.openstreetcam.org/processed.jpg"}'
            ),
        }
        self.assertEqual(
            _download_urls(row, "kartaview"),
            [
                "https://cdn.kartaview.org/processed",
                "https://cdn.kartaview.org/large-thumbnail",
                "https://storage7.openstreetcam.org/processed.jpg",
            ],
        )

    def test_non_kartaview_source_ignores_metadata_urls(self) -> None:
        row = {
            "download_url": "https://scontent.example.fbcdn.net/mapillary.jpg",
            "metadata_json": '{"imageLthUrl":"https://cdn.kartaview.org/not-mapillary"}',
        }
        self.assertEqual(_download_urls(row, "mapillary"), [row["download_url"]])

    def test_redact_url_drops_credentials_query_and_fragment(self) -> None:
        redacted = redact_url("https://user:password@example.test/image.jpg?access_token=secret#fragment")
        self.assertEqual(redacted, "https://example.test/image.jpg")

    def test_redact_url_handles_invalid_port_without_raising(self) -> None:
        self.assertEqual(redact_url("https://example.test:not-a-port/image.jpg?token=secret"), "<invalid-url>")


if __name__ == "__main__":
    unittest.main()
