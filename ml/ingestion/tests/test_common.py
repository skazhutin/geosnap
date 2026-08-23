import tempfile
import unittest
from io import BytesIO
from pathlib import Path

from PIL import Image

from ml.ingestion.common import (
    download_file,
    download_file_detailed,
    read_json,
    request_with_retry,
    retry_delay_seconds,
    sanitize_error_message,
    write_json,
)


def _jpeg_bytes(size: tuple[int, int] = (96, 96)) -> bytes:
    stream = BytesIO()
    Image.new("RGB", size, color=(100, 120, 140)).save(stream, format="JPEG", quality=95)
    return stream.getvalue()


class FakeResponse:
    def __init__(
        self,
        body: bytes,
        content_type: str = "image/jpeg",
        status_code: int = 200,
        content_length: int | str | None = None,
        chunks: list[bytes] | None = None,
        location: str | None = None,
    ):
        self.body = body
        self.status_code = status_code
        self.headers = {"Content-Type": content_type}
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)
        if location is not None:
            self.headers["Location"] = location
        self.chunks = chunks

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size: int):
        yield from self.chunks if self.chunks is not None else [self.body]

    def close(self) -> None:
        return None


class FakeSession:
    def __init__(self, response: FakeResponse | list[FakeResponse]):
        self.responses = response if isinstance(response, list) else [response]
        self.calls = 0

    def get(self, url: str, stream: bool, timeout: float, allow_redirects: bool):
        self.calls += 1
        self.asserted_allow_redirects = allow_redirects
        return self.responses[min(self.calls - 1, len(self.responses) - 1)]


class FakeRequestSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.calls = 0

    def get(self, url: str, params: dict, timeout: float):
        self.calls += 1
        return self.response


class CommonTests(unittest.TestCase):
    def test_json_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.json"
            payload = {"a": 1, "b": [1, 2, 3]}
            write_json(path, payload)
            self.assertEqual(read_json(path, default={}), payload)

    def test_download_file_skips_only_decodable_existing_image(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "file.jpg"
            destination.write_bytes(_jpeg_bytes())
            session = FakeSession(FakeResponse(_jpeg_bytes()))
            ok = download_file(
                "https://example.test/will-not-be-called.jpg",
                destination,
                session=session,
                min_valid_size_bytes=100,
            )
            self.assertTrue(ok)
            self.assertEqual(session.calls, 0)

    def test_corrupt_existing_file_is_replaced_and_decoded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "file.jpg"
            destination.write_bytes(b"x" * 10_000)
            session = FakeSession(FakeResponse(_jpeg_bytes()))
            result = download_file_detailed(
                "https://example.test/image.jpg?access_token=secret",
                destination,
                session=session,
                retries=1,
                min_valid_size_bytes=100,
                min_width=64,
                min_height=64,
            )
            self.assertEqual(result.status, "downloaded")
            self.assertEqual(session.calls, 1)
            with Image.open(destination) as image:
                image.load()
                self.assertEqual(image.size, (96, 96))

    def test_oversized_existing_image_is_not_trusted_as_a_skip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "file.jpg"
            destination.write_bytes(_jpeg_bytes() + b"padding" * 100)
            replacement = _jpeg_bytes((80, 80))
            session = FakeSession(FakeResponse(replacement))
            result = download_file_detailed(
                "https://example.test/replacement.jpg",
                destination,
                session=session,
                retries=1,
                min_valid_size_bytes=1,
                max_download_bytes=len(replacement) + 1,
            )
            self.assertEqual(result.status, "downloaded")
            self.assertEqual(session.calls, 1)
            self.assertEqual(destination.read_bytes(), replacement)

    def test_download_file_retries_guard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "file.jpg"
            with self.assertRaises(ValueError):
                download_file("https://example.test/file.jpg", destination, retries=0)

    def test_download_does_not_retry_permanent_http_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "file.jpg"
            session = FakeSession(FakeResponse(b"missing", content_type="text/plain", status_code=404))
            result = download_file_detailed(
                "https://example.test/missing.jpg",
                destination,
                session=session,
                retries=3,
                backoff_sec=0,
                min_valid_size_bytes=1,
            )
            self.assertEqual(result.status, "failed")
            self.assertEqual(session.calls, 1)
            self.assertEqual(result.network_attempts, 1)
            self.assertEqual(result.retry_attempts, 0)

    def test_download_rejects_oversized_declared_length_without_writing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "file.jpg"
            session = FakeSession(FakeResponse(_jpeg_bytes(), content_length=10_001))
            result = download_file_detailed(
                "https://example.test/large.jpg",
                destination,
                session=session,
                retries=3,
                backoff_sec=0,
                min_valid_size_bytes=1,
                max_download_bytes=10_000,
            )
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.reason, "download_too_large:content_length")
            self.assertEqual(session.calls, 1)
            self.assertFalse(destination.exists())
            self.assertEqual(list(Path(tmp).glob("*.part")), [])

    def test_download_caps_chunked_body_before_disk_growth(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "file.jpg"
            session = FakeSession(
                FakeResponse(b"", chunks=[b"a" * 8, b"b" * 8])
            )
            result = download_file_detailed(
                "https://example.test/chunked.jpg",
                destination,
                session=session,
                retries=3,
                backoff_sec=0,
                min_valid_size_bytes=1,
                max_download_bytes=10,
            )
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.reason, "download_too_large:stream")
            self.assertEqual(session.calls, 1)
            self.assertFalse(destination.exists())
            self.assertEqual(list(Path(tmp).glob("*.part")), [])

    def test_download_rejects_excessive_decoded_pixels_without_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "file.jpg"
            session = FakeSession(FakeResponse(_jpeg_bytes((100, 100))))
            result = download_file_detailed(
                "https://example.test/wide.jpg",
                destination,
                session=session,
                retries=3,
                backoff_sec=0,
                min_valid_size_bytes=1,
                max_image_pixels=9_999,
            )
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.reason, "too_many_pixels")
            self.assertEqual(session.calls, 1)
            self.assertFalse(destination.exists())

    def test_download_rejects_private_address_before_network(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = FakeSession(FakeResponse(_jpeg_bytes()))
            result = download_file_detailed(
                "https://127.0.0.1/internal.jpg",
                Path(tmp) / "image.jpg",
                session=session,
                retries=1,
                min_valid_size_bytes=1,
            )
            self.assertEqual(result.reason, "unsafe_download_url:non_public_address")
            self.assertEqual(session.calls, 0)

    def test_download_rejects_redirect_to_non_allowlisted_host(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = FakeSession(
                FakeResponse(b"", status_code=302, location="https://evil.example/image.jpg")
            )
            result = download_file_detailed(
                "https://cdn.kartaview.org/image.jpg",
                Path(tmp) / "image.jpg",
                session=session,
                retries=2,
                backoff_sec=0,
                min_valid_size_bytes=1,
                allowed_host_suffixes=frozenset({"kartaview.org", "openstreetcam.org"}),
            )
            self.assertEqual(result.reason, "unsafe_download_url:host_not_allowed")
            self.assertEqual(session.calls, 1)
            self.assertEqual(result.retry_attempts, 0)

    def test_download_follows_only_validated_redirects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            session = FakeSession(
                [
                    FakeResponse(b"", status_code=302, location="https://storage1.openstreetcam.org/a.jpg"),
                    FakeResponse(_jpeg_bytes()),
                ]
            )
            result = download_file_detailed(
                "https://cdn.kartaview.org/image.jpg",
                Path(tmp) / "image.jpg",
                session=session,
                retries=1,
                min_valid_size_bytes=1,
                allowed_host_suffixes=frozenset({"kartaview.org", "openstreetcam.org"}),
            )
            self.assertEqual(result.status, "downloaded")
            self.assertEqual(result.network_attempts, 2)
            self.assertFalse(session.asserted_allow_redirects)

    def test_request_does_not_retry_permanent_http_error(self) -> None:
        session = FakeRequestSession(FakeResponse(b"missing", status_code=404))
        with self.assertRaisesRegex(RuntimeError, "HTTP 404"):
            request_with_retry(
                session,
                url="https://example.test/missing",
                retries=3,
                backoff_sec=0,
            )
        self.assertEqual(session.calls, 1)

    def test_retry_delay_is_exponential_and_honors_retry_after(self) -> None:
        response = FakeResponse(b"")
        response.headers["Retry-After"] = "9"
        self.assertEqual(retry_delay_seconds(response, attempt=1, backoff_sec=1), 9)
        response.headers.clear()
        self.assertEqual(retry_delay_seconds(response, attempt=4, backoff_sec=1), 8)

    def test_error_messages_redact_urls_and_bare_tokens(self) -> None:
        reason = sanitize_error_message(
            "403 for https://example.test/image.jpg?access_token=secret&x=1 token=also-secret"
        )
        self.assertEqual(reason, "403 for https://example.test/image.jpg token=<redacted>")
        self.assertNotIn("secret", reason)


if __name__ == "__main__":
    unittest.main()
