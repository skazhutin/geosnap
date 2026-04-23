import unittest
from pathlib import Path
from unittest.mock import patch

from ml.ingestion.kartaview_loader import _extract_page, _request_with_retry, fetch_tile, run


class FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400 and self.status_code not in {429, 500, 502, 503, 504}:
            raise RuntimeError(f"http {self.status_code}")
        if self.status_code in {429, 500, 502, 503, 504}:
            raise RuntimeError(f"retryable {self.status_code}")

    def json(self):
        return self._payload

    def close(self) -> None:
        return None


class FakeSession:
    def __init__(self, responses):
        self._responses = responses
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append((url, params))
        return self._responses.pop(0)


class KartaViewLoaderTests(unittest.TestCase):
    def test_extract_page_from_result_payload(self) -> None:
        data, has_more = _extract_page({"result": {"data": [{"id": 1}], "currentPage": 1, "totalPages": 2}})
        self.assertEqual(len(data), 1)
        self.assertTrue(has_more)

    def test_fetch_tile_iterates_multiple_pages(self) -> None:
        session = FakeSession(
            [
                FakeResponse(200, {"result": {"data": [{"id": "a"}], "hasMore": True}}),
                FakeResponse(200, {"result": {"data": [{"id": "b"}], "hasMore": False}}),
            ]
        )
        with patch("ml.ingestion.kartaview_loader.time.sleep"):
            items = fetch_tile(
                session,
                bbox=(37.3, 55.5, 37.31, 55.51),
                limit=100,
                retries=3,
                backoff_sec=0.01,
                max_pages=10,
            )
        self.assertEqual([x["id"] for x in items], ["a", "b"])
        self.assertEqual(session.calls[0][1]["page"], 1)
        self.assertEqual(session.calls[1][1]["page"], 2)

    def test_request_with_retry_rejects_zero_retries(self) -> None:
        session = FakeSession([FakeResponse(200, {})])
        with self.assertRaises(ValueError):
            _request_with_retry(session, url="x", params={}, retries=0, backoff_sec=0.1)

    def test_run_rejects_invalid_limits(self) -> None:
        output = Path("/tmp/kartaview_invalid.json")
        with self.assertRaisesRegex(ValueError, "limit_per_tile must be >= 1"):
            run(output, limit_per_tile=0, request_pause_sec=0.1, request_retries=1, backoff_sec=0.1, max_pages_per_tile=1)
        with self.assertRaisesRegex(ValueError, "request_pause_sec must be >= 0"):
            run(
                output,
                limit_per_tile=1,
                request_pause_sec=-0.1,
                request_retries=1,
                backoff_sec=0.1,
                max_pages_per_tile=1,
            )
        with self.assertRaisesRegex(ValueError, "max_pages_per_tile must be >= 1"):
            run(output, limit_per_tile=1, request_pause_sec=0.1, request_retries=1, backoff_sec=0.1, max_pages_per_tile=0)


if __name__ == "__main__":
    unittest.main()
