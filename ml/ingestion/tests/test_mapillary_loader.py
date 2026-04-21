import unittest
from unittest.mock import patch

from ml.ingestion.mapillary_loader import _request_with_retry, fetch_tile


class FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400 and self.status_code not in {429, 500, 502, 503, 504}:
            raise RuntimeError(f"http {self.status_code}")
        if self.status_code in {429, 500, 502, 503, 504}:
            raise RuntimeError(f"retryable {self.status_code}")

    def json(self) -> dict:
        return self._payload

    def close(self) -> None:
        return None


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self._responses = responses
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict, timeout: int):
        self.calls.append((url, params))
        if not self._responses:
            raise RuntimeError("no more responses")
        return self._responses.pop(0)


class MapillaryLoaderTests(unittest.TestCase):
    def test_request_with_retry_eventually_succeeds(self) -> None:
        session = FakeSession(
            [
                FakeResponse(429, {}),
                FakeResponse(200, {"data": []}),
            ]
        )
        with patch("ml.ingestion.mapillary_loader.time.sleep"):
            response = _request_with_retry(
                session,
                url="https://example.test",
                params={"k": "v"},
                retries=2,
                backoff_sec=0.01,
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(session.calls), 2)

    def test_fetch_tile_follows_paging_next(self) -> None:
        session = FakeSession(
            [
                FakeResponse(200, {"data": [{"id": "1"}], "paging": {"next": "https://next-page"}}),
                FakeResponse(200, {"data": [{"id": "2"}], "paging": {}}),
            ]
        )
        with patch("ml.ingestion.mapillary_loader.time.sleep"):
            data = fetch_tile(
                session,
                token="token",
                bbox=(37.3, 55.5, 37.31, 55.51),
                limit=100,
                retries=3,
                backoff_sec=0.01,
                max_pages=10,
            )
        self.assertEqual([item["id"] for item in data], ["1", "2"])
        self.assertEqual(session.calls[1][0], "https://next-page")

    def test_request_with_retry_rejects_zero_retries(self) -> None:
        session = FakeSession([FakeResponse(200, {})])
        with self.assertRaises(ValueError):
            _request_with_retry(session, url="x", params={}, retries=0, backoff_sec=0.1)


if __name__ == "__main__":
    unittest.main()
