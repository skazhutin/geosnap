import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ml.ingestion.common import RetryMetrics, read_json
from ml.ingestion.mapillary_loader import MAPILLARY_FIELDS, TileFetchResult, _request_with_retry, fetch_tile, run


class FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.headers = {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")

    def json(self) -> dict:
        return self._payload

    def close(self) -> None:
        return None


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self._responses = responses
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict, timeout: float):
        self.calls.append((url, params))
        return self._responses.pop(0)


class MapillaryLoaderTests(unittest.TestCase):
    def test_fields_include_heading_creator_sequence_and_urls(self) -> None:
        for field in (
            "computed_compass_angle",
            "compass_angle",
            "creator",
            "sequence",
            "thumb_original_url",
        ):
            self.assertIn(field, MAPILLARY_FIELDS.split(","))

    def test_live_fields_omit_quality_score_that_silently_empties_spatial_results(self) -> None:
        self.assertNotIn("quality_score", MAPILLARY_FIELDS.split(","))

    def test_request_with_retry_eventually_succeeds(self) -> None:
        session = FakeSession([FakeResponse(429, {}), FakeResponse(200, {"data": []})])
        metrics = RetryMetrics()
        with patch("ml.ingestion.common.time.sleep"):
            response = _request_with_retry(
                session,
                url="https://example.test",
                params={"k": "v"},
                retries=2,
                backoff_sec=0.01,
                metrics=metrics,
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(metrics.network_attempts, 2)
        self.assertEqual(metrics.retry_attempts, 1)
        self.assertEqual(metrics.retryable_http_events, 1)
        self.assertEqual(metrics.rate_limit_events, 1)

    def test_fetch_tile_follows_paging_next(self) -> None:
        session = FakeSession(
            [
                FakeResponse(200, {"data": [{"id": "1"}], "paging": {"next": "https://next-page"}}),
                FakeResponse(200, {"data": [{"id": "2"}], "paging": {}}),
            ]
        )
        data = fetch_tile(
            session,
            token="token",
            bbox=(37.3, 55.5, 37.31, 55.51),
            limit=100,
            retries=3,
            backoff_sec=0,
            max_pages=10,
        )
        self.assertEqual([item["id"] for item in data], ["1", "2"])
        self.assertEqual(session.calls[1][0], "https://next-page")
        self.assertEqual(session.calls[1][1], {})

    def test_request_with_retry_rejects_zero_retries(self) -> None:
        with self.assertRaises(ValueError):
            _request_with_retry(FakeSession([]), url="x", params={}, retries=0, backoff_sec=0.1)

    def test_run_rejects_invalid_limits(self) -> None:
        output = Path("/tmp/mapillary_invalid.json")
        with self.assertRaisesRegex(ValueError, "between 1 and 2000"):
            run(output, 0, 0.1, 1, 0.1, 1)
        with self.assertRaisesRegex(ValueError, "request_pause_sec"):
            run(output, 1, -0.1, 1, 0.1, 1)
        with self.assertRaisesRegex(ValueError, "request_retries"):
            run(output, 1, 0, 0, 0.1, 1)
        with self.assertRaisesRegex(ValueError, "max_pages_per_tile"):
            run(output, 1, 0.1, 1, 0.1, 0)

    def test_missing_token_is_actionable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "fixture-based ingestion tests remain available"):
                run(Path(tmp) / "raw.json", 1, 0, 1, 0, 1, max_tiles=1)

    def test_checkpoint_skips_completed_tile(self) -> None:
        item = {
            "id": "m1",
            "captured_at": 1_700_000_000_000,
            "computed_geometry": {"coordinates": [37.305, 55.555]},
            "thumb_original_url": "https://images.test/m1.jpg",
            "computed_compass_angle": 42,
            "sequence": "seq-1",
        }
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}):
            output = Path(tmp) / "raw.json"
            with patch("ml.ingestion.mapillary_loader.fetch_tile", return_value=TileFetchResult([item], 1, False)):
                first = run(output, 10, 0, 1, 0, 1, max_tiles=1)
            self.assertEqual(first["metadata_normalized"], 1)
            with patch("ml.ingestion.mapillary_loader.fetch_tile") as mocked:
                second = run(output, 10, 0, 1, 0, 1, max_tiles=1)
            mocked.assert_not_called()
            self.assertEqual(second["tiles_skipped_checkpoint"], 1)

    def test_explicit_multi_area_points_become_small_bboxes_with_provenance(self) -> None:
        item = {
            "id": "m1",
            "captured_at": 1_700_000_000_000,
            "computed_geometry": {"coordinates": [37.61, 55.75]},
            "thumb_original_url": "https://images.test/m1.jpg",
            "sequence": "seq-1",
            "quality_score": 0.8,
        }
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            "os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}
        ):
            root = Path(tmp)
            config = root / "areas.json"
            config.write_text(
                """{
  "schema_version": 1,
  "dataset_id": "moscow_v1",
  "query_defaults": {"radius_m": 200},
  "areas": [{"area_id": "center", "points": [{"lat": 55.75, "lon": 37.61}]}]
}""",
                encoding="utf-8",
            )
            with patch(
                "ml.ingestion.mapillary_loader.fetch_tile",
                return_value=TileFetchResult([item], 1, False),
            ) as mocked:
                stats = run(
                    root / "raw.json",
                    10,
                    0,
                    1,
                    0,
                    1,
                    query_points_config=config,
                    query_radius_override_m=25,
                )
            bbox = mocked.call_args.kwargs["bbox"]
            self.assertLess(bbox[2] - bbox[0], 0.001)
            self.assertLess(bbox[3] - bbox[1], 0.001)
            self.assertEqual(stats["metadata_normalized"], 1)
            saved = read_json(root / "raw.json", default=[])
            self.assertEqual(saved[0]["quality_score"], 0.8)
            self.assertIn("config:moscow_v1:center:000", saved[0]["metadata_json"])

    def test_query_radius_override_requires_points_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict(
            "os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}
        ):
            with self.assertRaisesRegex(ValueError, "requires query_points_config"):
                run(Path(tmp) / "raw.json", 10, 0, 1, 0, 1, query_radius_override_m=25)

    def test_truncated_tile_remains_resumable(self) -> None:
        item = {
            "id": "m1",
            "computed_geometry": {"coordinates": [37.305, 55.555]},
            "thumb_original_url": "https://images.test/m1.jpg",
        }
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}):
            output = Path(tmp) / "raw.json"
            with patch(
                "ml.ingestion.mapillary_loader.fetch_tile",
                return_value=TileFetchResult([item], 1, True),
            ):
                first = run(output, 10, 0, 1, 0, 1, max_tiles=1)
            self.assertEqual(first["tiles_incomplete"], 1)
            checkpoint = read_json(output.with_suffix(".checkpoint.json"), default={})
            self.assertEqual(checkpoint["completed_tiles"], [])
            self.assertTrue(checkpoint["failed_tiles"])

            with patch(
                "ml.ingestion.mapillary_loader.fetch_tile",
                return_value=TileFetchResult([item], 1, False),
            ) as mocked:
                second = run(output, 10, 0, 1, 0, 2, max_tiles=1)
            mocked.assert_called_once()
            self.assertEqual(second["tiles_succeeded"], 1)

    def test_checkpoint_rejects_changed_acquisition_geometry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}):
            output = Path(tmp) / "raw.json"
            with patch(
                "ml.ingestion.mapillary_loader.fetch_tile",
                return_value=TileFetchResult([], 1, False),
            ):
                run(output, 10, 0, 1, 0, 1, max_tiles=1)
            with self.assertRaisesRegex(ValueError, "acquisition configuration changed"):
                run(output, 10, 0, 1, 0, 1, max_tiles=2)

    def test_all_live_failures_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, patch.dict("os.environ", {"MAPILLARY_ACCESS_TOKEN": "token"}):
            with patch("ml.ingestion.mapillary_loader.fetch_tile", side_effect=RuntimeError("offline")):
                with self.assertRaisesRegex(RuntimeError, "all 1 live Mapillary"):
                    run(Path(tmp) / "raw.json", 10, 0, 1, 0, 1, max_tiles=1)


if __name__ == "__main__":
    unittest.main()
