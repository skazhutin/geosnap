import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ml.ingestion.common import read_json
from ml.ingestion.kartaview_loader import (
    TileFetchResult,
    _extract_page,
    _request_with_retry,
    fetch_tile,
    load_query_points_config,
    run,
)


class FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.headers = {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")

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
    def test_extract_page_current_has_more_data(self) -> None:
        data, has_more = _extract_page({"result": {"data": [{"id": 1}], "hasMoreData": True}}, page_size=150)
        self.assertEqual(len(data), 1)
        self.assertTrue(has_more)

    def test_extract_page_rejects_logical_api_error(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Restricted"):
            _extract_page({"status": {"httpCode": 400, "apiMessage": "Restricted access!"}})

    def test_fetch_tile_uses_current_location_parameters_and_paginates(self) -> None:
        session = FakeSession(
            [
                FakeResponse(200, {"result": {"data": [{"id": "a"}], "hasMoreData": True}}),
                FakeResponse(200, {"result": {"data": [{"id": "b"}], "hasMoreData": False}}),
            ]
        )
        items = fetch_tile(
            session,
            limit=100,
            retries=3,
            backoff_sec=0,
            max_pages=10,
            lat=55.75,
            lon=37.61,
            radius_m=500,
        )
        self.assertEqual([item["id"] for item in items], ["a", "b"])
        params = session.calls[0][1]
        self.assertEqual(params["itemsPerPage"], 100)
        self.assertEqual(params["lat"], 55.75)
        self.assertEqual(params["lng"], 37.61)
        self.assertEqual(params["radius"], 500)
        self.assertEqual(params["zoomLevel"], 18)
        self.assertEqual(params["join"], "sequence")
        self.assertNotIn("bbox", params)
        self.assertNotIn("ipp", params)

    def test_request_with_retry_rejects_zero_retries(self) -> None:
        with self.assertRaises(ValueError):
            _request_with_retry(FakeSession([]), url="x", params={}, retries=0, backoff_sec=0.1)

    def test_run_rejects_invalid_limits(self) -> None:
        output = Path("/tmp/kartaview_invalid.json")
        with self.assertRaisesRegex(ValueError, "between 1 and 150"):
            run(output, 0, 0.1, 1, 0.1, 1)
        with self.assertRaisesRegex(ValueError, "request_pause_sec"):
            run(output, 1, -0.1, 1, 0.1, 1)
        with self.assertRaisesRegex(ValueError, "request_retries"):
            run(output, 1, 0, 0, 0.1, 1)
        with self.assertRaisesRegex(ValueError, "max_pages_per_tile"):
            run(output, 1, 0.1, 1, 0.1, 0)
        with self.assertRaisesRegex(ValueError, "official 500 m radius"):
            run(output, 1, 0, 1, 0, 1, max_tiles=1)
        with self.assertRaisesRegex(ValueError, "radius_override_m"):
            run(output, 1, 0, 1, 0, 1, max_tiles=1, radius_override_m=501)

    def test_checkpoint_skips_completed_tile(self) -> None:
        item = {
            "id": "k1",
            "lat": 55.555,
            "lng": 37.305,
            "shotDate": "2024-01-01 10:00:00",
            "heading": 90,
            "sequenceId": "seq-k",
            "sequenceIndex": 1,
            "fileurlProc": "https://images.test/k1.jpg",
        }
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "raw.json"
            with patch("ml.ingestion.kartaview_loader.fetch_tile", return_value=TileFetchResult([item], 1, False)):
                first = run(output, 10, 0, 1, 0, 1, max_tiles=1, radius_override_m=500)
            self.assertEqual(first["metadata_normalized"], 1)
            with patch("ml.ingestion.kartaview_loader.fetch_tile") as mocked:
                second = run(output, 10, 0, 1, 0, 1, max_tiles=1, radius_override_m=500)
            mocked.assert_not_called()
            self.assertEqual(second["tiles_skipped_checkpoint"], 1)

    def test_explicit_multi_area_query_points_are_validated_and_used(self) -> None:
        item = {
            "id": "k1",
            "lat": 55.75,
            "lng": 37.61,
            "sequenceId": "seq-k",
            "fileurlProc": "https://images.test/k1.jpg",
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "areas.json"
            config.write_text(
                """{
  "schema_version": 1,
  "dataset_id": "moscow_v1",
  "query_defaults": {"radius_m": 175},
  "areas": [
    {"area_id": "center", "points": [{"lat": 55.75, "lon": 37.61}]},
    {"area_id": "west", "points": [{"lat": 55.76, "lon": 37.42, "radius_m": 200}]}
  ]
}""",
                encoding="utf-8",
            )
            points = load_query_points_config(config)
            self.assertEqual(
                points,
                [
                    ("config:moscow_v1:center:000", 55.75, 37.61, 175),
                    ("config:moscow_v1:west:000", 55.76, 37.42, 200),
                ],
            )
            with patch(
                "ml.ingestion.kartaview_loader.fetch_tile",
                side_effect=(TileFetchResult([item], 1, False), TileFetchResult([], 1, False)),
            ) as mocked:
                stats = run(root / "raw.json", 10, 0, 1, 0, 1, query_points_config=config)
            self.assertEqual(stats["tiles_total"], 2)
            self.assertEqual(mocked.call_args_list[0].kwargs["radius_m"], 175)
            saved = read_json(root / "raw.json", default=[])
            self.assertIn("config:moscow_v1:center:000", saved[0]["metadata_json"])

    def test_query_points_config_rejects_duplicate_coordinates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "areas.json"
            config.write_text(
                """{
  "schema_version": 1,
  "dataset_id": "moscow_v1",
  "areas": [
    {"area_id": "a", "points": [{"lat": 55.75, "lon": 37.61}]},
    {"area_id": "b", "points": [{"lat": 55.75, "lon": 37.61}]}
  ]
}""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate query point"):
                load_query_points_config(config)

    def test_truncated_tile_remains_resumable(self) -> None:
        item = {
            "id": "k1",
            "lat": 55.555,
            "lng": 37.305,
            "fileurlProc": "https://images.test/k1.jpg",
        }
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "raw.json"
            with patch(
                "ml.ingestion.kartaview_loader.fetch_tile",
                return_value=TileFetchResult([item], 1, True),
            ):
                first = run(output, 10, 0, 1, 0, 1, max_tiles=1, radius_override_m=500)
            self.assertEqual(first["tiles_incomplete"], 1)
            checkpoint = read_json(output.with_suffix(".checkpoint.json"), default={})
            self.assertEqual(checkpoint["completed_tiles"], [])
            self.assertTrue(checkpoint["failed_tiles"])

            with patch(
                "ml.ingestion.kartaview_loader.fetch_tile",
                return_value=TileFetchResult([item], 1, False),
            ) as mocked:
                second = run(output, 10, 0, 1, 0, 2, max_tiles=1, radius_override_m=500)
            mocked.assert_called_once()
            self.assertEqual(second["tiles_succeeded"], 1)

    def test_checkpoint_rejects_changed_acquisition_geometry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "raw.json"
            with patch(
                "ml.ingestion.kartaview_loader.fetch_tile",
                return_value=TileFetchResult([], 1, False),
            ):
                run(output, 10, 0, 1, 0, 1, max_tiles=1, radius_override_m=500)
            with self.assertRaisesRegex(ValueError, "acquisition configuration changed"):
                run(output, 10, 0, 1, 0, 1, max_tiles=2, radius_override_m=500)

    def test_all_live_failures_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch("ml.ingestion.kartaview_loader.fetch_tile", side_effect=RuntimeError("offline")):
                with self.assertRaisesRegex(RuntimeError, "all 1 live KartaView"):
                    run(
                        Path(tmp) / "raw.json",
                        10,
                        0,
                        1,
                        0,
                        1,
                        max_tiles=1,
                        radius_override_m=500,
                    )


if __name__ == "__main__":
    unittest.main()
