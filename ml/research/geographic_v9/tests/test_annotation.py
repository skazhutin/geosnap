"""Blinding and append-only behavior of the local human annotation tool."""
from __future__ import annotations

import json
import threading
from http.server import ThreadingHTTPServer
from urllib.request import urlopen

import pytest

from ml.research.geographic_v9.annotation_server import AnnotationState, handler_for


def valid_payload(token):
    return {"token": token, "geolocatability": 2,
        "reasons": ["vegetation_dominated", "motion_blur"],
        "positive_evidence": ["intersection_or_road_geometry"],
        "would_request_another_photo": True,
        "recommended_retake_action": "hold_camera_still", "note": ""}


def test_validation_and_append_only(tmp_path):
    state = AnnotationState("smoketest")
    state.path = tmp_path / "rater_smoketest.jsonl"
    token = state.current()["token"]
    assert state.current()["completed"] == 0
    bad = valid_payload(token) | {"geolocatability": True}
    with pytest.raises(ValueError, match="score"):
        state.save(bad)
    bad = valid_payload(token) | {"reasons": ["baseline_wrong"]}
    with pytest.raises(ValueError, match="reason"):
        state.save(bad)
    bad = valid_payload(token) | {"recommended_retake_action": None}
    with pytest.raises(ValueError, match="action"):
        state.save(bad)
    after = state.save(valid_payload(token))
    assert after["completed"] == 1
    with pytest.raises(FileExistsError, match="already labeled"):
        state.save(valid_payload(token))
    saved = json.loads(state.path.read_text().strip())
    assert saved["token"] == token
    assert "hidden_bucket" not in saved
    assert "query_id" not in saved
    assert "image_path" not in saved


def test_browser_api_does_not_expose_outcomes():
    state = AnnotationState("apitest")
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(state))
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        with urlopen(f"http://127.0.0.1:{server.server_port}/api/current") as response:
            payload = json.load(response)
        assert set(payload) == {"token", "completed", "total", "rater", "done"}
        assert len(payload["token"]) == 24
        with urlopen(f"http://127.0.0.1:{server.server_port}/api/schema") as response:
            schema = json.load(response)
        assert "hidden_bucket" not in schema
        assert "ground_truth" not in schema
    finally:
        server.shutdown()
        worker.join(timeout=5)
        server.server_close()
