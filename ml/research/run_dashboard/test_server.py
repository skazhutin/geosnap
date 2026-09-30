from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from ml.research.run_dashboard import publish, server


def test_recent_rate_uses_completed_items_over_elapsed_time() -> None:
    rate, history = server.recent_rate([(500, 100.0), (1000, 150.0), (1500, 200.0)])
    assert rate == 10.0
    assert [point["per_second"] for point in history] == [10.0, 10.0]


def test_gallery_run_reports_only_committed_chunks(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(server, "GALLERY_DIR", tmp_path)
    monkeypatch.setattr(server, "GALLERY_TOTAL", 1000)
    monkeypatch.setattr(server, "CHUNK_TOTAL", 2)
    (tmp_path / "contract.json").write_text("{}")
    chunks = tmp_path / "chunks"
    chunks.mkdir()
    features = {"dark_fraction": 0.0, "p95_minus_p5_luminance": 150.0,
                "clipped_bright_fraction": 0.0, "luminance_entropy_bits": 7.0,
                "laplacian_variance": 500.0, "tenengrad": 4000.0,
                "canny_edge_density": 0.07}
    first = chunks / "000000-000499.jsonl"
    first.write_text(json.dumps({"id": "msls:sample", "source": "msls", "status": "ok", "features": features}) + "\n")
    first.with_suffix(".sha256").write_text("test\n")
    second = chunks / "000500-000999.jsonl"
    second.write_text(json.dumps({"id": "msls:incomplete", "source": "msls", "status": "ok", "features": features}) + "\n")
    first.touch()
    result = server.gallery_run(["123 python -m ml.research.geolocatability_v1.scan_gallery_technical"])
    assert result is not None
    assert result["completed"] == 500
    assert result["state"] == "running"
    assert result["grid"][0]["state"] == "done"
    assert result["grid"][1]["state"] == "working"
    assert result["photos"][0]["image_url"] == "/image/msls%3Asample"


def test_published_run_is_discoverable(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(publish, "RUN_FILES", tmp_path)
    monkeypatch.setattr(server, "RUN_FILES", tmp_path)
    publish.publish("encoder-1", title="Descriptors", phase="Encoding", completed=7, total=10)
    discovered = server.registered_runs()
    assert len(discovered) == 1
    assert discovered[0]["id"] == "encoder-1"
    assert discovered[0]["completed"] == 7
    path = tmp_path / "encoder-1.json"
    value = json.loads(path.read_text())
    value["updated_at"] = (datetime.now(UTC) - timedelta(minutes=6)).isoformat()
    path.write_text(json.dumps(value))
    assert server.registered_runs()[0]["state"] == "stalled"


def test_photo_details_reads_recorded_model_answer(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(server, "GALLERY_DIR", tmp_path)
    monkeypatch.setattr(server, "gallery_positions", lambda: {"msls:sample": 0})
    chunks = tmp_path / "chunks"
    chunks.mkdir()
    chunk = chunks / "000000-000499.jsonl"
    features = {"dark_fraction": 0.99, "p95_minus_p5_luminance": 4.0,
                "clipped_bright_fraction": 0.0, "luminance_entropy_bits": 0.5,
                "laplacian_variance": 1.0, "tenengrad": 2.0, "canny_edge_density": 0.0}
    chunk.write_text(json.dumps({"id": "msls:sample", "source": "msls", "status": "ok", "features": features}) + "\n")
    chunk.with_suffix(".sha256").write_text("test\n")
    raw = tmp_path / "vlm_review/full_gallery_candidates/raw/msls:sample"
    raw.mkdir(parents=True)
    answer = {"severe_technical_defect": True, "street_view_absent": True, "reason": "nearly_black"}
    (raw / "pass1.json").write_text(json.dumps({"valid": True, "attempts": [{"parsed": answer, "raw_text": json.dumps(answer)}]}))
    details = server.photo_details("msls:sample")
    assert "near_black" in details["technical_flags"]
    assert details["reviews"][0]["passes"][0]["answer"] == answer
    assert "image_url" in details
