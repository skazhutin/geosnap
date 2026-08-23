import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from ml.ingestion.common import RequestRateLimiter, read_json
from ml.ingestion.kartaview_sequences import (
    DEFAULT_MIN_REQUEST_INTERVAL_SEC,
    SequencePageRequest,
    _extract_page,
    build_sequence_plan,
    fetch_sequence_page,
    run,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers: dict[str, str] = {}
        self.closed = False

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")

    def json(self) -> object:
        return self._payload

    def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, object], float]] = []

    def get(self, url: str, params: dict[str, object], timeout: float) -> FakeResponse:
        self.calls.append((url, params, timeout))
        if not self.responses:
            raise AssertionError("unexpected network call")
        return self.responses.pop(0)


def discovery_record(
    photo_id: str,
    *,
    area: str,
    hotspot: int,
    sequence_id: str | None,
    sequence_index: int = 0,
    normalized: bool = False,
) -> dict[str, object]:
    query = f"config:moscow_v1:{area}:{hotspot:03d}"
    raw = {
        "id": photo_id,
        "sequenceId": sequence_id,
        "sequenceIndex": sequence_index,
        "_geosnap_acquisition_query": query,
    }
    if not normalized:
        return raw
    return {
        "id": photo_id,
        "source_image_id": photo_id,
        "sequence_id": sequence_id,
        "sequence_index": str(sequence_index),
        "metadata_json": json.dumps(raw),
    }


def api_photo(
    photo_id: str,
    *,
    lat: float = 55.75,
    lon: float = 37.61,
    sequence_id: str = "seq-a",
) -> dict[str, object]:
    return {
        "id": photo_id,
        "lat": lat,
        "lng": lon,
        "sequenceId": sequence_id,
        "sequenceIndex": 10,
        "fileurlProc": f"https://storage.openstreetcam.org/{photo_id}.jpg",
    }


def response_with(*items: dict[str, object]) -> FakeResponse:
    return FakeResponse(200, {"result": {"data": list(items)}})


def test_plan_is_deterministic_bounded_and_uses_strongest_hotspots() -> None:
    records = [
        # Strongest alpha hotspot: five unique photos and four sequences.
        discovery_record("a1", area="alpha", hotspot=0, sequence_id="seq-1", sequence_index=299),
        discovery_record("a2", area="alpha", hotspot=0, sequence_id="seq-1", sequence_index=301),
        discovery_record("a3", area="alpha", hotspot=0, sequence_id="seq-2", sequence_index=450),
        discovery_record("a4", area="alpha", hotspot=0, sequence_id="seq-3", sequence_index=1),
        discovery_record("a5", area="alpha", hotspot=0, sequence_id="seq-4", sequence_index=1),
        # Second and third strongest; hotspot 2 must be excluded by the per-area cap.
        discovery_record("b1", area="alpha", hotspot=1, sequence_id="seq-5", sequence_index=150),
        discovery_record("b2", area="alpha", hotspot=1, sequence_id="seq-6", sequence_index=0),
        discovery_record("b3", area="alpha", hotspot=1, sequence_id="seq-7", sequence_index=0),
        discovery_record("c1", area="alpha", hotspot=2, sequence_id="seq-8", sequence_index=0),
        discovery_record("d1", area="beta", hotspot=0, sequence_id="seq-9", normalized=True),
    ]

    plan, summary = build_sequence_plan(records, items_per_page=150, max_requests=5)
    reverse_plan, reverse_summary = build_sequence_plan(
        list(reversed(records)), items_per_page=150, max_requests=5
    )

    assert [request.as_dict() for request in plan] == [
        request.as_dict() for request in reverse_plan
    ]
    assert summary == reverse_summary
    assert len(plan) == 5
    assert summary.requests_before_global_limit == 7
    assert summary.requests_planned == 5
    assert {request.hotspot_query for request in plan}.isdisjoint(
        {"config:moscow_v1:alpha:002"}
    )
    assert sum(request.hotspot_query.endswith("alpha:000") for request in plan) == 3
    representative = next(request for request in plan if request.sequence_id == "seq-1")
    assert representative.representative_sequence_index == 299
    assert representative.page == 2


def test_fetch_page_uses_sequence_parameters_and_throttles_every_retry() -> None:
    throttled = FakeResponse(429, {"error": "slow down"})
    successful = response_with(api_photo("photo-1"))
    session = FakeSession([throttled, successful])
    limiter = RequestRateLimiter(0)
    limiter.wait = Mock(wraps=limiter.wait)  # type: ignore[method-assign]
    planned = SequencePageRequest(
        key="sequence:key",
        area_id="alpha",
        hotspot_query="config:moscow_v1:alpha:000",
        hotspot_strength=3,
        sequence_id="seq-a",
        sequence_strength=1,
        representative_sequence_index=151,
        page=2,
        items_per_page=150,
    )

    items = fetch_sequence_page(
        session,
        planned,
        limiter=limiter,
        retries=2,
        backoff_sec=0,
        timeout_sec=12,
    )

    assert [item["id"] for item in items] == ["photo-1"]
    assert limiter.wait.call_count == 2
    assert [call[1] for call in session.calls] == [
        {"sequenceId": "seq-a", "page": 2, "itemsPerPage": 150},
        {"sequenceId": "seq-a", "page": 2, "itemsPerPage": 150},
    ]
    assert all(call[2] == 12 for call in session.calls)
    assert throttled.closed
    assert successful.closed


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"result": None},
        {"result": {}},
        {"result": {"data": None}},
        {"result": {"data": [None]}},
    ],
)
def test_extract_page_rejects_malformed_success_payloads(payload: object) -> None:
    with pytest.raises(RuntimeError, match="KartaView response"):
        _extract_page(payload)


def test_extract_page_accepts_an_explicit_empty_data_array() -> None:
    assert _extract_page({"result": {"data": []}}) == []


def test_plan_only_writes_plan_and_stats_without_opening_a_session() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        discovery = root / "discovery.json"
        output = root / "expanded.json"
        discovery.write_text(
            json.dumps(
                [
                    discovery_record(
                        "a1", area="alpha", hotspot=0, sequence_id="seq-1", sequence_index=151
                    )
                ]
            ),
            encoding="utf-8",
        )

        with patch("requests.Session", side_effect=AssertionError("network must stay offline")):
            stats = run(discovery, output, plan_only=True)

        plan = read_json(output.with_suffix(".plan.json"), default={})
        saved_stats = read_json(output.with_suffix(".stats.json"), default={})
        assert not output.exists()
        assert stats == saved_stats
        assert stats["plan_only"] is True
        assert stats["requests_planned"] == 1
        assert stats["minimum_request_interval_sec"] == DEFAULT_MIN_REQUEST_INTERVAL_SEC
        assert plan["limits"]["items_per_page"] == 150
        assert plan["requests"][0]["page"] == 2


def test_run_deduplicates_photo_ids_filters_aoi_preserves_provenance_and_resumes() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        discovery = root / "discovery.json"
        output = root / "expanded.json"
        discovery.write_text(
            json.dumps(
                [
                    discovery_record("seed-1", area="alpha", hotspot=0, sequence_id="seq-a"),
                    discovery_record("seed-2", area="alpha", hotspot=0, sequence_id="seq-b"),
                ]
            ),
            encoding="utf-8",
        )
        session = FakeSession(
            [
                response_with(
                    api_photo("photo-1", sequence_id="seq-a"),
                    api_photo("outside", lat=56.0, sequence_id="seq-a"),
                    {"id": "invalid", "lat": 55.75, "lng": 37.61},
                ),
                response_with(
                    api_photo("photo-1", sequence_id="seq-b"),
                    api_photo("photo-2", sequence_id="seq-b"),
                ),
            ]
        )

        stats = run(
            discovery,
            output,
            min_request_interval_sec=0,
            request_retries=1,
            backoff_sec=0,
            session=session,
        )

        saved = read_json(output, default=[])
        assert [record["id"] for record in saved] == ["photo-1", "photo-2"]
        assert stats["metadata_normalized"] == 2
        assert stats["duplicate_records"] == 1
        assert stats["invalid_records"] == 1
        assert stats["records_outside_moscow_aoi"] == 1
        assert stats["last_run_throttled_attempts"] == 2
        provenance = json.loads(saved[0]["metadata_json"])
        assert provenance["_geosnap_acquisition_query"] == "config:moscow_v1:alpha:000"
        assert provenance["_geosnap_sequence_expansion"]["plan_key"].startswith("sequence:")

        resume_session = FakeSession([])
        resumed = run(
            discovery,
            output,
            min_request_interval_sec=0,
            request_retries=1,
            backoff_sec=0,
            session=resume_session,
        )
        assert resume_session.calls == []
        assert resumed["tiles_skipped_checkpoint"] == 2
        assert read_json(output, default=[]) == saved


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"items_per_page": 151}, "items_per_page"),
        ({"hotspots_per_area": 3}, "hotspots_per_area"),
        ({"sequences_per_hotspot": 4}, "sequences_per_hotspot"),
        ({"max_requests": 0}, "max_requests"),
    ],
)
def test_plan_rejects_unbounded_limits(kwargs: dict[str, int], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        build_sequence_plan([], **kwargs)
