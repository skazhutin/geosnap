from __future__ import annotations

import pytest

from ml.evaluation.replay_localizer import ReplayError, replay_payload


def _source(*, split: str = "calibration") -> dict[str, object]:
    matches = [
        {
            "rank": rank,
            "reference_id": f"reference-{rank}",
            "score": 0.95 - rank * 0.01,
            "lat": 55.75 + (rank - 1) * 0.00001,
            "lon": 37.61,
            "source": "fixture",
            "source_image_id": str(rank),
            "sequence_id": f"sequence-{rank}",
            "source_url": f"https://example.test/{rank}",
        }
        for rank in range(1, 11)
    ]
    return {
        "benchmark_kind": "real_moscow_street_view",
        "leakage_audit": {"passed": True},
        "localization": {"confidence_threshold": 0.0},
        "primary": {"retrieval": {"recall_at": {"10": 1.0, "20": 1.0, "50": 1.0}}},
        "per_query": [
            {
                "query_id": "query",
                "true_lat": 55.75,
                "true_lon": 37.61,
                "evaluation_split": split,
                "query_quality": {"confidence_signal": 1.0},
                "matches": matches,
            }
        ],
    }


def test_replay_uses_existing_matches_and_updates_estimator_metrics() -> None:
    payload = replay_payload(_source(), estimator="top1", top_k=10)

    assert payload["localization"]["estimator"] == "top1"
    assert payload["retrieval_configuration"]["top_k"] == 10
    assert payload["primary"]["retrieval"]["recall_at"] == {"10": 1.0}
    assert payload["experiment_replay"]["retrieval_matches_reused"] is True
    assert payload["per_query"][0]["error_m"] == pytest.approx(0.0)
    assert payload["primary"]["localization"]["accuracy_within_m"]["100"] == 1.0


def test_replay_rejects_test_scope() -> None:
    with pytest.raises(ReplayError, match="calibration-only"):
        replay_payload(_source(split="test"), estimator="top1", top_k=10)
