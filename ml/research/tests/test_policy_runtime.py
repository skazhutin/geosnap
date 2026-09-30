import json

import pytest

from ml.research.policy_runtime import authorize, summarize_rows
from ml.research.seal import SealError


def test_failed_inference_stays_raw_failure_even_when_gallery_has_positive():
    rows = {
        "errors": [None, 20.0, 800.0],
        "scores": [1.0, 0.95, 0.2],
        "ranks": [None, 1, None],
        "positive_counts": [2, 1, 0],
        "gallery_size": 5,
        "groups": ["a", "b", "c"],
    }
    result = summarize_rows(rows, 0.9)
    assert result["raw"]["query_count"] == 3
    assert result["raw"]["accuracy_100m"] == 1 / 3
    assert result["raw"]["catastrophic_gt500m_rate"] == 2 / 3
    assert result["retrieval"]["no_positive_count"] == 1
    assert result["retrieval"]["retrieval_failure_with_spatial_positive_count"] == 1
    assert result["product"]["accepted_count"] == 1
    assert result["product"]["answer_rate"] == 1 / 3
    assert result["product"]["conditional_accuracy_100m"] == 1.0
    assert result["threshold_optimized_on_this_set"] is False


def test_policy_without_a_feasible_calibration_threshold_answers_nothing():
    rows = {
        "errors": [20.0, 200.0],
        "scores": [0.99, 0.95],
        "ranks": [1, 2],
        "positive_counts": [1, 1],
        "gallery_size": 2,
        "groups": ["a", "b"],
    }
    result = summarize_rows(rows, None)
    assert result["raw"]["accuracy_100m"] == 0.5
    assert result["product"]["accepted_count"] == 0
    assert result["product"]["conditional_accuracy_100m"] is None


def test_inference_cannot_read_an_unregistered_private_manifest(tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    policy = bundle / "policy.json"
    policy.write_text(json.dumps({"threshold": 0.9}))
    private = bundle / "private.parquet"
    private.write_bytes(b"must not be read as parquet")
    with pytest.raises(SealError, match="neither registered development"):
        authorize(bundle, policy, private, bundle / "output")
