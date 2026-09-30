import json

import pandas as pd
import pytest

from ml.research.geolocatability_v1.analyze import TECHNICAL, summary
from ml.research.geolocatability_v1.annotate import NUMERIC, validate
from ml.research.geolocatability_v1.freeze import aggregate


def example(geo=0.5, reason="NO_LANDMARKS"):
    value = {key: 0.0 for key in NUMERIC}
    value["geolocatability"] = geo
    value.update({"usable_single_photo": True, "retake_recommended": True,
                  "retake_reason": reason, "short_reason": "Generic street without signs"})
    return value


def test_strict_schema_and_bounded_scores():
    assert validate(json.dumps(example()))["geolocatability"] == 0.5
    broken = example()
    broken["geolocatability"] = 1.2
    with pytest.raises(ValueError):
        validate(json.dumps(broken))
    broken = example()
    broken.pop("motion_blur")
    with pytest.raises(ValueError):
        validate(json.dumps(broken))


def test_unrecognized_free_text_reason_is_explicit_other():
    raw = example(reason="Too many trees to see the street")
    assert validate(json.dumps(raw))["retake_reason"] == "OTHER"
    assert raw["retake_reason"] != "OTHER"


def test_three_pass_aggregation_preserves_disagreement():
    a, b, c = example(0.2), example(0.5), example(0.8)
    b["retake_reason"] = "OTHER"
    c["retake_recommended"] = False
    c["retake_reason"] = None
    result = aggregate([a, b, c])
    assert result["geolocatability_median"] == 0.5
    assert result["retake_recommended"] is True
    assert result["retake_reason_consensus"] is None
    assert result["annotation_uncertain"] is True
    assert result["high_consensus"] is False


def test_analysis_summary_accepts_frozen_shaped_rows_without_real_outcomes():
    rows = []
    for bucket, geo in zip(("correct", "ranking_failure", "retrieval_failure", "no_coverage"),
                           (0.8, 0.6, 0.3, 0.2), strict=True):
        row = aggregate([example(geo)] * 3)
        row.update({key: 1.0 for key in TECHNICAL})
        row.update({"annotation_status": "valid", "failure_bucket": bucket,
                    "baseline_correct": bucket == "correct",
                    "top100_positive": bucket in ("correct", "ranking_failure")})
        rows.append(row)
    result = summary(pd.DataFrame(rows))
    assert result["n_valid"] == 4
    assert result["baseline_correct_all"] == 1
    assert result["top100_positive_all"] == 2
