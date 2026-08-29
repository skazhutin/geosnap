from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ml.evaluation.calibrate_confidence import (
    ConfidenceCalibrationError,
    calibrate_confidence,
    calibrate_confidence_payload,
)


def _row(
    query_id: str,
    *,
    confidence: float,
    error_m: float | None,
    status: str = "ok",
    reasons: list[str] | None = None,
    split: str = "calibration",
) -> dict[str, Any]:
    return {
        "query_id": query_id,
        "confidence": confidence,
        "error_m": error_m,
        "status": status,
        "reasons": reasons or [],
        "evaluation_split": split,
    }


def _benchmark(rows: list[dict[str, Any]], *, base_threshold: float = 0.0) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "benchmark_kind": "real_moscow_street_view",
        "dataset": {
            "fingerprint_sha256": "a" * 64,
            "queries": {
                "manifest_path": "/fixtures/calibration_queries.parquet",
                "manifest_sha256": "b" * 64,
                "count": len(rows),
                "columns": ["id", "evaluation_split"],
            },
        },
        "leakage_audit": {"passed": True},
        "model": {"model_name": "fixture-model"},
        "localization": {"confidence_threshold": base_threshold},
        "per_query": rows,
    }


def test_safety_first_curve_keeps_non_threshold_rejections_abstained() -> None:
    report = _benchmark(
        [
            _row("good-high", confidence=0.90, error_m=20.0),
            _row("bad-mid", confidence=0.80, error_m=180.0),
            _row("good-low", confidence=0.60, error_m=40.0),
            _row(
                "structural-high",
                confidence=0.95,
                error_m=5.0,
                status="low_confidence",
                reasons=["winning_geographic_mode_has_insufficient_support"],
            ),
            _row(
                "ooc",
                confidence=0.0,
                error_m=None,
                status="out_of_coverage",
                reasons=["no_retrieval_candidates"],
            ),
        ]
    )

    calibrated = calibrate_confidence_payload(report)

    assert [row["threshold"] for row in calibrated["curve"]] == [
        0.0,
        0.6,
        0.8,
        0.9,
        0.95,
        1.0,
    ]
    assert calibrated["chosen_threshold"] == 0.9
    assert calibrated["zero_false_confident_achieved"] is True
    assert calibrated["eligibility"] == {
        "eligible_query_count": 3,
        "non_threshold_rejected_query_count": 2,
        "non_threshold_rejection_reasons": {
            "no_retrieval_candidates": 1,
            "winning_geographic_mode_has_insufficient_support": 1,
        },
    }
    chosen = calibrated["chosen"]
    assert chosen["answered_count"] == 1
    assert chosen["answer_rate"] == pytest.approx(0.2)
    assert chosen["unconditional_accuracy_within_m"] == {
        "25": pytest.approx(0.2),
        "50": pytest.approx(0.2),
        "100": pytest.approx(0.2),
    }
    assert chosen["conditional_accuracy_within_m"] == {
        "25": 1.0,
        "50": 1.0,
        "100": 1.0,
    }
    assert chosen["false_confident_errors"]["count"] == 0


def test_higher_threshold_breaks_a_fully_equal_safety_and_accuracy_tie() -> None:
    report = _benchmark(
        [
            _row("low", confidence=0.4, error_m=10.0),
            _row("high", confidence=0.8, error_m=20.0),
        ]
    )

    calibrated = calibrate_confidence_payload(report)

    # Thresholds 0.0 and 0.4 answer the same rows; the explicit final tie-break
    # chooses the higher policy threshold.
    assert calibrated["chosen_threshold"] == 0.4
    assert calibrated["chosen"]["answer_rate"] == 1.0
    assert calibrated["chosen"]["unconditional_accuracy_within_m"]["100"] == 1.0


def test_nonzero_base_threshold_fails_closed_unless_explicitly_allowed() -> None:
    report = _benchmark(
        [
            _row(
                "threshold-only",
                confidence=0.4,
                error_m=10.0,
                status="low_confidence",
                reasons=["confidence_below_threshold"],
            ),
            _row("high", confidence=0.9, error_m=20.0),
        ],
        base_threshold=0.5,
    )

    with pytest.raises(ConfidenceCalibrationError, match="confidence_threshold=0.0"):
        calibrate_confidence_payload(report)

    calibrated = calibrate_confidence_payload(report, allow_base_threshold=True)
    assert calibrated["chosen_threshold"] == 0.4
    assert calibrated["chosen"]["answered_count"] == 2


@pytest.mark.parametrize(
    "mutate",
    [
        lambda report: report["per_query"][0].update(evaluation_split="test"),
        lambda report: report["dataset"]["queries"].update(
            manifest_path="/fixtures/test_queries.parquet"
        ),
    ],
)
def test_test_scope_is_rejected(mutate: Any) -> None:
    report = _benchmark([_row("query", confidence=0.8, error_m=10.0)])
    mutate(report)

    with pytest.raises(ConfidenceCalibrationError, match="test|calibration"):
        calibrate_confidence_payload(report)


def test_canonical_manifest_fallback_is_strict_when_row_labels_are_unavailable() -> None:
    report = _benchmark([_row("query", confidence=0.8, error_m=10.0)])
    report["per_query"][0].pop("evaluation_split")
    calibrated = calibrate_confidence_payload(report)
    assert calibrated["calibration_scope"]["evidence"] == (
        "canonical_manifest_name_and_split_column"
    )

    report["dataset"]["queries"]["manifest_path"] = "/fixtures/queries.parquet"
    with pytest.raises(ConfidenceCalibrationError, match="cannot prove calibration-only"):
        calibrate_confidence_payload(report)


def test_file_entrypoint_writes_auditable_json_and_markdown(tmp_path: Path) -> None:
    report = _benchmark(
        [
            _row("good", confidence=0.7, error_m=15.0),
            _row("bad", confidence=0.5, error_m=150.0),
        ]
    )
    source = tmp_path / "calibration_benchmark.json"
    source.write_text(json.dumps(report), encoding="utf-8")

    payload, json_path, markdown_path = calibrate_confidence(
        source, tmp_path / "reports", stem="fixture"
    )

    written = json.loads(json_path.read_text(encoding="utf-8"))
    assert written == payload
    assert written["source_benchmark"]["report_path"] == str(source.resolve())
    assert len(written["source_benchmark"]["report_sha256"]) == 64
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "No test result is read" in markdown
    assert "Chosen threshold: **0.7**" in markdown
    assert "Accuracy <= 100 m (all queries)" in markdown
