from __future__ import annotations

from ml.evaluation.product_recovery import hard_gate_audit, select_operating_point


def _rows(count: int = 100) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(count):
        correct = index < int(count * 0.90)
        rows.append(
            {
                "query_id": str(index),
                "source": "mapillary" if index % 2 else "kartaview",
                "region": f"region-{index % 3}",
                "correct_100m": correct,
                "catastrophic_500m": False,
                "error_m": 20.0 if correct else 200.0,
                "features": {
                    "top1_similarity": 0.8,
                    "geographic_mode_margin": 0.5,
                    "winning_candidate_count": 3.0,
                },
                "diagnostics": {"winning_cluster_mass": 0.7},
            }
        )
    return rows


def test_operating_point_maximizes_answer_rate_under_empirical_floor() -> None:
    rows = _rows()
    scores = [1.0 - index / 100.0 for index in range(100)]

    result = select_operating_point(rows, scores, precision_floor=0.90)

    assert result["feasible"] is True
    assert result["selected"]["answered_count"] == 100
    assert result["selected"]["conditional_accuracy_within_m"]["100"] == 0.90


def test_operating_point_does_not_fail_closed_on_wilson_endpoint() -> None:
    rows = _rows(40)
    scores = [0.9] * 40

    result = select_operating_point(rows, scores, precision_floor=0.90)

    assert result["selected"]["answered_count"] == 40
    assert result["selected"]["conditional_accuracy_lte_100m_wilson_95"][0] < 0.90


def test_hard_gate_audit_counts_correct_rejections() -> None:
    rows = _rows(40)
    rows[0]["features"]["top1_similarity"] = 0.1  # type: ignore[index]
    scores = [0.9] * 40

    audit = hard_gate_audit(rows, scores, confidence_threshold=0.5)

    assert audit["out_of_coverage_similarity"]["rejected_count"] == 1
    assert audit["out_of_coverage_similarity"]["correct_localizations_rejected"] == 1
