from __future__ import annotations

from pathlib import Path

import pytest

from ml.evaluation.select_v2_candidate import CandidateSelectionError, select_candidate


def _protocol() -> dict[str, object]:
    return {
        "protocol_id": "fixture",
        "dataset": {"bundle_fingerprint": "a" * 64, "calibration_sha256": "b" * 64},
        "baseline": {
            "id": "baseline",
            "retriever": "megaloc",
            "estimator": "weighted_medoid",
            "query_aggregation": "single",
        },
        "candidates": [
            {
                "id": "candidate",
                "retriever": "megaloc",
                "estimator": "top1",
                "query_aggregation": "single",
            }
        ],
        "selection": {"material_improvement_absolute": 0.05},
    }


def _report(*, estimator: str, localization: float) -> dict[str, object]:
    return {
        "dataset": {
            "queries": {
                "manifest_path": "/fixture/calibration_queries.parquet",
                "manifest_sha256": "b" * 64,
            }
        },
        "model": {"model_name": "megaloc"},
        "localization": {"estimator": estimator},
        "retrieval_configuration": {"query_aggregation": "single"},
        "primary": {
            "retrieval": {"recall_at": {"10": 0.3, "20": 0.4}},
            "retrieval_diagnostics": {
                "slices": {
                    "query_provider": {
                        "fixture": {"queries": 20, "recall_at_20": 0.4}
                    },
                    "region_h3_coarse": {
                        "region": {"queries": 20, "recall_at_20": 0.4}
                    },
                }
            },
            "localization": {"accuracy_within_m": {"100": localization}},
        },
        "per_query": [
            {
                "query_id": f"query-{index}",
                "retrieval_diagnostics": {
                    "by_positive_distance_m": {"100": {"positive_rank": 1 if index < 8 else 100}}
                },
            }
            for index in range(20)
        ],
    }


def test_keeps_baseline_when_nonmaterial_candidate_lacks_positive_evidence(tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text("baseline", encoding="utf-8")
    candidate_path.write_text("candidate", encoding="utf-8")
    result = select_candidate(
        _protocol(),
        {
            "baseline": (_report(estimator="weighted_medoid", localization=0.1), baseline_path),
            "candidate": (_report(estimator="top1", localization=0.16), candidate_path),
        },
    )

    assert result["selected_id"] == "baseline"
    assert result["material_gate_passed"] is False
    assert result["fallback_policy"] == "unchanged_baseline"
    assert result["test_metrics_read"] is False


def test_selects_preregistered_candidate_that_clears_material_gate(tmp_path: Path) -> None:
    baseline = _report(estimator="weighted_medoid", localization=0.1)
    candidate = _report(estimator="top1", localization=0.16)
    candidate["primary"]["retrieval"]["recall_at"] = {"10": 0.6, "20": 0.6}
    for index, row in enumerate(candidate["per_query"]):
        row["retrieval_diagnostics"]["by_positive_distance_m"]["100"]["positive_rank"] = (
            1 if index < 16 else 100
        )
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text("baseline", encoding="utf-8")
    candidate_path.write_text("candidate", encoding="utf-8")

    result = select_candidate(
        _protocol(),
        {
            "baseline": (baseline, baseline_path),
            "candidate": (candidate, candidate_path),
        },
    )

    assert result["selected_id"] == "candidate"
    assert result["material_gate_passed"] is True
    assert result["fallback_policy"] == "material_gate_winner"


def test_selection_rejects_noncanonical_query_scope(tmp_path: Path) -> None:
    report = _report(estimator="weighted_medoid", localization=0.1)
    report["dataset"]["queries"]["manifest_path"] = "/fixture/test_queries.parquet"
    path = tmp_path / "report.json"
    path.write_text("report", encoding="utf-8")
    with pytest.raises(CandidateSelectionError, match="calibration-only"):
        select_candidate(
            _protocol(),
            {
                "baseline": (report, path),
                "candidate": (_report(estimator="top1", localization=0.1), path),
            },
        )
