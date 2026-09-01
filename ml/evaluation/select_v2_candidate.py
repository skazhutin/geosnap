"""Freeze one preregistered v2 candidate from calibration reports only."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np


class CandidateSelectionError(RuntimeError):
    """Experiment inputs do not match the preregistered calibration protocol."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _metric(report: Mapping[str, Any], path: Sequence[str]) -> float:
    value: Any = report
    for key in path:
        if not isinstance(value, Mapping) or key not in value:
            raise CandidateSelectionError(f"report is missing metric {'.'.join(path)}")
        value = value[key]
    return float(value)


def _success_by_query(report: Mapping[str, Any], *, k: int) -> tuple[list[str], np.ndarray]:
    rows = report.get("per_query")
    if not isinstance(rows, list) or not rows:
        raise CandidateSelectionError("per_query retrieval diagnostics are required")
    ids: list[str] = []
    successes: list[bool] = []
    for row in rows:
        rank = row["retrieval_diagnostics"]["by_positive_distance_m"]["100"]["positive_rank"]
        ids.append(str(row["query_id"]))
        successes.append(rank is not None and int(rank) <= k)
    return ids, np.asarray(successes, dtype=np.float64)


def _paired_bootstrap(
    baseline_report: Mapping[str, Any],
    candidate_report: Mapping[str, Any],
    *,
    k: int,
    samples: int = 5000,
) -> dict[str, float | int]:
    baseline_ids, baseline = _success_by_query(baseline_report, k=k)
    candidate_ids, candidate = _success_by_query(candidate_report, k=k)
    if baseline_ids != candidate_ids:
        raise CandidateSelectionError("paired bootstrap requires identical ordered calibration query IDs")
    differences = candidate - baseline
    generator = np.random.default_rng(20260901 + k)
    sampled = differences[generator.integers(0, len(differences), size=(samples, len(differences)))].mean(axis=1)
    return {
        "samples": samples,
        "point_gain": float(np.mean(differences)),
        "ci95_low": float(np.percentile(sampled, 2.5)),
        "ci95_high": float(np.percentile(sampled, 97.5)),
    }


def _major_stratum_regression(
    baseline_report: Mapping[str, Any], candidate_report: Mapping[str, Any]
) -> dict[str, Any]:
    regressions: list[dict[str, Any]] = []
    for dimension in ("query_provider", "region_h3_coarse"):
        baseline = baseline_report["primary"]["retrieval_diagnostics"]["slices"][dimension]
        candidate = candidate_report["primary"]["retrieval_diagnostics"]["slices"][dimension]
        for label in sorted(set(baseline) & set(candidate)):
            if min(int(baseline[label]["queries"]), int(candidate[label]["queries"])) < 10:
                continue
            regressions.append(
                {
                    "dimension": dimension,
                    "label": label,
                    "queries": int(candidate[label]["queries"]),
                    "recall_at_20_gain": float(candidate[label]["recall_at_20"])
                    - float(baseline[label]["recall_at_20"]),
                }
            )
    worst = min((row["recall_at_20_gain"] for row in regressions), default=0.0)
    return {
        "evaluated": regressions,
        "worst_recall_at_20_gain": worst,
        "no_unexplained_material_regression": worst >= -0.05,
    }


def select_candidate(
    protocol: Mapping[str, Any],
    reports: Mapping[str, tuple[Mapping[str, Any], Path]],
) -> dict[str, Any]:
    baseline = protocol.get("baseline")
    candidates = protocol.get("candidates")
    if not isinstance(baseline, Mapping) or not isinstance(candidates, list):
        raise CandidateSelectionError("protocol baseline/candidates are invalid")
    configurations = [baseline, *candidates]
    expected_ids = [str(configuration["id"]) for configuration in configurations]
    if set(reports) != set(expected_ids):
        raise CandidateSelectionError("reports must exactly cover the preregistered baseline and candidates")
    expected_query_hash = str(protocol["dataset"]["calibration_sha256"])
    baseline_report = reports[str(baseline["id"])][0]
    rows: list[dict[str, Any]] = []
    for configuration in configurations:
        experiment_id = str(configuration["id"])
        report, path = reports[experiment_id]
        query_profile = report.get("dataset", {}).get("queries", {})
        if query_profile.get("manifest_sha256") != expected_query_hash:
            raise CandidateSelectionError(f"{experiment_id} does not use the preregistered calibration manifest")
        if Path(str(query_profile.get("manifest_path", ""))).name != "calibration_queries.parquet":
            raise CandidateSelectionError(f"{experiment_id} is not calibration-only")
        if report.get("model", {}).get("model_name") != configuration.get("retriever"):
            raise CandidateSelectionError(f"{experiment_id} retriever does not match the protocol")
        if report.get("localization", {}).get("estimator") != configuration.get("estimator"):
            raise CandidateSelectionError(f"{experiment_id} estimator does not match the protocol")
        if report.get("retrieval_configuration", {}).get("query_aggregation") != configuration.get(
            "query_aggregation"
        ):
            raise CandidateSelectionError(f"{experiment_id} query aggregation does not match the protocol")
        recall_at_10 = _metric(report, ("primary", "retrieval", "recall_at", "10"))
        recall_at_20 = _metric(report, ("primary", "retrieval", "recall_at", "20"))
        bootstrap_10 = _paired_bootstrap(baseline_report, report, k=10)
        bootstrap_20 = _paired_bootstrap(baseline_report, report, k=20)
        strata = _major_stratum_regression(baseline_report, report)
        rows.append(
            {
                "id": experiment_id,
                "configuration": dict(configuration),
                "production_eligible": bool(configuration.get("production_eligible", True)),
                "report_path": str(path),
                "report_sha256": _sha256(path),
                "recall_at_10": recall_at_10,
                "recall_at_20": recall_at_20,
                "localization_accuracy_within_100m_all_queries": _metric(
                    report,
                    ("primary", "localization", "accuracy_within_m", "100"),
                ),
                "paired_bootstrap_recall_at_10": bootstrap_10,
                "paired_bootstrap_recall_at_20": bootstrap_20,
                "major_strata": strata,
            }
        )
    baseline_row = next(row for row in rows if row["id"] == baseline["id"])
    material_floor = float(protocol["selection"]["material_improvement_absolute"])
    for row in rows:
        row["recall_at_10_gain"] = row["recall_at_10"] - baseline_row["recall_at_10"]
        row["recall_at_20_gain"] = row["recall_at_20"] - baseline_row["recall_at_20"]
        row["material_gate_passed"] = (
            (row["recall_at_10_gain"] >= material_floor or row["recall_at_20_gain"] >= material_floor)
            and (
                row["paired_bootstrap_recall_at_10"]["ci95_low"] > 0
                or row["paired_bootstrap_recall_at_20"]["ci95_low"] > 0
            )
            and row["major_strata"]["no_unexplained_material_regression"]
        )
    evidence_best = max(
        rows,
        key=lambda row: (
            (row["recall_at_10"] + row["recall_at_20"]) / 2.0,
            row["localization_accuracy_within_100m_all_queries"],
            row["id"] == baseline["id"],
        ),
    )
    eligible = [row for row in rows if row["production_eligible"]]
    if not eligible:
        raise CandidateSelectionError("protocol must contain a production-eligible configuration")
    passing = [
        row
        for row in eligible
        if row["id"] != baseline["id"] and row["material_gate_passed"]
    ]
    credible_nonmaterial = [
        row
        for row in eligible
        if row["id"] != baseline["id"]
        and row["major_strata"]["no_unexplained_material_regression"]
        and (
            row["paired_bootstrap_recall_at_10"]["ci95_low"] > 0
            or row["paired_bootstrap_recall_at_20"]["ci95_low"] > 0
        )
    ]
    fallback = credible_nonmaterial or [baseline_row]
    selected = max(
        passing or fallback,
        key=lambda row: (
            (row["recall_at_10"] + row["recall_at_20"]) / 2.0,
            row["localization_accuracy_within_100m_all_queries"],
            row["id"] == baseline["id"],
        ),
    )
    primary_gain = selected["recall_at_10"] - baseline_row["recall_at_10"]
    recall_at_20_gain = selected["recall_at_20"] - baseline_row["recall_at_20"]
    secondary_gain = (
        selected["localization_accuracy_within_100m_all_queries"]
        - baseline_row["localization_accuracy_within_100m_all_queries"]
    )
    return {
        "schema_version": 1,
        "selection_scope": "calibration_only",
        "protocol_id": protocol["protocol_id"],
        "dataset_bundle_fingerprint": protocol["dataset"]["bundle_fingerprint"],
        "experiments": rows,
        "baseline_id": baseline["id"],
        "best_evidence_id": evidence_best["id"],
        "selected_id": selected["id"],
        "selected_configuration": selected["configuration"],
        "primary_gain_absolute": primary_gain,
        "recall_at_20_gain_absolute": recall_at_20_gain,
        "secondary_gain_absolute": secondary_gain,
        "material_improvement_floor": material_floor,
        "material_gate_passed": bool(selected["material_gate_passed"]),
        "fallback_policy": (
            "material_gate_winner"
            if passing
            else (
                "credible_nonmaterial_without_major_stratum_regression"
                if credible_nonmaterial
                else "unchanged_baseline"
            )
        ),
        "test_metrics_read": False,
    }


def select_from_files(
    protocol_path: str | Path,
    report_specs: Sequence[str],
    output_path: str | Path,
) -> dict[str, Any]:
    protocol_file = Path(protocol_path).resolve()
    protocol = json.loads(protocol_file.read_text(encoding="utf-8"))
    reports: dict[str, tuple[Mapping[str, Any], Path]] = {}
    for spec in report_specs:
        experiment_id, separator, raw_path = spec.partition("=")
        if not separator or not experiment_id or not raw_path or experiment_id in reports:
            raise CandidateSelectionError("each --report must be a unique ID=JSON_PATH")
        path = Path(raw_path).resolve()
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, Mapping):
            raise CandidateSelectionError(f"report {path} must be a JSON object")
        reports[experiment_id] = (loaded, path)
    result = select_candidate(protocol, reports)
    result["protocol_path"] = str(protocol_file)
    result["protocol_sha256"] = _sha256(protocol_file)
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, destination)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--report", action="append", required=True, help="experiment_id=report.json")
    parser.add_argument("--output-json", type=Path, required=True)
    args = parser.parse_args()
    result = select_from_files(args.protocol, args.report, args.output_json)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
