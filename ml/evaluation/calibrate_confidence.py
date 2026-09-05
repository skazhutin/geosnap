"""Offline, calibration-only selection of the Moscow confidence threshold.

The calibrator consumes a completed :mod:`ml.evaluation.moscow_benchmark`
JSON report.  It never embeds images or accesses a test manifest.  Thresholds
are evaluated only against the report's original calibration queries and a
row rejected for any reason other than ``confidence_below_threshold`` remains
an abstention at every candidate threshold.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

BENCHMARK_KIND = "real_moscow_street_view"
CALIBRATION_KIND = "moscow_confidence_threshold_calibration"
REPORT_SCHEMA_VERSION = 1
ERROR_THRESHOLDS_M = (25, 50, 100)
THRESHOLD_REASON = "confidence_below_threshold"
EXPECTED_CALIBRATION_MANIFEST = "calibration_queries.parquet"
DEFAULT_WILSON_Z_95 = 1.959963984540054


class ConfidenceCalibrationError(RuntimeError):
    """The benchmark report cannot safely be used for calibration."""


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfidenceCalibrationError(f"{name} must be a JSON object")
    return value


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ConfidenceCalibrationError(f"{name} must be a finite number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfidenceCalibrationError(f"{name} must be a finite number") from exc
    if not math.isfinite(result):
        raise ConfidenceCalibrationError(f"{name} must be a finite number")
    return result


def _query_manifest(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    dataset = _mapping(payload.get("dataset"), "dataset")
    return _mapping(dataset.get("queries"), "dataset.queries")


def _split_counts(queries: Mapping[str, Any]) -> Mapping[str, Any] | None:
    for key in ("evaluation_split_counts", "split_counts"):
        value = queries.get(key)
        if value is not None:
            return _mapping(value, f"dataset.queries.{key}")
    return None


def _validate_calibration_scope(
    payload: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    """Prove from report-local evidence that no test query is being read."""

    queries = _query_manifest(payload)
    manifest_path_value = queries.get("manifest_path")
    manifest_path = "" if manifest_path_value is None else str(manifest_path_value).strip()
    manifest_name = Path(manifest_path).name.lower() if manifest_path else ""
    if "test" in manifest_name:
        raise ConfidenceCalibrationError(
            "refusing to calibrate from a test query manifest"
        )

    row_has_label = [row.get("evaluation_split") is not None for row in rows]
    if any(row_has_label):
        if not all(row_has_label):
            raise ConfidenceCalibrationError(
                "per_query evaluation_split labels are only partially populated"
            )
        labels = {
            str(row["evaluation_split"]).strip().lower()
            for row in rows
        }
        if labels != {"calibration"}:
            raise ConfidenceCalibrationError(
                "per_query must contain only evaluation_split='calibration'; "
                f"got {sorted(labels)}"
            )
        return {
            "passed": True,
            "evidence": "per_query_evaluation_split",
            "labels": ["calibration"],
            "query_manifest": manifest_path,
        }

    counts = _split_counts(queries)
    if counts is not None:
        normalized_counts: dict[str, int] = {}
        for raw_label, raw_count in counts.items():
            label = str(raw_label).strip().lower()
            try:
                count = int(raw_count)
            except (TypeError, ValueError) as exc:
                raise ConfidenceCalibrationError(
                    "dataset.queries split counts must be integers"
                ) from exc
            if count < 0:
                raise ConfidenceCalibrationError(
                    "dataset.queries split counts cannot be negative"
                )
            if count:
                normalized_counts[label] = count
        if set(normalized_counts) != {"calibration"}:
            raise ConfidenceCalibrationError(
                "query split counts must contain calibration rows only; "
                f"got {sorted(normalized_counts)}"
            )
        if normalized_counts["calibration"] != len(rows):
            raise ConfidenceCalibrationError(
                "calibration split count does not match per_query length"
            )
        return {
            "passed": True,
            "evidence": "dataset_query_split_counts",
            "labels": ["calibration"],
            "query_manifest": manifest_path,
        }

    columns_value = queries.get("columns")
    columns = (
        {str(column) for column in columns_value}
        if isinstance(columns_value, list)
        else set()
    )
    if manifest_name != EXPECTED_CALIBRATION_MANIFEST or "evaluation_split" not in columns:
        raise ConfidenceCalibrationError(
            "cannot prove calibration-only scope from the benchmark JSON: include "
            "per_query evaluation_split labels (preferred), query split counts, or "
            f"the canonical {EXPECTED_CALIBRATION_MANIFEST!r} manifest with an "
            "evaluation_split column"
        )
    return {
        "passed": True,
        "evidence": "canonical_manifest_name_and_split_column",
        "labels": ["calibration"],
        "query_manifest": manifest_path,
    }


def _validate_base_threshold(
    payload: Mapping[str, Any], *, allow_base_threshold: bool
) -> float:
    localization = _mapping(payload.get("localization"), "localization")
    if "confidence_threshold" not in localization:
        raise ConfidenceCalibrationError(
            "localization.confidence_threshold is required"
        )
    threshold = _finite_float(
        localization["confidence_threshold"],
        "localization.confidence_threshold",
    )
    if not 0.0 <= threshold <= 1.0:
        raise ConfidenceCalibrationError(
            "localization.confidence_threshold must be in [0, 1]"
        )
    if threshold != 0.0 and not allow_base_threshold:
        raise ConfidenceCalibrationError(
            "confidence calibration requires a benchmark run with "
            "confidence_threshold=0.0; pass --allow-base-threshold only for an "
            "explicitly audited non-zero base run"
        )
    return threshold


def _validated_rows(
    payload: Mapping[str, Any], *, base_threshold: float
) -> tuple[list[dict[str, Any]], Counter[str]]:
    rows_value = payload.get("per_query")
    if not isinstance(rows_value, list) or not rows_value:
        raise ConfidenceCalibrationError("per_query must be a non-empty JSON array")

    allowed_statuses = {"ok", "low_confidence", "out_of_coverage"}
    rows: list[dict[str, Any]] = []
    query_ids: set[str] = set()
    excluded_reasons: Counter[str] = Counter()
    for index, raw_row in enumerate(rows_value):
        row = _mapping(raw_row, f"per_query[{index}]")
        query_id = str(row.get("query_id", "")).strip()
        if not query_id:
            raise ConfidenceCalibrationError(
                f"per_query[{index}].query_id must be non-empty"
            )
        if query_id in query_ids:
            raise ConfidenceCalibrationError(f"duplicate query_id {query_id!r}")
        query_ids.add(query_id)

        confidence = _finite_float(
            row.get("confidence"), f"per_query[{index}].confidence"
        )
        if not 0.0 <= confidence <= 1.0:
            raise ConfidenceCalibrationError(
                f"per_query[{index}].confidence must be in [0, 1]"
            )
        status = str(row.get("status", "")).strip().lower()
        if status not in allowed_statuses:
            raise ConfidenceCalibrationError(
                f"per_query[{index}].status is unsupported: {status!r}"
            )
        reasons_value = row.get("reasons")
        if not isinstance(reasons_value, list) or any(
            not isinstance(reason, str) or not reason.strip()
            for reason in reasons_value
        ):
            raise ConfidenceCalibrationError(
                f"per_query[{index}].reasons must be an array of non-empty strings"
            )
        reasons = tuple(str(reason).strip() for reason in reasons_value)
        if len(reasons) != len(set(reasons)):
            raise ConfidenceCalibrationError(
                f"per_query[{index}].reasons contains duplicates"
            )
        non_threshold_reasons = tuple(
            reason for reason in reasons if reason != THRESHOLD_REASON
        )

        error_value = row.get("error_m")
        error_m = (
            None
            if error_value is None
            else _finite_float(error_value, f"per_query[{index}].error_m")
        )
        if error_m is not None and error_m < 0:
            raise ConfidenceCalibrationError(
                f"per_query[{index}].error_m cannot be negative"
            )

        threshold_reason_expected = status != "out_of_coverage" and confidence < base_threshold
        has_threshold_reason = THRESHOLD_REASON in reasons
        if threshold_reason_expected != has_threshold_reason:
            raise ConfidenceCalibrationError(
                f"per_query[{index}] threshold reason is inconsistent with the "
                f"benchmark base threshold {base_threshold:g}"
            )
        if status == "ok" and reasons:
            raise ConfidenceCalibrationError(
                f"per_query[{index}] status='ok' cannot carry rejection reasons"
            )
        if status == "low_confidence" and not reasons:
            raise ConfidenceCalibrationError(
                f"per_query[{index}] status='low_confidence' requires a reason"
            )
        if status == "out_of_coverage" and error_m is not None:
            raise ConfidenceCalibrationError(
                f"per_query[{index}] out_of_coverage must not carry error_m"
            )

        eligible = status != "out_of_coverage" and not non_threshold_reasons
        if eligible and error_m is None:
            raise ConfidenceCalibrationError(
                f"per_query[{index}] threshold-eligible row requires error_m"
            )
        if status == "ok" and not eligible:
            raise ConfidenceCalibrationError(
                f"per_query[{index}] status='ok' conflicts with non-threshold reasons"
            )
        if not eligible:
            for reason in non_threshold_reasons or reasons or (status,):
                excluded_reasons[reason] += 1

        rows.append(
            {
                "query_id": query_id,
                "confidence": confidence,
                "error_m": error_m,
                "input_status": status,
                "reasons": list(reasons),
                "eligible_for_thresholding": eligible,
                "evaluation_split": row.get("evaluation_split"),
            }
        )

    queries = _query_manifest(payload)
    if "count" in queries:
        try:
            declared_count = int(queries["count"])
        except (TypeError, ValueError) as exc:
            raise ConfidenceCalibrationError(
                "dataset.queries.count must be an integer"
            ) from exc
        if declared_count != len(rows):
            raise ConfidenceCalibrationError(
                "dataset.queries.count does not match per_query length"
            )
    return rows, excluded_reasons


def _curve_row(
    rows: Sequence[Mapping[str, Any]], threshold: float
) -> dict[str, Any]:
    total = len(rows)
    eligible = [row for row in rows if row["eligible_for_thresholding"]]
    answered = [
        row for row in eligible if float(row["confidence"]) >= threshold
    ]
    false_confident = [
        row for row in answered if float(row["error_m"]) > 100.0
    ]
    correct_within_100m = len(answered) - len(false_confident)
    wilson_lower, wilson_upper = _wilson_interval(correct_within_100m, len(answered))
    accepted_errors = sorted(float(row["error_m"]) for row in answered)
    unconditional: dict[str, float] = {}
    conditional: dict[str, float] = {}
    for distance in ERROR_THRESHOLDS_M:
        correct = sum(float(row["error_m"]) <= distance for row in answered)
        unconditional[str(distance)] = correct / total
        conditional[str(distance)] = correct / len(answered) if answered else 0.0
    failures = [
        {
            "query_id": str(row["query_id"]),
            "confidence": float(row["confidence"]),
            "error_m": float(row["error_m"]),
        }
        for row in false_confident
    ]
    return {
        "threshold": threshold,
        "query_count": total,
        "eligible_query_count": len(eligible),
        "answered_count": len(answered),
        "answer_rate": len(answered) / total,
        "correct_within_100m_count": correct_within_100m,
        "conditional_accuracy_within_100m_wilson_lower_95": wilson_lower,
        "conditional_accuracy_within_100m_wilson_interval_95": [wilson_lower, wilson_upper],
        "accepted_error_m": {
            "median": _percentile(accepted_errors, 50),
            "p90": _percentile(accepted_errors, 90),
            "p95": _percentile(accepted_errors, 95),
        },
        "conditional_accuracy_within_m": conditional,
        "unconditional_accuracy_within_m": unconditional,
        "false_confident_errors": {
            "definition": "answered_and_error>100m",
            "error_threshold_m": 100.0,
            "count": len(false_confident),
            "rate_all_queries": len(false_confident) / total,
            "rate_answered_queries": (
                len(false_confident) / len(answered) if answered else 0.0
            ),
            "errors": failures,
        },
    }


def _wilson_lower_bound(successes: int, trials: int, *, z: float = DEFAULT_WILSON_Z_95) -> float | None:
    """Return the one-sided lower endpoint of the conventional 95% Wilson interval."""

    return _wilson_interval(successes, trials, z=z)[0]


def _wilson_interval(
    successes: int, trials: int, *, z: float = DEFAULT_WILSON_Z_95
) -> tuple[float | None, float | None]:
    if trials == 0:
        return None, None
    if successes < 0 or successes > trials:
        raise ValueError("successes must be in [0, trials]")
    probability = successes / trials
    z_squared = z * z
    denominator = 1.0 + z_squared / trials
    center = probability + z_squared / (2.0 * trials)
    margin = z * math.sqrt((probability * (1.0 - probability) + z_squared / (4.0 * trials)) / trials)
    return (center - margin) / denominator, (center + margin) / denominator


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    position = (len(values) - 1) * percentile / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(values[lower])
    fraction = position - lower
    return float(values[lower] * (1.0 - fraction) + values[upper] * fraction)


def _confidence_quality(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    eligible = [row for row in rows if row["eligible_for_thresholding"]]
    if not eligible:
        return {"population": "threshold_eligible_queries", "count": 0, "brier": None, "ece_10_bin": None}
    probabilities = [float(row["confidence"]) for row in eligible]
    outcomes = [float(row["error_m"]) <= 100.0 for row in eligible]
    brier = sum((probability - float(outcome)) ** 2 for probability, outcome in zip(probabilities, outcomes, strict=True)) / len(
        eligible
    )
    reliability: list[dict[str, Any]] = []
    ece = 0.0
    for lower_index in range(10):
        lower = lower_index / 10.0
        upper = (lower_index + 1) / 10.0
        selected = [
            index
            for index, probability in enumerate(probabilities)
            if lower <= probability < upper or (upper == 1.0 and probability == 1.0)
        ]
        if not selected:
            continue
        mean_confidence = sum(probabilities[index] for index in selected) / len(selected)
        accuracy = sum(outcomes[index] for index in selected) / len(selected)
        ece += len(selected) / len(eligible) * abs(mean_confidence - accuracy)
        reliability.append(
            {
                "min_confidence": lower,
                "max_confidence": upper,
                "count": len(selected),
                "mean_confidence": mean_confidence,
                "accuracy_within_100m": accuracy,
            }
        )
    return {
        "population": "threshold_eligible_queries",
        "count": len(eligible),
        "outcome": "localization_error<=100m",
        "brier": brier,
        "ece_10_bin": ece,
        "reliability": reliability,
    }


def _selection_key(row: Mapping[str, Any]) -> tuple[float, float, float, float]:
    """Lexicographic safety objective; lower tuple is better."""

    return (
        float(_mapping(row["false_confident_errors"], "curve.false_confident_errors")["count"]),
        -float(_mapping(row["unconditional_accuracy_within_m"], "curve.unconditional")["100"]),
        -float(row["answer_rate"]),
        -float(row["threshold"]),
    )


def calibrate_confidence_payload(
    benchmark: Mapping[str, Any],
    *,
    allow_base_threshold: bool = False,
    minimum_conditional_accuracy_100m_wilson_lower_95: float | None = None,
) -> dict[str, Any]:
    """Calibrate one threshold without reading images or any test artifact."""

    if benchmark.get("benchmark_kind") != BENCHMARK_KIND:
        raise ConfidenceCalibrationError(
            f"benchmark_kind must be {BENCHMARK_KIND!r}"
        )
    leakage = _mapping(benchmark.get("leakage_audit"), "leakage_audit")
    if leakage.get("passed") is not True:
        raise ConfidenceCalibrationError(
            "benchmark leakage_audit must explicitly pass"
        )
    base_threshold = _validate_base_threshold(
        benchmark, allow_base_threshold=allow_base_threshold
    )
    rows, excluded_reasons = _validated_rows(
        benchmark, base_threshold=base_threshold
    )
    scope = _validate_calibration_scope(benchmark, rows)

    candidates = sorted(
        {0.0, 1.0, *(float(row["confidence"]) for row in rows)}
    )
    curve = [_curve_row(rows, threshold) for threshold in candidates]
    if minimum_conditional_accuracy_100m_wilson_lower_95 is None:
        chosen = min(curve, key=_selection_key)
        qualified = curve
        objective = {
            "name": "legacy_safety_first_lexicographic",
            "feasible": True,
            "minimum_conditional_accuracy_100m_wilson_lower_95": None,
        }
    else:
        target = float(minimum_conditional_accuracy_100m_wilson_lower_95)
        if not 0.0 <= target <= 1.0:
            raise ValueError("minimum Wilson lower bound must be in [0, 1]")
        qualified = [
            row
            for row in curve
            if row["conditional_accuracy_within_100m_wilson_lower_95"] is not None
            and float(row["conditional_accuracy_within_100m_wilson_lower_95"]) >= target
        ]
        chosen = (
            max(
                qualified,
                key=lambda row: (
                    float(row["answer_rate"]),
                    float(row["conditional_accuracy_within_m"]["100"]),
                    -float(row["threshold"]),
                ),
            )
            if qualified
            else None
        )
        objective = {
            "name": "maximize_answer_rate_subject_to_wilson_precision_floor",
            "feasible": chosen is not None,
            "minimum_conditional_accuracy_100m_wilson_lower_95": target,
            "confidence_level": 0.95,
            "wilson_z": DEFAULT_WILSON_Z_95,
            "qualified_candidate_count": len(qualified),
        }
    queries = _query_manifest(benchmark)
    dataset = _mapping(benchmark.get("dataset"), "dataset")
    model = _mapping(benchmark.get("model"), "model")
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "calibration_kind": CALIBRATION_KIND,
        "source_benchmark": {
            "benchmark_kind": benchmark["benchmark_kind"],
            "schema_version": benchmark.get("schema_version"),
            "dataset_fingerprint_sha256": dataset.get("fingerprint_sha256"),
            "query_manifest_path": queries.get("manifest_path"),
            "query_manifest_sha256": queries.get("manifest_sha256"),
            "query_count": len(rows),
            "model_name": model.get("model_name"),
            "base_confidence_threshold": base_threshold,
            "allow_base_threshold": allow_base_threshold,
        },
        "calibration_scope": scope,
        "policy": {
            "candidate_thresholds": "sorted unique {0, 1, observed per-query confidences}",
            "answered_definition": (
                "no non-threshold rejection reason and confidence >= threshold"
            ),
            "non_threshold_rejections_remain_abstentions": True,
            "selection_order": [
                "minimize_false_confident_errors_over_100m",
                "maximize_unconditional_accuracy_within_100m",
                "maximize_answer_rate",
                "prefer_higher_threshold",
            ],
            "accuracy_denominators": {
                "unconditional": "all_calibration_queries",
                "conditional": "answered_calibration_queries",
            },
            "objective": objective,
        },
        "eligibility": {
            "eligible_query_count": sum(
                bool(row["eligible_for_thresholding"]) for row in rows
            ),
            "non_threshold_rejected_query_count": sum(
                not bool(row["eligible_for_thresholding"]) for row in rows
            ),
            "non_threshold_rejection_reasons": dict(sorted(excluded_reasons.items())),
        },
        "candidate_count": len(curve),
        "curve": curve,
        "confidence_quality": _confidence_quality(rows),
        "chosen_threshold": None if chosen is None else chosen["threshold"],
        "chosen": chosen,
        "zero_false_confident_achieved": (
            None if chosen is None else chosen["false_confident_errors"]["count"] == 0
        ),
    }


def _percent(value: Any) -> str:
    return f"{float(value) * 100:.2f}%"


def _markdown_report(payload: Mapping[str, Any]) -> str:
    source = _mapping(payload["source_benchmark"], "source_benchmark")
    raw_chosen = payload["chosen"]
    if raw_chosen is None:
        objective = _mapping(_mapping(payload["policy"], "policy")["objective"], "policy.objective")
        return "\n".join(
            [
                "# Moscow confidence-threshold calibration",
                "",
                "> This report uses calibration queries only. No test result is read or used for threshold selection.",
                "",
                "## No feasible operating point",
                "",
                f"No threshold achieved the required 95% Wilson lower bound of "
                f"{float(objective['minimum_conditional_accuracy_100m_wilson_lower_95']):.2%} "
                "for conditional accuracy within 100 m. The system must not claim a calibrated threshold from this run.",
                "",
                f"- Model: `{source.get('model_name')}`",
                f"- Calibration query manifest: `{source.get('query_manifest_path')}`",
                f"- Query count: {source['query_count']}",
                "",
            ]
        )
    chosen = _mapping(raw_chosen, "chosen")
    chosen_unconditional = _mapping(
        chosen["unconditional_accuracy_within_m"], "chosen.unconditional"
    )
    chosen_conditional = _mapping(
        chosen["conditional_accuracy_within_m"], "chosen.conditional"
    )
    false_confident = _mapping(
        chosen["false_confident_errors"], "chosen.false_confident_errors"
    )
    scope = _mapping(payload["calibration_scope"], "calibration_scope")
    lines = [
        "# Moscow confidence-threshold calibration",
        "",
        "> This report uses calibration queries only. No test result is read or used for threshold selection.",
        "",
        "## Provenance and safety gate",
        "",
        f"- Model: `{source.get('model_name')}`",
        f"- Calibration query manifest: `{source.get('query_manifest_path')}`",
        f"- Query manifest SHA-256: `{source.get('query_manifest_sha256')}`",
        f"- Dataset fingerprint: `{source.get('dataset_fingerprint_sha256')}`",
        f"- Query count: {source['query_count']}",
        f"- Base confidence threshold: `{source['base_confidence_threshold']}`",
        f"- Calibration-only evidence: `{scope['evidence']}`",
        "- Rows rejected for a non-threshold reason remain abstentions at every candidate threshold.",
        "",
        "## Chosen policy point",
        "",
        f"Chosen threshold: **{float(payload['chosen_threshold']):.12g}**",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Answered / all | {chosen['answered_count']} / {chosen['query_count']} |",
        f"| Answer rate | {_percent(chosen['answer_rate'])} |",
        f"| 95% Wilson lower bound, conditional <=100 m | "
        f"{_percent(chosen['conditional_accuracy_within_100m_wilson_lower_95'])} |",
    ]
    for distance in ERROR_THRESHOLDS_M:
        key = str(distance)
        lines.append(
            f"| Accuracy <= {distance} m (all queries) | {_percent(chosen_unconditional[key])} |"
        )
        lines.append(
            f"| Accuracy <= {distance} m (answered only) | {_percent(chosen_conditional[key])} |"
        )
    lines.extend(
        [
            f"| False-confident errors >100 m | {false_confident['count']} |",
            "",
            "## Deterministic threshold curve",
            "",
            "| Threshold | Answer rate | <=25 m all / answered | <=50 m all / answered | <=100 m all / answered | False-confident >100 m |",
            "|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for raw_row in payload["curve"]:
        row = _mapping(raw_row, "curve row")
        unconditional = _mapping(
            row["unconditional_accuracy_within_m"], "curve unconditional"
        )
        conditional = _mapping(
            row["conditional_accuracy_within_m"], "curve conditional"
        )
        false_errors = _mapping(
            row["false_confident_errors"], "curve false_confident_errors"
        )
        values = [
            f"{_percent(unconditional[str(distance)])} / {_percent(conditional[str(distance)])}"
            for distance in ERROR_THRESHOLDS_M
        ]
        lines.append(
            f"| {float(row['threshold']):.12g} | {_percent(row['answer_rate'])} | "
            f"{values[0]} | {values[1]} | {values[2]} | {false_errors['count']} |"
        )
    lines.extend(
        [
            "",
            "Selection is lexicographic: fewest false-confident errors, highest unconditional <=100 m accuracy, highest answer rate, then the higher threshold.",
            "",
        ]
    )
    return "\n".join(lines)


def write_confidence_calibration_reports(
    payload: Mapping[str, Any],
    output_dir: str | Path,
    *,
    stem: str = "moscow_confidence_calibration",
) -> tuple[Path, Path]:
    if not stem or Path(stem).name != stem:
        raise ValueError("stem must be a non-empty filename stem")
    destination = Path(output_dir)
    json_path = destination / f"{stem}.json"
    markdown_path = destination / f"{stem}.md"
    _atomic_text(
        json_path,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    _atomic_text(markdown_path, _markdown_report(payload))
    return json_path, markdown_path


def calibrate_confidence(
    benchmark_json: str | Path,
    output_dir: str | Path,
    *,
    stem: str = "moscow_confidence_calibration",
    allow_base_threshold: bool = False,
    minimum_conditional_accuracy_100m_wilson_lower_95: float | None = None,
) -> tuple[dict[str, Any], Path, Path]:
    input_path = Path(benchmark_json).expanduser().resolve()
    if not input_path.is_file():
        raise ConfidenceCalibrationError(f"benchmark JSON is missing: {input_path}")
    try:
        loaded = json.loads(input_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfidenceCalibrationError(
            f"cannot read benchmark JSON {input_path}: {exc}"
        ) from exc
    benchmark = _mapping(loaded, "benchmark report")
    payload = calibrate_confidence_payload(
        benchmark,
        allow_base_threshold=allow_base_threshold,
        minimum_conditional_accuracy_100m_wilson_lower_95=(
            minimum_conditional_accuracy_100m_wilson_lower_95
        ),
    )
    payload["source_benchmark"]["report_path"] = str(input_path)
    payload["source_benchmark"]["report_sha256"] = _sha256_file(input_path)
    json_path, markdown_path = write_confidence_calibration_reports(
        payload, output_dir, stem=stem
    )
    return payload, json_path, markdown_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Select a safety-first confidence threshold from a real Moscow "
            "calibration benchmark JSON without reading test data"
        )
    )
    parser.add_argument("--benchmark-json", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--report-stem", default="moscow_confidence_calibration")
    parser.add_argument(
        "--allow-base-threshold",
        action="store_true",
        help="explicitly allow a benchmark generated with a non-zero base threshold",
    )
    parser.add_argument(
        "--minimum-conditional-accuracy-100m-wilson-lower-95",
        type=float,
        help=(
            "maximize answer rate subject to this 95%% Wilson lower-bound floor; "
            "report no feasible operating point if none qualifies"
        ),
    )
    args = parser.parse_args()
    payload, json_path, markdown_path = calibrate_confidence(
        args.benchmark_json,
        args.output_dir,
        stem=args.report_stem,
        allow_base_threshold=args.allow_base_threshold,
        minimum_conditional_accuracy_100m_wilson_lower_95=(
            args.minimum_conditional_accuracy_100m_wilson_lower_95
        ),
    )
    print(
        json.dumps(
            {
                "calibration_kind": payload["calibration_kind"],
                "chosen_threshold": payload["chosen_threshold"],
                "objective_feasible": payload["policy"]["objective"]["feasible"],
                "answer_rate": None if payload["chosen"] is None else payload["chosen"]["answer_rate"],
                "unconditional_accuracy_within_m": (
                    None if payload["chosen"] is None else payload["chosen"]["unconditional_accuracy_within_m"]
                ),
                "false_confident_error_count": (
                    None if payload["chosen"] is None else payload["chosen"]["false_confident_errors"]["count"]
                ),
                "json_report": str(json_path),
                "markdown_report": str(markdown_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
