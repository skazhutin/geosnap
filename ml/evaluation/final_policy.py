"""Final v4 development and calibration policy analysis.

Development selects the architecture using geographic-group out-of-fold scores.
Calibration is deliberately threshold-only and consumes already-fitted artifacts.
The sealed final-test manifest is rejected by every entry point in this module.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from ml.localization.confidence_model import (
    FEATURE_NAMES,
    PART2_5_FEATURE_NAMES,
    ConfidenceModel,
    MultinomialRiskModel,
    feature_matrix,
    fit_confidence_model,
)
from ml.localization.product_policy import AggregationStrategy

from .product_recovery import (
    _gallery_metadata_and_density,
    _wilson,
    mode_structure_metrics,
    raw_localization_metrics,
    replay_report,
    retrieval_metrics,
)

K_VALUES = (15, 20, 30, 40, 50)
BOOTSTRAP_RESAMPLES = 10_000
RANDOM_SEED = 20260903


class FinalPolicyError(RuntimeError):
    """A final-policy data-separation or artifact contract was violated."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _reject_test_manifest(path: Path) -> None:
    if "test" in path.name.lower():
        raise FinalPolicyError("final-policy selection refuses every test manifest")


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    return float(np.percentile(np.asarray(values, dtype=np.float64), quantile * 100.0))


def error_class(error_m: float) -> str:
    if error_m <= 25.0:
        return "A_lte25m"
    if error_m <= 50.0:
        return "B_25_to_50m"
    if error_m <= 100.0:
        return "C_50_to_100m"
    if error_m <= 500.0:
        return "D_100_to_500m"
    return "E_gt500m"


def _bootstrap_binary_interval(
    numerator: int,
    denominator: int,
    *,
    seed: int,
) -> list[float] | None:
    if denominator <= 0:
        return None
    rng = np.random.default_rng(seed)
    values = rng.binomial(
        denominator,
        numerator / denominator,
        size=BOOTSTRAP_RESAMPLES,
    ) / denominator
    return [float(value) for value in np.percentile(values, [2.5, 97.5])]


def _slice_metrics(
    rows: Sequence[Mapping[str, Any]],
    accepted: np.ndarray,
    key: str,
) -> dict[str, Any]:
    indexes: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        if accepted[index]:
            indexes[str(row.get(key) or "unknown")].append(index)
    output: dict[str, Any] = {}
    for value, selected in sorted(indexes.items()):
        errors = [float(rows[index]["error_m"]) for index in selected]
        successes = sum(error <= 100.0 for error in errors)
        catastrophic = sum(error > 500.0 for error in errors)
        output[value] = {
            "answered": len(selected),
            "successes_lte_100m": successes,
            "precision_lte_100m": successes / len(selected),
            "wilson_95": _wilson(successes, len(selected)),
            "catastrophic_gt_500m": catastrophic,
            "catastrophic_rate": catastrophic / len(selected),
        }
    return output


def policy_metrics(
    rows: Sequence[Mapping[str, Any]],
    correctness_scores: Sequence[float],
    correctness_threshold: float,
    *,
    risk_scores: Sequence[float] | None = None,
    risk_threshold: float | None = None,
    gate_mask: Sequence[bool] | None = None,
    bootstrap: bool = False,
) -> dict[str, Any]:
    correctness = np.asarray(correctness_scores, dtype=np.float64)
    if len(correctness) != len(rows):
        raise ValueError("score and row counts differ")
    accepted = correctness >= correctness_threshold
    if risk_scores is not None:
        if risk_threshold is None:
            raise ValueError("risk threshold is required with risk scores")
        risk = np.asarray(risk_scores, dtype=np.float64)
        if len(risk) != len(rows):
            raise ValueError("risk score and row counts differ")
        accepted &= risk <= risk_threshold
    if gate_mask is not None:
        gate = np.asarray(gate_mask, dtype=bool)
        if len(gate) != len(rows):
            raise ValueError("gate and row counts differ")
        accepted &= ~gate
    selected = [row for row, keep in zip(rows, accepted, strict=True) if keep]
    errors = [float(row["error_m"]) for row in selected]
    answered = len(errors)
    correct_25 = sum(error <= 25.0 for error in errors)
    correct_50 = sum(error <= 50.0 for error in errors)
    correct_100 = sum(error <= 100.0 for error in errors)
    moderate = sum(100.0 < error <= 500.0 for error in errors)
    catastrophic = sum(error > 500.0 for error in errors)
    return {
        "correctness_threshold": float(correctness_threshold),
        "catastrophic_risk_threshold": (
            None if risk_threshold is None else float(risk_threshold)
        ),
        "query_count": len(rows),
        "answered_count": answered,
        "answer_rate": answered / len(rows),
        "answer_rate_bootstrap_95": (
            _bootstrap_binary_interval(answered, len(rows), seed=RANDOM_SEED + 1)
            if bootstrap
            else None
        ),
        "conditional_accuracy_within_m": {
            "25": None if not answered else correct_25 / answered,
            "50": None if not answered else correct_50 / answered,
            "100": None if not answered else correct_100 / answered,
        },
        "conditional_accuracy_lte_100m_wilson_95": _wilson(correct_100, answered),
        "all_query_answered_and_correct_lte_100m": correct_100 / len(rows),
        "accepted_error_gt_100m_count": moderate + catastrophic,
        "accepted_error_100_to_500m_count": moderate,
        "accepted_error_gt_500m_count": catastrophic,
        "accepted_error_gt_500m_rate": None if not answered else catastrophic / answered,
        "catastrophic_rate_bootstrap_95": (
            _bootstrap_binary_interval(catastrophic, answered, seed=RANDOM_SEED + 2)
            if bootstrap
            else None
        ),
        "accepted_error_m": {
            "median": _percentile(errors, 0.5),
            "p90": _percentile(errors, 0.9),
            "p95": _percentile(errors, 0.95),
        },
        "provider": _slice_metrics(rows, accepted, "source"),
        "provider_pair": _slice_metrics(rows, accepted, "positive_provider_pair"),
        "density": _slice_metrics(rows, accepted, "local_gallery_density_bucket"),
        "geography": _slice_metrics(rows, accepted, "h3_coarse"),
        "heading": _slice_metrics(rows, accepted, "positive_heading_gap_bucket"),
        "temporal": _slice_metrics(rows, accepted, "positive_temporal_gap_bucket"),
        "resolution": _slice_metrics(rows, accepted, "resolution_bucket"),
    }


def _strata_pass(rows: Sequence[Mapping[str, Any]], accepted: np.ndarray) -> bool:
    correct = np.asarray([bool(row["correct_100m"]) for row in rows], dtype=bool)
    for key, floor in (("source", 0.80), ("h3_coarse", 0.75)):
        indexes: dict[str, list[int]] = defaultdict(list)
        for index, row in enumerate(rows):
            indexes[str(row.get(key) or "unknown")].append(index)
        for selected_values in indexes.values():
            selected = np.asarray(selected_values, dtype=np.int64)
            accepted_count = int(accepted[selected].sum())
            if accepted_count >= 30:
                successes = int(np.logical_and(accepted[selected], correct[selected]).sum())
                if successes / accepted_count < floor:
                    return False
    return True


def select_threshold_pair(
    rows: Sequence[Mapping[str, Any]],
    correctness_scores: Sequence[float],
    *,
    risk_scores: Sequence[float] | None = None,
) -> dict[str, Any]:
    """Select development frontier points or calibration-only numeric thresholds."""

    correctness = np.asarray(correctness_scores, dtype=np.float64)
    risk = (
        np.zeros(len(rows), dtype=np.float64)
        if risk_scores is None
        else np.asarray(risk_scores, dtype=np.float64)
    )
    correct = np.asarray([bool(row["correct_100m"]) for row in rows], dtype=bool)
    catastrophic = np.asarray([bool(row["catastrophic_500m"]) for row in rows], dtype=bool)
    moderate = np.asarray(
        [100.0 < float(row["error_m"]) <= 500.0 for row in rows],
        dtype=bool,
    )
    correctness_thresholds = np.asarray(
        sorted({0.0, 1.0, *(float(value) for value in correctness)}),
        dtype=np.float64,
    )
    risk_thresholds = np.asarray(
        [1.0]
        if risk_scores is None
        else sorted({0.0, 1.0, *(float(value) for value in risk)}),
        dtype=np.float64,
    )

    correctness_bins = np.searchsorted(correctness_thresholds, correctness)
    risk_bins = np.searchsorted(risk_thresholds, risk)

    def cumulative_counts(values: np.ndarray) -> np.ndarray:
        histogram = np.zeros(
            (len(correctness_thresholds), len(risk_thresholds)),
            dtype=np.int32,
        )
        np.add.at(histogram, (correctness_bins, risk_bins), values.astype(np.int32))
        return np.cumsum(
            np.cumsum(histogram[::-1, :], axis=0),
            axis=1,
        )[::-1, :]

    answered_grid = cumulative_counts(np.ones(len(rows), dtype=np.int8))
    success_grid = cumulative_counts(correct)
    catastrophic_grid = cumulative_counts(catastrophic)
    moderate_grid = cumulative_counts(moderate)
    denominator = np.maximum(answered_grid, 1)
    precision_grid = success_grid / denominator
    catastrophic_rate_grid = catastrophic_grid / denominator
    utility_grid = (
        success_grid - 1.5 * moderate_grid - 8.0 * catastrophic_grid
    ) / len(rows)
    strata_grid = np.ones(answered_grid.shape, dtype=bool)
    for key, floor in (("source", 0.80), ("h3_coarse", 0.75)):
        indexes: dict[str, list[int]] = defaultdict(list)
        for index, row in enumerate(rows):
            indexes[str(row.get(key) or "unknown")].append(index)
        for selected_values in indexes.values():
            selected_mask = np.zeros(len(rows), dtype=bool)
            selected_mask[np.asarray(selected_values, dtype=np.int64)] = True
            stratum_answered = cumulative_counts(selected_mask)
            stratum_success = cumulative_counts(selected_mask & correct)
            stratum_precision = stratum_success / np.maximum(stratum_answered, 1)
            strata_grid &= ~(
                (stratum_answered >= 30) & (stratum_precision < floor)
            )

    def candidate_at(left: int, right: int) -> dict[str, Any]:
        return {
            "correctness_threshold": float(correctness_thresholds[left]),
            "catastrophic_risk_threshold": (
                None if risk_scores is None else float(risk_thresholds[right])
            ),
            "answered_count": int(answered_grid[left, right]),
            "answer_rate": float(answered_grid[left, right] / len(rows)),
            "precision_lte_100m": float(precision_grid[left, right]),
            "catastrophic_count": int(catastrophic_grid[left, right]),
            "catastrophic_rate": float(catastrophic_rate_grid[left, right]),
            "stratum_guards_passed": bool(strata_grid[left, right]),
            "utility": float(utility_grid[left, right]),
        }

    def best_candidate(valid: np.ndarray, objective: str) -> dict[str, Any] | None:
        valid = valid & strata_grid
        coordinates = np.argwhere(valid)
        if not len(coordinates):
            return None
        left = coordinates[:, 0]
        right = coordinates[:, 1]
        if objective == "tertiary":
            order = np.lexsort(
                (
                    risk_thresholds[right],
                    -correctness_thresholds[left],
                    -precision_grid[left, right],
                    -answered_grid[left, right],
                    -utility_grid[left, right],
                )
            )
        else:
            order = np.lexsort(
                (
                    risk_thresholds[right],
                    -correctness_thresholds[left],
                    catastrophic_rate_grid[left, right],
                    -precision_grid[left, right],
                    -answered_grid[left, right],
                )
            )
        index = int(order[0])
        return candidate_at(int(left[index]), int(right[index]))

    eligible = answered_grid >= 40
    best: dict[str, dict[str, Any] | None] = {
        "primary": best_candidate(
            eligible & (precision_grid >= 0.90) & (catastrophic_rate_grid <= 0.01),
            "primary",
        ),
        "secondary": best_candidate(
            eligible & (precision_grid >= 0.90) & (catastrophic_rate_grid <= 0.02),
            "secondary",
        ),
        "tertiary": best_candidate(eligible, "tertiary"),
    }

    selected_objective = next(
        (name for name in ("primary", "secondary", "tertiary") if best[name] is not None),
        None,
    )
    selected = None if selected_objective is None else best[selected_objective]
    selected_metrics = None
    if selected is not None:
        selected_metrics = policy_metrics(
            rows,
            correctness,
            float(selected["correctness_threshold"]),
            risk_scores=None if risk_scores is None else risk,
            risk_threshold=selected["catastrophic_risk_threshold"],
        )
    compact_frontier: list[dict[str, Any]] = []
    valid_counts = sorted(
        {
            int(value)
            for value in answered_grid[strata_grid & (answered_grid >= 40)]
        }
    )
    for target in range(40, len(rows) + 1, 25):
        if not valid_counts:
            break
        answered = min(valid_counts, key=lambda value: (abs(value - target), value))
        selected_cells = strata_grid & (answered_grid == answered)
        coordinates = np.argwhere(selected_cells)
        left = coordinates[:, 0]
        right = coordinates[:, 1]
        order = np.lexsort(
            (
                risk_thresholds[right],
                -correctness_thresholds[left],
                catastrophic_rate_grid[left, right],
                -precision_grid[left, right],
            )
        )
        index = int(order[0])
        compact_frontier.append(candidate_at(int(left[index]), int(right[index])))
    return {
        "selected_objective": selected_objective,
        "selected": selected,
        "selected_metrics": selected_metrics,
        "primary": best["primary"],
        "secondary": best["secondary"],
        "tertiary": best["tertiary"],
        "frontier": compact_frontier,
    }


def _binary_model(
    rows: Sequence[Mapping[str, Any]],
    *,
    label_key: str,
    feature_names: Sequence[str],
    target: str,
) -> tuple[ConfidenceModel, dict[str, Any], list[float]]:
    model, report = fit_confidence_model(
        [row["features"] for row in rows],
        [bool(row[label_key]) for row in rows],
        groups=[str(row["evaluation_geo_group_id"]) for row in rows],
        development_ids=[str(row["query_id"]) for row in rows],
        feature_names=feature_names,
        target=target,
        random_seed=RANDOM_SEED,
    )
    return model, report, list(report["cross_validation"]["oof_probabilities"])


def _binary_quality(labels: np.ndarray, scores: np.ndarray) -> dict[str, float]:
    from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score

    ece = 0.0
    edges = np.linspace(0.0, 1.0, 11)
    for index, (lower, upper) in enumerate(zip(edges, edges[1:], strict=False)):
        selected = (scores >= lower) & (scores <= upper if index == 9 else scores < upper)
        if selected.any():
            ece += float(selected.mean()) * abs(float(scores[selected].mean()) - float(labels[selected].mean()))
    return {
        "roc_auc": float(roc_auc_score(labels, scores)),
        "pr_auc": float(average_precision_score(labels, scores)),
        "brier_score": float(brier_score_loss(labels, scores)),
        "ece": ece,
    }


def _multinomial_model(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import StratifiedGroupKFold
    from sklearn.preprocessing import StandardScaler

    x = feature_matrix([row["features"] for row in rows], feature_names=FEATURE_NAMES)
    y = np.asarray(
        [0 if row["correct_100m"] else (2 if row["catastrophic_500m"] else 1) for row in rows],
        dtype=np.int8,
    )
    groups = np.asarray([str(row["evaluation_geo_group_id"]) for row in rows])
    query_ids = [str(row["query_id"]) for row in rows]
    splitter = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=RANDOM_SEED)
    oof = np.full((len(rows), 3), np.nan, dtype=np.float64)
    fold_assignment = np.zeros(len(rows), dtype=np.int64)
    folds: list[dict[str, Any]] = []
    for fold, (train, validation) in enumerate(splitter.split(x, y, groups), start=1):
        scaler = StandardScaler().fit(x[train])
        classifier = LogisticRegression(
            C=1.0,
            solver="lbfgs",
            max_iter=2000,
            random_state=RANDOM_SEED,
        ).fit(scaler.transform(x[train]), y[train])
        probabilities = classifier.predict_proba(scaler.transform(x[validation]))
        for column, label in enumerate(classifier.classes_):
            oof[validation, int(label)] = probabilities[:, column]
        fold_assignment[validation] = fold
        folds.append(
            {
                "fold": fold,
                "train_count": len(train),
                "validation_count": len(validation),
                "train_group_count": len(set(groups[train])),
                "validation_group_count": len(set(groups[validation])),
                "group_overlap": len(set(groups[train]) & set(groups[validation])),
            }
        )
    if not np.isfinite(oof).all():
        raise FinalPolicyError("multinomial OOF predictions are incomplete")
    final_scaler = StandardScaler().fit(x)
    final_classifier = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=2000,
        random_state=RANDOM_SEED,
    ).fit(final_scaler.transform(x), y)
    model = MultinomialRiskModel(
        feature_names=tuple(FEATURE_NAMES),
        means=tuple(float(value) for value in final_scaler.mean_),
        scales=tuple(float(value) for value in final_scaler.scale_),
        classes=tuple(int(value) for value in final_classifier.classes_),
        coefficients=tuple(
            tuple(float(value) for value in values)
            for values in final_classifier.coef_
        ),
        intercepts=tuple(float(value) for value in final_classifier.intercept_),
    )
    return {
        "artifact": model.to_dict(),
        "folds": folds,
        "oof_rows": [
            {
                "query_id": query_id,
                "group_id": str(group),
                "fold": int(fold),
                "class": int(label),
                "probabilities": [float(value) for value in probabilities],
            }
            for query_id, group, fold, label, probabilities in zip(
                query_ids,
                groups,
                fold_assignment,
                y,
                oof,
                strict=True,
            )
        ],
        "correctness_quality": _binary_quality(y == 0, oof[:, 0]),
        "catastrophic_quality": _binary_quality(y == 2, oof[:, 2]),
        "correctness_scores": [float(value) for value in oof[:, 0]],
        "risk_scores": [float(value) for value in oof[:, 2]],
    }


def _score_distribution(
    rows: Sequence[Mapping[str, Any]],
    scores: Sequence[float],
) -> dict[str, Any]:
    groups = {
        "lte_100m": [float(score) for row, score in zip(rows, scores, strict=True) if row["correct_100m"]],
        "gt_100m": [float(score) for row, score in zip(rows, scores, strict=True) if not row["correct_100m"]],
        "gt_500m": [float(score) for row, score in zip(rows, scores, strict=True) if row["catastrophic_500m"]],
    }
    return {
        name: {
            "count": len(values),
            "median": _percentile(values, 0.5),
            "p10": _percentile(values, 0.1),
            "p90": _percentile(values, 0.9),
        }
        for name, values in groups.items()
    }


def _feature_audit(rows: Sequence[Mapping[str, Any]], model: ConfidenceModel) -> dict[str, Any]:
    x = feature_matrix([row["features"] for row in rows], feature_names=model.feature_names)
    correlations = np.corrcoef(x, rowvar=False)
    catastrophic = np.asarray([bool(row["catastrophic_500m"]) for row in rows])
    effects: list[dict[str, Any]] = []
    for index, name in enumerate(model.feature_names):
        values = x[:, index]
        standard = float(values.std())
        effect = (
            0.0
            if standard == 0.0
            else float((values[catastrophic].mean() - values[~catastrophic].mean()) / standard)
        )
        effects.append(
            {
                "feature": name,
                "catastrophic_standardized_mean_difference": effect,
                "catastrophic_mean": float(values[catastrophic].mean()),
                "non_catastrophic_mean": float(values[~catastrophic].mean()),
            }
        )
    high_correlations: list[dict[str, Any]] = []
    for left in range(len(model.feature_names)):
        for right in range(left + 1, len(model.feature_names)):
            value = float(correlations[left, right])
            if math.isfinite(value) and abs(value) >= 0.80:
                high_correlations.append(
                    {
                        "left": model.feature_names[left],
                        "right": model.feature_names[right],
                        "correlation": value,
                    }
                )
    return {
        "feature_names": list(model.feature_names),
        "means": list(model.means),
        "scales": list(model.scales),
        "coefficients": list(model.coefficients),
        "intercept": model.intercept,
        "coefficients_by_absolute_magnitude": sorted(
            [
                {"feature": name, "coefficient": coefficient}
                for name, coefficient in zip(model.feature_names, model.coefficients, strict=True)
            ],
            key=lambda row: abs(float(row["coefficient"])),
            reverse=True,
        ),
        "high_absolute_correlations": sorted(
            high_correlations,
            key=lambda row: abs(float(row["correlation"])),
            reverse=True,
        ),
        "catastrophic_effects": sorted(
            effects,
            key=lambda row: abs(float(row["catastrophic_standardized_mean_difference"])),
            reverse=True,
        ),
    }


def _gate_masks(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[bool]]:
    return {
        "distant_mode_ambiguity": [
            float(row["features"]["best_second_mode_separation_m"]) >= 500.0
            and float(row["features"]["geographic_mode_margin"]) < 0.02
            for row in rows
        ],
        "independent_support_minimum": [
            float(row["features"]["effective_independent_support_count"]) < 1.5
            for row in rows
        ],
        "sequence_dominance": [
            float(row["features"]["maximum_single_sequence_contribution"]) > 0.80
            for row in rows
        ],
        "geographic_compactness": [
            float(row["features"]["winning_cluster_diameter_m"]) > 100.0
            for row in rows
        ],
        "top1_mode_disagreement": [
            not bool(row["features"]["top1_agrees_with_winner"]) for row in rows
        ],
        "low_density_uncertainty": [
            float(row["features"]["local_gallery_density_100m"]) <= 1.0 for row in rows
        ],
    }


def _gate_audit(
    rows: Sequence[Mapping[str, Any]],
    correctness_scores: Sequence[float],
    risk_scores: Sequence[float],
    point: Mapping[str, Any],
) -> dict[str, Any]:
    correctness = np.asarray(correctness_scores, dtype=np.float64)
    risk = np.asarray(risk_scores, dtype=np.float64)
    accepted = (correctness >= float(point["correctness_threshold"])) & (
        risk <= float(point["catastrophic_risk_threshold"])
    )
    output: dict[str, Any] = {}
    for name, mask_values in _gate_masks(rows).items():
        mask = np.asarray(mask_values, dtype=bool)
        removed = accepted & mask
        catastrophes = int(
            sum(
                bool(row["catastrophic_500m"]) and removed[index]
                for index, row in enumerate(rows)
            )
        )
        correct_lost = int(
            sum(bool(row["correct_100m"]) and removed[index] for index, row in enumerate(rows))
        )
        moderate_removed = int(
            sum(
                100.0 < float(row["error_m"]) <= 500.0 and removed[index]
                for index, row in enumerate(rows)
            )
        )
        output[name] = {
            "rejected_total": int(mask.sum()),
            "accepted_answers_removed": int(removed.sum()),
            "catastrophic_errors_prevented": catastrophes,
            "moderate_errors_removed": moderate_removed,
            "correct_answers_lost": correct_lost,
            "correct_answers_lost_per_catastrophe_prevented": (
                None if catastrophes == 0 else correct_lost / catastrophes
            ),
            "answer_rate_pp_lost_per_catastrophe_prevented": (
                None
                if catastrophes == 0
                else 100.0 * int(removed.sum()) / len(rows) / catastrophes
            ),
            "policy_after_gate": policy_metrics(
                rows,
                correctness,
                float(point["correctness_threshold"]),
                risk_scores=risk,
                risk_threshold=float(point["catastrophic_risk_threshold"]),
                gate_mask=mask,
            ),
        }
    return output


def _architecture_comparison(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    baseline_model, baseline_report, baseline_scores = _binary_model(
        rows,
        label_key="correct_100m",
        feature_names=PART2_5_FEATURE_NAMES,
        target="localization_error_m_lte_100",
    )
    correctness_model, correctness_report, correctness_scores = _binary_model(
        rows,
        label_key="correct_100m",
        feature_names=FEATURE_NAMES,
        target="localization_error_m_lte_100",
    )
    risk_model, risk_report, risk_scores = _binary_model(
        rows,
        label_key="catastrophic_500m",
        feature_names=FEATURE_NAMES,
        target="localization_error_m_gt_500",
    )
    multinomial = _multinomial_model(rows)
    architectures = {
        "existing_correctness_logistic": {
            "correctness_artifact": baseline_model.to_dict(),
            "correctness_cross_validation": baseline_report["cross_validation"],
            "correctness_score_distribution": _score_distribution(rows, baseline_scores),
            "operating_points": select_threshold_pair(rows, baseline_scores),
        },
        "expanded_correctness_plus_catastrophic_risk_logistic": {
            "correctness_artifact": correctness_model.to_dict(),
            "catastrophic_risk_artifact": risk_model.to_dict(),
            "correctness_cross_validation": correctness_report["cross_validation"],
            "catastrophic_risk_cross_validation": risk_report["cross_validation"],
            "correctness_score_distribution": _score_distribution(rows, correctness_scores),
            "catastrophic_risk_score_distribution": _score_distribution(rows, risk_scores),
            "operating_points": select_threshold_pair(
                rows,
                correctness_scores,
                risk_scores=risk_scores,
            ),
        },
        "multinomial_risk_logistic": {
            **{key: value for key, value in multinomial.items() if not key.endswith("_scores")},
            "operating_points": select_threshold_pair(
                rows,
                multinomial["correctness_scores"],
                risk_scores=multinomial["risk_scores"],
            ),
        },
    }
    objective_rank = {"primary": 0, "secondary": 1, "tertiary": 2, None: 3}
    candidates = []
    for name, result in architectures.items():
        point = result["operating_points"]["selected"]
        objective = result["operating_points"]["selected_objective"]
        if point is None:
            continue
        candidates.append(
            (
                objective_rank[objective],
                -float(point["answer_rate"]),
                float(point["catastrophic_rate"]),
                -float(point["precision_lte_100m"]),
                name,
            )
        )
    if not candidates:
        raise FinalPolicyError("no confidence architecture produced a useful development point")
    selected_architecture = min(candidates)[-1]
    return {
        "architectures": architectures,
        "selected_architecture": selected_architecture,
        "selection_rule": (
            "prefer the strongest predeclared objective tier, then maximum answer rate, "
            "lower catastrophic rate, and higher precision"
        ),
        "feature_audit": {
            "existing_correctness": _feature_audit(rows, baseline_model),
            "expanded_correctness": _feature_audit(rows, correctness_model),
            "catastrophic_risk": _feature_audit(rows, risk_model),
        },
    }


def run_development(
    *,
    source_report: Path,
    gallery_manifest: Path,
    development_manifest: Path,
    output_path: Path,
    architecture_path: Path,
    correctness_artifact_path: Path,
    risk_artifact_path: Path,
    multinomial_artifact_path: Path,
) -> dict[str, Any]:
    _reject_test_manifest(development_manifest)
    configurations: dict[str, Any] = {}
    rows_by_key: dict[str, list[dict[str, Any]]] = {}
    gallery_context = _gallery_metadata_and_density(gallery_manifest)
    for k in K_VALUES:
        for aggregation in AggregationStrategy:
            rows, _ = replay_report(
                source_report,
                gallery_manifest=gallery_manifest,
                query_manifest=development_manifest,
                k=k,
                aggregation=aggregation,
                gallery_context=gallery_context,
            )
            for row in rows:
                row["error_class"] = error_class(float(row["error_m"]))
            key = f"k{k}_{aggregation.value}"
            correctness_model, correctness_report, correctness_scores = _binary_model(
                rows,
                label_key="correct_100m",
                feature_names=FEATURE_NAMES,
                target="localization_error_m_lte_100",
            )
            risk_model, risk_report, risk_scores = _binary_model(
                rows,
                label_key="catastrophic_500m",
                feature_names=FEATURE_NAMES,
                target="localization_error_m_gt_500",
            )
            configurations[key] = {
                "k": k,
                "aggregation": aggregation.value,
                "retrieval": retrieval_metrics(rows),
                "raw_localization": raw_localization_metrics(rows),
                "error_classes": {
                    name: sum(row["error_class"] == name for row in rows)
                    for name in (
                        "A_lte25m",
                        "B_25_to_50m",
                        "C_50_to_100m",
                        "D_100_to_500m",
                        "E_gt500m",
                    )
                },
                "mode_structure": mode_structure_metrics(rows),
                "dual_logistic_operating_points": select_threshold_pair(
                    rows,
                    correctness_scores,
                    risk_scores=risk_scores,
                ),
                "correctness_cv": {
                    key: correctness_report["cross_validation"][key]
                    for key in ("roc_auc", "pr_auc", "brier_score", "ece")
                },
                "catastrophic_risk_cv": {
                    key: risk_report["cross_validation"][key]
                    for key in ("roc_auc", "pr_auc", "brier_score", "ece")
                },
                "artifacts_for_reproducibility": {
                    "correctness": correctness_model.to_dict(),
                    "catastrophic_risk": risk_model.to_dict(),
                },
            }
            rows_by_key[key] = rows

    default_key = "k30_density_aware_mode_vote"
    default_point = configurations[default_key]["dual_logistic_operating_points"]["selected"]
    selected_key = default_key
    if default_point is not None:
        challengers = []
        for key, result in configurations.items():
            point = result["dual_logistic_operating_points"]["selected"]
            if point is None or result["dual_logistic_operating_points"]["selected_objective"] != configurations[default_key]["dual_logistic_operating_points"]["selected_objective"]:
                continue
            if (
                float(point["answer_rate"]) - float(default_point["answer_rate"]) >= 0.03
                and float(point["catastrophic_rate"]) <= float(default_point["catastrophic_rate"])
            ):
                challengers.append((float(point["answer_rate"]), key))
        if challengers:
            selected_key = max(challengers)[1]
    selected_rows = rows_by_key[selected_key]
    architecture = _architecture_comparison(selected_rows)
    selected_architecture = architecture["selected_architecture"]
    selected = architecture["architectures"][selected_architecture]
    development_point = selected["operating_points"]["selected"]
    if development_point is None:
        raise FinalPolicyError("development found no useful policy point with at least 40 answers")
    if selected_architecture == "multinomial_risk_logistic":
        selected_correctness_scores = [
            float(row["probabilities"][0]) for row in selected["oof_rows"]
        ]
        selected_risk_scores = [
            float(row["probabilities"][2]) for row in selected["oof_rows"]
        ]
        selected_artifact_path = multinomial_artifact_path
        atomic_json(selected_artifact_path, selected["artifact"])
    else:
        selected_correctness_scores = selected["correctness_cross_validation"][
            "oof_probabilities"
        ]
        if selected_architecture == "existing_correctness_logistic":
            selected_risk_scores = [0.0] * len(selected_rows)
            atomic_json(correctness_artifact_path, selected["correctness_artifact"])
            selected_artifact_path = correctness_artifact_path
        else:
            selected_risk_scores = selected["catastrophic_risk_cross_validation"][
                "oof_probabilities"
            ]
            atomic_json(correctness_artifact_path, selected["correctness_artifact"])
            atomic_json(risk_artifact_path, selected["catastrophic_risk_artifact"])
            selected_artifact_path = correctness_artifact_path
    gate_audit = _gate_audit(
        selected_rows,
        selected_correctness_scores,
        selected_risk_scores,
        development_point,
    )
    # Explicit gates survive only when they prevent a development catastrophe
    # without sacrificing more than five correct answers per prevented event.
    surviving_gates = [
        name
        for name, result in gate_audit.items()
        if result["catastrophic_errors_prevented"] > 0
        and result["correct_answers_lost_per_catastrophe_prevented"] <= 5.0
    ][:2]

    architecture_payload = {
        "schema_version": 1,
        "status": "architecture_frozen_before_calibration",
        "selection_split": "development_only",
        "development_manifest": str(development_manifest),
        "development_sha256": sha256_file(development_manifest),
        "source_report": str(source_report),
        "source_report_sha256": sha256_file(source_report),
        "selected_configuration": selected_key,
        "top_k": int(configurations[selected_key]["k"]),
        "aggregation": str(configurations[selected_key]["aggregation"]),
        "confidence_architecture": selected_architecture,
        "feature_names": list(FEATURE_NAMES),
        "confidence_artifact": str(selected_artifact_path),
        "confidence_artifact_sha256": sha256_file(selected_artifact_path),
        "catastrophic_risk_artifact": (
            str(risk_artifact_path)
            if selected_architecture
            == "expanded_correctness_plus_catastrophic_risk_logistic"
            else (
                str(selected_artifact_path)
                if selected_architecture == "multinomial_risk_logistic"
                else None
            )
        ),
        "catastrophic_risk_artifact_sha256": (
            sha256_file(risk_artifact_path)
            if selected_architecture
            == "expanded_correctness_plus_catastrophic_risk_logistic"
            else (
                sha256_file(selected_artifact_path)
                if selected_architecture == "multinomial_risk_logistic"
                else None
            )
        ),
        "safety_gates": surviving_gates,
        "gate_definitions": {
            "distant_mode_ambiguity": "second mode >=500m away and mode-mass margin <0.02",
            "independent_support_minimum": "effective independent support <1.5",
            "sequence_dominance": "largest sequence evidence share >0.80",
            "geographic_compactness": "winning-cluster diameter >100m",
            "top1_mode_disagreement": "top-ranked descriptor match is outside winning mode",
            "low_density_uncertainty": "winning support median gallery density <=1 within 100m",
        },
        "calibration_contract": "threshold_only_no_refit_or_feature_change",
    }
    atomic_json(architecture_path, architecture_payload)
    payload = {
        "schema_version": 1,
        "kind": "moscow_real_v4_final_policy_development",
        "selection_data": "development_only_out_of_fold",
        "gallery_manifest": str(gallery_manifest),
        "gallery_sha256": sha256_file(gallery_manifest),
        "development_manifest": str(development_manifest),
        "development_sha256": sha256_file(development_manifest),
        "source_report": str(source_report),
        "source_report_sha256": sha256_file(source_report),
        "k_values": list(K_VALUES),
        "aggregation_values": [value.value for value in AggregationStrategy],
        "configurations": configurations,
        "selected_configuration": selected_key,
        "architecture_comparison": architecture,
        "gate_audit": gate_audit,
        "surviving_explicit_gates": surviving_gates,
        "architecture_freeze": str(architecture_path),
        "architecture_freeze_sha256": sha256_file(architecture_path),
        "per_query": selected_rows,
    }
    atomic_json(output_path, payload)
    return payload


def _gate_mask_from_names(rows: Sequence[Mapping[str, Any]], names: Sequence[str]) -> list[bool]:
    available = _gate_masks(rows)
    unknown = sorted(set(names) - set(available))
    if unknown:
        raise FinalPolicyError(f"unknown frozen safety gates: {unknown}")
    return [any(available[name][index] for name in names) for index in range(len(rows))]


def run_calibration(
    *,
    source_report: Path,
    gallery_manifest: Path,
    calibration_manifest: Path,
    architecture_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    _reject_test_manifest(calibration_manifest)
    architecture = json.loads(architecture_path.read_text(encoding="utf-8"))
    if architecture.get("status") != "architecture_frozen_before_calibration":
        raise FinalPolicyError("policy architecture is not frozen before calibration")
    confidence_path = Path(str(architecture["confidence_artifact"]))
    if sha256_file(confidence_path) != architecture["confidence_artifact_sha256"]:
        raise FinalPolicyError("frozen confidence artifact changed before calibration")
    architecture_name = str(architecture["confidence_architecture"])
    artifact_value = json.loads(confidence_path.read_text(encoding="utf-8"))
    rows, _ = replay_report(
        source_report,
        gallery_manifest=gallery_manifest,
        query_manifest=calibration_manifest,
        k=int(architecture["top_k"]),
        aggregation=AggregationStrategy(str(architecture["aggregation"])),
    )
    development_ids = {
        str(row["query_id"])
        for row in json.loads(
            Path("data/evaluation/moscow_real_v4/reports/final_policy_development.json").read_text(
                encoding="utf-8"
            )
        )["per_query"]
    }
    calibration_ids = {str(row["query_id"]) for row in rows}
    if development_ids & calibration_ids:
        raise FinalPolicyError("development/calibration query ID overlap")
    if architecture_name == "multinomial_risk_logistic":
        multinomial = MultinomialRiskModel.from_dict(artifact_value)
        probabilities = [multinomial.predict_proba(row["features"]) for row in rows]
        correctness_scores = [value[0] for value in probabilities]
        risk_scores = [value[2] for value in probabilities]
    elif architecture_name == "existing_correctness_logistic":
        correctness = ConfidenceModel.from_dict(artifact_value)
        correctness_scores = [correctness.predict_proba(row["features"]) for row in rows]
        risk_scores = [0.0] * len(rows)
    else:
        correctness = ConfidenceModel.from_dict(artifact_value)
        risk_path = Path(str(architecture["catastrophic_risk_artifact"]))
        if sha256_file(risk_path) != architecture["catastrophic_risk_artifact_sha256"]:
            raise FinalPolicyError("frozen risk artifact changed before calibration")
        risk = ConfidenceModel.from_dict(json.loads(risk_path.read_text(encoding="utf-8")))
        correctness_scores = [correctness.predict_proba(row["features"]) for row in rows]
        risk_scores = [risk.predict_proba(row["features"]) for row in rows]
    operating_points = select_threshold_pair(rows, correctness_scores, risk_scores=risk_scores)
    selected = operating_points["selected"]
    if selected is None:
        raise FinalPolicyError("calibration could not select a useful threshold pair")
    gate_names = [str(value) for value in architecture.get("safety_gates", [])]
    gate_mask = _gate_mask_from_names(rows, gate_names)
    selected_metrics = policy_metrics(
        rows,
        correctness_scores,
        float(selected["correctness_threshold"]),
        risk_scores=risk_scores,
        risk_threshold=float(selected["catastrophic_risk_threshold"]),
        gate_mask=gate_mask,
        bootstrap=True,
    )
    payload = {
        "schema_version": 1,
        "kind": "moscow_real_v4_threshold_only_calibration",
        "fit_performed": False,
        "architecture_freeze": str(architecture_path),
        "architecture_freeze_sha256": sha256_file(architecture_path),
        "calibration_manifest": str(calibration_manifest),
        "calibration_sha256": sha256_file(calibration_manifest),
        "source_report": str(source_report),
        "source_report_sha256": sha256_file(source_report),
        "confidence_artifact_sha256": sha256_file(confidence_path),
        "catastrophic_risk_artifact_sha256": architecture.get(
            "catastrophic_risk_artifact_sha256"
        ),
        "operating_points": operating_points,
        "selected_objective": operating_points["selected_objective"],
        "selected_thresholds": {
            "correctness": float(selected["correctness_threshold"]),
            "catastrophic_risk_maximum": float(selected["catastrophic_risk_threshold"]),
        },
        "selected_metrics_with_frozen_gates": selected_metrics,
        "safety_gates": gate_names,
        "score_distributions": {
            "correctness": _score_distribution(rows, correctness_scores),
            "catastrophic_risk": _score_distribution(rows, risk_scores),
        },
        "per_query": [
            {
                **row,
                "correctness_score": score,
                "catastrophic_risk_score": risk_score,
                "explicit_gate_rejected": gate,
            }
            for row, score, risk_score, gate in zip(
                rows,
                correctness_scores,
                risk_scores,
                gate_mask,
                strict=True,
            )
        ],
    }
    atomic_json(output_path, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    development = subparsers.add_parser("development")
    calibration = subparsers.add_parser("calibration")
    for current in (development, calibration):
        current.add_argument("--source-report", type=Path, required=True)
        current.add_argument("--gallery-manifest", type=Path, required=True)
        current.add_argument("--output", type=Path, required=True)
    development.add_argument("--development-manifest", type=Path, required=True)
    development.add_argument("--architecture", type=Path, required=True)
    development.add_argument("--correctness-artifact", type=Path, required=True)
    development.add_argument("--risk-artifact", type=Path, required=True)
    development.add_argument("--multinomial-artifact", type=Path, required=True)
    calibration.add_argument("--calibration-manifest", type=Path, required=True)
    calibration.add_argument("--architecture", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "development":
        payload = run_development(
            source_report=args.source_report,
            gallery_manifest=args.gallery_manifest,
            development_manifest=args.development_manifest,
            output_path=args.output,
            architecture_path=args.architecture,
            correctness_artifact_path=args.correctness_artifact,
            risk_artifact_path=args.risk_artifact,
            multinomial_artifact_path=args.multinomial_artifact,
        )
        print(
            json.dumps(
                {
                    "selected_configuration": payload["selected_configuration"],
                    "selected_architecture": payload["architecture_comparison"]["selected_architecture"],
                    "surviving_explicit_gates": payload["surviving_explicit_gates"],
                    "output": str(args.output),
                },
                indent=2,
            )
        )
    else:
        payload = run_calibration(
            source_report=args.source_report,
            gallery_manifest=args.gallery_manifest,
            calibration_manifest=args.calibration_manifest,
            architecture_path=args.architecture,
            output_path=args.output,
        )
        print(
            json.dumps(
                {
                    "selected_objective": payload["selected_objective"],
                    "thresholds": payload["selected_thresholds"],
                    "metrics": payload["selected_metrics_with_frozen_gates"],
                    "output": str(args.output),
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
