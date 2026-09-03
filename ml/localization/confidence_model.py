"""Small, interpretable retrieval-feature confidence models with leakage guards."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np

PART2_5_FEATURE_NAMES = (
    "top1_similarity",
    "top1_top2_similarity_margin",
    "winning_cluster_score",
    "second_cluster_score",
    "geographic_mode_margin",
    "independent_sequence_count",
    "provider_diversity",
    "winning_candidate_count",
    "winning_cluster_p90_spread_m",
    "best_second_mode_separation_m",
    "local_gallery_density_100m",
    "best_winner_rank",
    "top1_agrees_with_winner",
    "cross_provider_winner_evidence",
)

FEATURE_NAMES = PART2_5_FEATURE_NAMES + (
    "top2_similarity",
    "top1_top5_similarity_margin",
    "top5_similarity_mean",
    "top5_similarity_std",
    "top5_similarity_min",
    "top5_similarity_max",
    "top10_similarity_mean",
    "top10_similarity_std",
    "top10_similarity_min",
    "top10_similarity_max",
    "winning_cluster_radius_m",
    "winning_cluster_diameter_m",
    "winning_cluster_median_pairwise_distance_m",
    "local_gallery_density_25m",
    "local_gallery_density_50m",
    "winner_rank_mean",
    "winner_rank_max",
    "medoid_is_top1",
    "top1_to_selected_distance_m",
    "geographic_mode_count",
    "winning_mode_retrieval_mass_fraction",
    "winner_similarity_variance",
    "outside_similarity_variance",
    "dominant_mode_support_fraction",
    "heading_bin_count",
    "maximum_single_sequence_contribution",
    "maximum_provider_contribution",
    "effective_independent_support_count",
    "winner_reference_age_days_median",
    "winner_reference_age_days_range",
    "winner_reference_age_available_fraction",
)


@dataclass(frozen=True, slots=True)
class ConfidenceModel:
    feature_names: tuple[str, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]
    coefficients: tuple[float, ...]
    intercept: float
    isotonic_x: tuple[float, ...] = ()
    isotonic_y: tuple[float, ...] = ()
    fitted_split: str = "development"
    target: str = "localization_error_m_lte_100"

    def __post_init__(self) -> None:
        size = len(self.feature_names)
        if not size or any(len(values) != size for values in (self.means, self.scales, self.coefficients)):
            raise ValueError("confidence model vectors must match a non-empty feature list")
        if any(not math.isfinite(value) for value in (*self.means, *self.scales, *self.coefficients)):
            raise ValueError("confidence model contains non-finite vector values")
        if any(value <= 0 for value in self.scales):
            raise ValueError("confidence model scales must be positive")
        if not math.isfinite(self.intercept):
            raise ValueError("confidence model intercept must be finite")
        if bool(self.isotonic_x) != bool(self.isotonic_y) or len(self.isotonic_x) != len(
            self.isotonic_y
        ):
            raise ValueError("isotonic x/y arrays must have equal non-zero lengths")
        if self.isotonic_x and any(
            right < left for left, right in zip(self.isotonic_x, self.isotonic_x[1:], strict=False)
        ):
            raise ValueError("isotonic x thresholds must be sorted")

    @property
    def method(self) -> str:
        return "logistic_isotonic" if self.isotonic_x else "logistic"

    def predict_proba(self, features: Mapping[str, Any]) -> float:
        row = feature_vector(features, feature_names=self.feature_names)
        standardized = (row - np.asarray(self.means)) / np.asarray(self.scales)
        logit = float(np.dot(standardized, np.asarray(self.coefficients)) + self.intercept)
        probability = 1.0 / (1.0 + math.exp(-max(-40.0, min(40.0, logit))))
        if self.isotonic_x:
            probability = float(np.interp(probability, self.isotonic_x, self.isotonic_y))
        return max(0.0, min(1.0, probability))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "method": self.method,
            "target": self.target,
            "feature_names": list(self.feature_names),
            "means": list(self.means),
            "scales": list(self.scales),
            "coefficients": list(self.coefficients),
            "intercept": self.intercept,
            "isotonic_x": list(self.isotonic_x),
            "isotonic_y": list(self.isotonic_y),
            "fitted_split": self.fitted_split,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ConfidenceModel:
        if int(value.get("schema_version", 0)) != 1:
            raise ValueError("unsupported confidence model schema")
        if value.get("fitted_split") != "development":
            raise ValueError("production confidence model must be fit on development only")
        return cls(
            feature_names=tuple(str(item) for item in value["feature_names"]),
            means=tuple(float(item) for item in value["means"]),
            scales=tuple(float(item) for item in value["scales"]),
            coefficients=tuple(float(item) for item in value["coefficients"]),
            intercept=float(value["intercept"]),
            isotonic_x=tuple(float(item) for item in value.get("isotonic_x", ())),
            isotonic_y=tuple(float(item) for item in value.get("isotonic_y", ())),
            fitted_split=str(value["fitted_split"]),
            target=str(value.get("target", "localization_error_m_lte_100")),
        )


@dataclass(frozen=True, slots=True)
class MultinomialRiskModel:
    """Portable three-class logistic model for correct/moderate/catastrophic risk."""

    feature_names: tuple[str, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]
    classes: tuple[int, ...]
    coefficients: tuple[tuple[float, ...], ...]
    intercepts: tuple[float, ...]
    fitted_split: str = "development"

    def __post_init__(self) -> None:
        size = len(self.feature_names)
        if not size or len(self.means) != size or len(self.scales) != size:
            raise ValueError("multinomial model vectors must match its feature list")
        if len(self.classes) != 3 or set(self.classes) != {0, 1, 2}:
            raise ValueError("multinomial model classes must be exactly 0, 1, and 2")
        if len(self.coefficients) != 3 or len(self.intercepts) != 3:
            raise ValueError("multinomial model must contain three class parameter sets")
        if any(len(values) != size for values in self.coefficients):
            raise ValueError("multinomial coefficient vectors do not match feature list")
        flattened = (
            *self.means,
            *self.scales,
            *self.intercepts,
            *(value for values in self.coefficients for value in values),
        )
        if any(not math.isfinite(value) for value in flattened):
            raise ValueError("multinomial model contains non-finite values")
        if any(value <= 0 for value in self.scales):
            raise ValueError("multinomial model scales must be positive")
        if self.fitted_split != "development":
            raise ValueError("production multinomial model must be fit on development only")

    @property
    def method(self) -> str:
        return "standardized_multinomial_logistic"

    def predict_proba(self, features: Mapping[str, Any]) -> dict[int, float]:
        row = feature_vector(features, feature_names=self.feature_names)
        standardized = (row - np.asarray(self.means)) / np.asarray(self.scales)
        logits = np.asarray(self.coefficients) @ standardized + np.asarray(self.intercepts)
        logits -= float(logits.max())
        probabilities = np.exp(logits)
        probabilities /= float(probabilities.sum())
        return {
            label: float(probability)
            for label, probability in zip(self.classes, probabilities, strict=True)
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "method": self.method,
            "targets": {
                "0": "localization_error_m_lte_100",
                "1": "localization_error_m_100_to_500",
                "2": "localization_error_m_gt_500",
            },
            "feature_names": list(self.feature_names),
            "means": list(self.means),
            "scales": list(self.scales),
            "classes": list(self.classes),
            "coefficients": [list(values) for values in self.coefficients],
            "intercepts": list(self.intercepts),
            "fitted_split": self.fitted_split,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> MultinomialRiskModel:
        if int(value.get("schema_version", 0)) != 1:
            raise ValueError("unsupported multinomial model schema")
        if value.get("method") != "standardized_multinomial_logistic":
            raise ValueError("unsupported multinomial model method")
        return cls(
            feature_names=tuple(str(item) for item in value["feature_names"]),
            means=tuple(float(item) for item in value["means"]),
            scales=tuple(float(item) for item in value["scales"]),
            classes=tuple(int(item) for item in value["classes"]),
            coefficients=tuple(
                tuple(float(item) for item in values)
                for values in value["coefficients"]
            ),
            intercepts=tuple(float(item) for item in value["intercepts"]),
            fitted_split=str(value["fitted_split"]),
        )


def feature_vector(
    features: Mapping[str, Any], *, feature_names: Sequence[str] = FEATURE_NAMES
) -> np.ndarray:
    missing = [name for name in feature_names if name not in features]
    if missing:
        raise ValueError(f"missing confidence features: {missing}")
    values = np.asarray([float(features[name]) for name in feature_names], dtype=np.float64)
    if not np.isfinite(values).all():
        raise ValueError("confidence features must all be finite")
    return values


def feature_matrix(
    rows: Sequence[Mapping[str, Any]], *, feature_names: Sequence[str] = FEATURE_NAMES
) -> np.ndarray:
    if not rows:
        raise ValueError("confidence fitting requires at least one feature row")
    return np.stack(
        [feature_vector(row, feature_names=feature_names) for row in rows],
        axis=0,
    )


def _validate_training_contract(
    *,
    labels: Sequence[bool | int],
    groups: Sequence[str],
    development_ids: Sequence[str],
    calibration_ids: Sequence[str] = (),
) -> np.ndarray:
    count = len(labels)
    if len(groups) != count or len(development_ids) != count:
        raise ValueError("labels, groups, and development IDs must have equal lengths")
    overlap = set(map(str, development_ids)) & set(map(str, calibration_ids))
    if overlap:
        raise ValueError(f"confidence train/calibration overlap: {sorted(overlap)[:5]}")
    if len(set(map(str, development_ids))) != count:
        raise ValueError("development IDs must be unique")
    values = np.asarray(labels, dtype=np.int8)
    if set(values.tolist()) != {0, 1}:
        raise ValueError("confidence fitting requires both target classes")
    if len(set(map(str, groups))) < 5:
        raise ValueError("confidence fitting requires at least five independent groups")
    return values


def fit_confidence_model(
    feature_rows: Sequence[Mapping[str, Any]],
    labels: Sequence[bool | int],
    *,
    groups: Sequence[str],
    development_ids: Sequence[str],
    calibration_ids: Sequence[str] = (),
    isotonic: bool = False,
    random_seed: int = 20260902,
    feature_names: Sequence[str] = FEATURE_NAMES,
    target: str = "localization_error_m_lte_100",
) -> tuple[ConfidenceModel, dict[str, Any]]:
    """Fit on development only and return group-CV evidence plus a portable artifact."""

    try:
        from sklearn.calibration import calibration_curve
        from sklearn.isotonic import IsotonicRegression
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
        from sklearn.model_selection import StratifiedGroupKFold
        from sklearn.preprocessing import StandardScaler
    except ImportError as exc:  # pragma: no cover - dependency is declared for offline training
        raise RuntimeError("scikit-learn is required for confidence model fitting") from exc

    y = _validate_training_contract(
        labels=labels,
        groups=groups,
        development_ids=development_ids,
        calibration_ids=calibration_ids,
    )
    selected_feature_names = tuple(str(value) for value in feature_names)
    x = feature_matrix(feature_rows, feature_names=selected_feature_names)
    group_values = np.asarray(list(map(str, groups)))
    splitter = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=random_seed)
    oof = np.full(len(y), np.nan, dtype=np.float64)
    calibrated_oof = np.full(len(y), np.nan, dtype=np.float64) if isotonic else None
    folds: list[dict[str, Any]] = []
    fold_assignment = np.zeros(len(y), dtype=np.int64)
    for fold, (train_index, validation_index) in enumerate(
        splitter.split(x, y, groups=group_values), start=1
    ):
        scaler = StandardScaler().fit(x[train_index])
        classifier = LogisticRegression(
            C=1.0,
            solver="lbfgs",
            max_iter=2000,
            random_state=random_seed,
        ).fit(scaler.transform(x[train_index]), y[train_index])
        predicted = classifier.predict_proba(scaler.transform(x[validation_index]))[:, 1]
        oof[validation_index] = predicted
        fold_assignment[validation_index] = fold
        if calibrated_oof is not None:
            inner_x = x[train_index]
            inner_y = y[train_index]
            inner_groups = group_values[train_index]
            inner_oof = np.full(len(train_index), np.nan, dtype=np.float64)
            inner_splitter = StratifiedGroupKFold(
                n_splits=4,
                shuffle=True,
                random_state=random_seed + fold,
            )
            for inner_train, inner_validation in inner_splitter.split(
                inner_x,
                inner_y,
                groups=inner_groups,
            ):
                inner_scaler = StandardScaler().fit(inner_x[inner_train])
                inner_classifier = LogisticRegression(
                    C=1.0,
                    solver="lbfgs",
                    max_iter=2000,
                    random_state=random_seed + fold,
                ).fit(inner_scaler.transform(inner_x[inner_train]), inner_y[inner_train])
                inner_oof[inner_validation] = inner_classifier.predict_proba(
                    inner_scaler.transform(inner_x[inner_validation])
                )[:, 1]
            if not np.isfinite(inner_oof).all():
                raise RuntimeError("nested group cross-validation did not cover the outer train fold")
            fold_isotonic = IsotonicRegression(
                y_min=0.0,
                y_max=1.0,
                out_of_bounds="clip",
            ).fit(inner_oof, inner_y)
            calibrated_oof[validation_index] = fold_isotonic.predict(predicted)
        folds.append(
            {
                "fold": fold,
                "train_count": len(train_index),
                "validation_count": len(validation_index),
                "validation_positive_count": int(y[validation_index].sum()),
                "train_group_count": len(set(group_values[train_index])),
                "validation_group_count": len(set(group_values[validation_index])),
                "group_overlap": len(
                    set(group_values[train_index]) & set(group_values[validation_index])
                ),
            }
        )
    if not np.isfinite(oof).all():
        raise RuntimeError("group cross-validation did not produce every out-of-fold score")

    isotonic_model = None
    evaluated = oof if calibrated_oof is None else calibrated_oof
    if isotonic:
        isotonic_model = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip").fit(
            oof, y
        )
    if not np.isfinite(evaluated).all():
        raise RuntimeError("nested group cross-validation did not produce every calibrated score")

    final_scaler = StandardScaler().fit(x)
    final_classifier = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=2000,
        random_state=random_seed,
    ).fit(final_scaler.transform(x), y)
    model = ConfidenceModel(
        feature_names=selected_feature_names,
        means=tuple(float(value) for value in final_scaler.mean_),
        scales=tuple(float(value) for value in final_scaler.scale_),
        coefficients=tuple(float(value) for value in final_classifier.coef_[0]),
        intercept=float(final_classifier.intercept_[0]),
        isotonic_x=(
            ()
            if isotonic_model is None
            else tuple(float(value) for value in isotonic_model.X_thresholds_)
        ),
        isotonic_y=(
            ()
            if isotonic_model is None
            else tuple(float(value) for value in isotonic_model.y_thresholds_)
        ),
        target=target,
    )
    fraction, mean_prediction = calibration_curve(y, evaluated, n_bins=10, strategy="quantile")
    reliability_bins: list[dict[str, Any]] = []
    ece = 0.0
    edges = np.linspace(0.0, 1.0, 11)
    for index, (lower, upper) in enumerate(zip(edges, edges[1:], strict=False)):
        selected = (evaluated >= lower) & (
            evaluated <= upper if index == len(edges) - 2 else evaluated < upper
        )
        count = int(selected.sum())
        if not count:
            reliability_bins.append(
                {"lower": float(lower), "upper": float(upper), "count": 0}
            )
            continue
        mean_score = float(evaluated[selected].mean())
        observed = float(y[selected].mean())
        ece += count / len(y) * abs(mean_score - observed)
        reliability_bins.append(
            {
                "lower": float(lower),
                "upper": float(upper),
                "count": count,
                "mean_score": mean_score,
                "observed_rate": observed,
            }
        )
    report = {
        "method": model.method,
        "target": target,
        "feature_names": list(selected_feature_names),
        "fitted_split": "development",
        "row_count": len(y),
        "positive_count": int(y.sum()),
        "group_count": len(set(group_values)),
        "cross_validation": {
            "method": (
                "nested_stratified_group_5x4_fold"
                if isotonic
                else "stratified_group_5_fold"
            ),
            "random_seed": random_seed,
            "folds": folds,
            "roc_auc": float(roc_auc_score(y, evaluated)),
            "pr_auc": float(average_precision_score(y, evaluated)),
            "brier_score": float(brier_score_loss(y, evaluated)),
            "ece": float(ece),
            "reliability_bins": reliability_bins,
            "oof_probabilities": [float(value) for value in evaluated],
            "oof_rows": [
                {
                    "query_id": str(query_id),
                    "group_id": str(group_id),
                    "fold": int(fold),
                    "label": int(label),
                    "score": float(score),
                }
                for query_id, group_id, fold, label, score in zip(
                    development_ids,
                    group_values,
                    fold_assignment,
                    y,
                    evaluated,
                    strict=True,
                )
            ],
            "calibration_curve": [
                {"mean_probability": float(predicted), "empirical_accuracy": float(observed)}
                for predicted, observed in zip(mean_prediction, fraction, strict=True)
            ],
        },
    }
    return model, report
