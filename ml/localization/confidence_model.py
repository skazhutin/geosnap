"""Small, interpretable retrieval-feature confidence models with leakage guards."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np

FEATURE_NAMES = (
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
            "target": "localization_error_m_lte_100",
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
) -> tuple[ConfidenceModel, dict[str, Any]]:
    """Fit on development only and return group-CV evidence plus a portable artifact."""

    try:
        from sklearn.calibration import calibration_curve
        from sklearn.isotonic import IsotonicRegression
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import brier_score_loss, roc_auc_score
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
    x = feature_matrix(feature_rows)
    group_values = np.asarray(list(map(str, groups)))
    splitter = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=random_seed)
    oof = np.full(len(y), np.nan, dtype=np.float64)
    calibrated_oof = np.full(len(y), np.nan, dtype=np.float64) if isotonic else None
    folds: list[dict[str, Any]] = []
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
        feature_names=FEATURE_NAMES,
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
    )
    fraction, mean_prediction = calibration_curve(y, evaluated, n_bins=10, strategy="quantile")
    report = {
        "method": model.method,
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
            "brier_score": float(brier_score_loss(y, evaluated)),
            "oof_probabilities": [float(value) for value in evaluated],
            "calibration_curve": [
                {"mean_probability": float(predicted), "empirical_accuracy": float(observed)}
                for predicted, observed in zip(mean_prediction, fraction, strict=True)
            ],
        },
    }
    return model, report
