from __future__ import annotations

import pytest

from ml.localization.confidence_model import FEATURE_NAMES, ConfidenceModel, fit_confidence_model


def _rows(count: int = 100) -> list[dict[str, float]]:
    return [
        {
            name: float(((row_index + feature_index * 3) % 17) / 16.0)
            for feature_index, name in enumerate(FEATURE_NAMES)
        }
        for row_index in range(count)
    ]


def test_confidence_fit_is_development_only_and_portable() -> None:
    rows = _rows()
    labels = [int(index % 4 in {0, 1}) for index in range(len(rows))]
    groups = [f"group-{index // 5}" for index in range(len(rows))]
    ids = [f"development-{index}" for index in range(len(rows))]

    model, report = fit_confidence_model(
        rows,
        labels,
        groups=groups,
        development_ids=ids,
        calibration_ids=["calibration-1"],
        isotonic=True,
    )
    restored = ConfidenceModel.from_dict(model.to_dict())

    assert restored.fitted_split == "development"
    assert restored.predict_proba(rows[0]) == pytest.approx(model.predict_proba(rows[0]))
    assert report["cross_validation"]["method"] == "nested_stratified_group_5x4_fold"
    assert all(
        fold["group_overlap"] == 0 for fold in report["cross_validation"]["folds"]
    )


def test_confidence_fit_rejects_train_calibration_overlap() -> None:
    rows = _rows()
    labels = [int(index % 4 in {0, 1}) for index in range(len(rows))]
    groups = [f"group-{index // 5}" for index in range(len(rows))]
    ids = [f"development-{index}" for index in range(len(rows))]

    with pytest.raises(ValueError, match="train/calibration overlap"):
        fit_confidence_model(
            rows,
            labels,
            groups=groups,
            development_ids=ids,
            calibration_ids=[ids[0]],
        )


def test_confidence_artifact_rejects_non_development_fit() -> None:
    row = {
        "schema_version": 1,
        "feature_names": list(FEATURE_NAMES),
        "means": [0.0] * len(FEATURE_NAMES),
        "scales": [1.0] * len(FEATURE_NAMES),
        "coefficients": [0.0] * len(FEATURE_NAMES),
        "intercept": 0.0,
        "fitted_split": "calibration",
    }
    with pytest.raises(ValueError, match="development only"):
        ConfidenceModel.from_dict(row)
