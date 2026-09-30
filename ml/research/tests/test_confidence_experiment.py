import numpy as np
import pytest
from threadpoolctl import threadpool_limits

from ml.research.confidence_experiment import estimator, portable, predict_portable


@pytest.mark.parametrize("family,parameter", [("logistic", 1.0), ("hgb", 7)])
def test_portable_confidence_preserves_trained_model_probabilities(family, parameter):
    rng = np.random.default_rng(51)
    x = rng.normal(size=(240, 2))
    y = x[:, 0] + x[:, 1] ** 2 > 0.7
    names = ["top1_similarity", "geographic_mode_margin"]
    with threadpool_limits(limits=2):
        fitted = estimator(family, parameter).fit(x, y)
        exported = portable(fitted, names, family)
        # Includes new points and exact tree decision boundaries.
        points = [*x[:10], *rng.normal(size=(30, 2)), [0, 0]]
        if family == "hgb":
            for tree in exported["trees"][:3]:
                for node in tree:
                    if not node["leaf"]:
                        row = np.zeros(2)
                        row[node["feature"]] = node["threshold"]
                        points.append(row)
        expected = fitted.predict_proba(np.asarray(points))[:, 1]
    features = [dict(zip(names, row, strict=True)) for row in points]
    np.testing.assert_allclose(predict_portable(exported, features), expected, atol=1e-12, rtol=1e-12)


def test_auxiliary_training_excludes_inner_validation_geography(monkeypatch):
    from ml.research import confidence_experiment as experiment

    x = np.arange(6, dtype=float).reshape(-1, 1)
    errors = np.array([10, 1000, 10, 1000, 10, 1000], dtype=float)
    groups = np.array(["a", "a", "b", "b", "c", "c"])
    auxiliary_x = np.arange(100, 104, dtype=float).reshape(-1, 1)
    auxiliary_groups = np.array(["a", "b", "c", "d"])
    group_by_id = dict(zip(x[:, 0], groups, strict=True)) | dict(zip(auxiliary_x[:, 0], auxiliary_groups, strict=True))
    seen = []

    class Model:
        def fit(self, inputs, labels):
            self.training_ids = inputs[:, 0].tolist()
            return self

        def predict_proba(self, inputs):
            validation_groups = {group_by_id[i] for i in inputs[:, 0]}
            assert not validation_groups & {group_by_id[i] for i in self.training_ids}
            assert 103 in self.training_ids  # An independent auxiliary area remains usable.
            seen.append(self.training_ids)
            return np.tile([0.5, 0.5], (len(inputs), 1))

    monkeypatch.setattr(experiment, "estimator", lambda family, parameter: Model())
    experiment.select_inner(
        x,
        errors,
        groups,
        "logistic",
        3,
        (auxiliary_x, np.array([True, False, True, False]), auxiliary_groups),
    )
    assert len(seen) == 9
