"""Nested geographic evaluation of confidence; raw coordinates stay fixed.

Outer-fold operating thresholds and hyperparameters are selected using only inner
training-fold outcomes. Full-development models are portable JSON; new calibration
will select the one frozen candidate's threshold, never its model family.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits

from ml.localization.confidence_model import FEATURE_NAMES, PART2_5_FEATURE_NAMES, ConfidenceModel, feature_vector
from ml.research.metrics import product_metrics, raw_metrics, select_threshold
from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256
from ml.research.vector_evaluation import development_frame

SEED = 20260905


def estimator(family, parameter):
    if family == "logistic":
        return make_pipeline(StandardScaler(), LogisticRegression(C=parameter, max_iter=2000, random_state=SEED))
    return HistGradientBoostingClassifier(
        max_leaf_nodes=parameter,
        max_iter=100,
        learning_rate=0.07,
        l2_regularization=5,
        min_samples_leaf=30,
        early_stopping=False,
        random_state=SEED,
    )


def folds(x, y, groups, n):
    return list(StratifiedGroupKFold(n_splits=n, shuffle=True, random_state=SEED).split(x, y, groups))


def fit_with_auxiliary(x, y, family, parameter, auxiliary=None):
    if auxiliary is not None:
        ax, ay, _ = auxiliary
        x, y = np.concatenate([x, ax]), np.concatenate([y, ay])
    return estimator(family, parameter).fit(x, y)


def exclude_auxiliary_groups(auxiliary, excluded):
    if auxiliary is None:
        return None
    x, y, groups = auxiliary
    keep = ~np.isin(groups, excluded)
    return x[keep], y[keep], groups[keep]


def select_inner(x, errors, groups, family, n, auxiliary=None):
    y = errors <= 100
    parameters = [0.1, 1.0, 10.0] if family == "logistic" else [3, 7, 15]
    comparisons = []
    for parameter in parameters:
        oof = np.full(len(x), np.nan)
        for train, validation in folds(x, y, groups, n):
            if set(groups[train]) & set(groups[validation]):
                raise ValueError("geographic fold leakage")
            model = fit_with_auxiliary(
                x[train], y[train], family, parameter, exclude_auxiliary_groups(auxiliary, groups[validation])
            )
            oof[validation] = model.predict_proba(x[validation])[:, 1]
        operating = select_threshold(errors, oof)
        comparisons.append({"parameter": parameter, "operating": operating, "log_loss": float(log_loss(y, oof))})
    # Numeric ordering is fixed before evaluation; no outer labels enter this choice.
    chosen = max(
        comparisons,
        key=lambda r: (
            r["operating"]["answer_rate"],
            -(r["operating"]["accepted_gt500m_rate"] or 0),
            -r["log_loss"],
            -r["parameter"],
        ),
    )
    return chosen, comparisons


def portable(model, names, family):
    if family == "logistic":
        scaler, classifier = model.steps[0][1], model.steps[1][1]
        fitted = ConfidenceModel(
            feature_names=tuple(names),
            means=tuple(scaler.mean_),
            scales=tuple(scaler.scale_),
            coefficients=tuple(classifier.coef_[0]),
            intercept=float(classifier.intercept_[0]),
        )
        return {"family": "logistic", "model": fitted.to_dict(), "fitted_split": "development"}
    trees = []
    for stage in model._predictors:
        if len(stage) != 1:
            raise ValueError("portable confidence expects binary classification")
        nodes = stage[0].nodes
        if nodes["is_categorical"].any():
            raise ValueError("categorical confidence trees are not supported")
        trees.append(
            [
                {
                    "leaf": bool(row["is_leaf"]),
                    "value": float(row["value"]),
                    "feature": int(row["feature_idx"]),
                    "threshold": float(row["num_threshold"]),
                    "left": int(row["left"]),
                    "right": int(row["right"]),
                    "missing_left": bool(row["missing_go_to_left"]),
                }
                for row in nodes
            ]
        )
    return {
        "family": "histogram_gradient_boosting",
        "feature_names": list(names),
        "trees": trees,
        "intercept": float(model._baseline_prediction[0, 0]),
        "fitted_split": "development",
    }


def predict_portable(payload, features):
    if payload.get("fitted_split") != "development":
        raise ValueError("confidence parameters must be fit on development")
    if payload["family"] == "logistic":
        model = ConfidenceModel.from_dict(payload["model"])
        return np.asarray([model.predict_proba(row) for row in features])
    if payload["family"] != "histogram_gradient_boosting":
        raise ValueError("unknown confidence family")
    x = np.asarray([feature_vector(row, feature_names=payload["feature_names"]) for row in features])
    logits = np.full(len(x), payload["intercept"])
    for tree in payload["trees"]:
        for i, row in enumerate(x):
            node = 0
            for _ in range(len(tree)):
                current = tree[node]
                if current["leaf"]:
                    logits[i] += current["value"]
                    break
                v = row[current["feature"]]
                left = current["missing_left"] if np.isnan(v) else v <= current["threshold"]
                node = current["left"] if left else current["right"]
            else:
                raise ValueError("invalid confidence tree")
    return 1 / (1 + np.exp(-np.clip(logits, -40, 40)))


def run(rows_path, queries_path, output, auxiliary_rows_path=None):
    if not rows_path.resolve().is_relative_to((ROOT / "development").resolve()):
        raise ValueError("registered development results only")
    queries = development_frame(queries_path).set_index("id")
    rows = json.loads(rows_path.read_text())
    ids = rows["query_ids"]
    if len(ids) != len(queries) or len(set(ids)) != len(ids) or set(ids) != set(queries.index):
        raise ValueError("development query identities changed")
    groups = np.asarray([str(queries.loc[i].h3_coarse) for i in ids])
    if groups.tolist() != rows["groups"]:
        raise ValueError("development geographic groups changed")
    errors = np.asarray(rows["errors"], float)
    if not np.isfinite(errors).all():
        raise ValueError("this confidence experiment requires complete finite raw predictions")
    y = errors <= 100
    auxiliary_rows = None
    if auxiliary_rows_path is not None:
        if not auxiliary_rows_path.resolve().is_relative_to((ROOT / "development").resolve()):
            raise ValueError("auxiliary confidence rows must be public development results")
        historical = development_frame(Path("data/evaluation/moscow_real_v4/development_queries.parquet")).set_index(
            "id"
        )
        auxiliary_rows = json.loads(auxiliary_rows_path.read_text())
        if len(auxiliary_rows["query_ids"]) != len(historical) or set(auxiliary_rows["query_ids"]) != set(
            historical.index
        ):
            raise ValueError("auxiliary rows must be exactly the historical development cohort")
        if auxiliary_rows["groups"] != [str(historical.loc[i].h3_coarse) for i in auxiliary_rows["query_ids"]]:
            raise ValueError("auxiliary geographic groups changed")
        if set(auxiliary_rows["query_ids"]) & set(ids) or not np.isfinite(auxiliary_rows["errors"]).all():
            raise ValueError("auxiliary cohort must have distinct queries and finite recorded errors")
    output.mkdir(parents=True, exist_ok=True)
    result = {
        "kind": "nested_development_confidence_only",
        "rows_sha256": sha256(rows_path),
        "queries_sha256": sha256(queries_path),
        "raw_unchanged": raw_metrics(errors),
        "calibration_opened": False,
        "final_opened": False,
        "families": {},
        "auxiliary_rows_sha256": sha256(auxiliary_rows_path) if auxiliary_rows_path else None,
        "auxiliary_policy": "training only; exclude every outer/inner validation geographic group; thresholds and comparisons use primary new development only",
    }
    with threadpool_limits(limits=2):
        for family in ["logistic", "hgb"]:
            feature_sets = rows.get("feature_sets", {"14": PART2_5_FEATURE_NAMES, "all": FEATURE_NAMES})
            for feature_name, names in feature_sets.items():
                label = family + "_" + feature_name
                x = np.asarray([feature_vector(f, feature_names=names) for f in rows["features"]])
                auxiliary = (
                    None
                    if auxiliary_rows is None
                    else (
                        np.asarray([feature_vector(f, feature_names=names) for f in auxiliary_rows["features"]]),
                        np.asarray(auxiliary_rows["errors"]) <= 100,
                        np.asarray(auxiliary_rows["groups"]),
                    )
                )
                oof, accepted = np.full(len(x), np.nan), np.zeros(len(x), bool)
                evidence = []
                for train, validation in folds(x, y, groups, 5):
                    training_auxiliary = exclude_auxiliary_groups(auxiliary, groups[validation])
                    chosen, inner = select_inner(x[train], errors[train], groups[train], family, 3, training_auxiliary)
                    fitted = fit_with_auxiliary(x[train], y[train], family, chosen["parameter"], training_auxiliary)
                    oof[validation] = fitted.predict_proba(x[validation])[:, 1]
                    threshold = chosen["operating"]["threshold"]
                    if threshold is not None:
                        accepted[validation] = oof[validation] >= threshold
                    evidence.append(
                        {
                            "train_ids": [ids[i] for i in train],
                            "validation_ids": [ids[i] for i in validation],
                            "chosen": chosen,
                            "inner_comparisons": inner,
                            "auxiliary_training_count": len(training_auxiliary[0])
                            if training_auxiliary is not None
                            else 0,
                            "excluded_auxiliary_geographic_groups": sorted(set(groups[validation].tolist())),
                        }
                    )
                chosen, full_cv = select_inner(x, errors, groups, family, 5, auxiliary)
                fitted = fit_with_auxiliary(x, y, family, chosen["parameter"], auxiliary)
                payload = portable(fitted, names, family)
                actual = predict_portable(payload, rows["features"])
                expected = fitted.predict_proba(x)[:, 1]
                if not np.allclose(actual, expected, atol=1e-12, rtol=1e-12):
                    raise ValueError("portable confidence differs from its training implementation")
                path = output / f"{label}.json"
                path.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n")
                result["families"][label] = {
                    "strict_nested_operating_point": product_metrics(errors, accepted),
                    "exploratory_oof_max_answer": select_threshold(errors, oof),
                    "chosen_parameter_for_full_development_fit": chosen["parameter"],
                    "portable_sha256": sha256(path),
                    "portable_max_probability_difference": float(np.max(abs(actual - expected))),
                    "full_development_parameter_selection": full_cv,
                }
                (output / f"{label}_folds.json").write_text(
                    json.dumps(
                        {"folds": evidence, "oof": oof.tolist(), "strict_nested_accepted": accepted.tolist()}, indent=2
                    )
                    + "\n"
                )
                print(label, json.dumps(result["families"][label]["strict_nested_operating_point"]), flush=True)
    (output / "summary.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=Path, required=True)
    parser.add_argument("--queries", type=Path, default=ROOT / "prospective/development.parquet")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--auxiliary-rows", type=Path)
    args = parser.parse_args()
    run(args.rows, args.queries, args.output, args.auxiliary_rows)
