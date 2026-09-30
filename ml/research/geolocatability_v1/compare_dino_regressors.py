"""Outcome-blind comparison of small heads on frozen DINOv2-S query features."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr
from sklearn.ensemble import ExtraTreesRegressor, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, roc_auc_score
from sklearn.model_selection import GroupKFold, GroupShuffleSplit
from sklearn.neighbors import KNeighborsRegressor
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import Normalizer, StandardScaler
from sklearn.svm import SVR

from .common import json_once, revision, sha256, now
from .validate_small_encoder import OUT, PILOT, V1, read_rows

RESULT = OUT / "regressor_comparison_v1_20260929.json"
PREDICTIONS = OUT / "regressor_comparison_v1_20260929_predictions.json"


def candidates():
    # The fixed grid is evaluated only on inner geographic folds.
    for alpha in (100.0, 1000.0, 10000.0):
        yield "ridge", {"alpha": alpha}, make_pipeline(
            SimpleImputer(strategy="median"), StandardScaler(), Ridge(alpha=alpha))
    for c in (0.1, 1.0, 10.0):
        for epsilon in (0.05, 0.10):
            yield "rbf_svr", {"C": c, "epsilon": epsilon}, make_pipeline(
                SimpleImputer(strategy="median"), StandardScaler(),
                SVR(kernel="rbf", C=c, epsilon=epsilon, gamma="scale"))
    for leaf in (3, 10):
        yield "extra_trees", {"min_samples_leaf": leaf}, make_pipeline(
            SimpleImputer(strategy="median"), ExtraTreesRegressor(
                n_estimators=120, min_samples_leaf=leaf, max_features=0.7,
                random_state=20260929, n_jobs=2))
    for leaves in (7, 15):
        yield "hist_gradient_boosting", {"max_leaf_nodes": leaves}, make_pipeline(
            SimpleImputer(strategy="median"), HistGradientBoostingRegressor(
                max_iter=100, learning_rate=0.05, max_leaf_nodes=leaves,
                l2_regularization=1.0, random_state=20260929))
    for neighbors in (10, 30):
        yield "cosine_knn", {"n_neighbors": neighbors}, make_pipeline(
            SimpleImputer(strategy="median"), Normalizer(), KNeighborsRegressor(
                n_neighbors=neighbors, weights="distance", metric="cosine"))


def main() -> None:
    if RESULT.exists() or PREDICTIONS.exists():
        raise FileExistsError("Comparison artifacts already exist; evidence is write-once")
    frame, freeze = read_rows()
    valid = frame[frame.annotation_status == "valid"].copy()
    sample = json.loads((PILOT / "quick20_private_sample.json").read_text())
    token_to_id = {item["token"]: item["query_id"] for item in sample["items"]}
    human = [json.loads(line) for line in
             (PILOT / "quick20_rater_reviewer1.jsonl").read_text().splitlines()]
    human_ids = [token_to_id[item["token"]] for item in human]
    assert len(human_ids) == len(set(human_ids)) == 11
    human_groups = set(valid.set_index("query_id").loc[human_ids].evaluation_geo_group_id)
    nonhuman = valid[~valid.evaluation_geo_group_id.isin(human_groups)].reset_index(drop=True)
    groups = nonhuman.evaluation_geo_group_id.to_numpy()
    train, test = next(GroupShuffleSplit(n_splits=1, test_size=0.2,
                                          random_state=20260929).split(nonhuman, groups=groups))
    assert len(train) == 828 and len(test) == 220
    assert not set(groups[train]) & set(groups[test])
    columns = [f"dino_{i}" for i in range(384)]
    x = nonhuman[columns].to_numpy(dtype=np.float32)
    y = nonhuman.geolocatability_median.to_numpy(dtype=np.float32)
    folds = list(GroupKFold(n_splits=3).split(x[train], y[train], groups[train]))
    grid = []
    selected = {}
    for family, params, model in candidates():
        losses = []
        for fit, valid_idx in folds:
            model.fit(x[train][fit], y[train][fit])
            predicted = np.clip(model.predict(x[train][valid_idx]), 0, 1)
            losses.append(float(mean_absolute_error(y[train][valid_idx], predicted)))
        record = {"family": family, "parameters": params,
                  "inner_grouped_mae": round(float(np.mean(losses)), 6),
                  "inner_fold_mae": [round(v, 6) for v in losses]}
        grid.append(record)
        if family not in selected or record["inner_grouped_mae"] < selected[family][0]["inner_grouped_mae"]:
            selected[family] = (record, model)
        print(json.dumps(record), flush=True)
    holdout = []
    predictions = {}
    for family, (choice, model) in selected.items():
        model.fit(x[train], y[train])
        predicted = np.clip(model.predict(x[test]), 0, 1)
        predictions[family] = predicted
        correlation = spearmanr(y[test], predicted).statistic
        holdout.append({"family": family, "selected_parameters": choice["parameters"],
                        "inner_grouped_mae": choice["inner_grouped_mae"],
                        "holdout_mae": round(float(mean_absolute_error(y[test], predicted)), 6),
                        "holdout_spearman": round(float(correlation), 6),
                        "holdout_qwen_low_auc": round(float(roc_auc_score(y[test] <= 0.4, -predicted)), 6)})
    ids = nonhuman.iloc[test].query_id.tolist()
    rows = [{"query_id": query_id, "qwen_median": round(float(y[test][i]), 6),
             **{family: round(float(values[i]), 6) for family, values in predictions.items()}}
            for i, query_id in enumerate(ids)]
    json_once(PREDICTIONS, rows)
    receipt = {
        "created_at": now(), "code_revision": revision(),
        "script_sha256": sha256(Path(__file__)),
        "teacher_annotation_sha256": freeze["canonical_jsonl_sha256"],
        "dino_feature_sha256": sha256(OUT / "query_features_dinov2_small.npz"),
        "split": "GroupShuffleSplit(seed=20260929, test_size=0.2), same as validate_small_encoder.py",
        "human_pilot_usage": "Eleven images and their geographic groups excluded; human scores not fitted or used for selection",
        "outcome_data_accessed": False,
        "train_n": len(train), "holdout_n": len(test),
        "train_ids_sha256": hashlib.sha256("\n".join(nonhuman.iloc[train].query_id).encode()).hexdigest(),
        "holdout_ids_sha256": hashlib.sha256("\n".join(ids).encode()).hexdigest(),
        "grid": grid, "holdout": holdout,
        "predictions_path": str(PREDICTIONS), "predictions_sha256": sha256(PREDICTIONS),
        "caveat": "Agreement with Qwen pseudo-labels, not human-ground-truth defect detection",
    }
    json_once(RESULT, receipt)
    print(json.dumps({"holdout": holdout, "result": str(RESULT)}, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
