"""Grouped evaluation of semantic encoder features against frozen Qwen pseudo-labels."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr
from sklearn.metrics import mean_absolute_error, roc_auc_score
from sklearn.model_selection import GroupShuffleSplit

from .common import json_once, revision, sha256
from .validate_small_encoder import PILOT, V1, choose_alpha, pipeline, read_rows

OUT = V1.parent / "geolocatability_fast_models_v1_20260929"


def metrics(y: np.ndarray, predictions: np.ndarray) -> dict:
    return {"mae": round(float(mean_absolute_error(y, predictions)), 6),
            "spearman": round(float(spearmanr(y, predictions).statistic), 6),
            "qwen_low_auc": round(float(roc_auc_score(y <= 0.4, -predictions)), 6)}


def bootstrap_delta(y: np.ndarray, baseline: np.ndarray, candidate: np.ndarray,
                    groups: np.ndarray) -> list[float]:
    unique = np.unique(groups)
    indexed = [np.flatnonzero(groups == group) for group in unique]
    rng = np.random.default_rng(20260929)
    deltas = []
    for _ in range(2000):
        sampled = rng.integers(0, len(unique), size=len(unique))
        indices = np.concatenate([indexed[i] for i in sampled])
        deltas.append(float(np.abs(y[indices] - candidate[indices]).mean() -
                            np.abs(y[indices] - baseline[indices]).mean()))
    return [round(float(value), 6) for value in np.quantile(deltas, [0.025, 0.975])]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, choices=("siglip2_base", "mobileclip2_s0"))
    args = parser.parse_args()
    feature_path = OUT / f"query_features_{args.model}.npz"
    if not feature_path.exists():
        raise FileNotFoundError(feature_path)
    features = np.load(feature_path)
    ids = features["query_id"].tolist()
    array = features["feature"].astype(np.float32)
    assert len(ids) == len(set(ids)) == len(array) == 1184 and np.isfinite(array).all()
    frame, freeze = read_rows()
    valid = frame[frame.annotation_status == "valid"].copy()
    sample = json.loads((PILOT / "quick20_private_sample.json").read_text())
    token_to_id = {item["token"]: item["query_id"] for item in sample["items"]}
    human = [json.loads(line) for line in
             (PILOT / "quick20_rater_reviewer1.jsonl").read_text().splitlines()]
    human_ids = [token_to_id[item["token"]] for item in human]
    human_groups = set(valid.set_index("query_id").loc[human_ids].evaluation_geo_group_id)
    nonhuman = valid[~valid.evaluation_geo_group_id.isin(human_groups)].reset_index(drop=True)
    groups = nonhuman.evaluation_geo_group_id.to_numpy()
    train, test = next(GroupShuffleSplit(n_splits=1, test_size=0.2,
                                          random_state=20260929).split(nonhuman, groups=groups))
    assert len(train) == 828 and len(test) == 220 and not set(groups[train]) & set(groups[test])
    row_by_id = {query_id: index for index, query_id in enumerate(ids)}
    semantic = np.stack([array[row_by_id[query_id]] for query_id in nonhuman.query_id])
    dino = nonhuman[[f"dino_{i}" for i in range(384)]].to_numpy(dtype=np.float32)
    y = nonhuman.geolocatability_median.to_numpy(dtype=np.float32)
    datasets = {"dino_baseline": dino, args.model: semantic,
                f"dino_plus_{args.model}": np.concatenate([dino, semantic], axis=1)}
    predictions = {}
    results = []
    for name, x in datasets.items():
        alpha = choose_alpha(x[train], y[train], groups[train])
        model = pipeline(alpha).fit(x[train], y[train])
        pred = np.clip(model.predict(x[test]), 0, 1)
        predictions[name] = pred
        results.append({"variant": name, "ridge_alpha_inner_group_cv": alpha,
                        "feature_dimensions": x.shape[1], **metrics(y[test], pred)})
    for item in results[1:]:
        item["paired_group_bootstrap_mae_delta_vs_dino_95ci"] = bootstrap_delta(
            y[test], predictions["dino_baseline"], predictions[item["variant"]], groups[test])
    rows = [{"query_id": query_id, "qwen_median": round(float(y[index]), 6),
             **{name: round(float(values[position]), 6) for name, values in predictions.items()}}
            for position, (index, query_id) in enumerate(
                zip(test, nonhuman.iloc[test].query_id, strict=True))]
    predictions_path = OUT / f"{args.model}_grouped_holdout_predictions.json"
    json_once(predictions_path, rows)
    receipt_path = OUT / f"{args.model}_grouped_holdout_report.json"
    json_once(receipt_path, {"model": args.model, "code_revision": revision(),
                             "source_script_sha256": sha256(Path(__file__)),
                             "feature_sha256": sha256(feature_path),
                             "teacher_annotation_sha256": freeze["canonical_jsonl_sha256"],
                             "train_n": len(train), "holdout_n": len(test),
                             "results": results, "predictions_sha256": sha256(predictions_path),
                             "outcome_data_accessed": False,
                             "limitation": "Qwen pseudo-label agreement, not human-ground-truth quality"})
    print(json.dumps(results, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
