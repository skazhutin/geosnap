"""Grouped, outcome-blind pilot validation of a small geolocatability student."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import average_precision_score, mean_absolute_error, roc_auc_score
from sklearn.model_selection import GroupKFold, GroupShuffleSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
V1 = ROOT / "data/evaluation/geolocatability_v1_20260928"
PILOT = ROOT / "data/evaluation/geographic_v9_20260928/annotations"
OUT = ROOT / "data/evaluation/geolocatability_speedpilot_20260929"
TECHNICAL = ["aspect_ratio", "bytes_per_pixel", "canny_edge_density", "clipped_bright_fraction",
             "dark_fraction", "frequency_high_band_ratio", "gradient_direction_coherence", "height",
             "laplacian_variance", "lower_edge_density", "luminance_entropy_bits", "mean_luminance",
             "mean_saturation", "megapixels", "p95_minus_p5_luminance", "tenengrad",
             "upper_edge_density", "width"]
ALPHAS = (0.1, 1.0, 10.0, 100.0, 1000.0)


def read_rows() -> tuple[pd.DataFrame, dict]:
    teacher_file = V1 / "geolocatability_annotations.jsonl"
    freeze = json.loads((V1 / "annotation_freeze.json").read_text())
    assert hashlib.sha256(teacher_file.read_bytes()).hexdigest() == freeze["canonical_jsonl_sha256"]
    teacher = pd.DataFrame(json.loads(line) for line in teacher_file.read_text().splitlines())
    metadata = pd.read_parquet(ROOT / "data/evaluation/moscow_research_v5/prospective/development.parquet",
                               columns=["id", "evaluation_geo_group_id"])
    features_file = OUT / "query_features_dinov2_small.npz"
    receipt = json.loads((OUT / "query_features_receipt.json").read_text())
    assert hashlib.sha256(features_file.read_bytes()).hexdigest() == receipt["features_sha256"]
    vectors = np.load(features_file)
    ids = vectors["query_id"].tolist()
    assert len(ids) == len(set(ids)) == 1184
    frame = pd.DataFrame(vectors["feature"])
    frame.columns = [f"dino_{i}" for i in range(frame.shape[1])]
    frame.insert(0, "query_id", ids)
    frame = frame.merge(teacher, on="query_id", validate="one_to_one").merge(
        metadata, left_on="query_id", right_on="id", validate="one_to_one")
    assert len(frame) == 1184
    return frame, freeze


def pipeline(alpha: float):
    return make_pipeline(SimpleImputer(strategy="median"), StandardScaler(), Ridge(alpha=alpha))


def choose_alpha(x: np.ndarray, y: np.ndarray, groups: np.ndarray) -> float:
    splits = list(GroupKFold(n_splits=3).split(x, y, groups))
    losses = []
    for alpha in ALPHAS:
        scores = []
        for train, valid in splits:
            model = pipeline(alpha).fit(x[train], y[train])
            scores.append(mean_absolute_error(y[valid], np.clip(model.predict(x[valid]), 0, 1)))
        losses.append(float(np.mean(scores)))
    return ALPHAS[int(np.argmin(losses))]


def measure(y: np.ndarray, pred: np.ndarray) -> dict:
    clipped = np.clip(pred, 0, 1)
    corr = spearmanr(y, clipped).statistic
    return {"n": len(y), "mae": round(float(mean_absolute_error(y, clipped)), 4),
            "spearman": round(float(corr), 4) if np.isfinite(corr) else None}


def main() -> None:
    frame, freeze = read_rows()
    valid = frame[frame.annotation_status == "valid"].copy()
    sample = json.loads((PILOT / "quick20_private_sample.json").read_text())
    token_to_id = {item["token"]: item["query_id"] for item in sample["items"]}
    human = [json.loads(line) for line in (PILOT / "quick20_rater_reviewer1.jsonl").read_text().splitlines()]
    human_ids = [token_to_id[item["token"]] for item in human]
    assert len(human_ids) == len(set(human_ids)) == 11
    human_frame = valid.set_index("query_id").loc[human_ids]
    human_groups = set(human_frame.evaluation_geo_group_id)
    nonhuman = valid[~valid.evaluation_geo_group_id.isin(human_groups)].reset_index(drop=True)
    group = nonhuman.evaluation_geo_group_id.to_numpy()
    train_idx, test_idx = next(GroupShuffleSplit(n_splits=1, test_size=0.2,
                                                 random_state=20260929).split(nonhuman, groups=group))
    assert not set(group[train_idx]) & set(group[test_idx])
    specs = {"DINOv2-S": [f"dino_{i}" for i in range(384)],
             "technical_only": TECHNICAL,
             "DINOv2-S_plus_technical": [f"dino_{i}" for i in range(384)] + TECHNICAL}
    result = {"population": "1162 valid frozen VLM pseudo-labels; human pilot held out by geographic group",
              "teacher_annotation_sha256": freeze["canonical_jsonl_sha256"],
              "human_pilot_n": len(human), "human_groups_excluded_from_training": len(human_groups),
              "pseudo_train_n": len(train_idx), "pseudo_validation_n": len(test_idx),
              "pseudo_train_geo_groups": len(set(group[train_idx])),
              "pseudo_validation_geo_groups": len(set(group[test_idx])),
              "pseudo_label_validation": {}, "human_validation": {}, "retake_validation": {},
              "warning": "The 11 human ratings are a small, stopped pilot; teacher pseudo-labels are not human ground truth."}
    for name, columns in specs.items():
        x = nonhuman[columns].to_numpy(dtype=np.float32)
        y = nonhuman.geolocatability_median.to_numpy(dtype=np.float32)
        alpha = choose_alpha(x[train_idx], y[train_idx], group[train_idx])
        model = pipeline(alpha).fit(x[train_idx], y[train_idx])
        result["pseudo_label_validation"][name] = {"ridge_alpha_inner_cv": alpha,
            **measure(y[test_idx], model.predict(x[test_idx]))}
        human_model = pipeline(alpha).fit(x, y)
        hpred = np.clip(human_model.predict(human_frame[columns].to_numpy(dtype=np.float32)), 0, 1)
        human_score = np.asarray([item["geolocatability"] / 4 for item in human])
        result["human_validation"][name] = measure(human_score, hpred)
        if name == "DINOv2-S_plus_technical":
            for i, (item, pred) in enumerate(zip(human, hpred, strict=True)):
                result.setdefault("human_per_image", []).append({
                    "pilot_order": i + 1, "query_id": human_ids[i],
                    "human_score_0_to_4": item["geolocatability"],
                    "teacher_score_0_to_1": float(human_frame.iloc[i].geolocatability_median),
                    "student_score_0_to_1": round(float(pred), 4),
                    "human_retake": item["would_request_another_photo"],
                    "teacher_retake": bool(human_frame.iloc[i].retake_recommended)})
    human_score = np.asarray([item["geolocatability"] / 4 for item in human])
    result["human_validation"]["Qwen3.5-4B_teacher"] = measure(
        human_score, human_frame.geolocatability_median.to_numpy(dtype=np.float32))
    result["human_retake_disagreement_teacher"] = sum(
        bool(item["would_request_another_photo"]) != bool(human_frame.iloc[i].retake_recommended)
        for i, item in enumerate(human))
    # This is a separate head predicting the frozen teacher's recommendation.
    # Its 0.5 decision threshold is fixed before inspecting the human pilot.
    binary = nonhuman.retake_recommended.astype(int).to_numpy()
    human_binary = np.asarray([int(item["would_request_another_photo"]) for item in human])
    for name, columns in specs.items():
        x = nonhuman[columns].to_numpy(dtype=np.float32)
        validation_model = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(),
                                         LogisticRegression(C=1.0, max_iter=2000))
        validation_model.fit(x[train_idx], binary[train_idx])
        probability = validation_model.predict_proba(x[test_idx])[:, 1]
        held_out_model = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(),
                                       LogisticRegression(C=1.0, max_iter=2000))
        held_out_model.fit(x, binary)
        human_probability = held_out_model.predict_proba(
            human_frame[columns].to_numpy(dtype=np.float32))[:, 1]
        human_predicted = human_probability >= 0.5
        result["retake_validation"][name] = {
            "pseudo_validation_n": len(test_idx),
            "pseudo_validation_teacher_positive": int(binary[test_idx].sum()),
            "pseudo_validation_auroc": round(float(roc_auc_score(binary[test_idx], probability)), 4),
            "pseudo_validation_auprc": round(float(average_precision_score(binary[test_idx], probability)), 4),
            "human_n": len(human), "human_retake_yes": int(human_binary.sum()),
            "human_predicted_retake_yes": int(human_predicted.sum()),
            "human_agreement_count": int((human_predicted == human_binary).sum()),
            "human_false_keep_count": int(((~human_predicted) & (human_binary == 1)).sum()),
        }
    target = OUT / "dinov2_small_grouped_validation.json"
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
