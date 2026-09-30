"""Blind-label quality benchmarks, with training only on geographic OOF folds."""
from __future__ import annotations

import argparse
import hashlib
import json

import h3
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, confusion_matrix, roc_auc_score
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v9.analyze_annotations import load
from ml.research.geographic_v9.common import OUT, V8, record

TECH = OUT / "technical_quality/image_features.npz"
CLIP = V8 / "geoclip/query.npy"


def diagnostics(y, score, threshold=None):
    if len(set(y)) != 2:
        return {"status": "insufficient_class_diversity", "positive": int(sum(y)), "count": len(y)}
    result = {"count": len(y), "positive": int(sum(y)),
        "auroc": float(roc_auc_score(y, score)),
        "average_precision": float(average_precision_score(y, score)),
        "base_rate": float(np.mean(y))}
    if threshold is not None:
        pred = score >= threshold
        result["threshold"] = threshold
        result["confusion_true_rows_0_1"] = confusion_matrix(y, pred, labels=[0, 1]).tolist()
        result["false_rejection_good_images"] = int(np.sum((~y) & pred))
    return result


def percentile_risk(x, all_x, names):
    # Unsupervised technical tail rule. No human labels or outcomes select cutoffs.
    columns = [names.index(v) for v in ["mean_luminance", "laplacian_variance",
        "p95_minus_p5_luminance", "clipped_bright_fraction"]]
    low = [np.searchsorted(np.sort(all_x[:, j]), x[:, j], side="right")/len(all_x)
           for j in columns[:3]]
    high = np.searchsorted(np.sort(all_x[:, columns[3]]), x[:, columns[3]],
                           side="right")/len(all_x)
    risk = np.maximum.reduce([1-low[0], 1-low[1], 1-low[2], high])
    return risk


def grouped_oof(x, y, groups):
    if len(y) < 100 or sum(y) < 15 or sum(~y) < 15 or len(set(groups)) < 5:
        return None
    pred = np.full(len(y), np.nan, np.float32)
    receipt = []
    groups = np.asarray(groups, str)
    for fold, (possible, test) in enumerate(GroupKFold(n_splits=5).split(x, y, groups)):
        banned = set().union(*(set(h3.grid_disk(g, 1)) for g in set(groups[test])))
        train = np.asarray([i for i in possible if groups[i] not in banned], int)
        if len(train) < 40 or len(set(y[train])) < 2:
            return None
        model = make_pipeline(StandardScaler(), LogisticRegression(C=.1,
            class_weight="balanced", max_iter=500, random_state=20260928))
        model.fit(x[train], y[train])
        pred[test] = model.predict_proba(x[test])[:, 1]
        receipt.append({"fold": fold, "train": len(train), "held_out": len(test),
                        "embargo": "one H3 ring"})
    if not np.isfinite(pred).all():
        raise RuntimeError("Missing OOF quality prediction")
    return pred, receipt


def run(sample, rater):
    items, ratings, hashes, sample_hash = load(sample, [rater])
    labels = ratings[rater]
    if len(labels) != len(items):
        print(json.dumps({"status": "awaiting_human_labels", "completed": len(labels),
                          "total": len(items)}))
        return
    with np.load(TECH, allow_pickle=False) as z:
        ids, all_technical, names = z["query_ids"].tolist(), z["features"], z["feature_names"].tolist()
    if len(ids) != 1184:
        raise RuntimeError("Technical feature population changed")
    lookup = {value: i for i, value in enumerate(ids)}
    positions = np.asarray([lookup[row["query_id"]] for row in items])
    technical = all_technical[positions]
    y = np.asarray([labels[row["token"]]["geolocatability"] <= 1 for row in items], bool)
    good = np.asarray([labels[row["token"]]["geolocatability"] >= 3 for row in items], bool)
    risk = percentile_risk(technical, all_technical, names)
    result = {"status": "complete", "sample": sample, "rater": rater,
        "human_label_sha256": hashes[rater], "sample_sha256": sample_hash,
        "poor_definition": "human ordinal geolocatability 0 or 1",
        "technical_rule": "max of unsupervised low-luminance, low-Laplacian, low-contrast, high-clipping population percentile risks",
        "technical_risk": diagnostics(y, risk, .9),
        "good_image_false_rejections_at_risk_0_9": int(np.sum(good & (risk >= .9))),
        "good_image_count": int(good.sum()),
        "warning": "Technical heuristics are diagnostic; 20-image pilot is too small for a learned model."}
    for key, signal in {
        "motion_blur_vs_laplacian": -np.log1p(technical[:, names.index("laplacian_variance")]),
        "too_dark_vs_dark_fraction": technical[:, names.index("dark_fraction")],
        "overexposed_vs_bright_fraction": technical[:, names.index("clipped_bright_fraction")],
        "no_stable_landmarks_vs_edge_density": -technical[:, names.index("canny_edge_density")],
    }.items():
        reason = key.split("_vs_")[0]
        target = np.asarray([reason in labels[row["token"]]["reasons"] for row in items])
        result[key] = diagnostics(target, signal)
    if len(items) >= 100:
        clip = np.load(CLIP, allow_pickle=False)
        if clip.shape[0] != 1184 or not np.isfinite(clip).all():
            raise RuntimeError("Frozen GeoCLIP image features changed")
        groups = [row["hidden_group"] for row in items]
        comparisons = {"technical": technical, "frozen_geoclip_image": clip[positions],
            "frozen_geoclip_plus_technical": np.column_stack((clip[positions], technical))}
        result["learned_oof"] = {}
        for name, x in comparisons.items():
            fitted = grouped_oof(x, y, groups)
            if fitted is None:
                result["learned_oof"][name] = {"status": "insufficient_embargoed_training_population"}
            else:
                probabilities, folds = fitted
                result["learned_oof"][name] = diagnostics(y, probabilities) | {
                    "brier": float(brier_score_loss(y, probabilities)), "folds": folds}
    else:
        result["learned_oof"] = {"status": "deferred_until_at_least_100_human_labeled_images"}
    version = hashlib.sha256(json.dumps({"sample": sample, "rater": rater,
        "labels": hashes, "technical": digest(TECH)}, sort_keys=True).encode()).hexdigest()[:12]
    path = OUT / "technical_quality" / f"benchmark_{sample}_{rater}_{version}.json"
    if path.exists():
        raise FileExistsError("Benchmark already sealed for this label version")
    save(path, result)
    any_fitted = any(isinstance(value, dict) and "auroc" in value
                     for value in result["learned_oof"].values())
    record(f"quality_benchmark_{sample}_{rater}_{version}", {"status": "complete",
        "fitted": any_fitted, "human_label_version": hashes,
        "evaluation_split": "blind human sample; any classifier is geographically embargoed OOF",
        "result": result, "artifact_paths": [str(path)],
        "artifact_hashes": {path.name: digest(path)}})
    print(json.dumps({"report": str(path), "technical_auroc": result["technical_risk"].get("auroc"),
                      "pilot": sample == "quick20"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", choices=["quick20", "full"], default="quick20")
    parser.add_argument("--rater", default="reviewer1")
    arguments = parser.parse_args()
    run(arguments.sample, arguments.rater)
