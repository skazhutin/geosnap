"""Train a visual KEEP/SWITCH verifier on geographically isolated references only."""
from __future__ import annotations

import json
import time

import numpy as np
import torch
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits

from ml.research import night_scale_context as context
from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.baseline import selected_vectors, staged_entries
from ml.research.geographic_v8.common import compare, metrics
from ml.research.geographic_v9.common import OUT, V8, frames, record
from ml.research.geographic_v9.visual_context30 import encoder

OUTDIR = OUT / "reference_pairwise_verifier"
PAIRS = V8 / "sage_adaptation_v3/pairs.json"
VISUAL = OUT / "hypotheses_visual30/visual_scores.npz"


def extract_references():
    started = time.perf_counter()
    _, gallery = frames()
    pairs = json.loads(PAIRS.read_text())
    records = pairs["train"] + pairs["validation"]
    roles = np.asarray([0]*len(pairs["train"]) + [1]*len(pairs["validation"]), np.int8)
    if len(pairs["train"]) != 1111 or len(pairs["validation"]) != 128:
        raise RuntimeError("Frozen reference roles changed")
    seq = gallery.sequence_key.astype(str).to_numpy()
    train_seq = {seq[i] for row in pairs["train"] for i in
                 (row["anchor"], row["positive"], *row["negatives"])}
    val_seq = {seq[i] for row in pairs["validation"] for i in
               (row["anchor"], row["positive"], *row["negatives"])}
    query, _ = frames()
    if train_seq & val_seq or train_seq & set(query.sequence_key.astype(str)):
        raise RuntimeError("Reference train/validation sequence leakage")
    pool = staged_entries(gallery)
    model = encoder()
    torch.set_num_threads(2)
    direct = np.empty((len(records), 9), np.float32)
    contextual = np.empty_like(direct)
    with threadpool_limits(limits=2):
        for start in range(0, len(records), 64):
            stop = min(start+64, len(records))
            entries = records[start:stop]
            rows = np.asarray([[r["anchor"], r["positive"], *r["negatives"]]
                               for r in entries], np.int32)
            unique, remap = np.unique(rows, return_inverse=True)
            vectors = selected_vectors(gallery, unique, pool)
            remap = remap.reshape(rows.shape)
            for position in range(start, stop):
                v = vectors[remap[position-start]]
                query = v[0]
                contextual[position], direct[position] = context.one_query(
                    model, query, query, query, v[1:])
            if stop % 256 == 0 or stop == len(records):
                print("reference verifier scores", stop, "/", len(records), flush=True)
    OUTDIR.mkdir(exist_ok=True)
    np.savez_compressed(OUTDIR / "reference_pair_scores.npz",
                        direct=direct, contextual=contextual, role=roles,
                        anchors=np.asarray([r["anchor"] for r in records], np.int32))
    receipt = {"status": "complete", "records": len(records),
        "training_records": len(pairs["train"]), "validation_records": len(pairs["validation"]),
        "negative_references_per_anchor": 8,
        "query_gt_used": False, "reference_GPS_uses": "preexisting strict spatial pair labels only",
        "sequence_overlap": 0, "geographic_embargo": "v8 pair contract, one H3 ring",
        "pairs_sha256": digest(PAIRS),
        "pair_contract_sha256": digest(V8 / "sage_adaptation_v3/mining_contract.json"),
        "context_checkpoint_sha256": digest("data/models/research_v5/sage_context_encoder.pth"),
        "reference_pair_scores_sha256": digest(OUTDIR / "reference_pair_scores.npz"),
        "runtime_s": time.perf_counter()-started}
    save(OUTDIR / "reference_feature_contract.json", receipt)
    record("reference_pairwise_visual_features_v1", {"status": "complete", "fitted": False,
        "result": receipt, "artifact_paths": [str(OUTDIR / "reference_pair_scores.npz"),
            str(OUTDIR / "reference_feature_contract.json")],
        "artifact_hashes": {p.name: digest(p) for p in OUTDIR.iterdir() if p.is_file()}})
    print("reference score runtime", round(time.perf_counter()-started, 1), flush=True)


def difference_features(direct, contextual):
    return np.column_stack((direct[:, 0, None]-direct[:, 1:],
                            contextual[:, 0, None]-contextual[:, 1:]))


def fit_and_evaluate():
    query, gallery = frames()
    contract = json.loads((OUTDIR / "reference_feature_contract.json").read_text())
    if digest(OUTDIR / "reference_pair_scores.npz") != contract["reference_pair_scores_sha256"]:
        raise RuntimeError("Reference visual evidence changed")
    with np.load(OUTDIR / "reference_pair_scores.npz", allow_pickle=False) as z:
        direct, visual, role = z["direct"].copy(), z["contextual"].copy(), z["role"].copy()
    if direct.shape != (1239, 9) or visual.shape != direct.shape:
        raise RuntimeError("Reference pair shape mismatch")
    # One positive image is column 0; columns 1..8 are geographically distant hard negatives.
    delta = np.stack((direct[:, 0, None]-direct[:, 1:],
                      visual[:, 0, None]-visual[:, 1:]), axis=-1)
    train_d = delta[role == 0].reshape(-1, 2)
    train_x = np.vstack((train_d, -train_d))
    train_y = np.r_[np.ones(len(train_d)), np.zeros(len(train_d))]
    scaler = StandardScaler().fit(train_x)
    model = LogisticRegression(C=1., max_iter=500, random_state=20260928).fit(
        scaler.transform(train_x), train_y)
    validation = delta[role == 1]
    positive = model.decision_function(scaler.transform(validation.reshape(-1, 2))).reshape(-1, 8)
    wrong = model.decision_function(scaler.transform((-validation).reshape(-1, 2))).reshape(-1, 8)
    validation_auc = roc_auc_score(np.r_[np.ones(positive.size), np.zeros(wrong.size)],
                                   np.r_[positive.ravel(), wrong.ravel()])
    reference_direct_accuracy = float((validation[:, :, 0] > 0).mean())
    reference_verifier_accuracy = float((positive > 0).mean())
    # Both thresholds are selected from isolated reference validation; no dev outcomes used.
    wrong_max = wrong.max(axis=1)
    thresholds = {"reference_familywise_fpr_5pct": float(np.quantile(wrong_max, .95)),
                  "reference_familywise_fpr_1pct": float(np.quantile(wrong_max, .99))}
    coefficients = {"feature_names": ["direct_cosine_difference", "patch_context_difference"],
        "scaler_mean": scaler.mean_.tolist(), "scaler_scale": scaler.scale_.tolist(),
        "logistic_coef": model.coef_[0].tolist(), "logistic_intercept": float(model.intercept_[0]),
        "C": 1., "training": "1111 reference anchors with eight hard negatives each; signs mirrored",
        "validation": "128 geographically/sequence isolated reference anchors",
        "thresholds": thresholds,
        "reference_validation_pair_auc": float(validation_auc),
        "reference_validation_direct_pair_accuracy": reference_direct_accuracy,
        "reference_validation_visual_pair_accuracy": reference_verifier_accuracy,
        "reference_validation_wrong_max_count": len(wrong_max),
        "source_contract_sha256": digest(OUTDIR / "reference_feature_contract.json")}
    save(OUTDIR / "frozen_reference_verifier.json", coefficients)
    with np.load(VISUAL, allow_pickle=False) as z:
        if z["query_ids"].tolist() != query.id.tolist():
            raise RuntimeError("Development query identity changed")
        base = z["baseline_rows"].copy()
        challengers = z["challenger_rows"].copy()
        context_scores = z["contextual_scores"].copy()
    mean = np.load(V8 / "staged/mean_scores.npy", mmap_mode="r")
    row_index = np.arange(len(query))
    base_cos = mean[row_index, base]
    challenge_cos = np.stack([mean[i, challengers[i]] for i in range(len(query))])
    dev_delta = np.stack((challenge_cos-base_cos[:, None],
                          context_scores[:, 1:]-context_scores[:, 0, None]), axis=-1)
    verifier_score = model.decision_function(scaler.transform(dev_delta.reshape(-1, 2))).reshape(len(query), 30)
    variants = {}
    for depth in (8, 30):
        for label, threshold in thresholds.items():
            subset = verifier_score[:, :depth]
            best = np.argmax(subset, axis=1)
            max_score = subset[row_index, best]
            selected = np.where(max_score > threshold, challengers[row_index, best], base)
            output, error, _ = metrics(query, gallery, selected[:, None])
            paired = compare(query, error)
            name = f"top{depth}_{label}"
            variants[name] = {"raw": output["raw"], "switches": int((selected != base).sum()),
                "transitions": paired["transitions"],
                "paired_geographic_bootstrap": paired["paired_geographic_bootstrap"]}
            np.savez_compressed(OUTDIR / f"{name}.npz", query_ids=query.id.to_numpy(str),
                baseline_rows=base, prediction_rows=selected, errors_m=error,
                selected_verifier_score=max_score)
            print(name, "correct", int((error <= 100).sum()),
                  "switches", variants[name]["switches"], flush=True)
    result = {"status": "complete", "source_training": "references only",
        "development_query_labels_fitted": False, "reference_verifier": coefficients,
        "development_results": variants,
        "note": "Development results are exploratory and must not select thresholds after the fact."}
    save(OUTDIR / "report.json", result)
    record("reference_pairwise_visual_verifier_v1", {"status": "complete",
        "fitted": True, "fitting_split": "strict reference-only train, geographically/sequence isolated reference validation",
        "evaluation_split": "1184 reused development queries, zero-shot transfer",
        "result": result, "artifact_paths": [str(OUTDIR / "frozen_reference_verifier.json"),
            str(OUTDIR / "report.json"), *[str(OUTDIR / f"{name}.npz") for name in variants]],
        "artifact_hashes": {p.name: digest(p) for p in OUTDIR.iterdir() if p.is_file()}})


if __name__ == "__main__":
    if not (OUTDIR / "reference_feature_contract.json").exists():
        extract_references()
    fit_and_evaluate()
