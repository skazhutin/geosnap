"""Replay frozen SAGE scores/context on the outcome-blind filtered gallery.

Only cached query/reference descriptors and a pinned context checkpoint enter
inference. Query coordinates are loaded after ranking, solely for evaluation.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from ml.research import night_scale_context as context, night_v7 as sage
from ml.research.gallery_scale_storage import WORKSPACE, digest
from ml.research.geographic_v8.baseline import selected_vectors, staged_entries
from ml.research.geographic_v8.common import GALLERY, QUERY, metrics, topk
from .analyze_filtered_gallery_v1 import exact_top100
from .common import sha256, verify_production
from .filter_validation_qwen_v1 import OUT, raw


STAGE = WORKSPACE / "data/evaluation/geographic_v8_20260928/staged"
BASE = WORKSPACE / "data/evaluation/geographic_v8_20260928/baseline"
FILTER = WORKSPACE / "data/evaluation/gallery_quality_filter_v3_20260929"


def main() -> None:
    receipt = json.loads((FILTER / "filter_receipt.json").read_text())
    qreceipt = json.loads((OUT / "query_filter_freeze.json").read_text())
    if (sha256(GALLERY) != receipt["original_gallery_sha256"] or
            sha256(FILTER / "gallery_filtered.parquet") != receipt["filtered_gallery_sha256"] or
            sha256(QUERY) != qreceipt["query_manifest_sha256"] or
            sha256(OUT / "query_filter_freeze.json") != json.loads((OUT / "postfreeze_metrics.json").read_text())["query_filter_freeze_sha256"]):
        raise RuntimeError("Frozen gallery/query contracts changed")
    scores_file = STAGE / "mean_scores.npy"
    scores_hash = json.loads((STAGE / "mean_scores.npy.sha256.json").read_text())["sha256"]
    if sha256(scores_file) != scores_hash:
        raise RuntimeError("Frozen SAGE score cache changed")
    gallery = pd.read_parquet(GALLERY, columns=["id", "file_sha256", "lat", "lon"])
    filtered = pd.read_parquet(FILTER / "gallery_filtered.parquet", columns=["id"])
    original_rows = np.flatnonzero(gallery.id.isin(set(filtered.id)).to_numpy())
    scores = np.load(scores_file, mmap_mode="r", allow_pickle=False)
    if scores.shape != (1184, 112163) or len(original_rows) != 111032:
        raise RuntimeError("Unexpected score/gallery shape")
    with np.load(BASE / "context.npz", allow_pickle=False) as cached:
        old_chosen = cached["chosen"].copy()
        old_context = cached["contextual"].copy()
        old_original = cached["original"].copy()
    with np.load(BASE / "predictions.npz", allow_pickle=False) as baseline:
        old_indices = baseline["indices"].copy()
        baseline_query_ids = baseline["query_ids"].copy()
        baseline_gallery_ids = baseline["gallery_ids"].copy()
    if baseline_gallery_ids.tolist() != gallery.id.tolist():
        raise RuntimeError("Original baseline gallery order changed")
    old_prefix = topk(scores)
    if (not np.array_equal(old_prefix[:, :30], old_chosen) or
            not np.array_equal(context.rank_prefix(old_prefix, old_context, old_original), old_indices)):
        raise RuntimeError("Cannot reproduce 411/1184 original SAGE baseline")
    prefix = np.stack([exact_top100(scores[i], original_rows) for i in range(1184)])
    chosen = prefix[:, :30]
    changed = np.flatnonzero((chosen != old_chosen).any(axis=1))
    evidence = old_context.copy()
    original = np.take_along_axis(scores, chosen, axis=1)
    if len(changed):
        q322 = np.load(STAGE / "query322.npy", allow_pickle=False)
        pieces = []
        for path in sorted((STAGE / "query504").glob("chunk-*.npz")):
            with np.load(path, allow_pickle=False) as part:
                pieces.append((part["ids"].copy(), part["vectors"].copy()))
        ids = np.concatenate([a for a, _ in pieces])
        q504 = np.concatenate([b for _, b in pieces])
        if ids.tolist() != baseline_query_ids.tolist() or q322.shape != q504.shape:
            raise RuntimeError("Query descriptor identities changed")
        means = context.mean_queries(q322, q504)
        unique, remap = np.unique(chosen[changed].ravel(), return_inverse=True)
        entries = staged_entries(gallery)
        refs = selected_vectors(gallery, unique, entries)
        remap = remap.reshape(len(changed), 30)
        checkpoint = WORKSPACE / "data/models/research_v5/sage_context_encoder.pth"
        if digest(checkpoint) != sage.ENCODER_SHA:
            raise RuntimeError("Pinned context encoder changed")
        torch.set_num_threads(2)
        encoder = torch.nn.TransformerEncoder(torch.nn.TransformerEncoderLayer(
            d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=.1,
            batch_first=False), 2)
        encoder.load_state_dict(torch.load(checkpoint, map_location="cpu", weights_only=True), strict=True)
        encoder.eval()
        with threadpool_limits(limits=2):
            for j, i in enumerate(changed):
                evidence[i], direct = context.one_query(encoder, means[i], q322[i], q504[i], refs[remap[j]])
                np.testing.assert_allclose(direct, original[i], atol=2e-6, rtol=1e-5)
    ranked = context.rank_prefix(prefix, evidence, original)
    if not np.isin(ranked, original_rows).all() or not np.array_equal(ranked[:, 30:], prefix[:, 30:]):
        raise RuntimeError("Filtered gallery ranking is invalid")
    # Evaluation boundary starts here; query GPS never enters candidate scoring.
    q = pd.read_parquet(QUERY)
    if q.id.tolist() != baseline_query_ids.tolist():
        raise RuntimeError("Query evaluation order changed")
    report, errors, distances = metrics(q, gallery, ranked)
    keep_ids = {json.loads(line)["query_id"] for line in (OUT / "retained_query_ids.jsonl").read_text().splitlines()}
    keep = q.id.isin(keep_ids).to_numpy()
    subset = raw(errors[keep])
    subset["recall"] = {str(k): int((distances[keep, :k] <= 100).any(axis=1).sum()) for k in (1, 5, 10, 20, 50, 100)}
    target = OUT / "sage_clean_gallery_predictions.npz"
    if not target.exists():
        np.savez_compressed(target, query_ids=q.id.to_numpy(str), indices=ranked,
                            errors_m=errors, prediction_gps=gallery[["lat", "lon"]].to_numpy(float)[ranked[:, 0]])
    else:
        with np.load(target, allow_pickle=False) as existing:
            if not np.array_equal(existing["indices"], ranked) or not np.allclose(existing["errors_m"], errors):
                raise RuntimeError("Existing postfreeze predictions differ")
    summary = {
        "status": "fixed_SAGE_replay_after_research_gallery_filter",
        "gallery_v3_sha256": receipt["filtered_gallery_sha256"],
        "query_filter_freeze_sha256": sha256(OUT / "query_filter_freeze.json"),
        "scores_sha256": scores_hash, "context_checkpoint_sha256": sage.ENCODER_SHA,
        "original_baseline_reproduced_all_1184": True,
        "queries_needing_context_recomputation": len(changed),
        "raw_all": report["raw"], "recall_all": report["recall_at"],
        "qwen_clean_subset": subset,
        "predictions_sha256": sha256(target),
        "production_guard_sha256": verify_production()["guard_sha256"],
    }
    path = OUT / "sage_clean_gallery_report.json"
    content = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text() != content:
            raise RuntimeError("Existing postfreeze report differs")
    else:
        path.write_text(content)
    print(content, flush=True)


if __name__ == "__main__":
    main()
