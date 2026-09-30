"""Frozen SAGE patch-context evidence for anchor and 30 location challengers."""
from __future__ import annotations

import json
import time

import numpy as np
import torch
from threadpoolctl import threadpool_limits

from ml.research import night_scale_context as context
from ml.research import night_v7 as sage
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.geographic_v8.baseline import selected_vectors, staged_entries
from ml.research.geographic_v8.stage import STAGE
from ml.research.geographic_v9.common import OUT, frames, record

SOURCE = OUT / "hypotheses_all"
DEST = OUT / "hypotheses_visual30"
DEPTH = 30
EXTRA = ["anchor_visual_context", "challenger_visual_context",
         "visual_context_difference", "visual_context_gap_from_top_challenger",
         "visual_context_gap_from_best_other"]


def load_query_vectors(ids):
    q322 = np.load(STAGE / "query322.npy", allow_pickle=False)
    parts = []
    for path in sorted((STAGE / "query504").glob("chunk-*.npz")):
        with np.load(path, allow_pickle=False) as z:
            offset = sum(len(x) for x in parts)
            if z["ids"].tolist() != ids[offset:offset+len(z["ids"])]:
                raise RuntimeError("504 query order mismatch")
            parts.append(z["vectors"].copy())
    q504 = np.concatenate(parts)
    if q322.shape != q504.shape or q322.shape[0] != len(ids):
        raise RuntimeError("Query feature population mismatch")
    return q322, q504, context.mean_queries(q322, q504)


def encoder():
    checkpoint = WORKSPACE / "data/models/research_v5/sage_context_encoder.pth"
    if digest(checkpoint) != sage.ENCODER_SHA:
        raise RuntimeError("Frozen context checkpoint changed")
    model = torch.nn.TransformerEncoder(torch.nn.TransformerEncoderLayer(
        d_model=768, nhead=16, dim_feedforward=1024, activation="gelu",
        dropout=.1, batch_first=False), 2)
    model.load_state_dict(torch.load(checkpoint, map_location="cpu", weights_only=True), strict=True)
    return model.eval()


def run():
    started = time.perf_counter()
    q, g = frames()
    contract = json.loads((SOURCE / "inference_contract.json").read_text())
    if contract["query_gt_used"] or digest(SOURCE / "features.npz") != contract["features_sha256"]:
        raise RuntimeError("Hypothesis inference contract changed")
    with np.load(SOURCE / "features.npz", allow_pickle=False) as z:
        if z["query_ids"].tolist() != q.id.tolist():
            raise RuntimeError("Query identity mismatch")
        base, full_challengers, full_offsets = (z[name].copy() for name in
            ("baseline_rows", "challenger_rows", "offsets"))
        old_features = z["features"].copy()
        old_names = z["feature_names"].tolist()
        union_rows, union_offsets = z["union_rows"].copy(), z["union_offsets"].copy()
    if min(np.diff(full_offsets)) < DEPTH:
        raise RuntimeError("Fewer than 30 challengers for a query")
    chosen = np.vstack([full_challengers[full_offsets[i]:full_offsets[i]+DEPTH]
                        for i in range(len(q))]).astype(np.int32)
    selected_rows = np.concatenate([np.arange(full_offsets[i], full_offsets[i]+DEPTH)
                                    for i in range(len(q))])
    q322, q504, means = load_query_vectors(q.id.tolist())
    model = encoder()
    pool = staged_entries(g)
    visual = np.empty((len(q), DEPTH+1), np.float32)
    torch.set_num_threads(2)
    with threadpool_limits(limits=2):
        for start in range(0, len(q), 80):
            stop = min(start+80, len(q))
            rows = np.concatenate((base[start:stop, None], chosen[start:stop]), axis=1)
            unique, remap = np.unique(rows, return_inverse=True)
            vectors = selected_vectors(g, unique, pool)
            remap = remap.reshape(rows.shape)
            for i in range(start, stop):
                c, _ = context.one_query(model, means[i], q322[i], q504[i],
                                         vectors[remap[i-start]])
                visual[i] = c
            print("visual context queries", stop, "/", len(q),
                  "elapsed_s", round(time.perf_counter()-started, 1), flush=True)
    if not np.isfinite(visual).all():
        raise RuntimeError("Non-finite visual verification signal")
    old = old_features[selected_rows]
    x = np.empty((len(old), len(old_names)+len(EXTRA)), np.float32)
    x[:, :len(old_names)] = old
    cursor = 0
    for i in range(len(q)):
        anchor = float(visual[i, 0])
        challenge = visual[i, 1:]
        top = float(challenge[0])
        for j in range(DEPTH):
            others = max(anchor, float(np.max(challenge[np.arange(DEPTH) != j])))
            x[cursor+j, len(old_names):] = [anchor, challenge[j], challenge[j]-anchor,
                                             challenge[j]-top, challenge[j]-others]
        cursor += DEPTH
    DEST.mkdir(exist_ok=True)
    np.savez_compressed(DEST / "features.npz", query_ids=q.id.to_numpy(str),
        baseline_rows=base, challenger_rows=chosen.ravel(),
        offsets=np.arange(len(q)+1, dtype=np.int32)*DEPTH,
        features=x, feature_names=np.asarray(old_names+EXTRA, str),
        union_rows=union_rows, union_offsets=union_offsets)
    np.savez_compressed(DEST / "visual_scores.npz", query_ids=q.id.to_numpy(str),
                        baseline_rows=base, challenger_rows=chosen,
                        contextual_scores=visual)
    receipt = {"query_gt_used": False, "reference_gps_used_only_by_upstream_hypotheses": True,
        "source_hypothesis_contract_sha256": digest(SOURCE / "inference_contract.json"),
        "source_hypothesis_features_sha256": digest(SOURCE / "features.npz"),
        "context_checkpoint_sha256": sage.ENCODER_SHA,
        "depth": DEPTH, "visual_evidence": "frozen SAGE 2-layer patch-context transformer over query, anchor, 30 challengers",
        "feature_names": old_names+EXTRA,
        "features_sha256": digest(DEST / "features.npz"),
        "visual_scores_sha256": digest(DEST / "visual_scores.npz"),
        "runtime_s": time.perf_counter()-started}
    save(DEST / "inference_contract.json", receipt)
    record("visual_context30_inference_v1", {"status": "complete",
        "fitted": False, "query_gt_used": False, "context_checkpoint_sha256": sage.ENCODER_SHA,
        "result": receipt,
        "artifact_paths": [str(DEST / "features.npz"), str(DEST / "visual_scores.npz"),
                           str(DEST / "inference_contract.json")],
        "artifact_hashes": {p.name: digest(p) for p in DEST.iterdir() if p.is_file()}})
    print("visual feature rows", x.shape, "runtime_s", round(time.perf_counter()-started, 1), flush=True)


if __name__ == "__main__":
    run()
