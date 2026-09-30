"""Zero-shot G3 full-coordinate retrieval, SAGE reranking and candidate union."""
from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
from numpy.lib.format import open_memmap
from threadpoolctl import threadpool_limits

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import GALLERY, LOCAL, NIGHT, compare, frames, metrics, registry, topk
from ml.research.geographic_v8.stage import STAGE
from ml.research.vector_evaluation import distances


def write_result(identifier, q, g, ranked, model, runtime, more=None):
    report, errors, _ = metrics(q, g, ranked)
    report |= compare(q, errors)
    report |= {"variant": identifier, "model": model, "runtime": runtime,
               "scope": "same 112163-reference gallery; external pretrained data; zero-shot exploratory development"}
    if more:
        report |= more
    out = LOCAL / "g3" / identifier
    out.mkdir(parents=True, exist_ok=True)
    np.savez(out / "predictions.npz", query_ids=q.id.to_numpy(str), indices=ranked,
             gallery_ids=g.id.to_numpy(str)[ranked], errors_m=errors)
    save(out / "report.json", report)
    registry(identifier, {**report, "model_revision": model["model_revision"],
        "preprocessing": model["preprocessing"], "candidate_generation": more.get("candidate_generation") if more else "full G3 image-GPS cosine",
        "reranking": more.get("reranking") if more else None, "fusion": None, "hyperparameters": {},
        "raw25": report["raw"]["accuracy_25m"], "raw50": report["raw"]["accuracy_50m"],
        "raw100": report["raw"]["accuracy_100m"], "r_at_k": report["recall_at"],
        "median_m": report["raw"]["median_error_m"], "p90_m": report["raw"]["p90_error_m"],
        "gt500_rate": report["raw"]["catastrophic_gt500m_rate"],
        "result_status": "complete", "decision": "exploratory; compare against baseline and union",
        "artifact_paths": [str(out / "predictions.npz"), str(out / "report.json")],
        "artifact_hashes": {name: digest(out / name) for name in ("predictions.npz", "report.json")}})
    print(identifier, report["raw"]["accuracy_100m"], report["recall_at"].get("100"), flush=True)
    return report, errors


def run():
    base_path = LOCAL / "baseline/predictions.npz"
    completed = LOCAL / "g3/complete.json"
    if not base_path.exists() or not completed.exists():
        raise RuntimeError("Fresh baseline and G3 embeddings must both complete first")
    q, g = frames()
    if digest(GALLERY) != digest(STAGE / "gallery.parquet"):
        raise RuntimeError("Staged gallery changed")
    info = json.loads(completed.read_text())
    out = LOCAL / "g3"
    if any(digest(out / n) != sha for n, sha in info["hashes"].items()):
        raise RuntimeError("G3 embedding hash mismatch")
    query = np.load(out / "query.npy", mmap_mode="r")
    location = np.load(out / "location.npy", mmap_mode="r")
    if query.shape != (len(q), 768) or location.shape != (len(g), 768):
        raise RuntimeError("G3 representation shape mismatch")
    with np.load(base_path, allow_pickle=False) as old:
        if old["query_ids"].tolist() != q.id.tolist():
            raise RuntimeError("G3/SAGE query order mismatch")
        sage = old["indices"].copy()
        buckets = old["buckets"].copy()
        sage_correct = old["errors_m"] <= 100
    model = info["contract"]
    scores_path = out / "scores.npy"
    scores_receipt = out / "scores.json"
    if scores_receipt.exists():
        if digest(scores_path) != json.loads(scores_receipt.read_text())["sha256"]:
            raise RuntimeError("Existing full G3 score matrix changed")
        scores = np.load(scores_path, mmap_mode="r")
        score_runtime = json.loads(scores_receipt.read_text())["runtime_s"]
    else:
        t = time.perf_counter()
        scores = open_memmap(scores_path, mode="w+", dtype="float32", shape=(len(q), len(g)))
        with threadpool_limits(limits=2):
            for start in range(0, len(q), 32):
                scores[start:start+32] = query[start:start+32] @ location.T
                if start % 160 == 0:
                    print(f"G3 exact cosine {start+min(32,len(q)-start)}/{len(q)}", flush=True)
        scores.flush()
        score_runtime = time.perf_counter() - t
        save(scores_receipt, {"sha256": digest(scores_path), "runtime_s": score_runtime,
            "query_sha256": info["hashes"]["query.npy"], "location_sha256": info["hashes"]["location.npy"],
            "exact_float32_cosine": True, "gallery_sha256": digest(GALLERY)})
    g3_order = topk(scores)
    runtime = {"query_encoding_s": model["query_runtime_s"], "location_encoding_s": model["location_runtime_s"],
               "exact_cosine_s": score_runtime}
    full, full_errors = write_result("g3_full", q, g, g3_order, model, runtime)
    positive_g3 = full_errors <= 100
    overlap = {"both_correct": int((sage_correct & positive_g3).sum()),
               "only_sage_correct": int((sage_correct & ~positive_g3).sum()),
               "only_g3_correct": int((~sage_correct & positive_g3).sum()),
               "neither_correct": int((~sage_correct & ~positive_g3).sum())}
    save(out / "correctness_overlap.json", overlap)
    for depth in (30, 50, 100):
        t = time.perf_counter()
        original = sage[:, :depth]
        values = np.take_along_axis(scores, original, axis=1)
        order = np.argsort(-values, axis=1, kind="stable")
        ranked = sage.copy()
        ranked[:, :depth] = np.take_along_axis(original, order, axis=1)
        report, _ = write_result(f"g3_rerank_sage{depth}", q, g, ranked, model,
            runtime | {"rerank_s": time.perf_counter()-t},
            {"candidate_generation": "frozen SAGE-L 322+504 context top100",
             "reranking": f"pure pretrained G3 image-GPS cosine over top{depth}"})
    union_rows, cardinality, positive50, positive100 = [], [], [], []
    for i, row in enumerate(q.itertuples()):
        unique = list(dict.fromkeys(np.concatenate([sage[i], g3_order[i]]).tolist()))
        union_rows.append(unique)
        cardinality.append(len(unique))
        loc = g[["lat", "lon"]].to_numpy()[unique]
        d = distances(row.lat, row.lon, np.radians(loc[:, 0]), np.radians(loc[:, 1]))
        positive50.append(bool((d <= 50).any()))
        positive100.append(bool((d <= 100).any()))
    retrieved = np.asarray(positive100)
    union = {"unique_candidates_per_query": {"min": int(min(cardinality)), "median": float(np.median(cardinality)),
        "max": int(max(cardinality)), "mean": float(np.mean(cardinality))},
        "oracle_50m": float(np.mean(positive50)), "oracle_100m": float(retrieved.mean()),
        "original_309_retrieval_failures_now_retrievable": int(((buckets == "retrieval_failure") & retrieved).sum()),
        "original_262_no_coverage_now_retrievable": int(((buckets == "no_coverage") & retrieved).sum()),
        "overlap_top1": overlap,
        "per_query": [{"query_id": q.id.iloc[i], "unique_candidates": cardinality[i],
                       "oracle50": positive50[i], "oracle100": positive100[i], "gallery_rows": union_rows[i]}
                      for i in range(len(q))]}
    save(out / "union.json", union)
    registry("sage_g3_top100_union_oracle", {"model_revision": model["model_revision"],
        "preprocessing": "SAGE official 322/504 and G3 official 224", "candidate_generation": "set union of frozen SAGE and G3 top100",
        "reranking": "oracle only; no inference-time selected winner", "fusion": "candidate union", "hyperparameters": {"depth": 100},
        "raw25": None, "raw50": None, "raw100": None, "r_at_k": {"50m_union_oracle": union["oracle_50m"],
        "100m_union_oracle": union["oracle_100m"]}, "median_m": None, "p90_m": None, "gt500_rate": None,
        "runtime": runtime, "result_status": "complete", "decision": "assess complementarity",
        "artifact_paths": [str(out / "union.json")], "artifact_hashes": {"union.json": digest(out / "union.json")}})
    print("G3 union oracle100", union["oracle_100m"], "recovered retrieval failures", union["original_309_retrieval_failures_now_retrievable"], flush=True)


if __name__ == "__main__":
    run()
