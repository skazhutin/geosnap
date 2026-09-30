"""Same-gallery image/GPS retrieval, SAGE reranking, and candidate complementarity."""
from __future__ import annotations

import argparse
import json
import time

import numpy as np
from numpy.lib.format import open_memmap
from threadpoolctl import threadpool_limits

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, compare, frames, metrics, registry, topk
from ml.research.vector_evaluation import distances


def run(name):
    out = LOCAL / name
    done = json.loads((out / "complete.json").read_text())
    for filename, expected in done["hashes"].items():
        if digest(out / filename) != expected:
            raise RuntimeError(f"{name} embedding changed: {filename}")
    query = np.load(out / "query.npy", mmap_mode="r")
    location = np.load(out / "location.npy", mmap_mode="r")
    q, g = frames()
    if query.shape[0] != len(q) or location.shape[0] != len(g) or query.shape[1] != location.shape[1]:
        raise RuntimeError("Embedding population/dimension mismatch")
    with np.load(LOCAL / "baseline/predictions.npz", allow_pickle=False) as base:
        if base["query_ids"].tolist() != q.id.tolist() or base["gallery_ids"].tolist() != g.id.tolist():
            raise RuntimeError("Embedding/SAGE identity mismatch")
        sage = base["indices"].copy()
        buckets = base["buckets"].copy()
        sage_correct = base["errors_m"] <= 100
    scores_path = out / "scores.npy"
    receipt_path = out / "scores.json"
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_text())
        if digest(scores_path) != receipt["sha256"]:
            raise RuntimeError("Existing score matrix changed")
        scores = np.load(scores_path, mmap_mode="r")
        scoring_runtime = receipt["runtime_s"]
    else:
        start_time = time.perf_counter()
        scores = open_memmap(scores_path, mode="w+", dtype="float32", shape=(len(q), len(g)))
        with threadpool_limits(limits=2):
            for start in range(0, len(q), 32):
                scores[start:start+32] = query[start:start+32] @ location.T
                if start % 160 == 0:
                    print(f"{name} exact cosine {min(start+32,len(q))}/{len(q)}", flush=True)
        scores.flush()
        scoring_runtime = time.perf_counter() - start_time
        save(receipt_path, {"sha256": digest(scores_path), "runtime_s": scoring_runtime,
            "query_sha256": done["hashes"]["query.npy"],
            "location_sha256": done["hashes"]["location.npy"]})
    ranked_model = topk(scores)
    runtime = {"query_encoding_s": done["contract"]["query_runtime_s"],
        "location_encoding_s": done["contract"]["location_runtime_s"],
        "exact_cosine_s": scoring_runtime}
    results = {}
    for variant, ranked, source in [(f"{name}_full", ranked_model, "full image-GPS exact cosine")]:
        results[variant] = write_result(out, variant, q, g, ranked, done["contract"], runtime, source)
    for depth in (30, 50, 100):
        candidates = sage[:, :depth]
        values = np.take_along_axis(scores, candidates, axis=1)
        order = np.argsort(-values, axis=1, kind="stable")
        ranked = sage.copy()
        ranked[:, :depth] = np.take_along_axis(candidates, order, axis=1)
        variant = f"{name}_rerank_sage{depth}"
        results[variant] = write_result(out, variant, q, g, ranked, done["contract"], runtime,
            f"{name} cosine reranks frozen SAGE top{depth}")
    full_correct = results[f"{name}_full"][1] <= 100
    overlap = {"both_correct": int((sage_correct & full_correct).sum()),
        "only_sage_correct": int((sage_correct & ~full_correct).sum()),
        "only_new_correct": int((~sage_correct & full_correct).sum()),
        "neither_correct": int((~sage_correct & ~full_correct).sum())}
    save(out / "correctness_overlap.json", overlap)
    coords = g[["lat", "lon"]].to_numpy()
    positive50, positive100, counts, union_rows = [], [], [], []
    for i, row in enumerate(q.itertuples()):
        unique = list(dict.fromkeys(np.concatenate((sage[i], ranked_model[i])).tolist()))
        d = distances(row.lat, row.lon, np.radians(coords[unique, 0]), np.radians(coords[unique, 1]))
        positive50.append(bool((d <= 50).any()))
        positive100.append(bool((d <= 100).any()))
        counts.append(len(unique))
        union_rows.append(unique)
    retrieved = np.asarray(positive100)
    union = {"unique_candidates_per_query": {"min": int(min(counts)), "median": float(np.median(counts)),
        "max": int(max(counts)), "mean": float(np.mean(counts))},
        "oracle_50m": float(np.mean(positive50)), "oracle_100m": float(retrieved.mean()),
        "original_309_retrieval_failures_now_retrievable": int(((buckets == "retrieval_failure") & retrieved).sum()),
        "original_262_no_coverage_now_retrievable": int(((buckets == "no_coverage") & retrieved).sum()),
        "overlap_top1": overlap,
        "per_query": [{"query_id": q.id.iloc[i], "unique_candidates": counts[i],
            "oracle50": positive50[i], "oracle100": positive100[i], "gallery_rows": union_rows[i]}
            for i in range(len(q))]}
    save(out / "union.json", union)
    registry(f"sage_{name}_top100_union_oracle", {"model_revision": done["contract"]["model_revision"],
        "preprocessing": done["contract"]["preprocessing"],
        "candidate_generation": f"set union of frozen SAGE and {name} top100", "reranking": "oracle only",
        "fusion": "candidate union", "hyperparameters": {"depth": 100},
        "raw25": None, "raw50": None, "raw100": None,
        "r_at_k": {"50m_union_oracle": union["oracle_50m"], "100m_union_oracle": union["oracle_100m"]},
        "median_m": None, "p90_m": None, "gt500_rate": None, "runtime": runtime,
        "result_status": "complete", "decision": "assess candidate complementarity",
        "artifact_paths": [str(out / "union.json")], "artifact_hashes": {"union.json": digest(out / "union.json")}})
    print(name, "union oracle100", union["oracle_100m"], "recovered", union["original_309_retrieval_failures_now_retrievable"], flush=True)


def write_result(out, name, q, g, ranked, contract, runtime, source):
    report, errors, _ = metrics(q, g, ranked)
    paired = compare(q, errors)
    report |= paired | {"variant": name, "model": contract, "runtime": runtime,
        "method": source, "scope": "same fixed 112163-reference gallery; zero-shot exploratory development"}
    path = out / name
    path.mkdir(exist_ok=True)
    np.savez(path / "predictions.npz", query_ids=q.id.to_numpy(str), indices=ranked,
             gallery_ids=g.id.to_numpy(str)[ranked], errors_m=errors)
    save(path / "report.json", report)
    registry(name, {"model_revision": contract["model_revision"],
        "preprocessing": contract["preprocessing"], "candidate_generation": source,
        "reranking": source if "rerank" in source else None, "fusion": None, "hyperparameters": {},
        "fitted_parameters": False, "raw25": report["raw"]["accuracy_25m"],
        "raw50": report["raw"]["accuracy_50m"], "raw100": report["raw"]["accuracy_100m"],
        "r_at_k": report["recall_at"], "median_m": report["raw"]["median_error_m"],
        "p90_m": report["raw"]["p90_error_m"],
        "gt500_rate": report["raw"]["catastrophic_gt500m_rate"], "runtime": runtime,
        "result_status": "complete", "decision": "assess complementarity",
        "artifact_paths": [str(path / "predictions.npz"), str(path / "report.json")],
        "artifact_hashes": {f: digest(path / f) for f in ("predictions.npz", "report.json")},
        "paired_bootstrap": paired["paired_geographic_bootstrap"]})
    print(name, "RAW100", int((errors <= 100).sum()), "R100", report["recall_at"]["100"], flush=True)
    return report, errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("model", choices=("geoclip",))
    run(parser.parse_args().model)
