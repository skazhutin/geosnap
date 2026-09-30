"""Fixed SAGE-L 322/504 query views as independent candidate generators."""
import json
import time

import numpy as np
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale
from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.baseline import staged_entries
from ml.research.geographic_v8.common import LOCAL, compare, frames, metrics, registry, topk
from ml.research.geographic_v8.stage import STAGE
from ml.research.vector_evaluation import distances


def run():
    q, g = frames()
    with np.load(LOCAL / "baseline/predictions.npz", allow_pickle=False) as old:
        sage = old["indices"].copy()
        buckets = old["buckets"].copy()
    q322 = np.load(STAGE / "query322.npy", allow_pickle=False)
    pieces = []
    for path in sorted((STAGE / "query504").glob("chunk-*.npz")):
        with np.load(path, allow_pickle=False) as x:
            offset = sum(len(p) for p in pieces)
            if x["ids"].tolist() != q.id.iloc[offset:offset+len(x["ids"])].tolist():
                raise RuntimeError("504 descriptor identity mismatch")
            pieces.append(x["vectors"])
    q504 = np.concatenate(pieces)
    if q322.shape != q504.shape or len(q322) != len(q):
        raise RuntimeError("Resolution descriptor dimension mismatch")
    pool = staged_entries(g)
    out = LOCAL / "resolution_union"
    out.mkdir(exist_ok=True)
    results = {}
    with threadpool_limits(limits=2):
        for name, query in (("322", q322), ("504", q504)):
            started = time.perf_counter()
            scores = gallery_scale.exact_scores(g, query, pool)
            ranked = topk(scores)
            runtime = time.perf_counter() - started
            report, errors, _ = metrics(q, g, ranked)
            paired = compare(q, errors)
            np.savez(out / f"{name}.npz", query_ids=q.id.to_numpy(str), gallery_ids=g.id.to_numpy(str),
                     indices=ranked, scores=np.take_along_axis(scores, ranked, axis=1), errors_m=errors)
            save(out / f"{name}.json", report | paired | {"runtime_s": runtime,
                 "scope": "SAGE-L pinned pretrained model; single query resolution, fixed 112163 gallery"})
            registry(f"sage_resolution_{name}_retrieval", {
                "model_revision": "pinned SAGE-L No-Encoder checkpoint in frozen candidate",
                "preprocessing": f"reference322 query{name}", "candidate_generation": "exact cosine 112163 fixed references",
                "reranking": None, "fusion": None, "hyperparameters": {}, "fitted_parameters": False,
                "raw25": report["raw"]["accuracy_25m"], "raw50": report["raw"]["accuracy_50m"],
                "raw100": report["raw"]["accuracy_100m"], "r_at_k": report["recall_at"],
                "median_m": report["raw"]["median_error_m"], "p90_m": report["raw"]["p90_error_m"],
                "gt500_rate": report["raw"]["catastrophic_gt500m_rate"], "runtime": runtime,
                "result_status": "complete", "decision": "assess complementary candidate identities",
                "artifact_paths": [str(out / f"{name}.npz"), str(out / f"{name}.json")],
                "artifact_hashes": {f: digest(out / f) for f in (f"{name}.npz", f"{name}.json")}})
            results[name] = ranked
            print(name, "R100", report["recall_at"]["100"], "RAW100", report["raw"]["accuracy_100m"], flush=True)
            del scores
    coords = g[["lat", "lon"]].to_numpy()
    union_results = {}
    for label, matrices in (("322_504", [results["322"], results["504"]]),
                            ("sage_322_504", [sage, results["322"], results["504"]])):
        count, o50, o100, rows = [], [], [], []
        for i, query in enumerate(q.itertuples()):
            unique = list(dict.fromkeys(np.concatenate([x[i] for x in matrices]).tolist()))
            d = distances(query.lat, query.lon, np.radians(coords[unique, 0]), np.radians(coords[unique, 1]))
            count.append(len(unique))
            o50.append(bool((d <= 50).any()))
            o100.append(bool((d <= 100).any()))
            rows.append(unique)
        recovered = int(((buckets == "retrieval_failure") & np.asarray(o100)).sum())
        union_results[label] = {"oracle_50m": float(np.mean(o50)), "oracle_100m": float(np.mean(o100)),
            "original_309_retrieval_failures_recovered": recovered,
            "unique_candidates_per_query": {"mean": float(np.mean(count)), "median": float(np.median(count))},
            "per_query": [{"query_id": q.id.iloc[i], "oracle50": o50[i], "oracle100": o100[i],
                           "gallery_rows": rows[i]} for i in range(len(q))]}
        save(out / f"union_{label}.json", union_results[label])
        registry(f"sage_resolution_union_{label}_oracle", {
            "model_revision": "pinned SAGE-L No-Encoder checkpoint in frozen candidate",
            "preprocessing": "reference322 query322 and query504", "candidate_generation": f"top100 set union {label}",
            "reranking": "oracle only", "fusion": "candidate union", "hyperparameters": {"depth": 100},
            "fitted_parameters": False, "raw25": None, "raw50": None, "raw100": None,
            "r_at_k": {"oracle50": union_results[label]["oracle_50m"],
                       "oracle100": union_results[label]["oracle_100m"]},
            "median_m": None, "p90_m": None, "gt500_rate": None,
            "runtime": "cached descriptors, exact cosine", "result_status": "complete",
            "decision": "assess candidate complementarity", "artifact_paths": [str(out / f"union_{label}.json")],
            "artifact_hashes": {f"union_{label}.json": digest(out / f"union_{label}.json")}})
        print(label, "oracle100", union_results[label]["oracle_100m"], "retrieval recovered", recovered, flush=True)


if __name__ == "__main__":
    run()
