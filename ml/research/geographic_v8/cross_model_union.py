"""Frozen-gallery candidate unions across independently measured encoders."""
import json

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, frames, registry
from ml.research.vector_evaluation import distances


def run():
    q, g = frames()
    sources = {"sage": LOCAL / "baseline/predictions.npz",
        "g3": LOCAL / "g3/g3_full/predictions.npz",
        "geoclip": LOCAL / "geoclip/geoclip_full/predictions.npz",
        "resolution322": LOCAL / "resolution_union/322.npz",
        "resolution504": LOCAL / "resolution_union/504.npz"}
    rankings = {}
    for name, path in sources.items():
        with np.load(path, allow_pickle=False) as values:
            if values["query_ids"].tolist() != q.id.tolist():
                raise RuntimeError(f"{name} query identity mismatch")
            rankings[name] = values["indices"].copy()
    with np.load(sources["sage"], allow_pickle=False) as old:
        buckets = old["buckets"].copy()
    groups = {
        "sage_g3": ["sage", "g3"],
        "sage_geoclip": ["sage", "geoclip"],
        "sage_g3_geoclip": ["sage", "g3", "geoclip"],
        "sage_both_resolutions": ["sage", "resolution322", "resolution504"],
        "all_verified_same_gallery": list(sources)}
    coordinates = g[["lat", "lon"]].to_numpy()
    output = {}
    for name, models in groups.items():
        counts, oracle25, oracle50, oracle100, unique_rows = [], [], [], [], []
        for i, query in enumerate(q.itertuples()):
            candidate = list(dict.fromkeys(np.concatenate([rankings[m][i] for m in models]).tolist()))
            xy = coordinates[candidate]
            d = distances(query.lat, query.lon, np.radians(xy[:, 0]), np.radians(xy[:, 1]))
            counts.append(len(candidate))
            oracle25.append(bool((d <= 25).any()))
            oracle50.append(bool((d <= 50).any()))
            oracle100.append(bool((d <= 100).any()))
            unique_rows.append(candidate)
        got = np.asarray(oracle100)
        result = {"models": models, "oracle25": float(np.mean(oracle25)),
            "oracle50": float(np.mean(oracle50)), "oracle100": float(np.mean(oracle100)),
            "oracle100_count": int(got.sum()),
            "original_309_retrieval_failures_recovered": int(((buckets == "retrieval_failure") & got).sum()),
            "original_262_no_coverage_recovered": int(((buckets == "no_coverage") & got).sum()),
            "unique_candidates_per_query": {"mean": float(np.mean(counts)), "median": float(np.median(counts)),
                "min": int(min(counts)), "max": int(max(counts))},
            "query_ids": q.id.tolist(), "oracle100_per_query": oracle100,
            "oracle50_per_query": oracle50, "candidate_gallery_rows": unique_rows,
            "source_hashes": {model: digest(sources[model]) for model in models}}
        output[name] = {k:v for k,v in result.items() if k not in ("query_ids", "oracle100_per_query",
                                                                 "oracle50_per_query", "candidate_gallery_rows")}
        save(LOCAL / f"cross_model_union/{name}.json", result)
        registry(f"{name}_candidate_union_oracle", {"model_revision": "see pinned source prediction artifacts",
            "preprocessing": "+".join(models), "candidate_generation": "set union of top100 from " + ", ".join(models),
            "reranking": "oracle only", "fusion": "candidate union", "hyperparameters": {"depth": 100},
            "fitted_parameters": False, "raw25": None, "raw50": None, "raw100": None,
            "r_at_k": {"oracle25": result["oracle25"], "oracle50": result["oracle50"],
                       "oracle100": result["oracle100"]},
            "median_m": None, "p90_m": None, "gt500_rate": None,
            "runtime": "cached rankings and gallery coordinates", "result_status": "complete",
            "decision": "candidate ceiling analysis; no inference-time winner",
            "artifact_paths": [str(LOCAL / f"cross_model_union/{name}.json")],
            "artifact_hashes": {"result": digest(LOCAL / f"cross_model_union/{name}.json")}})
        print(name, result["oracle100_count"], "recovered", result["original_309_retrieval_failures_recovered"], flush=True)
    save(LOCAL / "cross_model_union/summary.json", output)


if __name__ == "__main__":
    run()
