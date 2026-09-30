"""Measure incremental oracle recall when strict adapted SAGE joins all tested retrievers."""
from __future__ import annotations

import json
import time

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, frames, registry
from ml.research.metrics import paired_group_bootstrap
from ml.research.vector_evaluation import distances


def run():
    started = time.perf_counter()
    q, g = frames()
    root = LOCAL / "sage_adaptation_v3"
    complete = json.loads((root / "complete.json").read_text())
    epoch = complete["selected_epoch"]
    prior_path = LOCAL / "cross_model_union/all_verified_same_gallery.json"
    adapted_path = root / f"sage_asymmetric_v3_refonly_epoch{epoch}_primary/predictions.npz"
    prior = json.loads(prior_path.read_text())
    with np.load(adapted_path, allow_pickle=False) as saved:
        if saved["query_ids"].tolist() != q.id.tolist():
            raise RuntimeError("Adapted query identity differs")
        adapted = saved["indices"].copy()
    if prior["query_ids"] != q.id.tolist() or adapted.shape != (len(q), 100):
        raise RuntimeError("Prior union/adapted query contract differs")
    with np.load(LOCAL / "baseline/predictions.npz", allow_pickle=False) as baseline:
        buckets = baseline["buckets"].copy()
    xy = g[["lat", "lon"]].to_numpy(float)
    original = np.asarray(prior["oracle100_per_query"], bool)
    oracle25 = np.empty(len(q), bool)
    oracle50 = np.empty(len(q), bool)
    oracle100 = np.empty(len(q), bool)
    cardinality = np.empty(len(q), int)
    for i, row in enumerate(q.itertuples()):
        candidates = list(dict.fromkeys([*prior["candidate_gallery_rows"][i], *adapted[i].tolist()]))
        coords = xy[candidates]
        d = distances(row.lat, row.lon, np.radians(coords[:, 0]), np.radians(coords[:, 1]))
        oracle25[i], oracle50[i], oracle100[i] = (bool(np.any(d <= r)) for r in (25, 50, 100))
        cardinality[i] = len(candidates)
    if int(original.sum()) != 642 or np.any(original & ~oracle100):
        raise RuntimeError("Prior union oracle changed or union lost candidates")
    result = {"variant": "all verified same-gallery retrievers + strict adapted SAGE",
        "scope": "fixed 112163 gallery exploratory development candidate oracle",
        "models": [*prior["models"], "sage_asymmetric_v3"],
        "query_count": len(q), "oracle25": float(oracle25.mean()),
        "oracle50": float(oracle50.mean()), "oracle100": float(oracle100.mean()),
        "oracle100_count": int(oracle100.sum()),
        "unique_candidates_per_query": {"mean": float(cardinality.mean()),
            "median": float(np.median(cardinality)), "min": int(cardinality.min()),
            "max": int(cardinality.max())},
        "new_beyond_prior_all_models": int((oracle100 & ~original).sum()),
        "original_309_retrieval_failures_recovered": int(((buckets == "retrieval_failure") & oracle100).sum()),
        "gain_vs_prior_all_models": paired_group_bootstrap(np.where(original, 0., 200.),
            np.where(oracle100, 0., 200.), q.h3_coarse.tolist()),
        "prior_union_sha256": digest(prior_path), "adapted_predictions_sha256": digest(adapted_path),
        "adapted_checkpoint_sha256": complete["selected_checkpoint_sha256"],
        "query_ground_truth_only_for_oracle_metrics": True,
        "runtime_s": time.perf_counter()-started}
    path = root / "all_model_candidate_union.json"
    save(path, result)
    np.savez(root / "all_model_candidate_union_oracle.npz", query_ids=q.id.to_numpy(str),
        oracle25=oracle25, oracle50=oracle50, oracle100=oracle100,
        prior_oracle100=original, candidate_count=cardinality)
    registry(f"sage_asymmetric_v3_epoch{epoch}_all_model_union_oracle", {
        "model_revision": complete["selected_checkpoint_sha256"],
        "preprocessing": "+".join(result["models"]),
        "candidate_generation": "top100 set union of all verified models plus strict adapted SAGE",
        "reranking": None, "fusion": "candidate-set union only; no final prediction",
        "hyperparameters": {"k_per_model": 100}, "fitted_parameters": True,
        "fitting_split": "adapted SAGE reference-only all-role sequence/geographic embargo",
        "evaluation_split": "1184 reused exploratory development oracle",
        "raw25": None, "raw50": None, "raw100": None,
        "r_at_k": {"union_oracle100": result["oracle100"]},
        "median_m": None, "p90_m": None, "gt500_rate": None,
        "runtime": result["runtime_s"], "result_status": "complete",
        "decision": "incremental complementarity only; reranker not proven",
        "artifact_paths": [str(path), str(root / "all_model_candidate_union_oracle.npz")],
        "artifact_hashes": {item: digest(root / item) for item in
            ("all_model_candidate_union.json", "all_model_candidate_union_oracle.npz")}})
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    run()
