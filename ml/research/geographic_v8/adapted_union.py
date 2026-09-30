"""Measure candidate complementarity of the strict reference-trained SAGE model."""
from __future__ import annotations

import json
import time

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import GALLERY, LOCAL, frames, registry
from ml.research.metrics import paired_group_bootstrap
from ml.research.vector_evaluation import distances

OUT = LOCAL / "sage_adaptation_v3"


def run():
    started = time.perf_counter()
    complete = json.loads((OUT / "complete.json").read_text())
    epoch = complete["selected_epoch"]
    adapted_path = OUT / f"sage_asymmetric_v3_refonly_epoch{epoch}_primary/predictions.npz"
    baseline_path = LOCAL / "baseline/predictions.npz"
    q, g = frames()
    with np.load(baseline_path, allow_pickle=False) as old, np.load(adapted_path, allow_pickle=False) as new:
        if old["query_ids"].tolist() != q.id.tolist() or new["query_ids"].tolist() != q.id.tolist():
            raise RuntimeError("Adapted/baseline query order differs")
        base, adapted, buckets = old["indices"].copy(), new["indices"].copy(), old["buckets"].copy()
    if base.shape != adapted.shape or base.shape != (1184, 100):
        raise RuntimeError("Expected paired fixed-gallery top100 lists")
    coords = g[["lat", "lon"]].to_numpy(float)
    original100 = np.empty(len(q), bool)
    union50 = np.empty(len(q), bool)
    union100 = np.empty(len(q), bool)
    sizes = np.empty(len(q), np.int16)
    rows = []
    for i, row in enumerate(q.itertuples()):
        candidates = np.fromiter(dict.fromkeys((*base[i], *adapted[i])), np.int32)
        if len(candidates) < 100 or len(candidates) > 200:
            raise RuntimeError("Invalid adaptation union cardinality")
        baseline_xy = coords[base[i]]
        d0 = distances(row.lat, row.lon, np.radians(baseline_xy[:, 0]), np.radians(baseline_xy[:, 1]))
        xy = coords[candidates]
        d = distances(row.lat, row.lon, np.radians(xy[:, 0]), np.radians(xy[:, 1]))
        original100[i] = np.any(d0 <= 100)
        union50[i] = np.any(d <= 50)
        union100[i] = np.any(d <= 100)
        sizes[i] = len(candidates)
        rows.append(candidates)
    if int(original100.sum()) != 613:
        raise RuntimeError("Frozen baseline retrieval oracle changed")
    result = {"variant": "SAGE baseline top100 + strict reference-trained SAGE top100",
        "scope": "fixed 112163 gallery, exploratory development oracle only",
        "query_count": len(q), "unique_candidates_per_query": {"mean": float(sizes.mean()),
            "min": int(sizes.min()), "max": int(sizes.max())},
        "oracle50": float(union50.mean()), "oracle100": float(union100.mean()),
        "oracle100_count": int(union100.sum()),
        "gain_over_frozen_sage_retrieval_count": int((union100 & ~original100).sum()),
        "original_309_retrieval_failures_recovered": int(((buckets == "retrieval_failure") & union100).sum()),
        "paired_geographic_bootstrap": paired_group_bootstrap(np.where(original100, 0., 200.),
            np.where(union100, 0., 200.), q.h3_coarse.tolist()),
        "adapted_checkpoint_sha256": complete["selected_checkpoint_sha256"],
        "baseline_predictions_sha256": digest(baseline_path),
        "adapted_predictions_sha256": digest(adapted_path),
        "runtime_s": time.perf_counter()-started,
        "query_ground_truth_used_only_for_oracle_evaluation": True}
    folder = OUT / "candidate_union"
    folder.mkdir(exist_ok=True)
    np.savez(folder / "oracle.npz", query_ids=q.id.to_numpy(str),
        offsets=np.r_[0, np.cumsum(sizes, dtype=np.int32)], indices=np.concatenate(rows),
        base_oracle100=original100, union_oracle50=union50, union_oracle100=union100)
    save(folder / "oracle.json", result)
    registry(f"sage_asymmetric_v3_epoch{epoch}_union_oracle", {
        "model_revision": complete["selected_checkpoint_sha256"],
        "gallery": {"path": str(GALLERY), "sha256": digest(GALLERY), "count": len(g), "msls": "research-only"},
        "preprocessing": "SAGE-L reference322 query322+504",
        "candidate_generation": "union of frozen baseline top100 and strict reference-trained SAGE top100",
        "reranking": None, "fusion": "candidate-set union only; no final prediction",
        "hyperparameters": {"k_per_model": 100}, "fitted_parameters": True,
        "fitting_split": "reference-only all-role sequence-disjoint H3r6 one-ring geographic embargo",
        "evaluation_split": "1184 reused exploratory development oracle",
        "raw25": None, "raw50": None, "raw100": None,
        "r_at_k": {"union_oracle100": result["oracle100"]},
        "median_m": None, "p90_m": None, "gt500_rate": None,
        "runtime": result["runtime_s"], "result_status": "complete",
        "decision": "complementarity diagnostic, not a predictor",
        "artifact_paths": [str(folder / "oracle.npz"), str(folder / "oracle.json")],
        "artifact_hashes": {f: digest(folder / f) for f in ("oracle.npz", "oracle.json")}})
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    run()
