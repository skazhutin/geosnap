"""Unfitted distance-to-direct-prediction reranking of frozen SAGE candidates."""
import argparse
import json

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, compare, frames, metrics, registry


def run(name):
    model_dir = LOCAL / name
    done = json.loads((model_dir / "complete.json").read_text())
    if digest(model_dir / "predictions.npy") != done["hashes"]["predictions.npy"]:
        raise RuntimeError("Direct prediction cache hash changed")
    direct = np.load(model_dir / "predictions.npy", allow_pickle=False)
    with np.load(LOCAL / "baseline/predictions.npz", allow_pickle=False) as baseline:
        candidates = baseline["indices"]
        old_errors = baseline["errors_m"]
    q, g = frames()
    ref = np.radians(g[["lat", "lon"]].to_numpy()[candidates])
    direct = np.radians(direct)
    dlat = ref[:, :, 0] - direct[:, None, 0]
    dlon = ref[:, :, 1] - direct[:, None, 1]
    a = np.sin(dlat / 2)**2 + np.cos(ref[:, :, 0])*np.cos(direct[:, None, 0])*np.sin(dlon / 2)**2
    prior_m = 6371008.8 * 2*np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    model_dir.mkdir(exist_ok=True)
    np.save(model_dir / "sage_candidate_distance_prior_m.npy", prior_m.astype(np.float32))
    results = {}
    for k in (30, 50, 100):
        order = np.argsort(prior_m[:, :k], axis=1, kind="stable")
        ranked = np.concatenate((np.take_along_axis(candidates[:, :k], order, axis=1), candidates[:, k:]), axis=1)
        report, errors, _ = metrics(q, g, ranked)
        paired = compare(q, errors)
        report |= paired | {"direct_model": name, "candidate_count_reranked": k,
            "method": "unfitted nearest reference coordinate to official direct GPS prediction",
            "scope": "exploratory, fixed gallery, no query ground truth at inference",
            "prior_median_candidate_distance_m": float(np.median(prior_m[:, :k]))}
        stem = f"{name}_prior_sage{k}"
        np.savez(model_dir / f"{stem}.npz", query_ids=q.id.to_numpy(str), indices=ranked,
                 prediction_gps=g[["lat", "lon"]].to_numpy()[ranked[:, 0]], errors_m=errors)
        save(model_dir / f"{stem}.json", report)
        registry(stem, {"model_revision": done["contract"]["model_revision"],
            "preprocessing": done["contract"].get("preprocessing", done["contract"].get("input_preprocessing")),
            "candidate_generation": "frozen SAGE top100", "reranking": report["method"],
            "fusion": "direct-GPS distance only", "hyperparameters": {"candidate_count": k},
            "fitted_parameters": False, "raw25": report["raw"]["accuracy_25m"],
            "raw50": report["raw"]["accuracy_50m"], "raw100": report["raw"]["accuracy_100m"],
            "r_at_k": report["recall_at"], "median_m": report["raw"]["median_error_m"],
            "p90_m": report["raw"]["p90_error_m"],
            "gt500_rate": report["raw"]["catastrophic_gt500m_rate"],
            "runtime": "cached direct model; ranking only", "result_status": "complete",
            "decision": "assess signal; no weight tuned", "artifact_paths": [str(model_dir / f"{stem}.json"), str(model_dir / f"{stem}.npz")],
            "artifact_hashes": {n: digest(model_dir / n) for n in (f"{stem}.json", f"{stem}.npz")},
            "paired_bootstrap": paired["paired_geographic_bootstrap"]})
        results[str(k)] = {"raw100_count": int((errors <= 100).sum()),
            "old_wrong_new_correct": paired["transitions"]["old_wrong_new_correct"],
            "old_correct_new_wrong": paired["transitions"]["old_correct_new_wrong"]}
    save(model_dir / "prior_summary.json", {"model": name, "results": results,
        "prior_sha256": digest(model_dir / "sage_candidate_distance_prior_m.npy")})
    print(json.dumps(results), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("model", choices=("plonk", "osv5m"))
    run(parser.parse_args().model)
