"""Full-denominator direct-GPS evaluation and SAGE failure complementarity."""
import argparse
import json

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, GALLERY, compare, frames, metrics, registry

PATHS = {"plonk": LOCAL / "plonk", "osv5m": LOCAL / "osv5m"}


def run(name):
    out = PATHS[name]
    done = json.loads((out / "complete.json").read_text())
    if digest(out / "predictions.npy") != done["hashes"]["predictions.npy"]:
        raise RuntimeError("Direct model prediction cache changed")
    gps = np.load(out / "predictions.npy", allow_pickle=False)
    q, g = frames()
    if gps.shape != (len(q), 2) or not np.isfinite(gps).all():
        raise RuntimeError("Direct model lacks a GPS prediction for every query")
    report, errors, _ = metrics(q, None, gps=gps)
    matched = compare(q, errors)
    with np.load(LOCAL / "baseline/predictions.npz", allow_pickle=False) as old:
        old_errors = old["errors_m"]
        buckets = old["buckets"]
    close = {str(k): int((errors <= k).sum()) for k in (25, 50, 100, 500, 1000, 5000)}
    conditional = {"sage_catastrophic_gt500_count": int((old_errors > 500).sum()),
        "direct_within100_when_sage_gt500": int(((old_errors > 500) & (errors <= 100)).sum()),
        "direct_within500_when_sage_gt500": int(((old_errors > 500) & (errors <= 500)).sum()),
        "direct_within100_on_sage_top100_positive": int(((np.isin(buckets, ["correct", "ranking_failure"])) & (errors <= 100)).sum()),
        "direct_within100_on_309_retrieval_failures": int(((buckets == "retrieval_failure") & (errors <= 100)).sum()),
        "direct_within100_on_262_no_coverage": int(((buckets == "no_coverage") & (errors <= 100)).sum())}
    report |= matched | {"counts": close, "conditional": conditional,
        "scope": "official pretrained direct model; exploratory heavily reused development; no query GT enters model",
        "model": done["contract"], "runtime_s": done["runtime_s"],
        "gallery": "no gallery coordinates in direct inference; comparison is against fixed SAGE gallery"}
    np.savez(out / "evaluated.npz", query_ids=q.id.to_numpy(str), prediction_gps=gps, errors_m=errors)
    save(out / "report.json", report)
    model = done["contract"]
    registry(f"{name}_official_direct", {"model_revision": model["model_revision"],
        "checkpoint_hashes": done["hashes"], "preprocessing": model.get("input_preprocessing", model.get("preprocessing")),
        "candidate_generation": "direct GPS; no fixed gallery used", "reranking": None,
        "fusion": None, "hyperparameters": {"seed": model.get("seed"), "sampler": model.get("sampler")},
        "raw25": report["raw"]["accuracy_25m"], "raw50": report["raw"]["accuracy_50m"],
        "raw100": report["raw"]["accuracy_100m"], "r_at_k": None,
        "median_m": report["raw"]["median_error_m"], "p90_m": report["raw"]["p90_error_m"],
        "gt500_rate": report["raw"]["catastrophic_gt500m_rate"], "runtime": done["runtime_s"],
        "result_status": "complete", "decision": "assess complementarity before geographic prior",
        "artifact_paths": [str(out / "report.json"), str(out / "evaluated.npz")],
        "artifact_hashes": {n: digest(out / n) for n in ("report.json", "evaluated.npz")},
        "bucket_analysis": conditional, "paired_bootstrap": matched["paired_geographic_bootstrap"]})
    print(name, "direct100", close["100"], "direct500", close["500"], "sage_catastrophic_recovered500", conditional["direct_within500_when_sage_gt500"], flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("model", choices=PATHS)
    run(parser.parse_args().model)
