"""Development uncertainty across the complete reported night candidate family."""
import json

import numpy as np
from threadpoolctl import threadpool_limits

from ml.research.gallery_scale import fixed_development
from ml.research.gallery_scale_storage import digest, save
from ml.research.night_v7 import paths


def bootstrap_differences(differences, groups, *, resamples, seed):
    _, inverse = np.unique(groups, return_inverse=True)
    group_count = int(inverse.max()) + 1
    sizes = np.bincount(inverse, minlength=group_count)
    totals = np.stack([np.bincount(inverse, weights=row, minlength=group_count) for row in differences])
    random = np.random.default_rng(seed)
    samples = np.empty((resamples, len(differences)), dtype=np.float32)
    for start in range(0, resamples, 2000):
        counts = random.multinomial(group_count, np.full(group_count, 1 / group_count),
                                    size=min(2000, resamples - start))
        samples[start:start + len(counts)] = (counts @ totals.T) / (counts @ sizes)[:, None] * 100
    return samples, group_count


def run():
    _, gc, _, _, night = paths()
    queries = fixed_development(gc)
    board = json.loads((night / "leaderboard.json").read_text())
    records = {r["name"]: r for r in board["records"]}
    base = json.loads((night / "results/baseline_rows.json").read_text())
    if base["query_ids"] != queries.id.tolist():
        raise RuntimeError("uncertainty query order differs from frozen development")
    correct = np.array(base["errors_m"]) <= 100
    names = sorted(set(records) - {"baseline"})
    differences, hashes = [], {}
    for name in names:
        path = night / "results" / f"{name}_rows.json"
        rows = json.loads(path.read_text())
        if rows["query_ids"] != base["query_ids"]:
            raise RuntimeError("candidate changes the common uncertainty denominator")
        accuracy = np.array(rows["errors_m"]) <= 100
        if float(accuracy.mean()) != records[name]["raw"]["accuracy_100m"]:
            raise RuntimeError("candidate summary differs from its saved outcomes")
        differences.append(accuracy.astype(int) - correct.astype(int))
        hashes[name] = digest(path)
    with threadpool_limits(limits=1):
        samples, group_count = bootstrap_differences(np.array(differences), queries.h3_coarse.to_numpy(),
                                                    resamples=200000, seed=20260918)
    tail = .025 / len(names)
    intervals = np.quantile(samples, [tail, 1 - tail], axis=0)
    pointwise = np.quantile(samples, [.025, .975], axis=0)
    payload = {"kind": "exploratory_development_family_uncertainty_not_a_final_test",
        "method": "paired spatial-cluster percentile bootstrap; Bonferroni tail probability across reported non-baseline methods",
        "metric": "all-query raw accuracy within100m", "baseline": "baseline", "query_count": len(queries),
        "query_sha256": gc["query_sha256"], "candidate_count": len(names), "spatial_groups": group_count,
        "resamples": 200000, "seed": 20260918, "per_comparison_tail_probability": tail,
        "candidate_rows_sha256": hashes, "baseline_rows_sha256": digest(night / "results/baseline_rows.json"),
        "leaderboard_sha256": digest(night / "leaderboard.json"),
        "estimates": {name: {"gain_pp": float(np.mean(differences[i]) * 100),
            "pointwise_interval_pp": pointwise[:, i].tolist(),
            "bonferroni_percentile_interval_pp": intervals[:, i].tolist()} for i, name in enumerate(names)},
        "limitations": ["bootstrap intervals are approximate and do not certify independent generalization",
            "correction covers reported night methods only, not all historical or informal decisions",
            "adaptive development acquisition and fixed OOF predictions are not refitted in bootstrap samples",
            "no new queries, heldout outcomes, predictions, thresholds, or candidate selections are created"],
        "calibration_final_access": False}
    save(night / "family_uncertainty.json", payload)
    best = board["records"][0]["name"]
    print(json.dumps({"methods": len(names), "best": best, **payload["estimates"][best]}))


if __name__ == "__main__":
    run()
