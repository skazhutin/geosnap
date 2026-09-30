"""Attribute completed development gains without changing any prediction."""
import argparse
import json

import numpy as np
import pandas as pd

from ml.research.gallery_scale_storage import digest, save
from ml.research.night_v7 import paths

STAGES = {"scale_mean_context_added": "gallery_addition",
          "scale_mean_context_expanded2": "live_expansion2"}
CATEGORIES = ("no_coverage", "retrieval_miss_top100", "wrong_top1", "correct_top1")


def classes(rows, coverage):
    ranks = rows.get("positive_ranks_through100", rows.get("positive_ranks"))
    rank = np.array([np.inf if r is None else r for r in ranks])
    labels = np.where(~coverage, 0, np.where(rank > 100, 1, np.where(rank > 1, 2, 3)))
    if not np.array_equal(labels == 3, np.array(rows["errors_m"]) <= 100):
        raise RuntimeError("diagnostic categories disagree with committed raw errors")
    return labels


def comparison(before, after, old_coverage, new_coverage):
    old, new = np.array(before["errors_m"]), np.array(after["errors_m"])
    transitions = np.zeros((4, 4), dtype=int)
    np.add.at(transitions, (classes(before, old_coverage), classes(after, new_coverage)), 1)
    if transitions.sum() != len(old):
        raise RuntimeError("transition table changed the query denominator")
    return {"category_order": CATEGORIES, "rows_before_columns_after": transitions.tolist(),
            "distance_thresholds": {str(t): {"gained": int(((old > t) & (new <= t)).sum()),
                "lost": int(((old <= t) & (new > t)).sum()),
                "net": int((new <= t).sum() - (old <= t).sum())} for t in (25, 50, 100)},
            "catastrophic_recovered": int(((old > 500) & (new <= 500)).sum()),
            "catastrophic_introduced": int(((old <= 500) & (new > 500)).sum())}


def run(name):
    _, _, _, previous, night = paths()
    stage = night / STAGES[name]
    target = night / "gain_diagnosis" / f"{name}.json"
    manifest = stage / "gallery.parquet"
    rows_path = night / "results" / f"{name}_rows.json"
    summary = night / "results" / f"{name}.json"
    paths_to_pin = [manifest, rows_path, summary, previous / "results/G2_smart_rows.json",
                   night / "results/baseline_rows.json", night / "results/scale_mean_context_rows.json",
                   stage / "results" / f"{STAGES[name]}_rows.json"]
    contract = {str(p): digest(p) for p in paths_to_pin}
    if target.exists():
        if json.loads(target.read_text())["input_hashes"] != contract:
            raise RuntimeError("completed diagnostic inputs changed")
        return
    gallery = pd.read_parquet(manifest)
    result = json.loads(summary.read_text())
    if digest(manifest) != result["gallery_sha256"]:
        raise RuntimeError("prediction gallery identity changed")
    after = json.loads(rows_path.read_text())
    before = {key: json.loads((night / "results" / f"{key}_rows.json").read_text())
              for key in ("baseline", "scale_mean_context")}
    old_geometry = json.loads((previous / "results/G2_smart_rows.json").read_text())
    new_geometry = json.loads(paths_to_pin[-1].read_text())
    if any(r["query_ids"] != after["query_ids"] for r in [*before.values(), old_geometry, new_geometry]):
        raise RuntimeError("diagnostic query identities changed")
    old_coverage = np.array(old_geometry["positive_count_100m"]) > 0
    new_coverage = np.array(new_geometry["positive_count_100m"]) > 0
    if (old_coverage & ~new_coverage).any():
        raise RuntimeError("nested expanded gallery lost geographical coverage")
    chosen = np.array(after["top100_gallery_rows"], dtype=int)[:, 0]
    correct = np.array(after["errors_m"]) <= 100
    if chosen.min() < 0 or chosen.max() >= len(gallery):
        raise RuntimeError("diagnostic reference row outside gallery")
    selected = gallery.iloc[chosen]
    source_counts = {source: {"predictions": int((selected.source == source).sum()),
        "correct100": int(((selected.source == source).to_numpy() & correct).sum())}
        for source in sorted(selected.source.unique())}
    cohorts = {"original100k": chosen < 100000,
               "first2944_added": (chosen >= 100000) & (chosen < 102944),
               "second_tranche": chosen >= 102944}
    payload = {"kind": "posthoc_counts_only_not_an_inference_or_acquisition_policy",
        "candidate": name, "query_count": len(after["query_ids"]), "input_hashes": contract,
        "comparisons": {key: comparison(r, after, old_coverage, new_coverage) for key, r in before.items()},
        "newly_covered_queries": int((~old_coverage & new_coverage).sum()),
        "correct_among_newly_covered": int((correct & ~old_coverage & new_coverage).sum()),
        "correct_among_previously_covered": int((correct & old_coverage).sum()),
        "top1_reference_by_source": source_counts,
        "top1_reference_by_acquisition": {key: {"predictions": int(mask.sum()),
            "correct100": int((mask & correct).sum())} for key, mask in cohorts.items()},
        "limitations": ["query outcomes are consumed only after predictions were committed",
            "these development diagnostics must not target reference acquisition to query coordinates",
            "same-query comparisons after multiple experiments are not independent final results"],
        "calibration_final_access": False}
    save(target, payload)
    print(json.dumps(payload))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("candidate", choices=STAGES)
    run(parser.parse_args().candidate)
