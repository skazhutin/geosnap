"""Analyze outcome-blind human ratings only after the chosen sample is complete."""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter

import numpy as np
from sklearn.metrics import cohen_kappa_score

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v9.common import OUT, record

COUNTS = {"correct": 411, "ranking_failure": 202,
          "retrieval_failure": 309, "no_coverage": 262}
REASONS = ["motion_blur", "out_of_focus", "too_dark", "overexposed",
    "compression_or_corruption", "obstructed_view", "close_surface", "mostly_ground",
    "mostly_sky", "inside_vehicle", "indoor", "vegetation_dominated", "generic_road",
    "repetitive_scene", "no_stable_landmarks", "insufficient_scene_context"]


def load(sample, raters):
    folder = OUT / "annotations"
    prefix = "" if sample == "full" else "quick20_"
    sample_path = folder / f"{prefix}private_sample.json"
    receipt = json.loads((folder / f"{prefix}sample_receipt.json").read_text())
    if digest(sample_path) != receipt["private_sample_sha256"]:
        raise RuntimeError("Blind sample hash changed")
    items = json.loads(sample_path.read_text())["items"]
    by_token = {row["token"]: row for row in items}
    ratings, hashes = {}, {}
    for rater in raters:
        path = folder / f"{prefix}rater_{rater}.jsonl"
        if not path.exists():
            ratings[rater] = {}
            continue
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        if any(row["rater"] != rater or row["token"] not in by_token for row in rows):
            raise RuntimeError("Rater file has an unknown label or token")
        if len({row["token"] for row in rows}) != len(rows):
            raise RuntimeError("Duplicate rating")
        ratings[rater] = {row["token"]: row for row in rows}
        hashes[rater] = digest(path)
    return items, ratings, hashes, digest(sample_path)


def weighted_rate(items, labels, predicate):
    by_bucket = {}
    for bucket in COUNTS:
        sample = [row for row in items if row["hidden_bucket"] == bucket]
        values = np.asarray([predicate(labels[row["token"]]) for row in sample], bool)
        by_bucket[bucket] = {"positive": int(values.sum()), "sampled": len(values),
                             "rate": float(values.mean())}
    estimate = sum(COUNTS[b]/1184*by_bucket[b]["rate"] for b in COUNTS)
    rng = np.random.default_rng(20260928)
    boots = np.zeros(5000, np.float64)
    for bucket in COUNTS:
        sample = [row for row in items if row["hidden_bucket"] == bucket]
        values = np.asarray([predicate(labels[row["token"]]) for row in sample], float)
        groups = np.asarray([row["hidden_group"] for row in sample], str)
        unique = np.unique(groups)
        group_values = [values[groups == group] for group in unique]
        for j in range(len(boots)):
            chosen = rng.integers(0, len(unique), size=len(unique))
            draw = np.concatenate([group_values[k] for k in chosen])
            boots[j] += COUNTS[bucket]/1184 * draw.mean()
    return {"stratified_population_estimate": float(estimate),
        "stratified_geographic_group_bootstrap_95pct": np.quantile(boots, [.025, .975]).tolist(),
        "by_hidden_baseline_outcome": by_bucket}


def agreement(a, b, tokens):
    if len(tokens) < 2:
        return {"overlap": len(tokens), "status": "insufficient_overlap"}
    scores1 = [a[t]["geolocatability"] for t in tokens]
    scores2 = [b[t]["geolocatability"] for t in tokens]
    retake1 = [a[t]["would_request_another_photo"] for t in tokens]
    retake2 = [b[t]["would_request_another_photo"] for t in tokens]
    return {"overlap": len(tokens),
        "exact_ordinal_agreement": float(np.mean(np.equal(scores1, scores2))),
        "quadratic_weighted_kappa": float(cohen_kappa_score(scores1, scores2, weights="quadratic")),
        "retake_exact_agreement": float(np.mean(np.equal(retake1, retake2))),
        "retake_kappa": float(cohen_kappa_score(retake1, retake2)),
        "reason_exact_agreement": {reason: float(np.mean([
            (reason in a[t]["reasons"]) == (reason in b[t]["reasons"]) for t in tokens]))
            for reason in REASONS}}


def run(sample, raters):
    items, ratings, hashes, sample_hash = load(sample, raters)
    progress = {rater: {"completed": len(labels), "total": len(items)}
                for rater, labels in ratings.items()}
    complete = all(len(labels) == len(items) for labels in ratings.values())
    if not complete:
        print(json.dumps({"status": "awaiting_human_labels", "sample": sample,
            "progress": progress, "no_prevalence_report": True}, ensure_ascii=False))
        return
    key = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()[:12]
    path = OUT / "annotations" / f"analysis_{sample}_{key}.json"
    if path.exists():
        raise FileExistsError("Analysis already sealed for this label version")
    results = {}
    for rater, labels in ratings.items():
        results[rater] = {
            "poor_geolocatability_0_or_1": weighted_rate(items, labels,
                lambda row: row["geolocatability"] <= 1),
            "good_geolocatability_3_or_4": weighted_rate(items, labels,
                lambda row: row["geolocatability"] >= 3),
            "would_request_retake": weighted_rate(items, labels,
                lambda row: row["would_request_another_photo"]),
            "failure_reasons": {reason: weighted_rate(items, labels,
                lambda row, reason=reason: reason in row["reasons"]) for reason in REASONS},
            "retake_actions": dict(Counter(row["recommended_retake_action"] for row in labels.values()
                if row["recommended_retake_action"])),
        }
    pairs = {}
    for i, a in enumerate(raters):
        for b in raters[i+1:]:
            tokens = sorted(set(ratings[a]) & set(ratings[b]))
            pairs[f"{a}__{b}"] = agreement(ratings[a], ratings[b], tokens)
    result = {"status": "complete", "sample": sample, "sample_size": len(items),
        "warning": "This is association in a stratified, reused development sample, not a causal explanation or independent final estimate.",
        "pilot_warning": "20-image pilot has very wide uncertainty; use only to refine annotation workflow." if sample == "quick20" else None,
        "population_bucket_counts": COUNTS, "rater_progress": progress,
        "per_rater": results, "interrater": pairs,
        "label_hashes": hashes, "sample_sha256": sample_hash}
    save(path, result)
    record(f"human_quality_{sample}_{key}", {"status": "complete",
        "human_label_version": hashes, "sample_size": len(items),
        "analysis": "stratified weighting and bootstrap, blinded outcome joins after labeling",
        "result": result, "artifact_paths": [str(path)],
        "artifact_hashes": {path.name: digest(path)}})
    print(json.dumps({"report": str(path), "sample": sample,
        "poor_rate": {r: results[r]["poor_geolocatability_0_or_1"]["stratified_population_estimate"]
                      for r in raters}}, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", choices=["full", "quick20"], default="quick20")
    parser.add_argument("--raters", nargs="+", default=["reviewer1"])
    args = parser.parse_args()
    run(args.sample, args.raters)
