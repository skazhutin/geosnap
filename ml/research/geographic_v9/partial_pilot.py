"""Seal and describe a stopped, incomplete blind annotation pilot without prevalence claims."""
from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v9.analyze_annotations import load
from ml.research.geographic_v9.common import OUT, record
from ml.research.geographic_v9.quality_benchmark import percentile_risk


def run(rater="reviewer1"):
    items, rating_sets, hashes, sample_hash = load("quick20", [rater])
    ratings = rating_sets[rater]
    if len(ratings) == 0 or len(ratings) == len(items):
        raise RuntimeError("Partial-pilot analysis requires a stopped, incomplete labeled prefix")
    order = sorted((row["token"] for row in items),
                   key=lambda token: hashlib.sha256(f"v9-{rater}-{token}".encode()).digest())
    if set(ratings) != set(order[:len(ratings)]):
        raise RuntimeError("Ratings are not the randomized prefix of the blind pilot")
    labeled = [row for row in items if row["token"] in ratings]
    with np.load(OUT / "technical_quality/image_features.npz", allow_pickle=False) as z:
        ids, matrix, names = z["query_ids"].tolist(), z["features"].copy(), z["feature_names"].tolist()
    lookup = {identity: i for i, identity in enumerate(ids)}
    x = matrix[[lookup[row["query_id"]] for row in labeled]]
    risk = percentile_risk(x, matrix, names)
    by_bucket = defaultdict(lambda: {"count": 0, "poor_0_or_1": 0,
                                     "good_3_or_4": 0, "retake_yes": 0})
    rows = []
    for row, technical_risk in zip(labeled, risk, strict=True):
        label = ratings[row["token"]]
        bucket = row["hidden_bucket"]
        summary = by_bucket[bucket]
        summary["count"] += 1
        summary["poor_0_or_1"] += label["geolocatability"] <= 1
        summary["good_3_or_4"] += label["geolocatability"] >= 3
        summary["retake_yes"] += bool(label["would_request_another_photo"])
        rows.append({"opaque_token": row["token"], "hidden_bucket": bucket,
            "human_geolocatability": label["geolocatability"],
            "human_retake": label["would_request_another_photo"],
            "human_reasons": label["reasons"], "human_positive_evidence": label["positive_evidence"],
            "technical_tail_risk": float(technical_risk)})
    report = {"status": "stopped_partial_pilot", "rated": len(ratings),
        "planned": len(items), "rater": rater, "single_rater": True,
        "sampling_note": "The labeled images are the first randomized-order prefix; stopping was voluntary and the subset was not separately preregistered.",
        "interpretation_limit": "Descriptive counts only. No prevalence estimate, performance claim, classifier training, inter-rater agreement, or causal attribution from this partial pilot.",
        "rating_counts": dict(Counter(ratings[row["token"]]["geolocatability"] for row in labeled)),
        "retake_yes": int(sum(ratings[row["token"]]["would_request_another_photo"] for row in labeled)),
        "failure_reason_counts": dict(Counter(reason for row in labeled
            for reason in ratings[row["token"]]["reasons"])),
        "by_hidden_baseline_bucket": dict(by_bucket),
        "per_image_private_audit": rows,
        "label_log_sha256": hashes[rater], "sample_sha256": sample_hash,
        "technical_feature_sha256": digest(OUT / "technical_quality/image_features.npz")}
    key = hashes[rater][:12]
    path = OUT / "annotations" / f"partial_pilot_{len(ratings)}_{key}.json"
    if path.exists():
        raise FileExistsError("This exact partial label version was already sealed")
    save(path, report)
    record(f"human_quality_partial_pilot_{len(ratings)}_{key}", {"status": "partial_pilot_only",
        "fitted": False, "human_label_version": hashes[rater],
        "sample_size": len(ratings), "result": {k: v for k, v in report.items()
            if k != "per_image_private_audit"},
        "artifact_paths": [str(path)], "artifact_hashes": {path.name: digest(path)}})
    print(json.dumps({"report": str(path), "rated": len(ratings),
        "rating_counts": report["rating_counts"],
        "by_bucket": report["by_hidden_baseline_bucket"]}, ensure_ascii=False))


if __name__ == "__main__":
    run()
