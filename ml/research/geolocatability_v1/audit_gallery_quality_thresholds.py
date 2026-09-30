"""Compare prespecified SigLIP2 cutoffs after manual, outcome-blind review.

The Qwen consensus set is held constant. This diagnostic reads cached SAGE scores
and query GT only after the 20-image visual-review artifact has been frozen.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from .analyze_filtered_gallery_v1 import EARTH_M, distance_m, exact_top100
from .build_filtered_gallery_v2 import checked_sources
from .common import json_once, revision, sha256, verify_production
from .scan_siglip2_gallery import EXPECTED_GALLERY_SHA256, GALLERY

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/gallery_quality_threshold_audit_v1_20260929"
STAGED = ROOT / "data/evaluation/geographic_v8_20260928/staged"
BASELINE = ROOT / "data/evaluation/geographic_v8_20260928/baseline/predictions.npz"
THRESHOLDS = (0.10, 0.15, 0.20)


def main() -> None:
    before = verify_production()
    manual_file = OUT / "manual_review_20.jsonl"
    manual = list(map(json.loads, manual_file.open()))
    if (len(manual) != 20 or len({r["id"] for r in manual}) != 20 or
            not all(r["outcome_blind"] for r in manual)):
        raise RuntimeError("Manual review is absent or not outcome-blind")
    _, _, _, scores_by_id, qwen_extreme, qwen100_severe = checked_sources()
    qwen_ids = qwen_extreme | qwen100_severe
    gallery = pd.read_parquet(GALLERY, columns=["id", "source", "lat", "lon"])
    query = pd.read_parquet(STAGED / "development.parquet", columns=["id", "lat", "lon"])
    with np.load(BASELINE) as baseline:
        query_ids = baseline["query_ids"].copy()
        gallery_ids = baseline["gallery_ids"].copy()
        old_ranked = baseline["indices"].copy()
        old_buckets = baseline["buckets"].copy()
    if (len(gallery) != 112163 or len(query) != 1184 or
            gallery.id.tolist() != gallery_ids.tolist() or
            query.id.tolist() != query_ids.tolist()):
        raise RuntimeError("Original gallery/query population changed")
    siglip = np.array([scores_by_id[i] for i in gallery.id], dtype=np.float32)
    qwen = gallery.id.isin(qwen_ids).to_numpy()
    qcoords = query[["lat", "lon"]].to_numpy()
    gcoords = gallery[["lat", "lon"]].to_numpy()
    radius = 100 / EARTH_M
    old_coverage = BallTree(np.radians(gcoords), metric="haversine").query_radius(
        np.radians(qcoords), r=radius, count_only=True) > 0
    old_positive = (distance_m(qcoords, gcoords[old_ranked]) <= 100).any(axis=1)
    if int(old_coverage.sum()) != 922 or int(old_positive.sum()) != 613:
        raise RuntimeError("Original fixed-gallery metrics not reproduced")
    scores_file = STAGED / "mean_scores.npy"
    expected_score_hash = json.loads((STAGED / "mean_scores.npy.sha256.json").read_text())["sha256"]
    if sha256(scores_file) != expected_score_hash:
        raise RuntimeError("Cached exact SAGE scores changed")
    exact_scores = np.load(scores_file, mmap_mode="r", allow_pickle=False)
    if exact_scores.shape != (1184, 112163):
        raise RuntimeError("Wrong cached score shape")
    all_rows = np.arange(len(gallery))

    variants = [("qwen_consensus_only", None, qwen)]
    variants += [(f"qwen_or_siglip_lt_{threshold:.2f}", threshold,
                  qwen | (siglip < threshold)) for threshold in THRESHOLDS]
    results = []
    for name, threshold, excluded in variants:
        kept = np.flatnonzero(~excluded)
        coverage = BallTree(np.radians(gcoords[kept]), metric="haversine").query_radius(
            np.radians(qcoords), r=radius, count_only=True) > 0
        ranked = np.empty((len(query), 100), dtype=np.int64)
        for i in range(len(query)):
            if name == "qwen_consensus_only" and set(exact_top100(exact_scores[i], all_rows)) != set(old_ranked[i]):
                raise RuntimeError(f"Original cached top100 differs at query {i}")
            ranked[i] = exact_top100(exact_scores[i], kept)
        positive = (distance_m(qcoords, gcoords[ranked]) <= 100).any(axis=1)
        old_top1_removed = excluded[old_ranked[:, 0]]
        results.append({
            "variant": name,
            "siglip2_threshold_exclusive": threshold,
            "excluded": int(excluded.sum()),
            "retained": int((~excluded).sum()),
            "additional_excluded_vs_threshold_0_10": None,
            "excluded_by_source": dict(Counter(gallery.source[excluded])),
            "coverage_100m": int(coverage.sum()),
            "coverage_lost_vs_fixed": int((old_coverage & ~coverage).sum()),
            "top100_oracle": int(positive.sum()),
            "top100_oracle_lost_vs_fixed": int((old_positive & ~positive).sum()),
            "top100_oracle_gained_vs_fixed": int((~old_positive & positive).sum()),
            "old_baseline_top1_removed": int(old_top1_removed.sum()),
            "old_correct_top1_removed": int((old_top1_removed & (old_buckets == "correct")).sum()),
        })
    standard = next(r for r in results if r["siglip2_threshold_exclusive"] == 0.10)
    for row in results:
        row["additional_excluded_vs_threshold_0_10"] = row["excluded"] - standard["excluded"]
    v2 = json.loads((ROOT / "data/evaluation/gallery_quality_filter_v2_20260929/postfreeze_impact.json").read_text())
    if (standard["excluded"] != 1127 or standard["coverage_100m"] != v2["coverage_100m_after"] or
            standard["top100_oracle"] != v2["top100_oracle_after_exact_cached_score_retrieval"]):
        raise RuntimeError("Threshold 0.10 does not reproduce frozen v2 audit")
    after = verify_production()
    if before["guard_sha256"] != after["guard_sha256"] or sha256(GALLERY) != EXPECTED_GALLERY_SHA256:
        raise RuntimeError("Production or fixed gallery changed")
    result = {
        "status": "exploratory_reused_development_set",
        "thresholds_fixed_before_impact_analysis": list(THRESHOLDS),
        "manual_review_20_sha256": sha256(manual_file),
        "siglip2_scores_sha256": sha256(ROOT / "data/evaluation/geolocatability_siglip2_gallery_v1_20260929/gallery_scores.jsonl"),
        "qwen_consensus_count": int(qwen.sum()),
        "gallery_sha256": EXPECTED_GALLERY_SHA256,
        "cached_scores_sha256": expected_score_hash,
        "baseline_predictions_sha256": sha256(BASELINE),
        "original_coverage_100m": int(old_coverage.sum()),
        "original_top100_oracle": int(old_positive.sum()),
        "variants": results,
        "not_full_context_rerank_or_raw_accuracy": True,
        "code_revision": revision(),
        "production_guard_files": after["guarded_files"],
        "production_guard_sha256": after["guard_sha256"],
    }
    result_file = OUT / "threshold_impact.json"
    if result_file.exists():
        if json.loads(result_file.read_text()) != result:
            raise RuntimeError("Frozen threshold audit differs from current inputs")
    else:
        json_once(result_file, result)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
