"""Post-freeze coverage and cached-retrieval audit of gallery filter v2.

This reads query outcomes only after the image-only filter receipt exists. It does
not run a localization model or claim context-reranked RAW accuracy.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from .common import json_once, revision, sha256, verify_production
from .analyze_filtered_gallery_v1 import distance_m, exact_top100, EARTH_M
from .scan_siglip2_gallery import EXPECTED_GALLERY_SHA256, GALLERY

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/gallery_quality_filter_v2_20260929"
STAGED = ROOT / "data/evaluation/geographic_v8_20260928/staged"
BASELINE = ROOT / "data/evaluation/geographic_v8_20260928/baseline/predictions.npz"


def main() -> None:
    policy = json.loads((OUT / "filter_receipt.json").read_text())
    if (sha256(GALLERY) != EXPECTED_GALLERY_SHA256 or
            policy["gallery_manifest_sha256"] != EXPECTED_GALLERY_SHA256 or
            sha256(OUT / "gallery_filtered.parquet") != policy["filtered_gallery_sha256"]):
        raise RuntimeError("Frozen gallery/filter changed")
    scores_file = STAGED / "mean_scores.npy"
    score_hash = json.loads((STAGED / "mean_scores.npy.sha256.json").read_text())["sha256"]
    if sha256(scores_file) != score_hash:
        raise RuntimeError("Cached SAGE scores changed")
    original = pd.read_parquet(GALLERY, columns=["id", "lat", "lon"])
    filtered = pd.read_parquet(OUT / "gallery_filtered.parquet", columns=["id", "lat", "lon"])
    query = pd.read_parquet(STAGED / "development.parquet", columns=["id", "lat", "lon"])
    with np.load(BASELINE) as baseline:
        query_ids = baseline["query_ids"].copy()
        gallery_ids = baseline["gallery_ids"].copy()
        old_ranked = baseline["indices"].copy()
        old_buckets = baseline["buckets"].copy()
    if (query.id.tolist() != query_ids.tolist() or
            original.id.tolist() != gallery_ids.tolist() or
            len(query) != 1184 or len(original) != 112163 or
            len(filtered) != policy["filtered_gallery_count"]):
        raise RuntimeError("Baseline/gallery identity mismatch")
    keep_mask = original.id.isin(set(filtered.id)).to_numpy()
    kept_rows = np.flatnonzero(keep_mask)
    with (OUT / "retained_original_rows.npy").open("rb") as stream:
        if not np.array_equal(kept_rows, np.load(stream, allow_pickle=False)):
            raise RuntimeError("Frozen row mask differs from gallery")
    qcoords = query[["lat", "lon"]].to_numpy()
    gcoords = original[["lat", "lon"]].to_numpy()
    radius = 100 / EARTH_M
    old_coverage = BallTree(np.radians(gcoords), metric="haversine").query_radius(
        np.radians(qcoords), r=radius, count_only=True) > 0
    new_coverage = BallTree(np.radians(gcoords[keep_mask]), metric="haversine").query_radius(
        np.radians(qcoords), r=radius, count_only=True) > 0
    if int(old_coverage.sum()) != 922:
        raise RuntimeError("Original coverage not reproduced")
    old_positive = (distance_m(qcoords, gcoords[old_ranked]) <= 100).any(axis=1)
    if int(old_positive.sum()) != 613:
        raise RuntimeError("Original top100 recall not reproduced")
    scores = np.load(scores_file, mmap_mode="r", allow_pickle=False)
    if scores.shape != (1184, 112163):
        raise RuntimeError("Cached score matrix shape changed")
    all_rows = np.arange(len(original))
    new_ranked = np.empty((len(query), 100), dtype=np.int64)
    for i in range(len(query)):
        if set(exact_top100(scores[i], all_rows)) != set(old_ranked[i]):
            raise RuntimeError(f"Original top100 set mismatch: {i}")
        new_ranked[i] = exact_top100(scores[i], kept_rows)
    new_positive = (distance_m(qcoords, gcoords[new_ranked]) <= 100).any(axis=1)
    old_top1_removed = ~keep_mask[old_ranked[:, 0]]
    result = {
        "policy_receipt_sha256": sha256(OUT / "filter_receipt.json"),
        "selection_was_frozen_before_outcomes": True,
        "query_count": len(query),
        "coverage_100m_before": int(old_coverage.sum()),
        "coverage_100m_after": int(new_coverage.sum()),
        "coverage_100m_lost": int((old_coverage & ~new_coverage).sum()),
        "baseline_top1_reference_excluded": int(old_top1_removed.sum()),
        "baseline_correct_top1_reference_excluded": int((old_top1_removed & (old_buckets == "correct")).sum()),
        "queries_with_original_top100_reference_excluded": int((~keep_mask[old_ranked]).any(axis=1).sum()),
        "top100_oracle_before": int(old_positive.sum()),
        "top100_oracle_after_exact_cached_score_retrieval": int(new_positive.sum()),
        "top100_oracle_lost_queries": int((old_positive & ~new_positive).sum()),
        "top100_oracle_gained_queries": int((~old_positive & new_positive).sum()),
        "scores_sha256": score_hash,
        "baseline_predictions_sha256": sha256(BASELINE),
        "code_revision": revision(),
        "not_a_full_context_rerank_replay": True,
        "production_guard": verify_production(),
    }
    result_path = OUT / "postfreeze_impact.json"
    if result_path.exists():
        existing = json.loads(result_path.read_text())
        previous = json.loads(json.dumps(existing))
        current = json.loads(json.dumps(result))
        previous["production_guard"].pop("checked_at", None)
        current["production_guard"].pop("checked_at", None)
        if previous != current:
            raise RuntimeError("Existing post-freeze impact receipt differs from current inputs")
        result = existing
    else:
        json_once(result_path, result)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
