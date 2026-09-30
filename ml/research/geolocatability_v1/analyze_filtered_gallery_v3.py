"""Post-freeze cached-SAGE impact of the corrected v3 image-quality gallery."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from .analyze_filtered_gallery_v1 import EARTH_M, distance_m, exact_top100
from .common import json_once, revision, sha256, verify_production
from .scan_siglip2_gallery import EXPECTED_GALLERY_SHA256, GALLERY

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/gallery_quality_filter_v3_20260929"
V2 = ROOT / "data/evaluation/gallery_quality_filter_v2_20260929"
STAGED = ROOT / "data/evaluation/geographic_v8_20260928/staged"
BASELINE = ROOT / "data/evaluation/geographic_v8_20260928/baseline/predictions.npz"


def main() -> None:
    receipt = json.loads((OUT / "filter_receipt.json").read_text())
    if (sha256(GALLERY) != EXPECTED_GALLERY_SHA256 or
            sha256(OUT / "gallery_filtered.parquet") != receipt["filtered_gallery_sha256"] or
            sha256(OUT / "retained_original_rows.npy") != receipt["retained_original_rows_sha256"]):
        raise RuntimeError("Frozen gallery or v3 filter changed")
    original = pd.read_parquet(GALLERY, columns=["id", "lat", "lon"])
    filtered = pd.read_parquet(OUT / "gallery_filtered.parquet", columns=["id"])
    query = pd.read_parquet(STAGED / "development.parquet", columns=["id", "lat", "lon"])
    with np.load(BASELINE) as baseline:
        query_ids = baseline["query_ids"].copy()
        gallery_ids = baseline["gallery_ids"].copy()
        old_ranked = baseline["indices"].copy()
        old_buckets = baseline["buckets"].copy()
    if (len(original) != 112163 or len(filtered) != 111032 or len(query) != 1184 or
            original.id.tolist() != gallery_ids.tolist() or
            query.id.tolist() != query_ids.tolist()):
        raise RuntimeError("Baseline identity or filtered population changed")
    keep = original.id.isin(set(filtered.id)).to_numpy()
    retained = np.flatnonzero(keep)
    with (OUT / "retained_original_rows.npy").open("rb") as stream:
        if not np.array_equal(retained, np.load(stream, allow_pickle=False)):
            raise RuntimeError("Frozen retained-row index differs")
    qcoords = query[["lat", "lon"]].to_numpy()
    gcoords = original[["lat", "lon"]].to_numpy()
    old_coverage = BallTree(np.radians(gcoords), metric="haversine").query_radius(
        np.radians(qcoords), r=100 / EARTH_M, count_only=True) > 0
    new_coverage = BallTree(np.radians(gcoords[keep]), metric="haversine").query_radius(
        np.radians(qcoords), r=100 / EARTH_M, count_only=True) > 0
    old_positive = (distance_m(qcoords, gcoords[old_ranked]) <= 100).any(axis=1)
    if int(old_coverage.sum()) != 922 or int(old_positive.sum()) != 613:
        raise RuntimeError("Fixed-gallery reference metrics not reproduced")
    scores_file = STAGED / "mean_scores.npy"
    scores_hash = json.loads((STAGED / "mean_scores.npy.sha256.json").read_text())["sha256"]
    if sha256(scores_file) != scores_hash:
        raise RuntimeError("Cached exact SAGE scores changed")
    scores = np.load(scores_file, mmap_mode="r", allow_pickle=False)
    if scores.shape != (1184, 112163):
        raise RuntimeError("Wrong score-matrix shape")
    ranked = np.empty((len(query), 100), dtype=np.int64)
    for i in range(len(query)):
        ranked[i] = exact_top100(scores[i], retained)
    new_positive = (distance_m(qcoords, gcoords[ranked]) <= 100).any(axis=1)
    old_top1_removed = ~keep[old_ranked[:, 0]]
    v2 = json.loads((V2 / "postfreeze_impact.json").read_text())
    result = {
        "policy_receipt_sha256": sha256(OUT / "filter_receipt.json"),
        "selection_frozen_before_outcomes": True,
        "query_count": 1184,
        "coverage_100m_before": int(old_coverage.sum()),
        "coverage_100m_after": int(new_coverage.sum()),
        "coverage_lost_vs_fixed": int((old_coverage & ~new_coverage).sum()),
        "coverage_delta_vs_v2": int(new_coverage.sum()) - v2["coverage_100m_after"],
        "top100_oracle_before": int(old_positive.sum()),
        "top100_oracle_after": int(new_positive.sum()),
        "top100_oracle_lost_vs_fixed": int((old_positive & ~new_positive).sum()),
        "top100_oracle_gained_vs_fixed": int((~old_positive & new_positive).sum()),
        "top100_oracle_delta_vs_v2": int(new_positive.sum()) - v2["top100_oracle_after_exact_cached_score_retrieval"],
        "old_baseline_top1_reference_excluded": int(old_top1_removed.sum()),
        "old_correct_top1_reference_excluded": int((old_top1_removed & (old_buckets == "correct")).sum()),
        "scores_sha256": scores_hash,
        "baseline_predictions_sha256": sha256(BASELINE),
        "v2_impact_sha256": sha256(V2 / "postfreeze_impact.json"),
        "not_full_context_rerank_or_raw_accuracy": True,
        "code_revision": revision(),
        "production_guard_sha256": verify_production()["guard_sha256"],
    }
    path = OUT / "postfreeze_impact.json"
    if path.exists():
        if json.loads(path.read_text()) != result:
            raise RuntimeError("Frozen v3 impact differs from current inputs")
    else:
        json_once(path, result)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
