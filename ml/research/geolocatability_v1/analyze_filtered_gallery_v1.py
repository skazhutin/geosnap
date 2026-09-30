"""Post-freeze gallery-filter diagnostics using cached SAGE scores; no model inference."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from .common import json_once, revision, sha256, verify_production
from .scan_siglip2_gallery import EXPECTED_GALLERY_SHA256, GALLERY

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/gallery_quality_filter_v1_20260929"
STAGED = ROOT / "data/evaluation/geographic_v8_20260928/staged"
BASELINE = ROOT / "data/evaluation/geographic_v8_20260928/baseline/predictions.npz"
EARTH_M = 6371000.0


def distance_m(query: np.ndarray, gallery: np.ndarray) -> np.ndarray:
    a = np.radians(query)[:, None, :]
    b = np.radians(gallery)
    dlat = b[:, :, 0] - a[:, :, 0]
    dlon = b[:, :, 1] - a[:, :, 1]
    h = np.sin(dlat / 2) ** 2 + np.cos(a[:, :, 0]) * np.cos(b[:, :, 0]) * np.sin(dlon / 2) ** 2
    return EARTH_M * 2 * np.arcsin(np.sqrt(np.clip(h, 0, 1)))


def exact_top100(row: np.ndarray, candidate_rows: np.ndarray) -> np.ndarray:
    values = row[candidate_rows]
    if not np.isfinite(values).all():
        raise RuntimeError("Nonfinite cached SAGE scores")
    rough = np.argpartition(values, -100)[-100:]
    cutoff = values[rough].min()
    tied = np.flatnonzero(values >= cutoff)
    order = np.lexsort((candidate_rows[tied], -values[tied]))
    return candidate_rows[tied[order[:100]]]


def main() -> None:
    policy = json.loads((OUT / "filter_receipt.json").read_text())
    if sha256(GALLERY) != EXPECTED_GALLERY_SHA256 or policy["gallery_manifest_sha256"] != EXPECTED_GALLERY_SHA256:
        raise RuntimeError("Original gallery changed")
    if sha256(OUT / "gallery_filtered.parquet") != policy["filtered_gallery_sha256"]:
        raise RuntimeError("Frozen filtered gallery changed")
    scores_file = STAGED / "mean_scores.npy"
    expected_score_sha = json.loads((STAGED / "mean_scores.npy.sha256.json").read_text())["sha256"]
    if sha256(scores_file) != expected_score_sha:
        raise RuntimeError("Cached baseline scores changed")
    original = pd.read_parquet(GALLERY, columns=["id", "lat", "lon"])
    filtered = pd.read_parquet(OUT / "gallery_filtered.parquet", columns=["id", "lat", "lon"])
    query = pd.read_parquet(STAGED / "development.parquet", columns=["id", "lat", "lon"])
    with np.load(BASELINE) as baseline:
        query_ids = baseline["query_ids"].copy()
        gallery_ids = baseline["gallery_ids"].copy()
        old_ranked = baseline["indices"].copy()
        old_buckets = baseline["buckets"].copy()
    if query.id.tolist() != query_ids.tolist() or original.id.tolist() != gallery_ids.tolist():
        raise RuntimeError("Baseline IDs do not align with staged manifests")
    assert len(query) == 1184 and len(original) == 112163 and len(filtered) == 111763
    kept_ids = set(filtered.id)
    keep_mask = original.id.isin(kept_ids).to_numpy()
    kept_rows = np.flatnonzero(keep_mask)
    assert len(kept_rows) == len(filtered)
    qcoords = query[["lat", "lon"]].to_numpy()
    gcoords = original[["lat", "lon"]].to_numpy()
    radius_rad = 100 / EARTH_M
    old_tree = BallTree(np.radians(gcoords), metric="haversine")
    new_tree = BallTree(np.radians(gcoords[keep_mask]), metric="haversine")
    old_coverage = old_tree.query_radius(np.radians(qcoords), r=radius_rad, count_only=True) > 0
    new_coverage = new_tree.query_radius(np.radians(qcoords), r=radius_rad, count_only=True) > 0
    if int(old_coverage.sum()) != 922:
        raise RuntimeError("Original 100 m coverage not reproduced")
    top1_removed = ~keep_mask[old_ranked[:, 0]]
    original_positive = (distance_m(qcoords, gcoords[old_ranked]) <= 100).any(axis=1)
    if int(original_positive.sum()) != 613:
        raise RuntimeError("Original top100 oracle not reproduced")
    scores = np.load(scores_file, mmap_mode="r")
    if scores.shape != (1184, 112163):
        raise RuntimeError("Cached SAGE score matrix shape changed")
    all_rows = np.arange(len(original))
    new_ranked = np.empty((len(query), 100), dtype=np.int64)
    for i in range(len(query)):
        if set(exact_top100(scores[i], all_rows)) != set(old_ranked[i]):
            raise RuntimeError(f"Original cached top100 set mismatch at query {i}")
        new_ranked[i] = exact_top100(scores[i], kept_rows)
    new_positive = (distance_m(qcoords, gcoords[new_ranked]) <= 100).any(axis=1)
    new_r100 = int(new_positive.sum())
    affected_old = (~keep_mask[old_ranked]).any(axis=1)
    result = {"policy_receipt_sha256": sha256(OUT / "filter_receipt.json"),
              "selection_was_frozen_before_outcomes": True,
              "query_count": len(query),
              "coverage_100m_before": int(old_coverage.sum()),
              "coverage_100m_after": int(new_coverage.sum()),
              "coverage_100m_lost": int((old_coverage & ~new_coverage).sum()),
              "baseline_top1_reference_excluded": int(top1_removed.sum()),
              "baseline_correct_top1_reference_excluded": int((top1_removed & (old_buckets == "correct")).sum()),
              "queries_with_any_original_top100_reference_excluded": int(affected_old.sum()),
              "top100_oracle_before": int(original_positive.sum()),
              "top100_oracle_after_exact_cached_score_retrieval": new_r100,
              "top100_oracle_lost_queries": int((original_positive & ~new_positive).sum()),
              "top100_oracle_gained_queries": int((~original_positive & new_positive).sum()),
              "scores_sha256": expected_score_sha,
              "baseline_predictions_sha256": sha256(BASELINE),
              "code_revision": revision(),
              "not_a_full_context_rerank_replay": True,
              "production_guard": verify_production()}
    json_once(OUT / "postfreeze_impact.json", result)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
