"""Outcome join and descriptive analysis, callable only after the blind freeze."""
from __future__ import annotations

import io
import json
import math
from collections import Counter

import numpy as np
import pandas as pd
from scipy.stats import rankdata, spearmanr

from .common import OUT, QUERY, ROOT, json_once, now, sha256, verify_production, write_once

BASELINE = ROOT / "data/evaluation/geographic_v8_20260928/baseline/predictions.npz"
BUCKETS = ("correct", "ranking_failure", "retrieval_failure", "no_coverage")
DEFECTS = ("motion_blur", "out_of_focus", "too_dark", "overexposed", "obstructed_view",
           "dirty_or_glare_through_glass", "orientation_problem", "close_surface",
           "mostly_ground", "mostly_sky", "vegetation_dominated", "generic_repetitive_scene")
TECHNICAL = ("laplacian_variance", "tenengrad", "frequency_high_band_ratio",
             "mean_luminance", "dark_fraction", "clipped_bright_fraction",
             "p95_minus_p5_luminance", "canny_edge_density", "luminance_entropy_bits")
BANDS = ((0.0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.000001))


def safe_rho(x, y) -> float | None:
    x, y = np.asarray(x, float), np.asarray(y, float)
    if len(x) < 3 or len(np.unique(x)) < 2 or len(np.unique(y)) < 2:
        return None
    result = spearmanr(x, y)
    return float(result.statistic) if math.isfinite(result.statistic) else None


def rate(mask, truth) -> dict:
    n = int(np.sum(mask))
    k = int(np.sum(mask & truth))
    if n:
        z = 1.959963984540054
        p = k / n
        center = (p + z*z/(2*n)) / (1 + z*z/n)
        half = z * math.sqrt(p*(1-p)/n + z*z/(4*n*n)) / (1 + z*z/n)
        lo, hi = center-half, center+half
    else:
        lo = hi = None
    return {"n": n, "positive": k, "rate": k / n if n else None,
            "wilson_95": [float(lo), float(hi)] if n else None}


def summary(frame: pd.DataFrame) -> dict:
    valid = frame[frame.annotation_status == "valid"].copy()
    for column in ("baseline_correct", "top100_positive", "annotation_uncertain", "high_consensus",
                   "usable_single_photo_unanimous", "retake_recommended_unanimous", "retake_recommended"):
        valid[column] = valid[column].astype(bool)
    value = valid.geolocatability_median.to_numpy(float)
    correct = valid.baseline_correct.to_numpy(bool)
    positive = valid.top100_positive.to_numpy(bool)
    result = {
        "n_total": len(frame), "n_valid": len(valid), "n_failed": len(frame) - len(valid),
        "failure_type_counts": {str(k): int(v) for k, v in
                                Counter(frame.get("failure_type", pd.Series(index=frame.index,
                                                                           dtype="object"))[
                                    frame.annotation_status == "failure"].dropna()).items()},
        "baseline_correct_all": int(frame.baseline_correct.sum()),
        "top100_positive_all": int(frame.top100_positive.sum()),
        "geolocatability_mean": float(value.mean()) if len(value) else None,
        "geolocatability_median": float(np.median(value)) if len(value) else None,
        "annotation_uncertain": int(valid.annotation_uncertain.sum()),
        "high_consensus": int(valid.high_consensus.sum()),
        "geolocatability_range_median": float(valid.geolocatability_range.median()),
        "boolean_disagreement": {key: int((~valid[f"{key}_unanimous"]).sum())
                                 for key in ("usable_single_photo", "retake_recommended")},
        "retake_reason_no_majority": int((valid.retake_recommended & valid.retake_reason_consensus.isna()).sum()),
        "retake_reason_consensus_counts": {str(k): int(v) for k, v in
                                           Counter(valid.retake_reason_consensus.dropna()).items()},
        "dimension_disagreement": {key: {"median_range": float(valid[f"{key}_range"].median()),
                                           "range_ge_0_30": int((valid[f"{key}_range"] >= .30).sum())}
                                   for key in ("technical_quality", "geolocatability", "motion_blur",
                                               "obstructed_view", "stable_landmarks_visible",
                                               "sufficient_scene_context")},
        "cross_cases": {
            "high_technical_low_geolocatability": int(((valid.technical_quality_median >= .75) &
                (valid.geolocatability_median <= .4)).sum()),
            "low_technical_high_geolocatability": int(((valid.technical_quality_median <= .4) &
                (valid.geolocatability_median >= .6)).sum()),
            "high_geolocatability_retrieval_failure": int(((valid.geolocatability_median >= .6) &
                (valid.failure_bucket == "retrieval_failure")).sum()),
            "low_geolocatability_baseline_correct": int(((valid.geolocatability_median <= .4) &
                (valid.failure_bucket == "correct")).sum()),
        },
    }
    result["bands"] = []
    for low, high in BANDS:
        mask = (value >= low) & (value < high)
        result["bands"].append({"band": f"{low:.1f}–{min(high, 1.0):.1f}",
            "baseline_correct": rate(mask, correct), "retrieval_r100": rate(mask, positive),
            "ranking_given_positive": rate(mask & positive, correct)})
    result["buckets"] = {}
    for bucket in BUCKETS:
        subset = valid[valid.failure_bucket == bucket]
        result["buckets"][bucket] = {
            "n": len(subset), "mean_geolocatability": float(subset.geolocatability_median.mean()) if len(subset) else None,
            "median_geolocatability": float(subset.geolocatability_median.median()) if len(subset) else None,
            "retake_recommended": int(subset.retake_recommended.sum()),
            "low_geolocatability_le_0_4": int((subset.geolocatability_median <= 0.4).sum()),
        }
    result["defects"] = {}
    for defect in DEFECTS + ("no_stable_landmarks", "insufficient_scene_context"):
        score = ((1 - valid.stable_landmarks_visible_median) if defect == "no_stable_landmarks" else
                 (1 - valid.sufficient_scene_context_median) if defect == "insufficient_scene_context" else
                 valid[f"{defect}_median"])
        result["defects"][defect] = {
            bucket: {"n": int((valid.failure_bucket == bucket).sum()),
                     "present_at_0_5": int(((valid.failure_bucket == bucket) & (score >= 0.5)).sum()),
                     "mean_score": float(score[valid.failure_bucket == bucket].mean())}
            for bucket in BUCKETS}
    result["associations"] = {
        "geolocatability_vs_correct_spearman": safe_rho(value, correct),
        "geolocatability_vs_r100_spearman": safe_rho(value, positive),
        "vlm_technical_quality_vs_correct_spearman": safe_rho(valid.technical_quality_median, correct),
        "vlm_technical_quality_vs_r100_spearman": safe_rho(valid.technical_quality_median, positive),
        "classical_feature_spearman": {key: {"correct": safe_rho(valid[key], correct),
                                            "r100": safe_rho(valid[key], positive)} for key in TECHNICAL},
    }
    # Remove variation in semantic score predictable from classical diagnostics
    # without fitting on any outcome. Correlate the residual only after this step.
    x = np.column_stack([rankdata(valid[k].to_numpy(float)) for k in TECHNICAL])
    x = (x - x.mean(axis=0)) / np.maximum(x.std(axis=0), 1e-9)
    x = np.column_stack([np.ones(len(x)), x])
    y = rankdata(value)
    residual = y - x @ np.linalg.lstsq(x, y, rcond=None)[0]
    result["associations"]["semantic_rank_residual_after_classical_vs_correct_spearman"] = safe_rho(residual, correct)
    result["associations"]["semantic_rank_residual_after_classical_vs_r100_spearman"] = safe_rho(residual, positive)
    result["associations"]["residual_method"] = "Rank-regress semantic score on nine classical diagnostics without outcome labels; Spearman correlate residual with outcome. Exploratory association, not incremental deployable accuracy."
    high = valid[valid.high_consensus]
    result["high_consensus_subset"] = {
        "n": len(high), "correct": int(high.baseline_correct.sum()),
        "r100": int(high.top100_positive.sum()),
        "bands": [{"band": f"{low:.1f}–{min(high_edge, 1.0):.1f}",
                   "correct": rate(((high.geolocatability_median >= low) &
                                    (high.geolocatability_median < high_edge)).to_numpy(bool),
                                   high.baseline_correct.to_numpy(bool)),
                   "r100": rate(((high.geolocatability_median >= low) &
                                 (high.geolocatability_median < high_edge)).to_numpy(bool),
                                high.top100_positive.to_numpy(bool))}
                  for low, high_edge in BANDS],
        "semantic_vs_correct_spearman": safe_rho(high.geolocatability_median, high.baseline_correct),
        "semantic_vs_r100_spearman": safe_rho(high.geolocatability_median, high.top100_positive),
    }
    return result


def run() -> None:
    frozen = json.loads((OUT / "annotation_freeze.json").read_text())
    canonical = OUT / "geolocatability_annotations.jsonl"
    if sha256(canonical) != frozen["canonical_jsonl_sha256"]:
        raise RuntimeError("Canonical blind annotations changed after freeze")
    guard = verify_production()
    annotations = pd.read_parquet(OUT / "geolocatability_annotations.parquet")
    with np.load(BASELINE, allow_pickle=False) as z:
        ids = z["query_ids"].tolist()
        baseline = pd.DataFrame({"query_id": ids, "failure_bucket": z["buckets"],
                                 "localization_error_m": z["errors_m"],
                                 "baseline_retrieval_top1_score": z["scores"][:, 0]})
    baseline["baseline_correct"] = baseline.localization_error_m <= 100
    baseline["top100_positive"] = baseline.failure_bucket.isin(["correct", "ranking_failure"])
    if (len(baseline) != 1184 or baseline.query_id.nunique() != 1184 or
        int(baseline.baseline_correct.sum()) != 411 or int(baseline.top100_positive.sum()) != 613 or
        dict(Counter(baseline.failure_bucket)) != {"correct": 411, "ranking_failure": 202,
            "retrieval_failure": 309, "no_coverage": 262}):
        raise RuntimeError("Unexpected frozen GeoSnap outcome data")
    joined = annotations.merge(baseline, on="query_id", validate="one_to_one", how="left")
    if len(joined) != 1184 or joined.failure_bucket.isna().any():
        raise RuntimeError("Outcome join incomplete")
    buffer = io.BytesIO()
    joined.to_parquet(buffer, index=False)
    write_once(OUT / "analysis_outcome_join.parquet", buffer.getvalue())
    metrics = summary(joined)
    json_once(OUT / "analysis_metrics.json", metrics)
    json_once(OUT / "analysis_receipt.json", {
        "created_at": now(), "annotation_freeze_sha256": sha256(OUT / "annotation_freeze.json"),
        "canonical_annotation_sha256": frozen["canonical_jsonl_sha256"],
        "baseline_predictions_sha256": sha256(BASELINE), "query_manifest_sha256": sha256(QUERY),
        "outcome_join_sha256": sha256(OUT / "analysis_outcome_join.parquet"),
        "analysis_metrics_sha256": sha256(OUT / "analysis_metrics.json"),
        "outcome_join_only_after_annotation_freeze": True, "production_guard": guard,
    })
    print(json.dumps({"valid": metrics["n_valid"], "failed": metrics["n_failed"],
                      "analysis_metrics": str(OUT / "analysis_metrics.json")}))


if __name__ == "__main__":
    run()
