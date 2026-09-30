"""Write-once post-freeze phone-session evaluator; no independent data exists yet."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import h3
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.independent_validation import bootstrap
from ml.research.geographic_v9.common import frames
from ml.research.vector_evaluation import distances

LOCATION = {"location_id", "initial_image_id", "lat", "lon", "gt_uncertainty_m",
    "session_id", "photographer_id", "route_block", "selection_stratum", "captured_at"}
PHOTO = {"image_id", "location_id", "image_path", "file_sha256", "capture_order",
    "is_initial", "intentionally_weak", "captured_at"}
PREDICTIONS = {"location_ids", "single_gps", "adaptive_answered", "adaptive_gps",
               "retake_requested", "photos_used", "multi_photo_gps"}


def errors(loc, gps):
    return np.asarray([distances(row.lat, row.lon, np.radians(gps[i, 0:1]),
                                 np.radians(gps[i, 1:2]))[0]
                       for i, row in enumerate(loc.itertuples())])


def metrics(error):
    return {"count": len(error), "correct_count": {str(r): int((error <= r).sum())
            for r in (25, 50, 100, 500, 1000, 5000)},
        "raw": {str(r): float((error <= r).mean()) for r in (25, 50, 100, 500, 1000, 5000)},
        "median_error_m": float(np.median(error)),
        "p90_error_m": float(np.quantile(error, .9)),
        "gt500m_rate": float((error > 500).mean())}


def run(candidate_path, locations_path, photos_path, predictions_path, receipt_path, output_path):
    paths = [Path(x) for x in (candidate_path, locations_path, photos_path,
                              predictions_path, receipt_path, output_path)]
    candidate_path, locations_path, photos_path, predictions_path, receipt_path, output_path = paths
    if output_path.exists():
        raise FileExistsError("Independent result is write-once")
    candidate = json.loads(candidate_path.read_text())
    if not candidate.get("frozen_at") or not candidate.get("artifact_hashes"):
        raise RuntimeError("Freeze the entire v9 pipeline before final data collection")
    for source, expected in candidate["artifact_hashes"].items():
        if digest(source) != expected:
            raise RuntimeError(f"Frozen pipeline input changed: {source}")
    receipt = json.loads(receipt_path.read_text())
    expected = {"candidate_sha256": digest(candidate_path),
        "locations_sha256": digest(locations_path), "photos_sha256": digest(photos_path),
        "predictions_sha256": digest(predictions_path)}
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise RuntimeError("Prediction receipt does not bind frozen pipeline and population")
    loc, photo = pd.read_parquet(locations_path), pd.read_parquet(photos_path)
    if LOCATION - set(loc) or PHOTO - set(photo) or len(loc) == 0 or len(photo) == 0:
        raise RuntimeError("Phone manifests lack required fields")
    if loc.location_id.duplicated().any() or photo.image_id.duplicated().any():
        raise RuntimeError("Duplicate location or image identity")
    if set(photo.location_id) != set(loc.location_id):
        raise RuntimeError("Each location must have photos and each photo one location")
    if pd.to_datetime(loc.captured_at, utc=True).min() <= pd.Timestamp(candidate["frozen_at"]):
        raise RuntimeError("Locations were captured before the research freeze")
    if pd.to_datetime(photo.captured_at, utc=True).min() <= pd.Timestamp(candidate["frozen_at"]):
        raise RuntimeError("Photos were captured before the research freeze")
    if loc.gt_uncertainty_m.isna().any() or (loc.gt_uncertainty_m < 0).any():
        raise RuntimeError("Ground-truth uncertainty is missing")
    if (not np.isfinite(loc[["lat", "lon"]].to_numpy(float)).all()
            or not loc.lat.between(-90, 90).all() or not loc.lon.between(-180, 180).all()):
        raise RuntimeError("Location ground truth is not valid latitude/longitude")
    if loc[["session_id", "photographer_id", "route_block"]].isna().any().any():
        raise RuntimeError("Session, photographer, or route block is missing")
    if photo.file_sha256.duplicated().any():
        raise RuntimeError("Repeated photo bytes across new location population")
    prior, gallery = frames()
    if set(photo.image_id) & (set(prior.id) | set(gallery.id)):
        raise RuntimeError("Photo identity overlaps existing data")
    if set(photo.file_sha256) & (set(prior.file_sha256) | set(gallery.file_sha256)):
        raise RuntimeError("Photo bytes overlap existing data")
    for row in photo.itertuples():
        if digest(row.image_path) != row.file_sha256:
            raise RuntimeError(f"New image bytes changed: {row.image_id}")
    count_by_location = photo.groupby("location_id").size()
    for row in loc.itertuples():
        items = photo[photo.location_id == row.location_id].sort_values("capture_order")
        if len(items) < 2 or items.is_initial.sum() != 1 or items.iloc[0].image_id != row.initial_image_id:
            raise RuntimeError("Every location needs one first natural image and another direction")
        if items.iloc[0].intentionally_weak or items.iloc[0].capture_order != 0:
            raise RuntimeError("Primary image must be the natural initial photograph")
        if items.capture_order.tolist() != list(range(len(items))):
            raise RuntimeError("Capture order is noncontiguous")
    with np.load(predictions_path, allow_pickle=False) as z:
        if PREDICTIONS - set(z.files) or z["location_ids"].tolist() != loc.location_id.tolist():
            raise RuntimeError("Prediction artifact is incomplete or order changed")
        single, adaptive, multi = (z[key].copy() for key in
            ("single_gps", "adaptive_gps", "multi_photo_gps"))
        answered, requested = z["adaptive_answered"].copy(), z["retake_requested"].copy()
        used = z["photos_used"].copy()
    n = len(loc)
    if any(v.shape != (n, 2) for v in (single, adaptive, multi)):
        raise RuntimeError("Prediction coordinate shapes are wrong")
    if answered.shape != requested.shape or answered.shape != (n,) or used.shape != (n,):
        raise RuntimeError("Adaptive policy output shapes are wrong")
    if answered.dtype != np.bool_ or requested.dtype != np.bool_ or not np.issubdtype(used.dtype, np.integer):
        raise RuntimeError("Adaptive policy decisions need boolean flags and integer photo counts")
    if not np.isfinite(single).all() or not np.isfinite(multi).all() or not np.isfinite(adaptive[answered]).all():
        raise RuntimeError("Missing required finite coordinate")
    if np.any(used < 1) or np.any(used > count_by_location.loc[loc.location_id].to_numpy()):
        raise RuntimeError("Policy claims to use an unavailable number of photos")
    single_error, multi_error = errors(loc, single), errors(loc, multi)
    adaptive_error = np.full(n, np.inf)
    if answered.any():
        adaptive_error[answered] = errors(loc.iloc[np.flatnonzero(answered)], adaptive[answered])
    reference = BallTree(np.radians(gallery[["lat", "lon"]].to_numpy()), metric="haversine")
    closest = reference.query(np.radians(loc[["lat", "lon"]].to_numpy()), k=1)[0][:, 0]*6371008.8
    geo_group = np.asarray([h3.latlng_to_cell(row.lat, row.lon, 6) for row in loc.itertuples()])
    result = {"status": "independent_post_freeze", "location_count": n,
        "artifact_hashes": expected, "single_shot_all_initial": metrics(single_error),
        "multi_photo_fixed_protocol": metrics(multi_error),
        "adaptive": {"answer_rate": float(answered.mean()),
            "answered_count": int(answered.sum()), "retake_rate": float(requested.mean()),
            "mean_photos_used": float(used.mean()), "median_photos_used": float(np.median(used)),
            "conditional_raw100": float((adaptive_error[answered] <= 100).mean()) if answered.any() else None,
            "conditional_gt500m": float((adaptive_error[answered] > 500).mean()) if answered.any() else None,
            "full_denominator_success_100m": float((adaptive_error <= 100).mean())},
        "paired_transitions_single_to_multi": {
            "old_wrong_new_correct": int(((single_error > 100) & (multi_error <= 100)).sum()),
            "old_correct_new_wrong": int(((single_error <= 100) & (multi_error > 100)).sum())},
        "coverage": {"covered": int((closest <= 100).sum()),
                     "uncovered": int((closest > 100).sum())},
        "raw100_session_ci95": bootstrap((single_error <= 100).astype(float), loc.session_id.astype(str).to_numpy()),
        "raw100_geographic_ci95": bootstrap((single_error <= 100).astype(float), geo_group),
        "query_gt_in_inference": False}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    save(output_path, result)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("candidate", "locations", "photos", "predictions", "receipt", "output"):
        parser.add_argument(f"--{name}", required=True)
    args = parser.parse_args()
    run(args.candidate, args.locations, args.photos, args.predictions, args.receipt, args.output)
