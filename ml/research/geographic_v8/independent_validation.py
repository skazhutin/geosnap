"""Evaluate a genuinely new, post-freeze smartphone population once it exists."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import h3
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, frames
from ml.research.vector_evaluation import distances

REQUIRED = {"id", "image_path", "file_sha256", "lat", "lon", "captured_at",
            "session_id", "photographer_id", "selection_stratum"}


def bootstrap(values, groups, seed=20260928, resamples=5000):
    unique, inverse = np.unique(groups, return_inverse=True)
    rng = np.random.default_rng(seed)
    draws = np.empty(resamples)
    for i in range(resamples):
        picked = rng.integers(0, len(unique), size=len(unique))
        counts = np.bincount(picked, minlength=len(unique))
        weights = counts[inverse]
        draws[i] = np.average(values, weights=weights)
    return [float(x) for x in np.quantile(draws, [.025, .975])]


def run(candidate_path, manifest_path, predictions_path, receipt_path, output_path):
    candidate_path, manifest_path = Path(candidate_path), Path(manifest_path)
    predictions_path, receipt_path, output_path = Path(predictions_path), Path(receipt_path), Path(output_path)
    candidate = json.loads(candidate_path.read_text())
    if not candidate.get("frozen_at") or not candidate.get("artifact_hashes"):
        raise RuntimeError("Candidate is not fully frozen; do not evaluate independent data")
    for filename, expected in candidate["artifact_hashes"].items():
        if digest(filename) != expected:
            raise RuntimeError(f"Frozen candidate artifact changed: {filename}")
    receipt = json.loads(receipt_path.read_text())
    if (receipt.get("candidate_sha256") != digest(candidate_path) or
            receipt.get("manifest_sha256") != digest(manifest_path) or
            receipt.get("predictions_sha256") != digest(predictions_path)):
        raise RuntimeError("Prediction receipt does not bind frozen candidate, new queries and outputs")
    new = pd.read_parquet(manifest_path)
    if REQUIRED - set(new.columns) or len(new) == 0 or new.id.duplicated().any():
        raise RuntimeError("New smartphone query manifest is incomplete or non-unique")
    if new.session_id.isna().any() or new.photographer_id.isna().any():
        raise RuntimeError("Independent capture session/person structure missing")
    if pd.to_datetime(new.captured_at, utc=True).min() <= pd.Timestamp(candidate["frozen_at"]):
        raise RuntimeError("Smartphone image population was captured before candidate freeze")
    prior, gallery = frames()
    if set(new.id) & (set(prior.id) | set(gallery.id)):
        raise RuntimeError("New query ID overlaps opened development/gallery population")
    if set(new.file_sha256) & (set(prior.file_sha256) | set(gallery.file_sha256)):
        raise RuntimeError("New query image bytes overlap opened development/gallery population")
    if set(new.session_id.astype(str)) & set(prior.sequence_id.astype(str)):
        raise RuntimeError("New capture session overlaps the development population")
    for row in new.itertuples():
        if digest(row.image_path) != row.file_sha256:
            raise RuntimeError(f"New query image hash changed: {row.id}")
    with np.load(predictions_path, allow_pickle=False) as saved:
        if set(saved.files) < {"query_ids", "prediction_gps"}:
            raise RuntimeError("Prediction NPZ lacks mandatory fields")
        if saved["query_ids"].tolist() != new.id.tolist():
            raise RuntimeError("Prediction/query ordering mismatch")
        gps = saved["prediction_gps"].copy()
    if gps.shape != (len(new), 2) or not np.isfinite(gps).all():
        raise RuntimeError("One finite latitude/longitude pair required for every query")
    errors = np.asarray([distances(row.lat, row.lon, np.radians(gps[i, 0:1]),
                                    np.radians(gps[i, 1:2]))[0] for i, row in enumerate(new.itertuples())])
    tree = BallTree(np.radians(gallery[["lat", "lon"]].to_numpy()), metric="haversine")
    closest = tree.query(np.radians(new[["lat", "lon"]].to_numpy()), k=1)[0][:, 0] * 6371008.8
    covered = closest <= 100
    session_groups = new.session_id.astype(str).to_numpy()
    geo_groups = np.asarray([h3.latlng_to_cell(row.lat, row.lon, 6) for row in new.itertuples()])
    result = {"status": "independent_post_freeze", "query_count": len(new),
        "candidate_sha256": digest(candidate_path), "manifest_sha256": digest(manifest_path),
        "predictions_sha256": digest(predictions_path),
        "raw": {str(k): float((errors <= k).mean()) for k in (25, 50, 100, 500, 1000, 5000)},
        "counts": {str(k): int((errors <= k).sum()) for k in (25, 50, 100, 500, 1000, 5000)},
        "median_error_m": float(np.median(errors)), "p90_error_m": float(np.quantile(errors, .9)),
        "gt500m_rate": float((errors > 500).mean()),
        "raw100_session_cluster_ci95": bootstrap((errors <= 100).astype(float), session_groups),
        "raw100_geographic_cluster_ci95": bootstrap((errors <= 100).astype(float), geo_groups),
        "coverage": {"covered_count": int(covered.sum()), "uncovered_count": int((~covered).sum()),
            "covered_raw100": float((errors[covered] <= 100).mean()) if covered.any() else None,
            "uncovered_raw100": float((errors[~covered] <= 100).mean()) if (~covered).any() else None},
        "capture_session_count": len(set(session_groups)),
        "geographic_group_count": len(set(geo_groups)),
        "no_query_ground_truth_in_inference": True}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        raise FileExistsError("Independent evaluation is write-once")
    save(output_path, result)
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--predictions", required=True)
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args()
    run(arguments.candidate, arguments.manifest, arguments.predictions, arguments.receipt, arguments.output)
