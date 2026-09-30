"""Posthoc image-quality proxies by frozen SAGE failure bucket; never inference features."""
from __future__ import annotations

import json
import time

import cv2
import numpy as np
from PIL import Image

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, QUERY, frames, registry


def measure(path):
    with Image.open(path) as image:
        gray = np.asarray(image.convert("L"), dtype=np.uint8)
    h, w = gray.shape
    resized = cv2.resize(gray, (512, max(1, round(h * 512 / w))), interpolation=cv2.INTER_AREA)
    return [float(resized.mean()), float(np.mean(resized < 40)),
        float(np.mean(resized > 245)), float(np.percentile(resized, 95) - np.percentile(resized, 5)),
        float(cv2.Laplacian(resized, cv2.CV_64F).var())]


def run():
    started = time.perf_counter()
    q, _ = frames()
    with np.load(LOCAL / "baseline/predictions.npz", allow_pickle=False) as saved:
        if saved["query_ids"].tolist() != q.id.tolist():
            raise RuntimeError("Query/baseline identity differs")
        buckets = saved["buckets"].copy()
    names = ["mean_luminance", "dark_pixel_fraction", "saturated_pixel_fraction",
             "p95_minus_p5_contrast", "laplacian_variance"]
    values = np.asarray([measure(path) for path in q.image_path], np.float64)
    if values.shape != (len(q), len(names)) or not np.isfinite(values).all():
        raise RuntimeError("Invalid query quality measurements")
    # Thresholds depend on the image population alone, never outcome labels.
    thresholds = {"darkest_decile": float(np.quantile(values[:, 0], .1)),
        "lowest_contrast_decile": float(np.quantile(values[:, 3], .1)),
        "lowest_edge_variance_decile": float(np.quantile(values[:, 4], .1))}
    flags = {"darkest_decile": values[:, 0] <= thresholds["darkest_decile"],
        "lowest_contrast_decile": values[:, 3] <= thresholds["lowest_contrast_decile"],
        "lowest_edge_variance_decile": values[:, 4] <= thresholds["lowest_edge_variance_decile"]}
    source = q.source.astype(str).to_numpy()
    groups = {}
    for name in ("correct", "retrieval_failure", "ranking_failure", "no_coverage"):
        take = buckets == name
        groups[name] = {"count": int(take.sum()),
            "median": {feature: float(np.median(values[take, j])) for j, feature in enumerate(names)},
            "proxy_flag_rates": {feature: float(flags[feature][take].mean()) for feature in flags}}
    result = {"status": "posthoc_exploratory_quality_proxy_audit",
        "query_count": len(q), "source_counts": {item: int((source == item).sum()) for item in np.unique(source)},
        "manifest_quality_score_nonnull": int(q.quality_score.notna().sum()),
        "min_width": int(q.width.min()), "median_width": float(q.width.median()),
        "min_height": int(q.height.min()), "median_height": float(q.height.median()),
        "feature_names": names, "population_decile_thresholds": thresholds,
        "by_frozen_failure_bucket": groups,
        "query_manifest_sha256": digest(QUERY),
        "baseline_predictions_sha256": digest(LOCAL / "baseline/predictions.npz"),
        "query_GT_used_for_image_measurements": False,
        "warning": "Laplacian variance reflects scene texture and illumination as well as blur; association is not causal. No human image-quality labels are available.",
        "runtime_s": time.perf_counter()-started}
    out = LOCAL / "query_quality_audit"
    out.mkdir(exist_ok=True)
    np.savez(out / "per_query.npz", query_ids=q.id.to_numpy(str), buckets=buckets,
        source=source, features=values, feature_names=np.asarray(names, dtype=str))
    save(out / "report.json", result)
    registry("query_quality_proxy_posthoc", {"model_revision": "none; diagnostic only",
        "preprocessing": "PIL luminance, width-normalized 512px, OpenCV Laplacian",
        "candidate_generation": None, "reranking": None, "fusion": None,
        "hyperparameters": {"resize_width": 512, "flag_quantile": .1},
        "fitted_parameters": False, "fitting_split": None,
        "evaluation_split": "1184 reused exploratory development; descriptive only",
        "raw25": None, "raw50": None, "raw100": None, "r_at_k": None,
        "median_m": None, "p90_m": None, "gt500_rate": None,
        "runtime": result["runtime_s"], "result_status": "complete",
        "decision": "context for quality hypothesis; no inference feature or causal conclusion",
        "artifact_paths": [str(out / "per_query.npz"), str(out / "report.json")],
        "artifact_hashes": {item: digest(out / item) for item in ("per_query.npz", "report.json")}})
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    run()
