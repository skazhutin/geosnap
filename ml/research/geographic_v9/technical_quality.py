"""Deterministic query-image diagnostics; no outcomes or query GPS in features."""
from __future__ import annotations

import json
import os
import time

import cv2
import numpy as np
import pandas as pd
from PIL import Image

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import QUERY
from ml.research.geographic_v9.common import OUT, V8, record

FEATURES = ["width", "height", "megapixels", "bytes_per_pixel", "mean_luminance",
    "dark_fraction", "clipped_bright_fraction", "p95_minus_p5_luminance",
    "luminance_entropy_bits", "laplacian_variance", "tenengrad",
    "canny_edge_density", "upper_edge_density", "lower_edge_density",
    "gradient_direction_coherence", "frequency_high_band_ratio",
    "mean_saturation"]
OUTDIR = OUT / "technical_quality"


def measure(path):
    with Image.open(path) as source:
        image = source.convert("RGB")
        width, height = image.size
        if width < 16 or height < 16:
            raise ValueError(f"Image too small for diagnostics: {path}")
        resized = image.resize((512, max(16, round(height*512/width))), Image.Resampling.BILINEAR)
        rgb = np.asarray(resized, np.uint8)
    gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)
    hsv = cv2.cvtColor(rgb, cv2.COLOR_RGB2HSV)
    lap = cv2.Laplacian(gray, cv2.CV_32F)
    gx = cv2.Sobel(gray, cv2.CV_32F, 1, 0, ksize=3)
    gy = cv2.Sobel(gray, cv2.CV_32F, 0, 1, ksize=3)
    energy = gx*gx + gy*gy
    gxx, gyy, gxy = float((gx*gx).mean()), float((gy*gy).mean()), float((gx*gy).mean())
    anisotropy = np.hypot(gxx-gyy, 2*gxy)/(gxx+gyy+1e-6)
    edges = cv2.Canny(gray, 80, 160) > 0
    hist = np.bincount(gray.ravel(), minlength=256).astype(float)
    prob = hist[hist > 0]/hist.sum()
    gray_float = gray.astype(np.float32) - gray.mean()
    spectrum = np.abs(np.fft.rfft2(gray_float))**2
    fy = np.fft.fftfreq(gray.shape[0])[:, None]
    fx = np.fft.rfftfreq(gray.shape[1])[None, :]
    high = fy*fy+fx*fx >= .12**2
    high_ratio = float(spectrum[high].sum()/(spectrum.sum()+1e-9))
    result = [width, height, width*height/1e6,
        os.stat(path).st_size/(width*height),
        float(gray.mean()), float(np.mean(gray < 40)), float(np.mean(gray > 245)),
        float(np.percentile(gray, 95)-np.percentile(gray, 5)),
        float(-(prob*np.log2(prob)).sum()), float(lap.var()), float(energy.mean()),
        float(edges.mean()), float(edges[:len(edges)//2].mean()),
        float(edges[len(edges)//2:].mean()), float(anisotropy), high_ratio,
        float(hsv[:, :, 1].mean()/255)]
    if len(result) != len(FEATURES) or not np.isfinite(result).all():
        raise RuntimeError("Invalid technical features")
    return result


def run():
    started = time.perf_counter()
    query = pd.read_parquet(QUERY, columns=["id", "image_path", "file_sha256"])
    with np.load(V8 / "baseline/predictions.npz", allow_pickle=False) as z:
        if z["query_ids"].tolist() != query.id.tolist():
            raise RuntimeError("Query population/order changed")
    values = np.empty((len(query), len(FEATURES)), np.float32)
    for i, row in enumerate(query.itertuples()):
        if digest(row.image_path) != row.file_sha256:
            raise RuntimeError(f"Image hash mismatch at query {i}")
        values[i] = measure(row.image_path)
        if (i+1) % 200 == 0:
            print("technical images", i+1, "/", len(query), flush=True)
    OUTDIR.mkdir(exist_ok=True)
    path = OUTDIR / "image_features.npz"
    if path.exists():
        raise FileExistsError("Technical feature artifact already sealed")
    np.savez_compressed(path, query_ids=query.id.to_numpy(str),
        features=values, feature_names=np.asarray(FEATURES, str))
    receipt = {"status": "features_complete_human_benchmark_pending",
        "query_count": len(query), "feature_names": FEATURES,
        "query_gt_used": False, "outcome_labels_used": False,
        "diagnostic_warning": "Gradient/frequency/edge measurements also respond to texture and scene content; they are not validated blur or geolocatability labels.",
        "query_manifest_sha256": digest(QUERY),
        "baseline_predictions_sha256": digest(V8 / "baseline/predictions.npz"),
        "image_features_sha256": digest(path), "runtime_s": time.perf_counter()-started}
    save(OUTDIR / "feature_contract.json", receipt)
    record("technical_quality_image_features_v1", {"status": receipt["status"],
        "fitted": False, "query_gt_used": False, "outcome_labels_used": False,
        "human_label_version": None, "result": receipt,
        "artifact_paths": [str(path), str(OUTDIR / "feature_contract.json")],
        "artifact_hashes": {p.name: digest(p) for p in [path, OUTDIR / "feature_contract.json"]}})
    print(json.dumps({"count": len(query), "feature_sha256": receipt["image_features_sha256"],
                      "runtime_s": receipt["runtime_s"]}), flush=True)


if __name__ == "__main__":
    run()
