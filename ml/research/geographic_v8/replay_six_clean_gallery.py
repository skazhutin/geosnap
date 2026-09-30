"""Outcome-blind six-view replay against the frozen 111,032-row research gallery.

The location supplied by the user is read only after inference.json is written.
Existing descriptors and checkpoints are reused; no gallery images are encoded.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale, night_scale_context
from ml.research.gallery_scale_storage import WORKSPACE, digest
from ml.research.geographic_v8.baseline import selected_vectors, staged_entries
from ml.research.geographic_v8.common import GALLERY
from ml.research.vector_evaluation import distances


ROOT = WORKSPACE / "data/evaluation/user_six_view_case_20260930"
OUT = ROOT / "cleaned_sage_l_replay_v1"
FILTER = WORKSPACE / "data/evaluation/gallery_quality_filter_v3_20260929"
CHECKPOINT = WORKSPACE / "data/models/research_v5/sage_context_encoder.pth"


def _write_once(path: Path, value: dict) -> None:
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    temp.replace(path)


def _context_encoder() -> torch.nn.Module:
    if digest(CHECKPOINT) != "b3b49e7aa0e7c848c4c57b2d79660c3654a9946fb9f423f6f0e91681afc414b2":
        raise RuntimeError("Frozen SAGE context checkpoint changed")
    encoder = torch.nn.TransformerEncoder(
        torch.nn.TransformerEncoderLayer(
            d_model=768, nhead=16, dim_feedforward=1024, activation="gelu",
            dropout=0.1, batch_first=False,
        ), 2,
    )
    encoder.load_state_dict(torch.load(CHECKPOINT, map_location="cpu", weights_only=True), strict=True)
    return encoder.eval()


def infer() -> None:
    manifest_path = ROOT / "input_manifest.json"
    manifest = json.loads(manifest_path.read_text())["images"]
    if len(manifest) != 6 or [row["image"] for row in manifest] != list(range(1, 7)):
        raise RuntimeError("Six-view input manifest changed")
    for row in manifest:
        if digest(row["path"]) != row["sha256"]:
            raise RuntimeError(f"Input image {row['image']} changed")
    receipt = json.loads((FILTER / "filter_receipt.json").read_text())
    gallery_path = FILTER / "gallery_filtered.parquet"
    if digest(GALLERY) != receipt["original_gallery_sha256"] or digest(gallery_path) != receipt["filtered_gallery_sha256"]:
        raise RuntimeError("Frozen gallery changed")
    gallery = pd.read_parquet(gallery_path)
    if len(gallery) != 111_032 or gallery.id.duplicated().any():
        raise RuntimeError("Cleaned gallery population changed")
    config = json.loads((WORKSPACE / "configs/moscow_gallery_scale.json").read_text())
    paths = [row["path"] for row in manifest]
    started = time.perf_counter()
    model = gallery_scale.create_model(config)
    try:
        q322 = model.embed_batch(paths)
        model.image_size = (504, 504)
        model.batch_size = 2
        model._transform = model._build_transform()
        q504 = model.embed_batch(paths)
    finally:
        model.close()
    print(f"encoded six views at 322+504 in {time.perf_counter()-started:.1f}s", flush=True)
    means = night_scale_context.mean_queries(q322, q504)
    entries = staged_entries(gallery)
    with threadpool_limits(limits=2):
        scores = gallery_scale.exact_scores(gallery, q322, entries)
        scores += gallery_scale.exact_scores(gallery, q504, entries)
        scores *= 0.5
    print(f"scored {len(gallery)} references in {time.perf_counter()-started:.1f}s", flush=True)
    prefix = np.argsort(-scores, axis=1, kind="stable")[:, :100]
    chosen = prefix[:, :30]
    unique, remap = np.unique(chosen, return_inverse=True)
    references = selected_vectors(gallery, unique, entries)[remap.reshape(6, 30)]
    encoder = _context_encoder()
    contextual = np.empty((6, 30), np.float32)
    original = np.take_along_axis(scores, chosen, axis=1)
    with threadpool_limits(limits=2):
        for i in range(6):
            contextual[i], direct = night_scale_context.one_query(
                encoder, means[i], q322[i], q504[i], references[i]
            )
            np.testing.assert_allclose(direct, original[i], atol=2e-6, rtol=1e-5)
    ranked = night_scale_context.rank_prefix(prefix, contextual, original)
    rows = []
    for i, row in enumerate(ranked):
        candidates = []
        for rank, index in enumerate(row, 1):
            ref = gallery.iloc[int(index)]
            candidates.append({
                "rank": rank, "reference_id": str(ref.id), "source": str(ref.source),
                "lat": float(ref.lat), "lon": float(ref.lon),
                "mean_cosine": float(scores[i, index]),
            })
        rows.append({"image": manifest[i]["image"], "candidates": candidates})
    output = {
        "method": "SAGE-L query322+504 exact cosine, context30 0.5 standardized blend",
        "gallery_count": len(gallery), "gallery_sha256": digest(gallery_path),
        "input_manifest_sha256": digest(manifest_path),
        "source_model_sha256": "31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc",
        "context_checkpoint_sha256": digest(CHECKPOINT),
        "elapsed_seconds": time.perf_counter() - started,
        "query_ground_truth_accessed": False,
        "individual": rows,
    }
    _write_once(OUT / "inference.json", output)
    print(f"frozen inference: {OUT/'inference.json'}", flush=True)


def analyze() -> None:
    frozen = OUT / "inference.json"
    result = json.loads(frozen.read_text())
    if result["query_ground_truth_accessed"] is not False:
        raise RuntimeError("Inference artifact is not outcome-blind")
    prior = json.loads((ROOT / "analysis.json").read_text())
    gt = prior["ground_truth"]
    lat, lon = float(gt["lat"]), float(gt["lon"])
    summaries = []
    for item in result["individual"]:
        c = item["candidates"]
        err = distances(lat, lon, np.radians([x["lat"] for x in c]), np.radians([x["lon"] for x in c]))
        good = np.flatnonzero(err <= 100)
        summaries.append({
            "image": item["image"], "top1_m": float(err[0]),
            "best_top10_m": float(err[:10].min()), "best_top100_m": float(err.min()),
            "first_within_100m_rank": int(good[0]) + 1 if len(good) else None,
            "top1": c[0],
        })
    report = {
        "inference_sha256": digest(frozen), "ground_truth": gt,
        "individual": summaries,
        "old_production_individual_m": [r["prediction_error_m"] for r in prior["individual"]],
        "old_production_batch_status": prior["batch"]["status"],
        "old_production_batch_prediction": prior["batch"]["prediction"],
    }
    _write_once(OUT / "analysis.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=("infer", "analyze"))
    args = parser.parse_args()
    (infer if args.stage == "infer" else analyze)()
