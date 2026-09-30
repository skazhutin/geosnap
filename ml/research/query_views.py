"""Exact fixed-gallery query-view and query-expansion ablation, dev only."""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image
from threadpoolctl import threadpool_limits

from ml.research.metrics import paired_group_bootstrap, raw_metrics, retrieval_metrics
from ml.research.mode_experiment import BUNDLE, DEVELOPMENT_HASH
from ml.retrieval import create_retriever
from ml.retrieval.embedding_job import load_embedding_artifacts
from ml.retrieval.image_io import load_rgb_image

ROOT = Path("data/evaluation/moscow_research_v5/development/query_views")
VIEWS = ("original", "rot90", "rot180", "rot270", "center75", "upper70")


def views(image):
    w, h = image.size
    return [
        image,
        image.transpose(Image.Transpose.ROTATE_90),
        image.transpose(Image.Transpose.ROTATE_180),
        image.transpose(Image.Transpose.ROTATE_270),
        image.crop((int(w * 0.125), int(h * 0.125), int(w * 0.875), int(h * 0.875))),
        image.crop((0, 0, w, int(h * 0.7))),
    ]


def run():
    ROOT.mkdir(parents=True, exist_ok=True)
    query_path = BUNDLE / "development_queries.parquet"
    if hashlib.sha256(query_path.read_bytes()).hexdigest() != DEVELOPMENT_HASH:
        raise ValueError("development identity mismatch")
    q = pd.read_parquet(query_path)
    production = json.loads(Path("configs/moscow_production_frozen.json").read_text())
    gallery, ids, refs, meta = load_embedding_artifacts(Path(production["embedding"]["directory"]))
    os.environ.setdefault("HF_HOME", str(Path(".cache/huggingface").resolve()))
    import torch

    torch.set_num_threads(2)
    model = create_retriever(
        "sage-vitb", device="mps", allow_device_fallback=False, batch_size=6, cache_dir=".cache/torch/hub"
    )
    vectors = []
    started = time.monotonic()
    for i, row in enumerate(q.itertuples()):
        path = ROOT / f"{row.id}.npz"
        image_hash = hashlib.sha256(Path(row.image_path).read_bytes()).hexdigest()
        if path.exists():
            cache = np.load(path, allow_pickle=False)
            if str(cache["image_sha256"]) != image_hash or str(cache["checkpoint_sha256"]) != model.checkpoint_sha256:
                raise ValueError("query cache identity mismatch")
            current = cache["descriptors"]
        else:
            model.load()
            current = model.embed_batch(views(load_rgb_image(row.image_path)))
            np.savez(path, descriptors=current, image_sha256=image_hash, checkpoint_sha256=model.checkpoint_sha256)
        vectors.append(current)
        if (i + 1) % 50 == 0:
            print("embedded", i + 1, "/", len(q), "seconds", round(time.monotonic() - started), flush=True)
    model.close()
    vectors = np.asarray(vectors)
    lat = np.radians(np.array([r["metadata"]["lat"] for r in refs]))
    lon = np.radians(np.array([r["metadata"]["lon"] for r in refs]))
    groups = q.h3_coarse.astype(str).tolist()
    predictions = {}
    metrics = {}
    methods = list(VIEWS) + [
        "rotation_max",
        "original_upper_max",
        "all_views_max",
        "original_center_mean",
        "aqe3",
        "aqe5",
    ]
    records = {name: {"errors": [], "ranks": [], "top100": []} for name in methods}
    with threadpool_limits(limits=2):
        for i, row in enumerate(q.itertuples()):
            qlat, qlon = np.radians([row.lat, row.lon])
            distance = (
                2
                * 6371008.8
                * np.arcsin(
                    np.sqrt(
                        np.clip(
                            np.sin((lat - qlat) / 2) ** 2 + np.cos(lat) * np.cos(qlat) * np.sin((lon - qlon) / 2) ** 2,
                            0,
                            1,
                        )
                    )
                )
            )
            similarities = vectors[i] @ gallery.T
            base_order = np.argsort(-similarities[0], kind="stable")
            combined = {name: similarities[j] for j, name in enumerate(VIEWS)}
            combined.update(
                rotation_max=similarities[:4].max(axis=0),
                original_upper_max=similarities[[0, 5]].max(axis=0),
                all_views_max=similarities.max(axis=0),
                original_center_mean=similarities[[0, 4]].mean(axis=0),
            )
            for k in (3, 5):
                neighbors = base_order[:k]
                expanded = vectors[i, 0] + np.sum(
                    gallery[neighbors] * np.maximum(similarities[0, neighbors, None], 0) ** 3, axis=0
                )
                expanded /= np.linalg.norm(expanded)
                combined[f"aqe{k}"] = expanded @ gallery.T
            for name, score in combined.items():
                order = np.argsort(-score, kind="stable")
                positive = np.flatnonzero(distance[order] <= 100)
                records[name]["errors"].append(float(distance[order[0]]))
                records[name]["ranks"].append(int(positive[0] + 1) if len(positive) else None)
                records[name]["top100"].append(
                    {"ids": [ids[j] for j in order[:100]], "scores": score[order[:100]].tolist()}
                )
            if (i + 1) % 100 == 0:
                print("retrieved", i + 1, flush=True)
    for name in methods:
        record = records[name]
        metrics[name] = {
            "raw_top1": raw_metrics(record["errors"]),
            "retrieval": retrieval_metrics(record["ranks"], gallery_size=len(gallery)),
            "paired_gain_vs_original_top1": paired_group_bootstrap(
                records["original"]["errors"], record["errors"], groups
            ),
        }
        predictions[name] = record
    payload = {
        "kind": "historical_development_query_view_exploration",
        "query_manifest_sha256": DEVELOPMENT_HASH,
        "gallery_descriptors_sha256": meta["artifact_sha256"]["descriptors.npy"],
        "views": VIEWS,
        "preprocessing": "SAGE official 322; rotations/crops applied after EXIF normalization",
        "metrics": metrics,
        "elapsed_seconds": time.monotonic() - started,
    }
    (ROOT / "summary.json").write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n")
    (ROOT / "retrieval.json").write_text(
        json.dumps({"query_ids": q.id.tolist(), "methods": predictions}, allow_nan=False) + "\n"
    )
    print(json.dumps({name: metrics[name]["raw_top1"] for name in methods}, indent=2), flush=True)


if __name__ == "__main__":
    run()
