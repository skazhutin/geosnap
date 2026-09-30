"""Development-only database augmentation, with and without reference GPS constraints."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
from threadpoolctl import threadpool_limits

from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256
from ml.research.vector_evaluation import development_frame, distances, evaluate_scores, save_results
from ml.retrieval.embedding_job import load_embedding_artifacts


def normalize(x):
    return x / np.maximum(np.linalg.norm(x, axis=-1, keepdims=True), 1e-12)


def run():
    path = Path("data/evaluation/moscow_real_v4/development_queries.parquet")
    queries = development_frame(path)
    gallery, ids, refs, metadata = load_embedding_artifacts("data/embeddings/moscow_real_v4/sage-vitb")
    q = np.asarray(
        [
            np.load(ROOT / f"development/query_views/{r.id}.npz", allow_pickle=False)["descriptors"][0]
            for r in queries.itertuples()
        ]
    )
    output = ROOT / "development/database_augmentation"
    output.mkdir(parents=True, exist_ok=True)
    cache = output / "neighbors.npz"
    contract = output / "neighbors.json"
    gallery_sha = metadata["artifact_sha256"]["descriptors.npy"]
    if cache.exists():
        saved = json.loads(contract.read_text())
        if saved["gallery_sha256"] != gallery_sha or saved["cache_sha256"] != sha256(cache):
            raise ValueError("augmentation neighbor cache changed")
        with np.load(cache, allow_pickle=False) as data:
            visual, geo = data["visual"], data["geo"]
    else:
        visual = np.empty((len(ids), 20), dtype=np.int32)
        geo = np.empty((len(ids), 20), dtype=np.int32)
        lat = np.radians([r["metadata"]["lat"] for r in refs])
        lon = np.radians([r["metadata"]["lon"] for r in refs])
        with threadpool_limits(limits=2):
            for start in range(0, len(ids), 128):
                sim = gallery[start : start + 128] @ gallery.T
                for j, similarities in enumerate(sim):
                    i = start + j
                    order = np.argsort(-similarities, kind="stable")
                    visual[i] = order[:20]
                    r = refs[i]["metadata"]
                    nearby = distances(r["lat"], r["lon"], lat, lon) <= 100
                    chosen = order[nearby[order]][:20]
                    geo[i] = np.pad(chosen, (0, 20 - len(chosen)), constant_values=i)
                if start % 1280 == 0:
                    print("gallery neighbors", start, "/", len(ids), flush=True)
        np.savez_compressed(cache, visual=visual, geo=geo)
        contract.write_text(
            json.dumps(
                {
                    "gallery_sha256": gallery_sha,
                    "cache_sha256": sha256(cache),
                    "rule": "top20 cosine incl self; geo arm within100m references only, padding original when fewer neighbors",
                },
                indent=2,
            )
            + "\n"
        )
    records = {}
    with threadpool_limits(limits=2):
        records["sage"] = evaluate_scores(q @ gallery.T, queries, ids, refs)
        for family, neighbors in [("visual", visual), ("geo", geo)]:
            for k in (3, 10):
                augmented = np.empty_like(gallery)
                for start in range(0, len(ids), 128):
                    selected = gallery[neighbors[start : start + 128, :k]]
                    weights = np.maximum(np.einsum("bd,bkd->bk", gallery[start : start + 128], selected), 0) ** 3
                    augmented[start : start + 128] = normalize(np.einsum("bk,bkd->bd", weights, selected))
                scores = q @ augmented.T
                name = f"{family}_dba{k}"
                records[name] = evaluate_scores(scores, queries, ids, refs)
                print("evaluated", name, flush=True)
                if family == "visual":
                    nearest = np.argsort(-scores, axis=1, kind="stable")[:, :3]
                    expanded = normalize(q + np.mean(augmented[nearest], axis=1))
                    records[f"{name}_aqe3"] = evaluate_scores(expanded @ augmented.T, queries, ids, refs)
                del augmented, scores
    save_results(
        output,
        path,
        queries,
        records,
        {name: len(ids) for name in records},
        {
            "gallery_sha256": gallery_sha,
            "neighbor_cache_sha256": sha256(cache),
            "fit_data": "reference gallery only; no query ground truth or held-out queries used in descriptors",
        },
    )


if __name__ == "__main__":
    run()
