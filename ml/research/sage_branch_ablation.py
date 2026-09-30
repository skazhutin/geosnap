"""Reweight official SAGE's 256-D scene token and 8192-D pooled branch.

The pinned aggregator concatenates the scene token first. Its pooled branch is
normalized per cluster, so the scene token has about 1/65 of the final squared
norm, not half. Every alternative remains an independent-image cosine descriptor.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256
from ml.research.vector_evaluation import development_frame, evaluate_scores, save_results
from ml.retrieval.embedding_job import load_embedding_artifacts


def branch_normalize(matrix):
    if matrix.ndim != 2 or matrix.shape[1] != 8448:
        raise ValueError("this ablation requires the pinned SAGE descriptor layout")
    branches = []
    for part in [matrix[:, :256], matrix[:, 256:]]:
        norm = np.linalg.norm(part, axis=1, keepdims=True)
        if not np.isfinite(part).all() or (norm <= 1e-12).any():
            raise ValueError("invalid descriptor branch")
        branches.append(np.ascontiguousarray(part / norm))
    return branches


def run(model, split, arm):
    query_path = (
        Path("data/evaluation/moscow_real_v4/development_queries.parquet")
        if split == "historical"
        else ROOT / "prospective/development.parquet"
    )
    queries = development_frame(query_path)
    if model == "sage-vitb" and split == "historical":
        q = np.asarray(
            [
                np.load(ROOT / f"development/query_views/{r.id}.npz", allow_pickle=False)["descriptors"][0]
                for r in queries.itertuples()
            ]
        )
    else:
        path = ROOT / f"development/global_{model}_{split}"
        contract = json.loads((path / "query_contract.json").read_text())
        if contract["contract"]["manifest_sha256"] != sha256(query_path) or contract["descriptors_sha256"] != sha256(
            path / "queries.npy"
        ):
            raise ValueError("query features changed")
        q = np.load(path / "queries.npy", allow_pickle=False)
    directory = (
        Path("data/embeddings/moscow_real_v4/sage-vitb")
        if model == "sage-vitb"
        else Path("data/embeddings/moscow_research_v5") / model
    )
    gallery, ids, refs, metadata = load_embedding_artifacts(directory)
    provenance = {"base_artifacts": metadata["artifact_sha256"]}
    if arm == "union":
        extra, extra_ids, extra_refs, extra_meta = load_embedding_artifacts(
            Path("data/embeddings/moscow_research_v5/gallery_union") / model
        )
        gallery = np.concatenate([gallery, extra])
        ids, refs = ids + extra_ids, refs + extra_refs
        target = ROOT / "gallery_expansion/gallery_union.parquet"
        union = json.loads((ROOT / "gallery_expansion/union.json").read_text())
        if sha256(target) != union["manifest_sha256"]:
            raise ValueError("gallery union changed")
        selected_ids = pd.read_parquet(target).id.tolist()
        lookup = {v: i for i, v in enumerate(ids)}
        selected = [lookup[v] for v in selected_ids]
        gallery, refs, ids = gallery[selected], [refs[i] for i in selected], selected_ids
        provenance["added_artifacts"] = extra_meta["artifact_sha256"]
        provenance["gallery_sha256"] = sha256(target)
    with threadpool_limits(limits=2):
        records = {"original": evaluate_scores(q @ gallery.T, queries, ids, refs)}
        qparts, gparts = branch_normalize(q), branch_normalize(gallery)
        scene = qparts[0] @ gparts[0].T
        pooled = qparts[1] @ gparts[1].T
        for weight in [0.0, 1 / 65, 0.05, 0.1, 0.25, 0.5, 1.0]:
            records[f"scene_weight_{weight:.6f}"] = evaluate_scores(
                weight * scene + (1 - weight) * pooled, queries, ids, refs
            )
    provenance.update(
        {
            "layout": "scene token [:256], pooled branch [256:] from official SoftP.withtoken",
            "query_scene_norm2_quantiles": np.quantile(np.sum(q[:, :256] ** 2, axis=1), [0, 0.5, 1]).tolist(),
            "deployable_transform": "branch normalization followed by sqrt(weight)-scaled concatenation",
            "comparison_baseline": "same model and gallery, unchanged descriptor",
        }
    )
    save_results(
        ROOT / f"development/branch_{model}_{arm}_{split}",
        query_path,
        queries,
        records,
        {name: len(ids) for name in records},
        provenance,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=["sage-vitb", "sage-vitl"], default="sage-vitl")
    parser.add_argument("--split", choices=["historical", "new"], default="new")
    parser.add_argument("--gallery", choices=["baseline", "union"], default="baseline")
    args = parser.parse_args()
    run(args.model, args.split, args.gallery)
