"""B/L cosine fusion on each fixed reference arm, development only."""

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


def scores_for(model, split, query_path, queries, selected_ids):
    if model == "sage-vitb" and split == "historical":
        q = np.asarray(
            [
                np.load(ROOT / f"development/query_views/{r.id}.npz", allow_pickle=False)["descriptors"][0]
                for r in queries.itertuples()
            ]
        )
    else:
        directory = ROOT / f"development/global_{model}_{split}"
        contract = json.loads((directory / "query_contract.json").read_text())
        if contract["contract"]["manifest_sha256"] != sha256(query_path) or contract["descriptors_sha256"] != sha256(
            directory / "queries.npy"
        ):
            raise ValueError("fusion query cache changed")
        q = np.load(directory / "queries.npy", allow_pickle=False)
    base_path = (
        Path("data/embeddings/moscow_real_v4/sage-vitb")
        if model == "sage-vitb"
        else Path("data/embeddings/moscow_research_v5") / model
    )
    base, base_ids, base_refs, base_meta = load_embedding_artifacts(base_path)
    extra, extra_ids, extra_refs, extra_meta = load_embedding_artifacts(
        Path("data/embeddings/moscow_research_v5/gallery_union") / model
    )
    for key in ["checkpoint", "preprocessing", "descriptor_dim"]:
        if base_meta["retriever"][key] != extra_meta["retriever"][key]:
            raise ValueError("fusion gallery sources use different descriptor contracts")
    ids = base_ids + extra_ids
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate reference identities")
    lookup = {identity: i for i, identity in enumerate(ids)}
    selected = [lookup[i] for i in selected_ids]
    gallery = np.concatenate([base, extra])
    refs = base_refs + extra_refs
    return (
        q @ gallery[selected].T,
        [refs[i] for i in selected],
        {"base": base_meta["artifact_sha256"], "added": extra_meta["artifact_sha256"]},
    )


def run(split):
    query_path = (
        Path("data/evaluation/moscow_real_v4/development_queries.parquet")
        if split == "historical"
        else ROOT / "prospective/development.parquet"
    )
    queries = development_frame(query_path)
    root = ROOT / "gallery_expansion"
    union = json.loads((root / "union.json").read_text())
    if sha256(root / "gallery_union.parquet") != union["manifest_sha256"]:
        raise ValueError("reference union changed")
    # Each arm deduplicates independently and may retain a different member of
    # a near-duplicate pair. The catalog must contain the entire audited pool.
    union_ids = pd.read_parquet("data/evaluation/moscow_real_v4/gallery.parquet").id.tolist()
    union_ids += pd.read_parquet(root / "audited_union.parquet").id.tolist()
    with threadpool_limits(limits=2):
        b, refs, bmeta = scores_for("sage-vitb", split, query_path, queries, union_ids)
        large, lrefs, lmeta = scores_for("sage-vitl", split, query_path, queries, union_ids)
    # Latitude/longitude, provider and local density must come from the same
    # fixed gallery arm when the resulting stream is geographically localized.
    if [(r["metadata"]["lat"], r["metadata"]["lon"]) for r in refs] != [
        (r["metadata"]["lat"], r["metadata"]["lon"]) for r in lrefs
    ]:
        raise ValueError("fusion references disagree on geography")
    lookup = {identity: i for i, identity in enumerate(union_ids)}
    arms = json.loads((root / "arms.json").read_text())
    for arm, path in [
        ("baseline", Path("data/evaluation/moscow_real_v4/gallery.parquet")),
        ("random", root / "gallery_random.parquet"),
        ("diversity", root / "gallery_diversity.parquet"),
        ("union_larger_budget", root / "gallery_union.parquet"),
    ]:
        if arm in {"random", "diversity"} and sha256(path) != arms["files"][path.name]:
            raise ValueError("fixed gallery arm changed")
        ids = pd.read_parquet(path).id.tolist()
        selection = [lookup[i] for i in ids]
        current_refs = [refs[i] for i in selection]
        left, right = b[:, selection], large[:, selection]
        records = {
            "sage-vitb": evaluate_scores(left, queries, ids, current_refs),
            "sage-vitl": evaluate_scores(right, queries, ids, current_refs),
        }
        for weight in [0.25, 0.5, 0.75]:
            records[f"fusion{int(weight * 100)}"] = evaluate_scores(
                (1 - weight) * left + weight * right, queries, ids, current_refs
            )
        save_results(
            ROOT / f"development/gallery_fusion_{split}/{arm}",
            query_path,
            queries,
            records,
            {name: len(ids) for name in records},
            {
                "gallery_manifest_sha256": sha256(path),
                "sage_b": bmeta,
                "sage_l": lmeta,
                "deployable_equivalence": "exact cosine of concatenated sqrt(weight)-scaled unit descriptors",
            },
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--split", choices=["historical", "new"], default="new")
    run(parser.parse_args().split)
