"""Matched-budget gallery causal ablation on public development queries only."""

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


def run(split, model_name="sage-vitb"):
    query_path = (
        Path("data/evaluation/moscow_real_v4/development_queries.parquet")
        if split == "historical"
        else ROOT / "prospective/development.parquet"
    )
    queries = development_frame(query_path)
    root = ROOT / "gallery_expansion"
    arms = json.loads((root / "arms.json").read_text())
    for filename, digest in arms["files"].items():
        if sha256(root / filename) != digest:
            raise ValueError("fixed gallery arm manifest changed")
    if split == "historical" and model_name == "sage-vitb":
        q = np.asarray(
            [
                np.load(ROOT / f"development/query_views/{r.id}.npz", allow_pickle=False)["descriptors"][0]
                for r in queries.itertuples()
            ]
        )
    else:
        cache = ROOT / f"development/global_{model_name}_{split}"
        contract = json.loads((cache / "query_contract.json").read_text())
        if contract["contract"]["manifest_sha256"] != sha256(query_path) or contract["descriptors_sha256"] != sha256(
            cache / "queries.npy"
        ):
            raise ValueError("baseline query cache changed")
        q = np.load(cache / "queries.npy", allow_pickle=False)
    base_root = (
        Path("data/embeddings/moscow_real_v4/sage-vitb")
        if model_name == "sage-vitb"
        else Path("data/embeddings/moscow_research_v5") / model_name
    )
    base, base_ids, base_refs, base_meta = load_embedding_artifacts(base_root)
    added, added_ids, added_refs, added_meta = load_embedding_artifacts(
        Path("data/embeddings/moscow_research_v5/gallery_union") / model_name
    )
    if base_meta["retriever"]["checkpoint"] != added_meta["retriever"]["checkpoint"]:
        raise ValueError("gallery ablation must use the identical retriever")
    ids = base_ids + added_ids
    if len(set(ids)) != len(ids):
        raise ValueError("gallery union overlaps production identities")
    refs = base_refs + added_refs
    lookup = {identity: i for i, identity in enumerate(ids)}
    gallery = np.concatenate([base, added])
    del base, added
    selections = {"baseline": list(range(len(base_ids)))}
    for arm in ["random", "diversity"]:
        frame = pd.read_parquet(root / f"gallery_{arm}.parquet")
        selections[arm] = [lookup[identity] for identity in frame.id]
    # Larger-budget deployment option; never presented as the matched-budget contrast.
    union = json.loads((root / "union.json").read_text())
    if sha256(root / "gallery_union.parquet") != union["manifest_sha256"]:
        raise ValueError("union gallery changed")
    selections["union_larger_budget"] = [lookup[i] for i in pd.read_parquet(root / "gallery_union.parquet").id]
    records = {}
    with threadpool_limits(limits=2):
        scores = q @ gallery.T
        for name, selection in selections.items():
            records[name] = evaluate_scores(
                scores[:, selection], queries, [ids[j] for j in selection], [refs[j] for j in selection]
            )
            print("evaluated gallery", name, len(selection), flush=True)
    save_results(
        ROOT / f"development/gallery_{model_name}_{split}",
        query_path,
        queries,
        records,
        {n: len(s) for n, s in selections.items()},
        {
            "arms_sha256": sha256(root / "arms.json"),
            "baseline_descriptors": base_meta["artifact_sha256"],
            "added_descriptors": added_meta["artifact_sha256"],
            "model": model_name,
            "comparison_baseline": "same model with unchanged production gallery; cross-model comparison reported separately",
            "union_sha256": sha256(root / "union.json"),
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--split", choices=["historical", "new"], default="new")
    parser.add_argument("--model", default="sage-vitb", choices=["sage-vitb", "sage-vitl", "boq", "edtformer"])
    args = parser.parse_args()
    run(args.split, args.model)
