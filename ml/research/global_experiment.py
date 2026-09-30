"""Pinned development global-retriever and complementary-fusion experiments."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import asdict
from pathlib import Path

import numpy as np
import torch
from threadpoolctl import threadpool_limits

from ml.research.metrics import paired_group_bootstrap, raw_metrics, retrieval_metrics
from ml.research.prepare_queries import ROOT
from ml.research.retrievers import research_retriever
from ml.research.seal import sha256
from ml.research.vector_evaluation import development_frame
from ml.retrieval.embedding_job import load_embedding_artifacts


def embed_queries(model, query_path, output):
    queries = development_frame(query_path)
    metadata = asdict(model.metadata)
    metadata.pop("device", None)
    if "extra" in metadata:
        metadata["extra"].pop("device", None)
    contract = {
        "manifest_sha256": sha256(query_path),
        "retriever": metadata,
        "image_sha256": [sha256(Path(p)) for p in queries.image_path],
    }
    signature = hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()
    vector_path = output / "queries.npy"
    contract_path = output / "query_contract.json"
    if vector_path.exists():
        saved_contract = json.loads(contract_path.read_text())
        if saved_contract["signature"] != signature:
            raise ValueError("query descriptor cache no longer matches its input contract")
        if sha256(vector_path) != saved_contract["descriptors_sha256"]:
            raise ValueError("query descriptor cache bytes changed")
        return queries, np.load(vector_path, allow_pickle=False)
    model.load()
    vectors = []
    for start in range(0, len(queries), 12):
        vectors.append(model.embed_batch(queries.image_path.iloc[start : start + 12].tolist()))
        if start % 120 == 0:
            print("embedded queries", start, "/", len(queries), flush=True)
    vectors = np.concatenate(vectors)
    np.save(vector_path, vectors)
    contract_path.write_text(
        json.dumps({"signature": signature, "contract": contract, "descriptors_sha256": sha256(vector_path)}, indent=2)
        + "\n"
    )
    model.close()
    return queries, vectors


def run(model_name, split, device="mps"):
    if split not in {"historical", "new"}:
        raise ValueError("development splits only")
    query_path = (
        Path("data/evaluation/moscow_real_v4/development_queries.parquet")
        if split == "historical"
        else ROOT / "prospective/development.parquet"
    )
    if split == "new" and not (ROOT / "prospective/seal.json").exists():
        raise ValueError("new benchmark must be sealed before development inference")
    output = ROOT / f"development/global_{model_name}_{split}"
    output.mkdir(parents=True, exist_ok=True)
    torch.set_num_threads(2)
    os.environ.setdefault("HF_HOME", str(Path(".cache/huggingface").resolve()))
    model = research_retriever(
        model_name, device=device, allow_device_fallback=False, batch_size=4, cache_dir=".cache/torch/hub"
    )
    queries, qvectors = embed_queries(model, query_path, output)
    gallery_path = (
        Path("data/embeddings/moscow_research_v5") / model_name
        if model_name in {"edtformer", "sage-vitl", "boq", "sage-full-features"}
        else Path("data/embeddings/moscow_real_v4") / model_name
    )
    gallery, ids, refs, gmeta = load_embedding_artifacts(gallery_path)
    sage, sage_ids, _, smeta = load_embedding_artifacts("data/embeddings/moscow_real_v4/sage-vitb")
    lookup = {identity: i for i, identity in enumerate(sage_ids)}
    sage = sage[[lookup[identity] for identity in ids]]
    if split == "historical":
        sq = np.asarray(
            [
                np.load(ROOT / f"development/query_views/{r.id}.npz", allow_pickle=False)["descriptors"][0]
                for r in queries.itertuples()
            ]
        )
    else:
        sage_query_path = ROOT / "development/global_sage-vitb_new/queries.npy"
        if model_name == "sage-vitb":
            sq = qvectors
        elif sage_query_path.exists():
            saved = json.loads(sage_query_path.with_name("query_contract.json").read_text())
            if saved["contract"]["manifest_sha256"] != sha256(query_path) or saved["descriptors_sha256"] != sha256(
                sage_query_path
            ):
                raise ValueError("SAGE fusion query cache changed")
            sq = np.load(sage_query_path, allow_pickle=False)
        else:
            raise ValueError("run new-development SAGE baseline first")
    latitude = np.radians([r["metadata"]["lat"] for r in refs])
    longitude = np.radians([r["metadata"]["lon"] for r in refs])
    groups = queries.h3_coarse.astype(str).tolist()
    records = {
        name: {"errors": [], "ranks": [], "top100": []}
        for name in ["sage", model_name, "fusion25", "fusion50", "fusion75", "zscore50", "rrf10", "rrf60"]
    }
    with threadpool_limits(limits=2):
        for i, row in enumerate(queries.itertuples()):
            a, b = np.radians([row.lat, row.lon])
            distance = (
                2
                * 6371008.8
                * np.arcsin(
                    np.sqrt(
                        np.clip(
                            np.sin((latitude - a) / 2) ** 2
                            + np.cos(latitude) * np.cos(a) * np.sin((longitude - b) / 2) ** 2,
                            0,
                            1,
                        )
                    )
                )
            )
            first = sq[i] @ sage.T
            second = qvectors[i] @ gallery.T
            scores = {
                "sage": first,
                model_name: second,
                "fusion25": 0.75 * first + 0.25 * second,
                "fusion50": 0.5 * first + 0.5 * second,
                "fusion75": 0.25 * first + 0.75 * second,
                "zscore50": (first - first.mean()) / first.std() + (second - second.mean()) / second.std(),
            }
            ranks = []
            for sim in [first, second]:
                order = np.argsort(-sim, kind="stable")
                r = np.empty(len(order))
                r[order] = np.arange(1, len(order) + 1)
                ranks.append(r)
            for k in [10, 60]:
                scores[f"rrf{k}"] = 1 / (k + ranks[0]) + 1 / (k + ranks[1])
            for name, values in scores.items():
                order = np.argsort(-values, kind="stable")
                positive = np.flatnonzero(distance[order] <= 100)
                records[name]["errors"].append(float(distance[order[0]]))
                records[name]["ranks"].append(int(positive[0]) + 1 if len(positive) else None)
                records[name]["top100"].append(
                    {"ids": [ids[j] for j in order[:100]], "scores": values[order[:100]].tolist()}
                )
            if (i + 1) % 100 == 0:
                print("retrieved", i + 1, flush=True)
    result = {
        "kind": "development_only_global_retrieval",
        "split": split,
        "query_manifest_sha256": sha256(query_path),
        "candidate_gallery_sha256": gmeta["artifact_sha256"]["descriptors.npy"],
        "sage_gallery_sha256": smeta["artifact_sha256"]["descriptors.npy"],
        "metrics": {
            name: {
                "raw_top1": raw_metrics(record["errors"]),
                "retrieval": retrieval_metrics(record["ranks"], gallery_size=len(ids)),
                "paired_gain_vs_sage_top1": paired_group_bootstrap(records["sage"]["errors"], record["errors"], groups),
            }
            for name, record in records.items()
        },
    }
    (output / "summary.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    (output / "retrieval.json").write_text(
        json.dumps({"query_ids": queries.id.tolist(), "methods": records}, allow_nan=False) + "\n"
    )
    print(json.dumps({name: r["raw_top1"] for name, r in result["metrics"].items()}, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="edtformer")
    parser.add_argument("--split", default="historical", choices=["historical", "new"])
    parser.add_argument("--device", default="mps", choices=["mps", "cpu"])
    args = parser.parse_args()
    run(args.model, args.split, args.device)
