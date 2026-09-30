"""Exact frozen FAISS baseline on registered development, including complete ranks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import faiss
import numpy as np
import pandas as pd

from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256
from ml.research.vector_evaluation import development_frame, distances, save_results


def run(split):
    path = (
        Path("data/evaluation/moscow_real_v4/development_queries.parquet")
        if split == "historical"
        else ROOT / "prospective/development.parquet"
    )
    frame = development_frame(path)
    if split == "historical":
        vectors = np.asarray(
            [
                np.load(ROOT / f"development/query_views/{r.id}.npz", allow_pickle=False)["descriptors"][0]
                for r in frame.itertuples()
            ]
        )
    else:
        cache = ROOT / "development/global_sage-vitb_new"
        contract = json.loads((cache / "query_contract.json").read_text())
        if contract["contract"]["manifest_sha256"] != sha256(path) or contract["descriptors_sha256"] != sha256(
            cache / "queries.npy"
        ):
            raise ValueError("query cache changed")
        vectors = np.load(cache / "queries.npy", allow_pickle=False)
    root = Path("data/indexes/moscow_real_v4/sage-vitb")
    index_path = root / "index.faiss"
    if sha256(index_path) != "cda9739cc7269881ece33f70de9c4b6bfb027b38150bd6c228fd736cc4270ad2":
        raise ValueError("frozen production FAISS changed")
    index = faiss.read_index(str(index_path))
    faiss.omp_set_num_threads(2)
    ids = [r["reference_id"] for r in json.loads((root / "id_mapping.json").read_text())]
    gallery = pd.read_parquet("data/evaluation/moscow_real_v4/gallery.parquet").set_index("id").loc[ids]
    lat = np.radians(gallery.lat.to_numpy())
    lon = np.radians(gallery.lon.to_numpy())
    record = {"errors": [], "ranks": [], "top100": []}
    for start in range(0, len(frame), 16):
        scores, order = index.search(vectors[start : start + 16], len(ids))
        for j, row in enumerate(frame.iloc[start : start + 16].itertuples()):
            d = distances(row.lat, row.lon, lat, lon)
            positive = np.flatnonzero(d[order[j]] <= 100)
            record["errors"].append(float(d[order[j, 0]]))
            record["ranks"].append(int(positive[0]) + 1 if len(positive) else None)
            record["top100"].append({"ids": [ids[i] for i in order[j, :100]], "scores": scores[j, :100].tolist()})
        if start % 160 == 0:
            print("exact baseline queries", start, "/", len(frame), flush=True)
    save_results(
        ROOT / f"development/exact_faiss_baseline_{split}",
        path,
        frame,
        {"sage": record},
        {"sage": len(ids)},
        {
            "index_sha256": sha256(index_path),
            "query_descriptor_source": "verified frozen SAGE checkpoint and preprocessing",
            "ranking": "production FAISS index.search with complete gallery K; no numpy ordering approximation",
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--split", choices=["historical", "new"], default="new")
    run(parser.parse_args().split)
