"""Official SAGE context encoder as a deployable per-query second stage.

The context contains one query and its retrieved references. No other query,
query GPS, or evaluation label is passed into the transformer.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import torch
from threadpoolctl import threadpool_limits

from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256
from ml.research.vector_evaluation import development_frame, evaluate_scores, save_results
from ml.retrieval.embedding_job import load_embedding_artifacts


def run(split):
    query_path = (
        Path("data/evaluation/moscow_real_v4/development_queries.parquet")
        if split == "historical"
        else ROOT / "prospective/development.parquet"
    )
    queries = development_frame(query_path)
    root = ROOT / f"development/global_sage-full-features_{split}"
    contract = json.loads((root / "query_contract.json").read_text())
    if contract["contract"]["manifest_sha256"] != sha256(query_path) or contract["descriptors_sha256"] != sha256(
        root / "queries.npy"
    ):
        raise ValueError("pre-encoder query features changed")
    q = np.load(root / "queries.npy", allow_pickle=False)
    gallery, ids, refs, metadata = load_embedding_artifacts("data/embeddings/moscow_research_v5/sage-full-features")
    checkpoint = Path("data/models/research_v5/sage_context_encoder.pth")
    smoke = json.loads((ROOT / "model_audit/sage_full_smoke.json").read_text())
    if sha256(checkpoint) != smoke["encoder_sha256"]:
        raise ValueError("official extracted context encoder changed")
    torch.set_num_threads(2)
    encoder = torch.nn.TransformerEncoder(
        torch.nn.TransformerEncoderLayer(
            d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=0.1, batch_first=False
        ),
        num_layers=2,
    )
    encoder.load_state_dict(torch.load(checkpoint, map_location="cpu", weights_only=True), strict=True)
    encoder.eval()
    with threadpool_limits(limits=2):
        base = q @ gallery.T
    scores = {"full_preencoder": base}
    for depth in (30, 100):
        contextual = np.empty((len(queries), depth), dtype=np.float32)
        selected = np.argsort(-base, axis=1, kind="stable")[:, :depth]
        with torch.inference_mode():
            for i in range(len(queries)):
                features = np.concatenate([q[i : i + 1], gallery[selected[i]]])
                output = encoder(torch.from_numpy(features).view(depth + 1, 11, 768)).flatten(1)
                output = torch.nn.functional.normalize(output, dim=1)
                contextual[i] = (output[0] @ output[1:].T).numpy()
                if (i + 1) % 100 == 0:
                    print("single-query contexts", depth, i + 1, "/", len(queries), flush=True)
        for mixing in (0.25, 0.5, 0.75, 1.0):
            current = base.copy()
            for i in range(len(queries)):
                old = base[i, selected[i]]
                new = contextual[i]
                aligned = (new - new.mean()) / max(float(new.std()), 1e-6) * max(float(old.std()), 1e-6) + old.mean()
                blended = (1 - mixing) * old + mixing * aligned
                # Rerank this retrieved prefix only; the untouched tail retains
                # its original ordering. Shift the entire prefix above its tail.
                floor = float(np.partition(base[i], -depth - 1)[-depth - 1])
                if blended.min() <= floor:
                    blended = blended + (floor - blended.min() + 1e-6)
                current[i, selected[i]] = blended
            scores[f"context{depth}_mix{mixing}"] = current
    records = {name: evaluate_scores(values, queries, ids, refs) for name, values in scores.items()}
    save_results(
        ROOT / f"development/context_{split}",
        query_path,
        queries,
        records,
        {name: len(ids) for name in records},
        {
            "encoder_sha256": sha256(checkpoint),
            "gallery_sha256": metadata["artifact_sha256"]["descriptors.npy"],
            "context": "one query plus retrieved references; no other queries or GPS",
            "score_rule": "context similarities aligned to prefix global-score mean/std; blend and preserve prefix membership",
            "comparison_baseline": "official full checkpoint pre-encoder retrieval; frozen production reported separately",
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--split", choices=["historical", "new"], default="historical")
    run(parser.parse_args().split)
