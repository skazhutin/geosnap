"""Explicit experimental transfer of full-SAGE context to stronger B/L retrieval.

These are no-encoder L descriptors, not mislabeled official full-SAGE features.
One query and its retrieved references form each context; no query labels or
other evaluation queries enter the encoder.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256
from ml.research.vector_evaluation import development_frame, evaluate_scores, save_results


def run(split="new"):
    query_path = (
        ROOT / "prospective/development.parquet"
        if split == "new"
        else Path("data/evaluation/moscow_real_v4/development_queries.parquet")
    )
    queries = development_frame(query_path)
    gallery_path = ROOT / "gallery_expansion/gallery_union.parquet"
    gallery_frame = pd.read_parquet(gallery_path)
    bundle = ROOT / "preflight/bl_union_v3"
    assembly = json.loads((bundle / "assembly.json").read_text())
    ids = gallery_frame.id.tolist()
    if json.loads((bundle / "assets/candidate_descriptor_ids.json").read_text()) != ids:
        raise ValueError("staged vectors differ from the fixed union gallery")
    matrices, qvectors = {}, {}
    for name in ("sage-vitb", "sage-vitl"):
        path = bundle / f"assets/{name}_union.npy"
        if sha256(path) != assembly["artifacts"][str(path.relative_to(bundle))]:
            raise ValueError("staged reference descriptors changed")
        matrices[name] = np.load(path, mmap_mode="r", allow_pickle=False)
        if name == "sage-vitb" and split == "historical":
            qvectors[name] = np.asarray(
                [
                    np.load(ROOT / f"development/query_views/{row.id}.npz", allow_pickle=False)["descriptors"][0]
                    for row in queries.itertuples()
                ]
            )
            continue
        query_root = ROOT / f"development/global_{name}_{split}"
        contract = json.loads((query_root / "query_contract.json").read_text())
        if contract["contract"]["manifest_sha256"] != sha256(query_path) or contract["descriptors_sha256"] != sha256(
            query_root / "queries.npy"
        ):
            raise ValueError("registered query descriptor cache changed")
        qvectors[name] = np.load(query_root / "queries.npy", allow_pickle=False)
    with threadpool_limits(limits=2):
        base = 0.5 * (qvectors["sage-vitb"] @ matrices["sage-vitb"].T) + 0.5 * (
            qvectors["sage-vitl"] @ matrices["sage-vitl"].T
        )
    checkpoint = Path("data/models/research_v5/sage_context_encoder.pth")
    if sha256(checkpoint) != "b3b49e7aa0e7c848c4c57b2d79660c3654a9946fb9f423f6f0e91681afc414b2":
        raise ValueError("official extracted encoder changed")
    torch.set_num_threads(2)
    encoder = torch.nn.TransformerEncoder(
        torch.nn.TransformerEncoderLayer(
            d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=0.1, batch_first=False
        ),
        num_layers=2,
    )
    encoder.load_state_dict(torch.load(checkpoint, map_location="cpu", weights_only=True), strict=True)
    encoder.eval()
    refs = [{"metadata": r} for r in gallery_frame.to_dict("records")]
    records = {"fusion50": evaluate_scores(base, queries, ids, refs)}
    for depth in (30, 100):
        selected = np.argsort(-base, axis=1, kind="stable")[:, :depth]
        contextual = np.empty((len(queries), depth), dtype=np.float32)
        with torch.inference_mode():
            for i in range(len(queries)):
                features = np.concatenate([qvectors["sage-vitl"][i : i + 1], matrices["sage-vitl"][selected[i]]])
                output = encoder(torch.from_numpy(features).view(depth + 1, 11, 768)).flatten(1)
                output = torch.nn.functional.normalize(output, dim=1)
                contextual[i] = (output[0] @ output[1:].T).numpy()
        for mixing in (0.25, 0.5, 0.75, 1.0):
            current = base.copy()
            for i in range(len(queries)):
                old, new = base[i, selected[i]], contextual[i]
                aligned = (new - new.mean()) / max(float(new.std()), 1e-6) * max(float(old.std()), 1e-6) + old.mean()
                blended = (1 - mixing) * old + mixing * aligned
                floor = float(np.partition(base[i], -depth - 1)[-depth - 1])
                if blended.min() <= floor:
                    blended = blended + (floor - blended.min() + 1e-6)
                current[i, selected[i]] = blended
            name = f"hybrid_context{depth}_mix{mixing}"
            records[name] = evaluate_scores(current, queries, ids, refs)
            print(name, float(np.mean(np.asarray(records[name]["errors"]) <= 100)), flush=True)
    save_results(
        ROOT / f"development/hybrid_context_union_{split}",
        query_path,
        queries,
        records,
        {n: len(ids) for n in records},
        {
            "gallery_manifest_sha256": sha256(gallery_path),
            "encoder_sha256": sha256(checkpoint),
            "source_sha256": sha256(Path(__file__)),
            "kind": "experimental_encoder_transfer_to_noencoder_L_features",
            "context": "one query plus references from fixed B/L cosine retrieval; no other queries or GPS",
            "official_full_checkpoint_claim": False,
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--split", choices=["historical", "new"], default="new")
    run(parser.parse_args().split)
