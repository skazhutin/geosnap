"""Reference-only residual metric learning, with development-only evaluation.

All supervision comes from the frozen production gallery: different capture
sequences within 25 m (heading difference <=60 degrees when known), and visually
hard negatives at least 200 m away. Queries never enter training or mining.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from sklearn.neighbors import BallTree
from threadpoolctl import threadpool_limits

from ml.research.prepare_queries import ROOT, score
from ml.research.seal import sha256, write_once
from ml.research.vector_evaluation import development_frame, distances, evaluate_scores, save_results
from ml.retrieval.embedding_job import load_embedding_artifacts

GALLERY = Path("data/evaluation/moscow_real_v4/gallery.parquet")
GALLERY_HASH = "ff7cbc7e7147226e5aed41c4ccb1048d4bc17fec965c7fc5e0cb94e5bb1c35da"


class ResidualMetric(torch.nn.Module):
    def __init__(self, dim, rank=64):
        super().__init__()
        self.down = torch.nn.Linear(dim, rank, bias=False)
        self.up = torch.nn.Linear(rank, dim, bias=False)
        torch.nn.init.zeros_(self.up.weight)

    def forward(self, x, strength=1.0):
        residual = self.up(torch.nn.functional.gelu(self.down(x)))
        return torch.nn.functional.normalize(x + strength * residual, dim=-1), residual


def references(model):
    if sha256(GALLERY) != GALLERY_HASH:
        raise ValueError("fixed reference-only training population changed")
    path = Path("data/embeddings/moscow_research_v5") / model
    matrix, ids, refs, metadata = load_embedding_artifacts(path)
    frame = pd.read_parquet(GALLERY).set_index("id").loc[ids]
    return matrix, ids, refs, metadata, frame


def training_pairs(frame, matrix, ids):
    coords = np.radians(frame[["lat", "lon"]].to_numpy(float))
    neighbors = BallTree(coords, metric="haversine").query_radius(coords, r=25 / 6371008.8)
    sequence = (frame.source.astype(str) + "::" + frame.sequence_id.astype(str)).to_numpy()
    heading = frame.heading.to_numpy(float)
    positive = {}
    for i, nearby in enumerate(neighbors):
        nearby = nearby[sequence[nearby] != sequence[i]]
        difference = np.abs(heading[nearby] - heading[i]) % 360
        nearby = nearby[(~np.isfinite(difference)) | (np.minimum(difference, 360 - difference) <= 60)]
        if len(nearby):
            positive[i] = nearby.tolist()
    # Equal area rounds keep prolific captures from owning the training batch.
    ordered = sorted(positive, key=lambda i: score("metric-anchor", ids[i]))
    ranks, rank = {}, {}
    for i in ordered:
        cell = str(frame.iloc[i].h3_coarse)
        ranks[i] = rank.get(cell, 0)
        rank[cell] = ranks[i] + 1
    anchors = sorted(ordered, key=lambda i: (ranks[i], score("metric-anchor", ids[i])))[:4096]
    if len(anchors) < 128:
        raise ValueError("insufficient independent reference positive pairs")
    negatives = {}
    rng = np.random.default_rng(20260906)
    for start in range(0, len(anchors), 64):
        batch = anchors[start : start + 64]
        # Exact cosine mining through BLAS avoids the host's conflicting FAISS
        # and PyTorch OpenMP runtimes within a training process.
        retrieved = np.argsort(-(matrix[batch] @ matrix.T), axis=1, kind="stable")[:, :200]
        for anchor, match in zip(batch, retrieved, strict=True):
            d = distances(frame.iloc[anchor].lat, frame.iloc[anchor].lon, coords[:, 0], coords[:, 1])
            hard = match[d[match] >= 200][:8].tolist()
            if len(hard) < 8:
                remaining = np.setdiff1d(np.flatnonzero(d >= 200), hard)
                hard.extend(rng.choice(remaining, size=8 - len(hard), replace=False).tolist())
            negatives[anchor] = hard
        if start % 512 == 0:
            print("mined reference anchors", start, "/", len(anchors), flush=True)
    return {
        "anchors": anchors,
        "positive": {str(i): positive[i] for i in anchors},
        "negative": {str(i): negatives[i] for i in anchors},
        "eligible_anchor_count": len(positive),
    }


def train(model_name):
    output = ROOT / f"development/reference_adaptation_{model_name}"
    output.mkdir(parents=True, exist_ok=True)
    matrix, ids, _, metadata, frame = references(model_name)
    contract = {
        "gallery_manifest_sha256": GALLERY_HASH,
        "gallery_artifacts": metadata["artifact_sha256"],
        "code_sha256": sha256(Path(__file__)),
        "rank": 64,
        "steps": [100, 300, 600],
        "batch": 64,
        "temperature": 0.07,
        "learning_rate": 0.001,
        "residual_penalty": 5.0,
        "training_queries_used": False,
        "positive": "different sequence <=25m, known heading difference <=60deg",
        "negative": "eight base-embedding hard negatives >=200m; random far fallback",
        "seed": 20260906,
    }
    fixed = output / "training_contract.json"
    if fixed.exists():
        if json.loads(fixed.read_text()) != contract:
            raise ValueError("training contract changed; do not reuse cached learned parameters")
        if (output / "trained.json").exists():
            receipt = json.loads((output / "trained.json").read_text())
            for name, digest in receipt["checkpoints"].items():
                if sha256(output / name) != digest:
                    raise ValueError("trained checkpoint changed")
            return output
    else:
        write_once(fixed, contract)
    mined = output / "training_pairs.json"
    if mined.exists():
        pairs = json.loads(mined.read_text())
    else:
        pairs = training_pairs(frame, matrix, ids)
        write_once(mined, pairs)
    torch.manual_seed(20260906)
    rng = np.random.default_rng(20260906)
    model = ResidualMetric(matrix.shape[1])
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.001, weight_decay=0.01)
    history, checkpoints = [], {}
    for step in range(1, 601):
        anchors = rng.choice(pairs["anchors"], size=64, replace=False)
        selected = [[int(i), int(rng.choice(pairs["positive"][str(i)])), *pairs["negative"][str(i)]] for i in anchors]
        x = torch.from_numpy(np.asarray(matrix[selected]))
        vectors, residual = model(x)
        logits = torch.einsum("bd,bkd->bk", vectors[:, 0], vectors[:, 1:]) / 0.07
        loss = torch.nn.functional.cross_entropy(logits, torch.zeros(len(x), dtype=torch.long))
        penalty = residual.square().sum(-1).mean()
        objective = loss + 5.0 * penalty
        optimizer.zero_grad()
        objective.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        if step % 50 == 0:
            row = {
                "step": step,
                "reference_contrastive_loss": float(loss.detach()),
                "residual_norm2": float(penalty.detach()),
            }
            history.append(row)
            print(row, flush=True)
        if step in [100, 300, 600]:
            path = output / f"step-{step}.pth"
            torch.save(model.state_dict(), path)
            checkpoints[path.name] = sha256(path)
    write_once(
        output / "trained.json",
        {
            "checkpoints": checkpoints,
            "history": history,
            "contract_sha256": sha256(fixed),
            "pairs_sha256": sha256(mined),
            "queries_used": False,
        },
    )
    return output


def transform(model, matrix, strength):
    out = np.empty(matrix.shape, np.float32)
    with torch.inference_mode():
        for start in range(0, len(matrix), 128):
            values, _ = model(torch.from_numpy(np.array(matrix[start : start + 128])), strength)
            out[start : start + len(values)] = values.numpy()
    return out


def evaluate(model_name, trained, split):
    query_path = (
        Path("data/evaluation/moscow_real_v4/development_queries.parquet")
        if split == "historical"
        else ROOT / "prospective/development.parquet"
    )
    queries = development_frame(query_path)
    cache = ROOT / f"development/global_{model_name}_{split}"
    contract = json.loads((cache / "query_contract.json").read_text())
    if contract["contract"]["manifest_sha256"] != sha256(query_path) or contract["descriptors_sha256"] != sha256(
        cache / "queries.npy"
    ):
        raise ValueError("development query descriptors changed")
    q = np.load(cache / "queries.npy", allow_pickle=False)
    gallery, ids, refs, _, _ = references(model_name)
    records = {"unadapted": evaluate_scores(q @ gallery.T, queries, ids, refs)}
    for step in [100, 300, 600]:
        model = ResidualMetric(gallery.shape[1])
        model.load_state_dict(
            torch.load(trained / f"step-{step}.pth", map_location="cpu", weights_only=True), strict=True
        )
        model.eval()
        for strength in [0.25, 0.5, 1.0]:
            adapted = transform(model, gallery, strength)
            scores = transform(model, q, strength) @ adapted.T
            name = f"step{step}_strength{strength}"
            records[name] = evaluate_scores(scores, queries, ids, refs)
            del adapted, scores
            print("evaluated", split, name, flush=True)
    save_results(
        ROOT / f"development/reference_adapted_{model_name}_{split}",
        query_path,
        queries,
        records,
        {name: len(ids) for name in records},
        {
            "training_receipt_sha256": sha256(trained / "trained.json"),
            "queries_in_training": False,
            "comparison": "same base model and gallery; true production comparison separate",
        },
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=["sage-vitl", "boq", "edtformer"], default="sage-vitl")
    args = parser.parse_args()
    torch.set_num_threads(2)
    with threadpool_limits(limits=2):
        output = train(args.model)
        for split in ["historical", "new"]:
            evaluate(args.model, output, split)


if __name__ == "__main__":
    main()
