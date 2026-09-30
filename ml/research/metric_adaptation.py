"""Geographically out-of-fold supervised query descriptor adaptation.

A regularized dual linear map learns query-to-positive-reference descriptors.
Only training-fold query GPS constructs targets. Held-out GPS is used solely
by scoring after the descriptors have been produced. Hyperparameter results
are exploratory development comparisons, never final-test claims.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
from sklearn.model_selection import GroupKFold
from threadpoolctl import threadpool_limits

from ml.research.database_augmentation import normalize
from ml.research.prepare_queries import ROOT
from ml.research.vector_evaluation import development_frame, distances, evaluate_scores, save_results
from ml.retrieval.embedding_job import load_embedding_artifacts


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
    lat = np.radians([r["metadata"]["lat"] for r in refs])
    lon = np.radians([r["metadata"]["lon"] for r in refs])
    settings = [(regularization, mix) for regularization in (0.01, 0.1, 1.0, 10.0) for mix in (0.1, 0.25, 0.5)]
    transformed = {f"ridge{reg}_mix{mix}": np.empty_like(q) for reg, mix in settings}
    folds = np.empty(len(queries), dtype=int)
    groups = queries.h3_coarse.astype(str).to_numpy()
    with threadpool_limits(limits=2):
        for fold, (train, valid) in enumerate(GroupKFold(5).split(q, groups=groups)):
            folds[valid] = fold
            targets = []
            for i in train:
                row = queries.iloc[i]
                d = distances(row.lat, row.lon, lat, lon)
                positive = np.flatnonzero(d <= 100)
                if not len(positive):
                    raise ValueError("this historical adaptation probe expects covered training queries")
                # Closest in appearance among genuine spatial positives, then average up to three.
                order = positive[np.argsort(-(q[i] @ gallery[positive].T), kind="stable")[:3]]
                targets.append(normalize(gallery[order].mean(axis=0)))
            targets = np.asarray(targets)
            gram = q[train] @ q[train].T
            cross = q[valid] @ q[train].T
            for reg in (0.01, 0.1, 1.0, 10.0):
                coefficient = np.linalg.solve(gram + reg * np.eye(len(train)), targets).astype(np.float32)
                prediction = normalize(cross @ coefficient)
                for mix in (0.1, 0.25, 0.5):
                    transformed[f"ridge{reg}_mix{mix}"][valid] = normalize((1 - mix) * q[valid] + mix * prediction)
            print("finished geographic fold", fold, flush=True)
        records = {"sage": evaluate_scores(q @ gallery.T, queries, ids, refs)}
        for name, vector in transformed.items():
            records[name] = evaluate_scores(vector @ gallery.T, queries, ids, refs)
            print("evaluated", name, flush=True)
    save_results(
        ROOT / "development/metric_adaptation",
        path,
        queries,
        records,
        {name: len(ids) for name in records},
        {
            "gallery_sha256": metadata["artifact_sha256"]["descriptors.npy"],
            "fold_assignment": folds.tolist(),
            "method": "five geographic group folds; targets constructed on training rows only; linear ridge query map",
            "hyperparameters": "all reported as exploratory development; no claim of nested estimate after winner selection",
        },
    )


if __name__ == "__main__":
    run()
