"""Geographically embargoed OOF reranking of complementary SAGE resolutions."""
from __future__ import annotations

import json
import time

import h3
import numpy as np
import torch
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from ml.research import gallery_scale, night_scale_context as context, night_v7
from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.baseline import staged_entries
from ml.research.geographic_v8.common import LOCAL, compare, frames, metrics, registry
from ml.research.geographic_v8.stage import STAGE
from ml.research.vector_evaluation import distances

OUT = LOCAL / "oof_union_rerank"
FEATURE_NAMES = ["cos322", "cos504", "mean_cos", "gap322", "gap504", "gap_mean",
    "rank322_log", "rank504_log", "baseline_rank_log", "in322", "in504", "in_baseline",
    "current_winner", "context_delta", "has_context", "distance_to_322_top1_logm",
    "distance_to_504_top1_logm", "distance_to_baseline_top1_logm", "branch_disagreement_logm",
    "support_100m_log", "independent_sequence_support_100m_log",
    "mapillary", "kartaview", "msls"]


def distance_matrix_m(xy):
    rad = np.radians(xy)
    lat = rad[:, 0]
    lon = rad[:, 1]
    dlat = lat[:, None] - lat[None, :]
    dlon = lon[:, None] - lon[None, :]
    a = np.sin(dlat/2)**2 + np.cos(lat[:, None])*np.cos(lat[None, :])*np.sin(dlon/2)**2
    return 6371008.8 * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def make_candidates(base, a, b):
    rows = [list(dict.fromkeys(np.concatenate((base[i], a[i], b[i])).tolist())) for i in range(len(base))]
    if any(len(row) < 100 or len(row) > 300 for row in rows):
        raise RuntimeError("Unexpected SAGE union cardinality")
    offsets = np.r_[0, np.cumsum([len(row) for row in rows])]
    return rows, offsets


def extract():
    """Inference feature boundary: gallery/reference GPS only; no query GT."""
    q, g = frames()
    with np.load(LOCAL / "baseline/predictions.npz", allow_pickle=False) as base:
        base_rank = base["indices"].copy()
        if base["query_ids"].tolist() != q.id.tolist():
            raise RuntimeError("SAGE baseline query identity mismatch")
    paths = [LOCAL / f"resolution_union/{n}.npz" for n in ("322", "504")]
    with np.load(paths[0], allow_pickle=False) as x:
        rank322, scores322_top = x["indices"].copy(), x["scores"].copy()
    with np.load(paths[1], allow_pickle=False) as x:
        rank504, scores504_top = x["indices"].copy(), x["scores"].copy()
    rows, offsets = make_candidates(base_rank, rank322, rank504)
    flat = np.concatenate([np.asarray(r, np.int32) for r in rows])
    q322 = np.load(STAGE / "query322.npy", allow_pickle=False)
    parts = []
    for path in sorted((STAGE / "query504").glob("chunk-*.npz")):
        with np.load(path, allow_pickle=False) as block:
            offset = sum(len(p) for p in parts)
            if block["ids"].tolist() != q.id.iloc[offset:offset+len(block["ids"])].tolist():
                raise RuntimeError("504 query cache order changed")
            parts.append(block["vectors"])
    q504 = np.concatenate(parts)
    pool = staged_entries(g)
    cosine = []
    for name, query in (("322", q322), ("504", q504)):
        started = time.perf_counter()
        scores = gallery_scale.exact_scores(g, query, pool)
        selected = np.concatenate([scores[i, row] for i, row in enumerate(rows)]).astype(np.float32)
        cosine.append(selected)
        print("candidate cosine", name, len(selected), "runtime", time.perf_counter()-started, flush=True)
        del scores
    cos322, cos504 = cosine
    mean = .5 * (cos322 + cos504)
    original = np.load(STAGE / "mean_scores.npy", mmap_mode="r")
    expected = np.concatenate([original[i, row] for i, row in enumerate(rows)])
    np.testing.assert_allclose(mean, expected, atol=2e-6, rtol=1e-5)
    with np.load(LOCAL / "baseline/context.npz", allow_pickle=False) as ctx:
        chosen = ctx["chosen"].copy()
        blend = night_v7.align_blend(ctx["original"], ctx["contextual"], .5)
        delta = blend - ctx["original"]
    coords = g[["lat", "lon"]].to_numpy()
    source = g.source.astype(str).to_numpy()
    sequence = g.sequence_key.astype(str).to_numpy()
    x = np.empty((len(flat), len(FEATURE_NAMES)), np.float32)
    for i, row in enumerate(rows):
        start, stop = offsets[i:i+2]
        arr = np.asarray(row, dtype=int)
        n = len(arr)
        r0 = {int(value): pos for pos, value in enumerate(base_rank[i])}
        r1 = {int(value): pos for pos, value in enumerate(rank322[i])}
        r2 = {int(value): pos for pos, value in enumerate(rank504[i])}
        cr = {int(value): pos for pos, value in enumerate(chosen[i])}
        ds = distance_matrix_m(coords[arr])
        first = {v: int(np.flatnonzero(arr == v)[0]) for v in (base_rank[i, 0], rank322[i, 0], rank504[i, 0])}
        disagree = ds[first[rank322[i, 0]], first[rank504[i, 0]]]
        seq = sequence[arr]
        src = source[arr]
        for j, cand in enumerate(row):
            ix = start+j
            in_context = cand in cr
            nearby = np.flatnonzero(ds[j] <= 100)
            x[ix] = [cos322[ix], cos504[ix], mean[ix],
                scores322_top[i, 0]-cos322[ix], scores504_top[i, 0]-cos504[ix],
                max(mean[start:stop])-mean[ix],
                np.log1p(r1.get(cand, 101)), np.log1p(r2.get(cand, 101)),
                np.log1p(r0.get(cand, 101)), float(cand in r1), float(cand in r2),
                float(cand in r0), float(cand == base_rank[i, 0]),
                float(delta[i, cr[cand]]) if in_context else 0., float(in_context),
                np.log1p(ds[j, first[rank322[i, 0]]]),
                np.log1p(ds[j, first[rank504[i, 0]]]),
                np.log1p(ds[j, first[base_rank[i, 0]]]),
                np.log1p(disagree), np.log1p(len(nearby)),
                np.log1p(len(set(seq[nearby]))),
                float(src[j] == "mapillary"), float(src[j] == "kartaview"), float(src[j] == "msls")]
        if i % 160 == 0:
            print("feature queries", i+1, "/", len(rows), flush=True)
    if not np.isfinite(x).all():
        raise RuntimeError("Nonfinite inference feature")
    OUT.mkdir(exist_ok=True)
    np.savez(OUT / "features.npz", query_ids=q.id.to_numpy(str), indices=flat,
             offsets=offsets, features=x, feature_names=np.asarray(FEATURE_NAMES, dtype=str))
    save(OUT / "feature_contract.json", {"feature_names": FEATURE_NAMES,
        "query_ground_truth_in_features": False,
        "candidate_sources": [str(LOCAL / "baseline/predictions.npz"), *map(str, paths)],
        "source_hashes": {str(p): digest(p) for p in [LOCAL / "baseline/predictions.npz", *paths,
                          LOCAL / "baseline/context.npz", STAGE / "mean_scores.npy"]},
        "feature_sha256": digest(OUT / "features.npz"),
        "reference_gps_used": True, "query_gps_used": False})
    print("features complete", x.shape, flush=True)


def fit_oof():
    q, g = frames()
    contract = json.loads((OUT / "feature_contract.json").read_text())
    if digest(OUT / "features.npz") != contract["feature_sha256"]:
        raise RuntimeError("Feature cache changed")
    with np.load(OUT / "features.npz", allow_pickle=False) as cache:
        if cache["query_ids"].tolist() != q.id.tolist() or cache["feature_names"].tolist() != FEATURE_NAMES:
            raise RuntimeError("Feature identity/schema mismatch")
        flat, offsets, x = cache["indices"], cache["offsets"], cache["features"]
    coords = g[["lat", "lon"]].to_numpy()
    labels = np.empty(len(flat), bool)
    for i, row in enumerate(q.itertuples()):
        start, stop = offsets[i:i+2]
        xy = coords[flat[start:stop]]
        labels[start:stop] = distances(row.lat, row.lon, np.radians(xy[:, 0]), np.radians(xy[:, 1])) <= 100
    np.savez(OUT / "training_labels.npz", query_ids=q.id.to_numpy(str), candidate_rows=flat, positive_100m=labels)
    group = q.evaluation_geo_group_id.astype(str).to_numpy()
    fold_rows = []
    variants = {
        "logistic": lambda: make_pipeline(StandardScaler(), LogisticRegression(C=1, max_iter=300,
                                                                solver="lbfgs", random_state=20260928)),
        "shallow_boosted": lambda: HistGradientBoostingClassifier(max_iter=120, max_leaf_nodes=7,
            min_samples_leaf=100, l2_regularization=1., learning_rate=.05, early_stopping=False,
            random_state=20260928)}
    predicted = {name: np.full(len(q), -1, np.int64) for name in variants}
    for fold, (train_base, test) in enumerate(GroupKFold(n_splits=5).split(q, groups=group)):
        banned = set().union(*(set(h3.grid_disk(value, 1)) for value in set(group[test])))
        train = np.asarray([i for i in train_base if group[i] not in banned], int)
        if len(train) < 100 or set(group[train]) & banned:
            raise RuntimeError("Insufficient geographically embargoed fit population")
        train_rows = np.concatenate([np.arange(offsets[i], offsets[i+1]) for i in train])
        weights = np.empty(len(train_rows), np.float32)
        cursor = 0
        for i in train:
            lo, hi = offsets[i:i+2]
            positive = labels[lo:hi]
            count_pos, count_neg = int(positive.sum()), int((~positive).sum())
            weights[cursor:cursor+len(positive)] = np.where(positive,
                1/max(count_pos, 1), 1/max(count_neg, 1))
            cursor += len(positive)
        fold_rows.append({"fold": fold, "train_queries": len(train), "test_queries": len(test),
            "train_geo_groups": len(set(group[train])), "test_geo_groups": len(set(group[test])),
            "embargo": "one H3 ring at evaluation_geo_group_id resolution",
            "test_query_ids": q.id.iloc[test].tolist()})
        for name, create in variants.items():
            model = create()
            model.fit(x[train_rows], labels[train_rows], **({"logisticregression__sample_weight": weights}
                if name == "logistic" else {"sample_weight": weights}))
            for i in test:
                start, stop = offsets[i:i+2]
                probs = model.predict_proba(x[start:stop])[:, 1]
                predicted[name][i] = flat[start+int(np.argmax(probs))]
            print("OOF", name, "fold", fold, "train", len(train), "test", len(test), flush=True)
    save(OUT / "folds.json", {"folds": fold_rows, "grouping": "H3 geographic groups",
        "sequence_group_overlap": 0, "one_ring_embargo": True, "parameter_selection": "none: two fixed preregistered simple learners"})
    for name, selected in predicted.items():
        if (selected < 0).any():
            raise RuntimeError("Missing OOF prediction")
        report, errors, _ = metrics(q, g, selected[:, None])
        paired = compare(q, errors)
        report |= paired | {"variant": name, "scope": "exploratory geographically embargoed OOF",
            "feature_contract_sha256": digest(OUT / "feature_contract.json"),
            "folds_sha256": digest(OUT / "folds.json"), "fitted_on_query_labels": True,
            "candidate_generation": "union of fixed SAGE baseline, 322 and 504 top100"}
        np.savez(OUT / f"{name}_oof.npz", query_ids=q.id.to_numpy(str), indices=selected,
                 prediction_gps=coords[selected], errors_m=errors)
        save(OUT / f"{name}_oof.json", report)
        registry(f"sage_resolution_union_{name}_oof", {"model_revision": "fixed SAGE-L plus learned simple reranker",
            "preprocessing": "reference322 query322/504", "candidate_generation": report["candidate_generation"],
            "reranking": f"{name} candidate correctness probability", "fusion": "candidate union and query-only features",
            "hyperparameters": {"logistic": {"C": 1}, "shallow_boosted": {"max_iter": 120,
                "max_leaf_nodes": 7, "min_samples_leaf": 100}}[name],
            "fitted_parameters": True, "fitting_split": "5 outer geographic folds with one-ring embargo",
            "evaluation_split": "1184 OOF predictions, each query held out with geographic embargo",
            "raw25": report["raw"]["accuracy_25m"], "raw50": report["raw"]["accuracy_50m"],
            "raw100": report["raw"]["accuracy_100m"], "r_at_k": None,
            "median_m": report["raw"]["median_error_m"], "p90_m": report["raw"]["p90_error_m"],
            "gt500_rate": report["raw"]["catastrophic_gt500m_rate"], "runtime": "see OOF logs",
            "result_status": "complete", "decision": "compare paired CI; exploratory only",
            "artifact_paths": [str(OUT / f"{name}_oof.npz"), str(OUT / f"{name}_oof.json")],
            "artifact_hashes": {f: digest(OUT / f) for f in (f"{name}_oof.npz", f"{name}_oof.json")},
            "paired_bootstrap": paired["paired_geographic_bootstrap"]})
        print(name, "OOF RAW100", int((errors <= 100).sum()), "transition", paired["transitions"], flush=True)


if __name__ == "__main__":
    torch.set_num_threads(2)
    if not (OUT / "feature_contract.json").exists():
        extract()
    fit_oof()
