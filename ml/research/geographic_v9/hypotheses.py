"""Build location hypotheses from frozen same-gallery rankings.

This module is an inference boundary: it reads reference coordinates and
prediction artifacts, never query coordinates, outcomes, or failure buckets.
"""
from __future__ import annotations

import time

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v9.common import OUT, V8, frames, record

SOURCES = {
    "sage": "baseline/predictions.npz",
    "sage322": "resolution_union/322.npz",
    "sage504": "resolution_union/504.npz",
    "g3": "g3/g3_full/predictions.npz",
    "geoclip": "geoclip/geoclip_full/predictions.npz",
    "adapted_sage": "sage_adaptation_v3/sage_asymmetric_v3_refonly_epoch2_primary/predictions.npz",
}
NAMES = tuple(SOURCES)
RADIUS_M = 75.0
MAX_CHALLENGERS = 8
FEATURES = [
    "anchor_rrf", "challenger_rrf", "rrf_difference", "anchor_models",
    "challenger_models", "model_support_difference", "anchor_sage_rank",
    "challenger_sage_rank", "anchor_322_rank", "challenger_322_rank",
    "anchor_504_rank", "challenger_504_rank", "anchor_g3_rank",
    "challenger_g3_rank", "anchor_geoclip_rank", "challenger_geoclip_rank",
    "anchor_adapted_rank", "challenger_adapted_rank", "anchor_mean_cos",
    "challenger_mean_cos", "cos_difference", "anchor_adapted_cos",
    "challenger_adapted_cos", "anchor_context", "challenger_context",
    "anchor_references", "challenger_references", "anchor_sequences",
    "challenger_sequences", "anchor_providers", "challenger_providers",
    "anchor_heading_bins", "challenger_heading_bins", "distance_log_m",
    "challenger_order", "top1_sage_margin", "top1_resolution_disagreement_log_m",
]


def load_rankings(query_ids, gallery_ids):
    rankings, scores, paths = {}, {}, {}
    for name, relative in SOURCES.items():
        path = V8 / relative
        with np.load(path, allow_pickle=False) as z:
            if z["query_ids"].tolist() != query_ids:
                raise RuntimeError(f"Query order changed in {name}")
            idx = z["indices"].astype(np.int32)
            if idx.shape != (len(query_ids), 100) or (idx < 0).any() or (idx >= len(gallery_ids)).any():
                raise RuntimeError(f"Invalid ranking shape or row in {name}")
            if "gallery_ids" in z and z["gallery_ids"].shape == idx.shape:
                if not np.array_equal(z["gallery_ids"], gallery_ids[idx]):
                    raise RuntimeError(f"Gallery row identity changed in {name}")
            elif "gallery_ids" in z and not np.array_equal(z["gallery_ids"], gallery_ids):
                raise RuntimeError(f"Gallery identity changed in {name}")
            rankings[name] = idx
            if "scores" in z:
                scores[name] = z["scores"].astype(np.float32)
        paths[name] = {"path": str(path), "sha256": digest(path)}
    return rankings, scores, paths


def pairwise_m(coords):
    """Local tangent plane is adequate for the sub-kilometre cluster radius."""
    lat = np.deg2rad(coords[:, 0])
    lon = np.deg2rad(coords[:, 1])
    y = (lat - lat[0]) * 6371008.8
    x = (lon - lon[0]) * 6371008.8 * np.cos(lat.mean())
    return np.hypot(x[:, None] - x[None, :], y[:, None] - y[None, :])


def cluster_candidates(rows, evidence, coords):
    """Greedy 75 m seed clusters, ordered by rank fusion, with no chaining."""
    order = np.lexsort((rows, -evidence))
    ds = pairwise_m(coords)
    seeds, clusters = [], []
    for j in order:
        if seeds:
            near = int(np.argmin(ds[j, seeds]))
            if ds[j, seeds[near]] <= RADIUS_M:
                clusters[near].append(int(j))
                continue
        seeds.append(int(j))
        clusters.append([int(j)])
    return clusters


def summarize(cluster, rows, rrfs, ranks, source, sequence, heading, mean, adapted, context):
    cand = rows[cluster]
    # The highest pooled rank-evidence image is the coordinate returned on switch.
    rep_local = int(cluster[np.argmax(rrfs[cluster])])
    rep = int(rows[rep_local])
    ranks_by_source = [min((ranks[n].get(int(v), 101) for v in cand), default=101) for n in NAMES]
    top_model_count = sum(v <= 100 for v in ranks_by_source)
    relevant_headings = [int(heading[v] // 45) for v in cand if np.isfinite(heading[v])]
    return {
        "representative": rep,
        "rrf": float(sum(1 / (60 + ranks[n][int(v)]) for n in NAMES
                         for v in cand if int(v) in ranks[n])),
        "model_count": top_model_count,
        "ranks": ranks_by_source,
        "mean_cos": float(max(mean[cand])),
        "adapted_cos": float(max(adapted[cand])),
        "context": float(max((context.get(int(v), -1.) for v in cand), default=-1.)),
        "references": len(cand),
        "sequences": len(set(sequence[cand])),
        "providers": len(set(source[cand])),
        "heading_bins": len(set(relevant_headings)),
    }


def feature(anchor, other, dist_m, challenger_order, sage_margin, res_disagreement):
    a, c = anchor, other
    ar, cr = a["ranks"], c["ranks"]
    return [a["rrf"], c["rrf"], c["rrf"] - a["rrf"], a["model_count"],
        c["model_count"], c["model_count"] - a["model_count"],
        *[v for pair in zip(ar, cr, strict=True) for v in pair],
        a["mean_cos"], c["mean_cos"], c["mean_cos"] - a["mean_cos"],
        a["adapted_cos"], c["adapted_cos"], a["context"], c["context"],
        a["references"], c["references"], a["sequences"], c["sequences"],
        a["providers"], c["providers"], a["heading_bins"], c["heading_bins"],
        np.log1p(dist_m), challenger_order, sage_margin, np.log1p(res_disagreement)]


def extract(all_hypotheses=False):
    started = time.perf_counter()
    q, g = frames()
    query_ids, gallery_ids = q.id.tolist(), g.id.to_numpy(str)
    rankings, rank_scores, paths = load_rankings(query_ids, gallery_ids)
    coords = g[["lat", "lon"]].to_numpy(float)
    source = g.source.astype(str).to_numpy()
    sequence = g.sequence_key.astype(str).to_numpy()
    heading = g.heading.to_numpy(float)
    mean = np.load(V8 / "staged/mean_scores.npy", mmap_mode="r")
    adapted = np.load(V8 / "sage_adaptation_v3/mean_scores.npy", mmap_mode="r")
    if mean.shape != adapted.shape or mean.shape != (len(q), len(g)):
        raise RuntimeError("Score cache identity/shape mismatch")
    with np.load(V8 / "baseline/context.npz", allow_pickle=False) as z:
        context_rows, context_values = z["chosen"].copy(), z["contextual"].copy()
    feature_rows, challenger_rows, offsets, summaries = [], [], [0], []
    full_rows, full_offsets = [], [0]
    for i in range(len(q)):
        row_lists = [rankings[n][i] for n in NAMES]
        rows = np.unique(np.concatenate(row_lists))
        local = {int(v): j for j, v in enumerate(rows)}
        ranks = {n: {int(v): pos+1 for pos, v in enumerate(rankings[n][i])} for n in NAMES}
        rrf = np.zeros(len(rows), np.float64)
        for n in NAMES:
            for v, rank in ranks[n].items():
                rrf[local[v]] += 1 / (60 + rank)
        clusters = cluster_candidates(rows, rrf, coords[rows])
        context = {int(v): float(s) for v, s in zip(context_rows[i], context_values[i], strict=True)}
        hs = [summarize(c, rows, rrf, ranks, source, sequence, heading, mean[i], adapted[i], context)
              for c in clusters]
        base_row = int(rankings["sage"][i, 0])
        anchor_j = next(j for j, c in enumerate(clusters) if base_row in rows[c])
        anchor = hs[anchor_j]
        challengers = sorted((j for j in range(len(hs)) if j != anchor_j),
                             key=lambda j: (-hs[j]["rrf"], hs[j]["representative"]))
        if not all_hypotheses:
            challengers = challengers[:MAX_CHALLENGERS]
        discrepancy = pairwise_m(coords[[rankings["sage322"][i, 0], rankings["sage504"][i, 0]]])[0, 1]
        margin = float(rank_scores["sage"][i, 0] - rank_scores["sage"][i, 1])
        for order, j in enumerate(challengers):
            h = hs[j]
            d = pairwise_m(coords[[base_row, h["representative"]]])[0, 1]
            feature_rows.append(feature(anchor, h, d, order, margin, discrepancy))
            challenger_rows.append(h["representative"])
        offsets.append(len(feature_rows))
        full_rows.extend(int(v) for v in rows)
        full_offsets.append(len(full_rows))
        summaries.append({"query_id": query_ids[i], "anchor_reference_row": base_row,
            "anchor_hypothesis": anchor, "hypothesis_count": len(hs),
            "unique_candidate_count": len(rows),
            "challengers": [hs[j] for j in challengers]})
        if (i+1) % 200 == 0:
            print("hypotheses", i+1, "/", len(q), "elapsed_s", round(time.perf_counter()-started, 1), flush=True)
    x = np.asarray(feature_rows, np.float32)
    if x.shape[1] != len(FEATURES) or not np.isfinite(x).all():
        raise RuntimeError(f"Invalid inference features {x.shape}, expected {len(FEATURES)}")
    out = OUT / ("hypotheses_all" if all_hypotheses else "hypotheses")
    out.mkdir(exist_ok=True)
    np.savez_compressed(out / "features.npz", query_ids=np.asarray(query_ids, dtype=str),
        baseline_rows=rankings["sage"][:, 0], challenger_rows=np.asarray(challenger_rows, np.int32),
        offsets=np.asarray(offsets, np.int32), features=x,
        feature_names=np.asarray(FEATURES, dtype=str),
        union_rows=np.asarray(full_rows, np.int32), union_offsets=np.asarray(full_offsets, np.int32))
    save(out / "hypotheses.json", summaries)
    save(out / "inference_contract.json", {
        "query_gt_used": False, "reference_coordinates_used": True,
        "radius_m": RADIUS_M,
        "max_challengers": "all" if all_hypotheses else MAX_CHALLENGERS,
        "candidate_sources": paths,
        "score_sources": {str(p): digest(p) for p in [V8 / "staged/mean_scores.npy",
            V8 / "sage_adaptation_v3/mean_scores.npy", V8 / "baseline/context.npz"]},
        "features_sha256": digest(out / "features.npz"),
        "hypotheses_sha256": digest(out / "hypotheses.json"),
        "feature_names": FEATURES, "runtime_s": time.perf_counter()-started})
    record("hypotheses_inference_all_v2" if all_hypotheses else "hypotheses_inference_v1",
        {"status": "complete", "fitted": False,
        "query_gt_used": False, "candidate_sources": list(SOURCES),
        "artifact_paths": [str(out / "features.npz"), str(out / "hypotheses.json"),
                           str(out / "inference_contract.json")],
        "artifact_hashes": {p.name: digest(p) for p in out.iterdir() if p.is_file()}})
    print("inference features", x.shape, "runtime_s", round(time.perf_counter()-started, 1), flush=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Keep every non-anchor hypothesis")
    extract(all_hypotheses=parser.parse_args().all)
