"""Auxiliary multi-photo feasibility on cached panorama directions, CPU only.

This is synthetic camera rotation on reference-role panoramas, not the frozen
1184-query development benchmark or a real-user accuracy estimate. All parents,
their provider sequences and detected duplicate sequences are withheld globally.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import time
from collections import defaultdict
from pathlib import Path

import h3
import imagehash
import numpy as np
import pandas as pd
from PIL import Image, ImageOps
from scipy.spatial import cKDTree
from threadpoolctl import threadpool_limits

from ml.evaluation.moscow_split import _PerceptualHashIndex
from ml.research.gallery_scale import perspective
from ml.research.gallery_scale_storage import WORKSPACE, digest, save, settings
from ml.research.metrics import paired_group_bootstrap, raw_metrics
from ml.research.night_place import atomic_npz, xyz
from ml.research.night_v7 import allowed_to_start
from ml.research.vector_evaluation import distances

LOCAL = WORKSPACE / "data/evaluation/moscow_night_v7"
SETUP = {"yaws": [0, 90, 180, 270], "search_depth_per_view": 1000, "support_radius_m": 50,
         "methods": ["single", "mean_two90", "mean_four", "place_two90", "place_four"],
         "query_kind": "synthetic directions of the same panorama, auxiliary only",
         "preprocessing": "existing cached SAGE-L/322 perspective90 descriptors",
         "calibration_final_access": False, "cpu_threads": 1}


def stage():
    _, root, _, previous = settings()
    cfg = json.loads((WORKSPACE / "configs/moscow_night_v7.json").read_text())
    night = root / cfg["run_id"]
    out = night / "aux_multiview"
    out.mkdir(exist_ok=True)
    return cfg, previous, night, out


def status(out, phase, completed, total):
    value = {"phase": phase, "completed": completed, "total": total, "pid": os.getpid(),
             "updated": time.time(), "auxiliary_only": True}
    save(out / "status.json", value)
    save(LOCAL / "multiview_status.json", value)
    print(json.dumps(value), flush=True)


def prepare():
    cfg, previous, night, out = stage()
    if not allowed_to_start(cfg, "multiview", out):
        return
    contract = {"setup": SETUP, "source_sha256": digest(Path(__file__)),
                "input_contract_sha256": digest(night / "input_contract.json"), "config": cfg}
    marker = out / "multiview.started.json"
    if marker.exists() and json.loads(marker.read_text())["contract"] != contract:
        raise RuntimeError("started auxiliary protocol changed")
    if not marker.exists():
        save(marker, {"started": time.time(), "contract": contract})
    gallery_path = previous / "manifests/G2_smart.parquet"
    inputs = json.loads((night / "input_contract.json").read_text())
    if digest(gallery_path) != inputs["gallery_sha256"]:
        raise RuntimeError("auxiliary gallery changed")
    if (out / "prepared.json").exists():
        prepared = json.loads((out / "prepared.json").read_text())
        for name, key in [("queries.npz", "query_vectors_sha256"), ("parents.json", "groups_sha256")]:
            if digest(out / name) != prepared[key]:
                raise RuntimeError("prepared auxiliary inputs changed")
        return
    g = pd.read_parquet(gallery_path)
    descriptors, parents = [], []
    index = _PerceptualHashIndex(4)
    fingerprint_number = 0
    pixels, parent_hashes = set(), set()
    for j, row in g[g.is_pano.fillna(False)].iterrows():
        cache = night / "panorama_descriptors" / f"{row.file_sha256}.npz"
        if not cache.exists():
            continue
        with np.load(cache, allow_pickle=False) as data:
            if str(data["image_sha256"]) != row.file_sha256 or str(data["contract_sha256"]) != inputs["descriptor_contract_sha256"]:
                raise RuntimeError("auxiliary panorama descriptor mismatch")
            vectors = data["vectors"]
        if vectors.shape != (4, 8448) or not np.allclose(np.linalg.norm(vectors, axis=1), 1, atol=1e-5):
            raise RuntimeError("invalid cached panorama views")
        if digest(row.image_path) != row.file_sha256:
            raise RuntimeError("panorama bytes changed")
        with Image.open(row.image_path) as opened:
            image = opened.convert("RGB")
            if abs(image.width / image.height - 2) > .08:
                raise RuntimeError("parent is not verified equirectangular")
            views = [image] + [perspective(image, yaw, 322) for yaw in SETUP["yaws"]]
            for view in views:
                pixels.add(hashlib.sha256(view.tobytes()).hexdigest())
                for rotation in (None, Image.Transpose.ROTATE_90, Image.Transpose.ROTATE_180, Image.Transpose.ROTATE_270):
                    transformed = view if rotation is None else view.transpose(rotation)
                    for variant in (transformed, ImageOps.mirror(transformed)):
                        index.add(fingerprint_number, int(str(imagehash.phash(variant)), 16))
                        fingerprint_number += 1
        parent_hashes.add(row.file_sha256)
        parents.append(int(j))
        descriptors.append(vectors)
    if not parents:
        raise RuntimeError("no cached eligible reference-side panorama views")
    duplicate_rows = set(parents)
    for j, row in enumerate(g.itertuples()):
        if row.file_sha256 in parent_hashes or row.pixel_sha256 in pixels or index.matches(int(row.perceptual_hash, 16)):
            duplicate_rows.add(j)
    # Exclude whole normalized provider sequences, including cross-source matches.
    forbidden = set(g.sequence_key.iloc[list(duplicate_rows)])
    parent_ids = set(zip(g.source.iloc[parents].replace({"msls": "mapillary"}),
                         g.source_image_id.iloc[parents].astype(str).str.removeprefix("msls:"), strict=True))
    for row in g.itertuples():
        provider = "mapillary" if row.source == "msls" else row.source
        if (provider, str(row.source_image_id).removeprefix("msls:")) in parent_ids:
            forbidden.add(row.sequence_key)
    eligible = np.flatnonzero(~g.sequence_key.isin(forbidden)).astype(np.int32)
    if set(g.sequence_key.iloc[parents]) & set(g.sequence_key.iloc[eligible]):
        raise RuntimeError("auxiliary sequence split failed")
    atomic_npz(out / "queries.npz", vectors=np.concatenate(descriptors), parents=np.asarray(parents), eligible=eligible)
    save(out / "parents.json", [{"gallery_row": j, "id": g.id.iloc[j], "sequence_key": g.sequence_key.iloc[j],
         "sha256": g.file_sha256.iloc[j], "h3_group": h3.latlng_to_cell(float(g.lat.iloc[j]), float(g.lon.iloc[j]), 6)} for j in parents])
    save(out / "prepared.json", {"parents": len(parents), "direction_queries": 4 * len(parents),
         "remaining_references": len(eligible), "excluded_references": len(g) - len(eligible),
         "forbidden_sequences": sorted(forbidden), "duplicate_matched_rows": len(duplicate_rows),
         "duplicate_check": "source identity, SHA, decoded-pixel SHA and pHash4 original/four views with rotations/mirrors; whole sequence removal",
         "groups_sha256": digest(out / "parents.json"), "query_vectors_sha256": digest(out / "queries.npz"),
         "contract": contract, "limitation": "synthetic panorama directions, no guarantee against unknown provider aliases or partial image duplicates"})
    status(out, "multiview_prepared", len(parents), len(parents))


def regional_scores(scores, positions, radius=50, depth=1000):
    """Each photograph contributes one best local reference score per hypothesis.

    No query GPS or true heading is an input. Different views may match different
    gallery photographs of the same region; repeated refs do not add votes.
    """
    shortlist = np.unique(np.argsort(-scores, axis=1, kind="stable")[:, :min(depth, scores.shape[1])])
    tree = cKDTree(positions)
    local = tree.query_ball_point(positions[shortlist], radius)
    evidence = np.stack([scores[:, members].max(axis=1) for members in local], axis=1)
    return shortlist, evidence


def predict_bundle(current, positions, radius=50, depth=1000):
    """Two-view arms can access only those two images, including shortlisting."""
    four_candidates, four_evidence = regional_scores(current, positions, radius, depth)
    results = []
    for yaw in range(4):
        selected = [yaw, (yaw + 1) % 4]
        direct = {"single": current[yaw], "mean_two90": current[selected].mean(axis=0),
                  "mean_four": current.mean(axis=0)}
        orders = {name: np.argsort(-values, kind="stable")[:100] for name, values in direct.items()}
        pair_candidates, pair_evidence = regional_scores(current[selected], positions, radius, depth)
        for name, candidates, evidence in [("place_two90", pair_candidates, pair_evidence),
                                            ("place_four", four_candidates, four_evidence)]:
            values = evidence.mean(axis=0)
            order = np.lexsort((candidates, -current[yaw, candidates], -values))[:100]
            orders[name] = candidates[order]
        results.append(orders)
    return results


def compute_scores(previous, night, out):
    with np.load(out / "queries.npz", allow_pickle=False) as data:
        qv, eligible = data["vectors"], data["eligible"]
    if digest(out / "queries.npz") != json.loads((out / "prepared.json").read_text())["query_vectors_sha256"]:
        raise RuntimeError("auxiliary cached queries changed")
    inputs = json.loads((night / "input_contract.json").read_text())
    pool_path = previous / "descriptor_pool.json"
    if digest(pool_path) != inputs["pool_sha256"]:
        raise RuntimeError("auxiliary descriptor pool changed")
    entries = json.loads(pool_path.read_text())["entries"]
    g = pd.read_parquet(previous / "manifests/G2_smart.parquet", columns=["id", "file_sha256"])
    shards = defaultdict(list)
    for dest, j in enumerate(eligible):
        row = g.iloc[j]
        entry = entries[row.id]
        if entry["image_sha256"] != row.file_sha256:
            raise RuntimeError("auxiliary reference identity mismatch")
        shards[entry["path"]].append((dest, entry["row"]))
    score_path = out / "scores.npy"
    receipt = out / "score_chunks.json"
    if receipt.exists() and not score_path.exists():
        raise RuntimeError("committed auxiliary score chunks exist without their matrix")
    scoremap = np.lib.format.open_memmap(score_path, mode="r+" if score_path.exists() else "w+",
                                       dtype=np.float32, shape=(len(qv), len(eligible)))
    if scoremap.shape != (len(qv), len(eligible)) or scoremap.dtype != np.float32:
        raise RuntimeError("auxiliary score cache shape/dtype changed")
    done = set(json.loads(receipt.read_text())["paths"]) if receipt.exists() else set()
    for filename, rows in sorted(shards.items()):
        if filename in done:
            continue
        vectors = np.load(filename, mmap_mode="r", allow_pickle=False)
        for start in range(0, len(rows), 256):
            dest, source = np.asarray(rows[start:start + 256]).T
            block = vectors[source]
            if not np.isfinite(block).all() or not np.allclose(np.linalg.norm(block, axis=1), 1, atol=1e-5):
                raise RuntimeError("non-unit cached reference")
            scoremap[:, dest] = qv @ block.T
        vectors._mmap.close()
        scoremap.flush()
        done.add(filename)
        save(receipt, {"paths": sorted(done), "shards": len(shards)})
        status(out, "multiview_scores", len(done), len(shards))
    del scoremap
    save(out / "scores.done.json", {"scores_sha256": digest(score_path), "shape": [len(qv), len(eligible)]})


def evaluate(previous, out):
    if digest(out / "scores.npy") != json.loads((out / "scores.done.json").read_text())["scores_sha256"]:
        raise RuntimeError("completed auxiliary scores changed")
    with np.load(out / "queries.npz", allow_pickle=False) as data:
        parents, eligible = data["parents"], data["eligible"]
    g = pd.read_parquet(previous / "manifests/G2_smart.parquet", columns=["lat", "lon"])
    geo = g.iloc[eligible]
    positions = xyz(geo.lat.to_numpy(float), geo.lon.to_numpy(float))
    lat, lon = np.radians(geo.lat.to_numpy(float)), np.radians(geo.lon.to_numpy(float))
    scores = np.load(out / "scores.npy", mmap_mode="r", allow_pickle=False)
    rows, groups, coverage = [], [], []
    errors = {name: [] for name in SETUP["methods"]}
    recalls = {name: [] for name in SETUP["methods"]}
    for i, parent in enumerate(parents):
        current = np.asarray(scores[4 * i:4 * i + 4])
        predictions = predict_bundle(current, positions, SETUP["support_radius_m"], SETUP["search_depth_per_view"])
        truth = g.iloc[parent]
        distance = distances(truth.lat, truth.lon, lat, lon)
        coverage.append(float(distance.min()))
        for yaw in range(4):
            orders = predictions[yaw]
            row = {"parent": int(parent), "yaw": 90 * yaw, "methods": {}}
            for name, order in orders.items():
                d = distance[order]
                positive = np.flatnonzero(d <= 100)
                first = int(positive[0]) + 1 if len(positive) else None
                errors[name].append(float(d[0]))
                recalls[name].append(first or np.inf)
                row["methods"][name] = {"error_m": float(d[0]), "positive_rank_through100": first,
                                         "top100_gallery_rows": eligible[order].tolist()}
            groups.append(h3.latlng_to_cell(float(truth.lat), float(truth.lon), 6))
            rows.append(row)
    results = {name: {"raw": raw_metrics(value), "R_at": {str(k): float(np.mean(np.asarray(recalls[name]) <= k)) for k in (1, 10, 100)},
                     "paired_vs_single": paired_group_bootstrap(errors["single"], value, groups)} for name, value in errors.items()}
    report = {"setup": SETUP, "physical_places": len(parents), "orientation_anchored_cases": len(rows),
         "statistical_unit": "geographic blocks; four orientations within each panorama are not independent cases",
         "h3_r6_blocks": len(set(groups)), "coverage_ceiling": {str(k): float(np.mean(np.asarray(coverage) <= k)) for k in (25, 50, 100)},
         "results": results, "unchanged_main_development": True, "abstention": False,
         "limitations": ["synthetic camera rotation, not a real-user multi-photo benchmark",
             "panorama source/capture distribution differs from 1184 main queries",
             "query-descriptor mean ranking equals mean cached cosine; no new encoding needed",
             "small sample and exploratory selection; no final/production claim",
             "place consensus shortlists union of per-view top1000 references"]}
    save(out / "rows.json", rows)
    save(out / "report.json", report)
    text = "# Multi-photo auxiliary feasibility\n\nSynthetic directions of43 reference-side panoramas (actual count below), not real-user or main-development accuracy.\n\n"
    text += f"Places: {len(parents)}; yaw-anchored cases: {len(rows)}; geographic blocks: {len(set(groups))}.\n\n"
    text += "| Method | <=25m | <=50m | <=100m | median m | p90 m | >500m | R1/10/100 |\n|---|---:|---:|---:|---:|---:|---:|---|\n"
    for name, result in results.items():
        m = result["raw"]
        text += f"| {name} | {100*m['accuracy_25m']:.2f}% | {100*m['accuracy_50m']:.2f}% | {100*m['accuracy_100m']:.2f}% | {m['median_error_m']:.0f} | {m['p90_error_m']:.0f} | {100*m['catastrophic_gt500m_rate']:.2f}% | " + "/".join(f"{100*result['R_at'][str(k)]:.2f}" for k in (1,10,100)) + " |\n"
    (out / "report.md").write_text(text)
    (LOCAL / "multiview_report.md").write_text(text)
    save(out / "done.json", {"completed": time.time(), "report_sha256": digest(out / "report.json")})
    status(out, "multiview_completed", len(parents), len(parents))


def run():
    cfg, previous, night, out = stage()
    with (LOCAL / "multiview.lock").open("a") as lock, threadpool_limits(limits=1):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (out / "done.json").exists() or not allowed_to_start(cfg, "multiview", out):
            return
        # Do not compete with the current prototype worker for external-disk IO.
        with (LOCAL / "place.lock").open("a") as place_lock:
            status(out, "multiview_waiting_for_place", 0, 1)
            fcntl.flock(place_lock, fcntl.LOCK_EX)
            prepare()
            if not (out / "prepared.json").exists():
                return
            if not (out / "scores.done.json").exists():
                compute_scores(previous, night, out)
            evaluate(previous, out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["prepare", "run"])
    args = parser.parse_args()
    if args.action == "prepare":
        with (LOCAL / "multiview.lock").open("a") as lock, threadpool_limits(limits=1):
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            prepare()
    else:
        run()
