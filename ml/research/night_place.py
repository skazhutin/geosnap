"""One CPU-only place-prototype trial, safe alongside the FoL GPU worker.

Reference-only groups, no new image encoding. Exact normalized prototype cosine
is recovered from cached query/reference scores and reference sum norms.
Results are staged separately and published only under the main worker lock.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import time
from collections import OrderedDict
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from threadpoolctl import threadpool_limits

from ml.research.gallery_scale_storage import WORKSPACE, digest, save, settings

LOCAL = WORKSPACE / "data/evaluation/moscow_night_v7"
SETUP = {"radius_m": 25.0, "heading_gap_degrees": 45.0, "maximum_views": 4,
         "distinct_provider_sequences": True, "missing_metadata": "singleton",
         "arms": ["place_prototype", "place_equal_blend"], "cpu_threads": 1}


def xyz(lat, lon):
    a, b = np.radians(lat), np.radians(lon)
    return 6371000 * np.column_stack((np.cos(a) * np.cos(b), np.cos(a) * np.sin(b), np.sin(a)))


def make_groups(g):
    """Disjoint complete-link groups; order and eligibility use references only."""
    points = xyz(g.lat.to_numpy(float), g.lon.to_numpy(float))
    heading = pd.to_numeric(g.heading, errors="coerce").to_numpy(float)
    sequence = g.sequence_key.fillna("").astype(str).to_numpy()
    eligible = np.isfinite(heading) & ~np.isin(sequence, ["", "None", "nan", "<NA>"])
    # Normalized keys still contain provider prefixes for missing metadata.
    eligible &= np.array([s.split("::")[-1] not in {"", "None", "nan", "<NA>"} for s in sequence])
    tree = cKDTree(points)
    order = np.argsort(g.id.astype(str).to_numpy(), kind="stable")
    priority = np.empty(len(g), dtype=np.int32)
    priority[order] = np.arange(len(g))
    assigned = np.zeros(len(g), dtype=bool)
    groups = []
    for i in order:
        if assigned[i]:
            continue
        members = [int(i)]
        assigned[i] = True
        if eligible[i]:
            nearby = tree.query_ball_point(points[i], SETUP["radius_m"])
            nearby.sort(key=lambda j: (float(np.linalg.norm(points[j] - points[i])), int(priority[j])))
            sequences = {sequence[i]}
            for j in nearby:
                if assigned[j] or not eligible[j] or sequence[j] in sequences:
                    continue
                gap = np.abs((heading[j] - heading[members] + 180) % 360 - 180)
                if (gap > SETUP["heading_gap_degrees"]).any():
                    continue
                if (np.linalg.norm(points[members] - points[j], axis=1) > SETUP["radius_m"]).any():
                    continue
                members.append(int(j))
                assigned[j] = True
                sequences.add(sequence[j])
                if len(members) == SETUP["maximum_views"]:
                    break
        groups.append(members)
    return groups


def atomic_npz(path, **arrays):
    temporary = path.with_suffix(".writing")
    with temporary.open("wb") as f:
        np.savez(f, **arrays)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)


class VectorReader:
    """At most four mapped shards; copies only individual descriptor rows."""
    def __init__(self, entries, gallery):
        self.entries, self.gallery, self.cache = entries, gallery, OrderedDict()

    def get(self, i):
        row = self.gallery.iloc[i]
        entry = self.entries[row.id]
        if entry["image_sha256"] != row.file_sha256:
            raise RuntimeError("prototype descriptor identity mismatch")
        path = entry["path"]
        if path in self.cache:
            self.cache.move_to_end(path)
        else:
            self.cache[path] = np.load(path, mmap_mode="r", allow_pickle=False)
            if len(self.cache) > 4:
                _, old = self.cache.popitem(last=False)
                old._mmap.close()
        vector = np.array(self.cache[path][entry["row"]], dtype=np.float32, copy=True)
        if vector.shape != (8448,) or not np.isfinite(vector).all() or abs(np.linalg.norm(vector) - 1) > 1e-5:
            raise RuntimeError("prototype input is not the frozen unit SAGE-L descriptor")
        return vector

    def close(self):
        for value in self.cache.values():
            value._mmap.close()
        self.cache.clear()


def score_batch(base, flat, offsets, owner, norms, sizes):
    grouped = np.add.reduceat(base[:, flat], offsets[:-1], axis=1) / norms
    replacement = grouped[:, owner]
    # Singleton descriptors/scores must remain bit-for-bit unchanged.
    singleton = sizes[owner] == 1
    replacement[:, singleton] = base[:, singleton]
    return replacement


def rank(scores, original):
    ids = np.arange(len(scores))
    return np.lexsort((ids, -original, -scores))[:100].astype(np.int32)


def progress(directory, phase, completed, total, started):
    value = {"phase": phase, "completed": completed, "total": total, "pid": os.getpid(),
             "updated": time.time(), "elapsed_seconds": time.monotonic() - started,
             "parallel_cpu_only": True}
    save(directory / "status.json", value)
    save(LOCAL / "place_status.json", value)
    print(json.dumps(value), flush=True)


def compute():
    from ml.research.night_v7 import allowed_to_start

    _, root, _, previous = settings()
    cfg = json.loads((WORKSPACE / "configs/moscow_night_v7.json").read_text())
    out = root / cfg["run_id"]
    directory = out / "cpu_place"
    directory.mkdir(exist_ok=True)
    with (LOCAL / "place.lock").open("a") as lock, threadpool_limits(limits=1):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (directory / "done.json").exists():
            return
        if not allowed_to_start(cfg, "place", directory):
            return
        contract = {"setup": SETUP, "source_sha256": digest(Path(__file__)),
                    "input_contract_sha256": digest(out / "input_contract.json"),
                    "query_coordinates_used_for_groups_or_scores": False,
                    "final_or_calibration_access": False, "config": cfg}
        start = directory / "place.started.json"
        if start.exists():
            if json.loads(start.read_text())["contract"] != contract:
                raise RuntimeError("started place trial contract changed")
        else:
            save(start, {"started": time.time(), "contract": contract})
        gpath = previous / "manifests/G2_smart.parquet"
        inputs = json.loads((out / "input_contract.json").read_text())
        if digest(gpath) != inputs["gallery_sha256"]:
            raise RuntimeError("place gallery changed")
        g = pd.read_parquet(gpath, columns=["id", "lat", "lon", "heading", "sequence_key", "source", "file_sha256"])
        started = time.monotonic()
        group_path = directory / "groups.json"
        groups = json.loads(group_path.read_text()) if group_path.exists() else make_groups(g)
        if not group_path.exists():
            save(group_path, groups)
        sizes = np.array([len(x) for x in groups], dtype=np.int32)
        offsets = np.r_[0, np.cumsum(sizes)]
        flat = np.concatenate(groups).astype(np.int32)
        if not np.array_equal(np.sort(flat), np.arange(len(g))):
            raise RuntimeError("groups must partition every reference exactly once")
        owner = np.empty(len(g), dtype=np.int32)
        owner[flat] = np.repeat(np.arange(len(groups)), sizes)
        save(directory / "group_inventory.json", {"references": len(g), "prototypes": len(groups),
             "group_sizes": {str(k): int((sizes == k).sum()) for k in np.unique(sizes)},
             "references_in_multiview_groups": int(sizes[sizes > 1].sum()),
             "cross_dataset_groups": sum(len(set(g.source.iloc[m])) > 1 for m in groups if len(m) > 1),
             "group_sha256": digest(group_path), "setup": SETUP,
             "query_truth_used": False, "groups_are_not_independent_geographic_votes": True})
        norms = np.where(sizes == 1, 1.0, np.nan).astype(np.float64)
        norm_path = directory / "norms.npz"
        if norm_path.exists():
            with np.load(norm_path, allow_pickle=False) as data:
                if str(data["group_sha256"]) != digest(group_path):
                    raise RuntimeError("prototype norm membership changed")
                norms = data["norms"]
        pool = json.loads((previous / "descriptor_pool.json").read_text())
        if digest(previous / "descriptor_pool.json") != inputs["pool_sha256"]:
            raise RuntimeError("prototype descriptor pool changed")
        if pool["contract_sha256"] != inputs["descriptor_contract_sha256"]:
            raise RuntimeError("prototype descriptor fingerprint changed")
        reader = VectorReader(pool["entries"], g)
        try:
            for n in np.flatnonzero(~np.isfinite(norms)):
                vectors = np.stack([reader.get(j) for j in groups[n]])
                norms[n] = np.linalg.norm(vectors.sum(axis=0, dtype=np.float64))
                if norms[n] <= 1e-6 or norms[n] > sizes[n] + 1e-4:
                    raise RuntimeError("invalid prototype norm")
                if n % 128 == 0:
                    atomic_npz(norm_path, norms=norms, group_sha256=np.asarray(digest(group_path)))
                    progress(directory, "place_norms", int(np.isfinite(norms).sum()), len(norms), started)
            atomic_npz(norm_path, norms=norms, group_sha256=np.asarray(digest(group_path)))
            # Numerical identity smoke before any result/label evaluation.
            from ml.research.gallery_scale import query_vectors

            gc, _, _, _ = settings()
            queries, qv = query_vectors(gc)
            score_map = np.load(out / "base_scores.npy", mmap_mode="r", allow_pickle=False)
            differences = []
            for n in np.flatnonzero(sizes > 1)[:8]:
                total = np.stack([reader.get(j) for j in groups[n]]).sum(axis=0, dtype=np.float64)
                direct = qv[:8].astype(float) @ (total / np.linalg.norm(total))
                cached = np.asarray(score_map[:8, groups[n]], dtype=float).sum(axis=1) / norms[n]
                differences.append(float(np.max(np.abs(direct - cached))))
            if differences and max(differences) > 2e-6:
                raise RuntimeError("prototype cached algebra differs from direct cosine")
            save(directory / "smoke.json", {"max_cosine_difference": max(differences, default=0),
                 "groups_checked": len(differences), "passed": True})
        finally:
            reader.close()
        del pool, reader
        coherence = (norms[sizes > 1] ** 2 - sizes[sizes > 1]) / (sizes[sizes > 1] * (sizes[sizes > 1] - 1))
        save(directory / "coherence.json", {"pairwise_cosine_quantiles": np.quantile(coherence, [0, .25, .5, .75, 1]).tolist() if len(coherence) else [],
             "no_filtering_or_tuning_on_query_outcomes": True})
        predictions = {name: np.full((len(queries), 100), -1, np.int32) for name in SETUP["arms"]}
        prediction_path = directory / "predictions.npz"
        if prediction_path.exists():
            with np.load(prediction_path, allow_pickle=False) as data:
                predictions = {name: data[name] for name in SETUP["arms"]}
        for first in range(0, len(queries), 4):
            end = min(first + 4, len(queries))
            if all((x[first:end] >= 0).all() for x in predictions.values()):
                continue
            base = np.asarray(score_map[first:end], dtype=np.float64)
            replacement = score_batch(base, flat, offsets, owner, norms, sizes)
            for k, i in enumerate(range(first, end)):
                predictions["place_prototype"][i] = rank(replacement[k], base[k])
                predictions["place_equal_blend"][i] = rank((replacement[k] + base[k]) / 2, base[k])
            if end % 32 == 0 or end == len(queries):
                atomic_npz(prediction_path, **predictions)
                progress(directory, "place_retrieval", end, len(queries), started)
        del score_map
        # Redirect this subprocess's evaluator to a private output directory.
        # No shared status/report writes while the GPU worker is active.
        import ml.research.night_v7 as evaluator

        original_paths = evaluator.paths
        evaluator.paths = lambda: (*original_paths()[:4], directory)
        evaluator.LOCAL = directory
        for name, prefix in predictions.items():
            if not (directory / "results" / f"{name}.json").exists():
                evaluator.record_result(name, prefix, note=contract | {
                    "estimator": "original reference coordinate; original cosine resolves prototype member ties",
                    "reference_rank_unit": "physical image, not independent place vote",
                    "median_distinct_prototypes_in_top100": float(np.median([len(set(owner[r])) for r in prefix])),
                    "normalization_smoke": json.loads((directory / "smoke.json").read_text())})
        save(directory / "done.json", {"completed": time.time(), "contract": contract,
             "predictions_sha256": digest(prediction_path), "results": SETUP["arms"]})
        progress(directory, "place_completed", len(queries), len(queries), started)


def publish():
    """Merge committed private results only when the main worker is idle."""
    from ml.research.night_v7 import paths, report

    *_, out = paths()
    directory = out / "cpu_place"
    if not (directory / "done.json").exists():
        return
    with (LOCAL / "controller.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for name in SETUP["arms"]:
            for suffix in (".json", "_rows.json"):
                src, dst = directory / "results" / (name + suffix), out / "results" / (name + suffix)
                if dst.exists() and digest(dst) != digest(src):
                    raise RuntimeError("refusing to overwrite different prototype results")
                if not dst.exists():
                    save(dst, json.loads(src.read_text()))
        save(directory / "published.json", {"published": time.time()})
        report()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["compute", "publish"])
    args = parser.parse_args()
    (compute if args.action == "compute" else publish)()
