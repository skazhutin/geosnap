"""Single reference-only mean-centering ablation, no training or new encoder."""
from __future__ import annotations

import fcntl
import json
import os
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from ml.research.gallery_scale_storage import digest, save
from ml.research.night_place import atomic_npz
from ml.research.night_v7 import LOCAL, allowed_to_start, paths


def centered_scores(base, query_dot_mean, reference_dot_mean, mean_square):
    """Exact cosine of normalized (unit descriptor - reference mean)."""
    qnorm = np.sqrt(np.maximum(1 - 2 * query_dot_mean + mean_square, 1e-12))
    rnorm = np.sqrt(np.maximum(1 - 2 * reference_dot_mean + mean_square, 1e-12))
    return (base - query_dot_mean[:, None] - reference_dot_mean[None] + mean_square) / (qnorm[:, None] * rnorm[None])


def state(out, phase, completed=0, total=0):
    value = {"phase": phase, "completed": completed, "total": total,
             "pid": os.getpid(), "updated": time.time(), "cpu_only": True}
    save(out / "status.json", value)
    save(LOCAL / "centering_status.json", value)
    print(json.dumps(value), flush=True)


def compute():
    cfg, _, _, previous, night = paths()
    out = night / "cpu_centering"
    out.mkdir(exist_ok=True)
    with (LOCAL / "centering.lock").open("a") as lock, threadpool_limits(limits=1):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if not (out / "done.json").exists():
            state(out, "centering_waiting_for_cpu_queue")
            # Ordering matters: multiview itself waits for place.lock. Never
            # hold place.lock while waiting for multiview.lock (would deadlock).
            with (LOCAL / "multiview.lock").open("a") as dependency:
                fcntl.flock(dependency, fcntl.LOCK_EX)
                if not allowed_to_start(cfg, "centering", out):
                    return
                contract = {"source_sha256": digest(Path(__file__)), "input_contract_sha256": digest(night / "input_contract.json"),
                     "config": cfg, "method": "subtract uniform mean of all100000 reference descriptors, then unit-normalize query and reference",
                     "no_hyperparameter_sweep": True, "no_query_truth_or_query_distribution_used_for_mean": True}
                marker = out / "centering.started.json"
                if marker.exists():
                    registered = json.loads(marker.read_text())["contract"]
                    if registered != contract:
                        repair_path = out / "runtime_repair.json"
                        repair = json.loads(repair_path.read_text()) if repair_path.exists() else {}
                        old = dict(registered, source_sha256=contract["source_sha256"])
                        if (old != contract or repair.get("previous_source_sha256") != registered["source_sha256"]
                                or repair.get("current_source_sha256") != contract["source_sha256"]):
                            raise RuntimeError("centering contract changed")
                if not marker.exists():
                    save(marker, {"started": time.time(), "contract": contract})
                run_trial(cfg, previous, night, out, contract)
        # Publication is bookkeeping, never a new experiment after the cutoff.
        state(out, "centering_waiting_to_publish")
        with (LOCAL / "controller.lock").open("a") as main:
            fcntl.flock(main, fcntl.LOCK_EX)
            for suffix in (".json", "_rows.json"):
                src = out / "results" / ("reference_centered" + suffix)
                dst = night / "results" / src.name
                if dst.exists() and digest(src) != digest(dst):
                    raise RuntimeError("centering result differs from published result")
                if not dst.exists():
                    save(dst, json.loads(src.read_text()))
            save(out / "published.json", {"published": time.time()})
            from ml.research.night_v7 import report
            report()
        state(out, "centering_completed")


def run_trial(cfg, previous, night, out, contract):
    inputs = json.loads((night / "input_contract.json").read_text())
    gp, pp = previous / "manifests/G2_smart.parquet", previous / "descriptor_pool.json"
    if digest(gp) != inputs["gallery_sha256"] or digest(pp) != inputs["pool_sha256"]:
        raise RuntimeError("centering frozen inputs changed")
    g = pd.read_parquet(gp, columns=["id", "file_sha256"])
    entries = json.loads(pp.read_text())["entries"]
    shards = defaultdict(list)
    for i, row in enumerate(g.itertuples()):
        entry = entries[row.id]
        if entry["image_sha256"] != row.file_sha256:
            raise RuntimeError("centering descriptor identity changed")
        shards[entry["path"]].append((i, entry["row"]))
    partials = out / "sums"
    partials.mkdir(exist_ok=True)
    total = np.zeros(8448, dtype=float)
    for n, (path, rows) in enumerate(sorted(shards.items())):
        receipt = partials / (hashlib_name(path) + ".npz")
        if receipt.exists():
            with np.load(receipt, allow_pickle=False) as data:
                if str(data["source_path"]) != path or data["count"] != len(rows):
                    raise RuntimeError("centering shard receipt changed")
                summed = data["sum"]
        else:
            block = np.load(path, mmap_mode="r", allow_pickle=False)
            summed = np.zeros(8448, dtype=float)
            for start in range(0, len(rows), 256):
                selected = block[np.asarray(rows[start:start + 256])[:, 1]]
                if not np.isfinite(selected).all() or not np.allclose(np.linalg.norm(selected, axis=1), 1, atol=1e-5):
                    raise RuntimeError("invalid centering reference vectors")
                summed += selected.sum(axis=0, dtype=float)
            block._mmap.close()
            atomic_npz(receipt, source_path=np.asarray(path), count=np.asarray(len(rows)), sum=summed)
        total += summed
        state(out, "centering_reference_mean", n + 1, len(shards))
    mean = total / len(g)
    mean_square = float(mean @ mean)
    projection_path = out / "reference_projection.npz"
    dot = np.full(len(g), np.nan)
    if projection_path.exists():
        with np.load(projection_path, allow_pickle=False) as data:
            np.testing.assert_array_equal(mean, data["mean"])
            dot = data["dot"]
    for n, (path, rows) in enumerate(sorted(shards.items())):
        selected_rows = np.asarray(rows)
        if np.isfinite(dot[selected_rows[:, 0]]).all():
            continue
        block = np.load(path, mmap_mode="r", allow_pickle=False)
        for start in range(0, len(rows), 256):
            dest, source = selected_rows[start:start + 256].T
            dot[dest] = block[source].astype(float) @ mean
        block._mmap.close()
        atomic_npz(projection_path, mean=mean, dot=dot)
        state(out, "centering_reference_projection", n + 1, len(shards))
    scores = np.load(night / "base_scores.npy", mmap_mode="r", allow_pickle=False)
    from ml.research.gallery_scale import query_vectors
    _, gc, _, _, _ = paths()
    _, qv = query_vectors(gc)
    smoke_indices = np.linspace(0, len(g) - 1, 8, dtype=int)
    smoke_refs = []
    for j in smoke_indices:
        entry = entries[g.id.iloc[j]]
        block = np.load(entry["path"], mmap_mode="r", allow_pickle=False)
        smoke_refs.append(np.array(block[entry["row"]], dtype=float, copy=True))
        block._mmap.close()
    smoke_r, smoke_q = np.array(smoke_refs) - mean, qv[:8].astype(float) - mean
    smoke_r /= np.linalg.norm(smoke_r, axis=1, keepdims=True)
    smoke_q /= np.linalg.norm(smoke_q, axis=1, keepdims=True)
    algebra = centered_scores(np.asarray(scores[:8, smoke_indices], dtype=float),
                              np.asarray(scores[:8], dtype=float).mean(axis=1), dot[smoke_indices], mean_square)
    error = float(np.max(np.abs(algebra - smoke_q @ smoke_r.T)))
    if error > 2e-6:
        raise RuntimeError("centering cached algebra does not match direct descriptors")
    save(out / "smoke.json", {"passed": True, "maximum_cosine_difference": error})
    del qv
    predictions = np.full((cfg["query_count"], 100), -1, dtype=np.int32)
    for first in range(0, len(predictions), 4):
        base = np.asarray(scores[first:first + 4], dtype=float)
        # For the exact uniform reference mean, q·mean is the row's mean cosine.
        current = centered_scores(base, base.mean(axis=1), dot, mean_square)
        if not np.isfinite(current).all():
            raise RuntimeError("nonfinite centered retrieval")
        predictions[first:first + 4] = np.argsort(-current, axis=1, kind="stable")[:, :100]
    atomic_npz(out / "predictions.npz", indices=predictions)
    del scores, entries, g
    import ml.research.night_v7 as evaluator
    original_paths, original_local = evaluator.paths, evaluator.LOCAL
    evaluator.paths = lambda: (*original_paths()[:4], out)
    evaluator.LOCAL = out
    try:
        evaluator.record_result("reference_centered", predictions, note=contract | {"reference_mean_norm": float(np.sqrt(mean_square)),
            "estimator": "top1 original reference coordinate", "training": "none; uniform reference-only mean"})
    finally:
        evaluator.paths, evaluator.LOCAL = original_paths, original_local
    save(out / "done.json", {"completed": time.time(), "contract": contract,
         "projection_sha256": digest(projection_path), "predictions_sha256": digest(out / "predictions.npz")})


def hashlib_name(text):
    import hashlib
    return hashlib.sha256(text.encode()).hexdigest()


if __name__ == "__main__":
    compute()
