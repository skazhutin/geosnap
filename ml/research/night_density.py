"""One CSLS-inspired reference-only density proxy; cached SAGE-L descriptors.

This deliberately uses reference landmarks, not the source/query population of
published cross-domain CSLS. No query distribution, coordinates or labels enter
the density estimate. No parameters are searched.
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

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from ml.research.gallery_scale_storage import digest, save
from ml.research.night_place import atomic_npz
from ml.research.night_v7 import LOCAL, allowed_to_start, paths

SETUP = {"landmarks": 4096, "neighbors": 10, "penalty": .5, "reference_batch_size": 256,
         "query_batch_size": 4, "cpu_threads": 1, "nice": 10, "seed": 20260909,
         "gallery_count": 100000, "descriptor_dim": 8448, "query_count": 1184,
         "method": "cos(q,r)-0.5*mean(top10 cosine(r,reference_landmarks))",
         "landmark_selection": "smallest stable image ID per normalized provider sequence, then fixed hash order; unknown sequence singleton by image ID",
         "exclude_from_neighbors": "self and every landmark with the same normalized provider sequence",
         "scope": "CSLS-inspired reference-only proxy, not exact published cross-domain CSLS",
         "query_distribution_used": False, "query_coordinates_used": False,
         "calibration_final_access": False, "parameter_sweep": False}
NAME = "reference_density"


def state(out, phase, completed=0, total=0, **extra):
    value = {"phase": phase, "completed": completed, "total": total, "pid": os.getpid(),
             "updated": time.time(), "cpu_only": True, **extra}
    save(out / "status.json", value)
    save(LOCAL / "density_status.json", value)
    print(json.dumps(value), flush=True)


def stable(value):
    return hashlib.sha256(f"{SETUP['seed']}:density:{value}".encode()).hexdigest()


def valid_sequence(value):
    return isinstance(value, str) and "::" in value and value.split("::")[-1] not in {"", "None", "nan", "<NA>"}


def sequence_labels(gallery):
    return np.array([key if valid_sequence(key) else f"unknown-image::{identity}"
                     for key, identity in zip(gallery.sequence_key, gallery.id, strict=True)])


def select_landmarks(gallery, count):
    """Reference identity/sequence only; no geography, scores or query arguments."""
    if gallery.id.duplicated().any():
        raise RuntimeError("repeated reference identity")
    if count < 1:
        raise ValueError("positive landmark count required")
    labels = sequence_labels(gallery)
    chosen = {}
    for j in np.argsort(gallery.id.astype(str).to_numpy(), kind="stable"):
        chosen.setdefault(labels[j], int(j))
    if len(chosen) < count:
        raise RuntimeError("not enough independent reference sequences for fixed landmark count")
    ordered = sorted(chosen.values(), key=lambda j: (stable(str(gallery.id.iloc[j])), str(gallery.id.iloc[j])))
    return np.asarray(ordered[:count], dtype=np.int32)


def density_values(references, landmarks, reference_sequences, landmark_sequences, neighbors=10):
    """Top-neighbor density with same-sequence exclusions, without query data."""
    similarities = references @ landmarks.T
    excluded = np.asarray(reference_sequences)[:, None] == np.asarray(landmark_sequences)[None, :]
    if neighbors < 1 or (np.count_nonzero(~excluded, axis=1) < neighbors).any():
        raise RuntimeError("insufficient cross-sequence density neighbors")
    similarities[excluded] = -np.inf
    selected = np.partition(similarities, similarities.shape[1] - neighbors, axis=1)[:, -neighbors:]
    density = selected.mean(axis=1, dtype=np.float64).astype(np.float32)
    if not np.isfinite(density).all() or (np.abs(density) > 1.00001).any():
        raise RuntimeError("invalid reference density")
    return density


def corrected_scores(cosine, density):
    return cosine - SETUP["penalty"] * density[None, :]


def stage():
    cfg, gc, _, previous, night = paths()
    return cfg, gc, previous, night, night / "cpu_density"


def load_reference_inputs(previous, night):
    inputs = json.loads((night / "input_contract.json").read_text())
    gp, pp = previous / "manifests/G2_smart.parquet", previous / "descriptor_pool.json"
    if digest(gp) != inputs["gallery_sha256"] or digest(pp) != inputs["pool_sha256"]:
        raise RuntimeError("density frozen reference inputs changed")
    if digest(previous / "descriptor_contract.json") != inputs["descriptor_contract_sha256"]:
        raise RuntimeError("SAGE descriptor contract changed")
    g = pd.read_parquet(gp, columns=["id", "file_sha256", "sequence_key"])
    if len(g) != SETUP["gallery_count"]:
        raise RuntimeError("density requires the unchanged100k reference gallery")
    pool = json.loads(pp.read_text())
    if pool["contract_sha256"] != inputs["descriptor_contract_sha256"]:
        raise RuntimeError("reference pool uses a different encoder contract")
    return g, pool["entries"], inputs


def contract_for(cfg, night):
    return {"setup": SETUP, "config": cfg, "source_sha256": digest(Path(__file__)),
            "input_contract_sha256": digest(night / "input_contract.json")}


def prepare():
    cfg, _, previous, night, out = stage()
    contract = contract_for(cfg, night)
    marker = out / "density.started.json"
    if marker.exists():
        saved = json.loads(marker.read_text())
        if saved["contract"] != contract or digest(out / "landmarks.json") != saved["landmarks_sha256"]:
            raise RuntimeError("density preregistration or landmark identities changed")
        return saved
    if not allowed_to_start(cfg, "density", out):
        raise RuntimeError("06:00 cutoff: do not start another density experiment")
    g, entries, _ = load_reference_inputs(previous, night)
    selected = select_landmarks(g, SETUP["landmarks"])
    labels = sequence_labels(g)
    rows = [{"gallery_row": int(j), "id": str(g.id.iloc[j]), "sequence_key": str(labels[j]),
             "image_sha256": str(g.file_sha256.iloc[j])} for j in selected]
    source_paths = set()
    for row in g.itertuples():
        entry = entries[row.id]
        if entry["image_sha256"] != row.file_sha256:
            raise RuntimeError("reference descriptor image identity mismatch")
        source_paths.add(entry["path"])
    out.mkdir(exist_ok=True)
    save(out / "landmarks.json", {"rows": rows, "contract": contract})
    # Registration contains no query coordinates, labels or candidate outcomes.
    registered = {"started": time.time(), "contract": contract, "landmarks_sha256": digest(out / "landmarks.json"),
         "unknown_sequence_references": sum(not valid_sequence(key) for key in g.sequence_key),
         "unknown_sequence_landmarks": sum(not valid_sequence(g.sequence_key.iloc[j]) for j in selected),
         "cost": {"dot_product_FLOPs": 2 * len(g) * len(rows) * SETUP["descriptor_dim"],
                  "landmark_matrix_bytes": len(rows) * SETUP["descriptor_dim"] * 4,
                  "reference_payload_bytes": len(g) * SETUP["descriptor_dim"] * 4,
                  "mapped_source_file_bytes": sum(Path(p).stat().st_size for p in source_paths),
                  "IO_note": "landmark gather, source checksum pass and sequential reference pass; OS cache may reuse pages",
                  "RAM_note": "landmarks138MB + reference block9MB + similarity block4MB; one descriptor mmap at a time"},
         "no_new_descriptors_or_model": True}
    save(marker, registered)
    state(out, "density_prepared", len(selected), len(selected))
    return registered


def shard_rows(gallery, entries, selected):
    shards = defaultdict(list)
    for dest, j in enumerate(selected):
        row = gallery.iloc[int(j)]
        entry = entries[row.id]
        if entry["image_sha256"] != row.file_sha256:
            raise RuntimeError("density descriptor identity changed")
        shards[entry["path"]].append((dest, int(entry["row"])))
    return shards


def checked_block(mapped, positions):
    values = mapped[positions]
    if values.ndim != 2 or values.shape[1] != SETUP["descriptor_dim"] or values.dtype != np.float32:
        raise RuntimeError("unexpected cached descriptor shape or dtype")
    if not np.isfinite(values).all() or not np.allclose(np.linalg.norm(values, axis=1), 1, atol=1e-5):
        raise RuntimeError("invalid cached unit descriptors")
    return values


def gather_landmarks(gallery, entries, selected):
    vectors = np.empty((len(selected), SETUP["descriptor_dim"]), dtype=np.float32)
    for filename, rows in sorted(shard_rows(gallery, entries, selected).items()):
        mapped = np.load(filename, mmap_mode="r", allow_pickle=False)
        try:
            for start in range(0, len(rows), SETUP["reference_batch_size"]):
                dest, source = np.asarray(rows[start:start + SETUP["reference_batch_size"]]).T
                vectors[dest] = checked_block(mapped, source)
        finally:
            mapped._mmap.close()
    return vectors


def calculate_density(gallery, entries, selected, out, contract):
    landmarks = gather_landmarks(gallery, entries, selected)
    landmarks_sha = hashlib.sha256(memoryview(landmarks)).hexdigest()
    _, codes = np.unique(sequence_labels(gallery), return_inverse=True)
    landmark_codes = codes[selected]
    partials = out / "partials"
    partials.mkdir(exist_ok=True)
    shards = shard_rows(gallery, entries, np.arange(len(gallery)))
    density = np.full(len(gallery), np.nan, dtype=np.float32)
    started = time.monotonic()
    completed = 0
    for filename, rows in sorted(shards.items()):
        positions = np.asarray(rows, dtype=np.int64)
        source_sha = digest(filename)
        expected = {"contract": contract, "landmark_vectors_sha256": landmarks_sha,
                    "source_path": filename, "source_sha256": source_sha,
                    "positions_sha256": hashlib.sha256(memoryview(positions)).hexdigest()}
        name = hashlib.sha256(filename.encode()).hexdigest()
        target, receipt = partials / f"{name}.npz", partials / f"{name}.json"
        if receipt.exists():
            saved = json.loads(receipt.read_text())
            if saved["inputs"] != expected or digest(target) != saved["partial_sha256"]:
                raise RuntimeError("density partial checkpoint or source vectors changed")
            with np.load(target, allow_pickle=False) as cached:
                if not np.array_equal(cached["gallery_rows"], positions[:, 0]):
                    raise RuntimeError("density partial row identities changed")
                values = cached["density"]
        else:
            values = np.empty(len(rows), dtype=np.float32)
            mapped = np.load(filename, mmap_mode="r", allow_pickle=False)
            try:
                for start in range(0, len(rows), SETUP["reference_batch_size"]):
                    dest, source = positions[start:start + SETUP["reference_batch_size"]].T
                    refs = checked_block(mapped, source)
                    values[start:start + len(dest)] = density_values(refs, landmarks, codes[dest], landmark_codes, SETUP["neighbors"])
            finally:
                mapped._mmap.close()
            atomic_npz(target, gallery_rows=positions[:, 0], density=values)
            save(receipt, {"inputs": expected, "partial_sha256": digest(target)})
        if values.shape != (len(rows),) or not np.isfinite(values).all() or (np.abs(values) > 1.00001).any():
            raise RuntimeError("invalid committed density values")
        density[positions[:, 0]] = values
        completed += len(rows)
        state(out, "reference_density", completed, len(gallery), elapsed_seconds=time.monotonic() - started)
    atomic_npz(out / "density.npz", density=density)
    save(out / "density.done.json", {"density_sha256": digest(out / "density.npz"), "contract": contract,
         "landmark_vectors_sha256": landmarks_sha, "partial_count": len(shards),
         "minimum": float(density.min()), "median": float(np.median(density)), "maximum": float(density.max())})
    return density


def run_trial(cfg, previous, night, out, registration):
    gallery, entries, inputs = load_reference_inputs(previous, night)
    record = json.loads((out / "landmarks.json").read_text())
    selected = np.array([r["gallery_row"] for r in record["rows"]], dtype=np.int32)
    if not np.array_equal(selected, select_landmarks(gallery, SETUP["landmarks"])):
        raise RuntimeError("fixed landmark selection is not reproducible")
    contract = registration["contract"]
    if (out / "density.done.json").exists():
        done = json.loads((out / "density.done.json").read_text())
        if done["contract"] != contract or digest(out / "density.npz") != done["density_sha256"]:
            raise RuntimeError("completed reference density changed")
        with np.load(out / "density.npz", allow_pickle=False) as saved:
            density = saved["density"]
    else:
        density = calculate_density(gallery, entries, selected, out, contract)
    if digest(night / "base_scores.npy") != inputs["scores_sha256"]:
        raise RuntimeError("original exact query/reference scores changed")
    base = np.load(night / "base_scores.npy", mmap_mode="r", allow_pickle=False)
    if base.shape != (SETUP["query_count"], len(gallery)) or base.dtype != np.float32 or density.shape != (len(gallery),):
        raise RuntimeError("unexpected fixed scoring dimensions")
    predictions_path = out / "predictions.npz"
    prediction_contract = {"density_sha256": digest(out / "density.npz"), "scores_sha256": inputs["scores_sha256"],
                           "landmarks_sha256": registration["landmarks_sha256"], "contract": contract}
    checkpoint = out / "predictions.checkpoint.json"
    indices = np.full((SETUP["query_count"], 100), -1, dtype=np.int32)
    completed = 0
    if checkpoint.exists():
        previous_prediction = json.loads(checkpoint.read_text())
        if previous_prediction["inputs"] != prediction_contract or digest(predictions_path) != previous_prediction["sha256"]:
            raise RuntimeError("density predictions checkpoint changed")
        with np.load(predictions_path, allow_pickle=False) as saved:
            indices = saved["indices"]
        completed = previous_prediction["completed"]
        if indices.shape != (SETUP["query_count"], 100) or not 0 <= completed <= len(indices):
            raise RuntimeError("invalid prediction checkpoint dimensions")
    try:
        for start in range(completed, len(indices), SETUP["query_batch_size"]):
            cosine = np.asarray(base[start:start + SETUP["query_batch_size"]])
            scores = corrected_scores(cosine, density)
            if not np.isfinite(scores).all():
                raise RuntimeError("nonfinite density-corrected retrieval")
            indices[start:start + len(scores)] = np.argsort(-scores, axis=1, kind="stable")[:, :100]
            completed = start + len(scores)
            if completed % 64 == 0 or completed == len(indices):
                atomic_npz(predictions_path, indices=indices)
                save(checkpoint, {"inputs": prediction_contract, "completed": completed, "sha256": digest(predictions_path)})
                state(out, "density_ranking", completed, len(indices))
    finally:
        base._mmap.close()
    del gallery, entries
    import ml.research.night_v7 as evaluator
    original_paths, original_local = evaluator.paths, evaluator.LOCAL
    evaluator.paths = lambda: (*original_paths()[:4], out)
    evaluator.LOCAL = out
    try:
        result = evaluator.record_result(NAME, indices, note=contract | {
            "estimator": "top1 original reference coordinates; no confidence filtering",
            "scope": SETUP["scope"], "training": "none; only reference-neighbor density"})
    finally:
        evaluator.paths, evaluator.LOCAL = original_paths, original_local
    if result["raw"]["accuracy_100m"] <= inputs["baseline_raw100"]:
        save(out / "closed.json", {"reason": "no raw100 gain over baseline; no further density parameters tested"})
    save(out / "done.json", {"completed": time.time(), "contract": contract,
         "density_sha256": digest(out / "density.npz"), "predictions_sha256": digest(predictions_path),
         "result_sha256": digest(out / f"results/{NAME}.json"), "rows_sha256": digest(out / f"results/{NAME}_rows.json"),
         "further_parameter_trials": False})


def publish(night, out, contract):
    completed = json.loads((out / "done.json").read_text())
    if completed["contract"] != contract:
        raise RuntimeError("completed density trial contract changed")
    for name, field in [("density.npz", "density_sha256"), ("predictions.npz", "predictions_sha256"),
                        (f"results/{NAME}.json", "result_sha256"), (f"results/{NAME}_rows.json", "rows_sha256")]:
        if digest(out / name) != completed[field]:
            raise RuntimeError("committed density result changed before publication")
    state(out, "density_waiting_to_publish")
    with (LOCAL / "controller.lock").open("a") as main:
        fcntl.flock(main, fcntl.LOCK_EX)
        for suffix in (".json", "_rows.json"):
            source, target = out / "results" / (NAME + suffix), night / "results" / (NAME + suffix)
            if target.exists() and digest(source) != digest(target):
                raise RuntimeError("density result differs from published result")
            if not target.exists():
                save(target, json.loads(source.read_text()))
        save(out / "published.json", {"published": time.time()})
        from ml.research.night_v7 import report
        report()
    state(out, "density_completed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "compute"])
    action = parser.parse_args().action
    current_nice = os.getpriority(os.PRIO_PROCESS, 0)
    if current_nice < SETUP["nice"]:
        os.nice(SETUP["nice"] - current_nice)
    LOCAL.mkdir(parents=True, exist_ok=True)
    with (LOCAL / "density.lock").open("a") as own, threadpool_limits(limits=1):
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        registration = prepare()
        if action == "prepare":
            return
        cfg, _, previous, night, out = stage()
        if not (out / "done.json").exists():
            run_trial(cfg, previous, night, out, registration)
        publish(night, out, registration["contract"])


if __name__ == "__main__":
    main()
