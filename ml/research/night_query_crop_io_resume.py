"""Resume identical crop scoring with sequential writes on the external NTFS store."""
import fcntl
import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path

import numpy as np
from threadpoolctl import threadpool_limits

from ml.research import night_multiview_collage as original
from ml.research import night_query_square_crop as trial
from ml.research.gallery_scale_storage import digest, save
from ml.research.night_gallery_addition import atomic_array


def sequential_scores(previous, gallery, eligible, queries, out, contract):
    expected = {"contract_sha256": original.hash_json(contract), "query_sha256": digest(out / "queries.npz")}
    repair = {"source_sha256": digest(Path(__file__)), "original_scorer_sha256": digest(Path(original.__file__)),
              "inputs": expected, "change": "same256-reference dot products; RAM score matrix, sequential atomic part/final writes",
              "image_encoding_repeated": False, "ranking_or_population_changed": False}
    repair_path = out / "io_runtime_repair.json"
    if repair_path.exists() and json.loads(repair_path.read_text()) != repair:
        raise RuntimeError("registered crop I/O repair source or inputs changed")
    save(repair_path, repair)
    checkpoint = out / "score_chunks.json"
    legacy_checkpoint = out / "score_chunks.before_io_repair.json"
    legacy_scores = out / "scores.before_io_repair.npy"
    initialization = out / "io_runtime_repair.initialized.json"
    if not initialization.exists():
        if checkpoint.exists() or legacy_checkpoint.exists():
            if not legacy_checkpoint.exists():
                legacy_checkpoint.write_bytes(checkpoint.read_bytes())
            if not legacy_scores.exists():
                if not (out / "scores.npy").exists():
                    raise RuntimeError("original committed score matrix is missing")
                os.replace(out / "scores.npy", legacy_scores)
        save(initialization, {"legacy_checkpoint_sha256": digest(legacy_checkpoint) if legacy_checkpoint.exists() else None})
    original_sha = json.loads(initialization.read_text())["legacy_checkpoint_sha256"]
    if original_sha != (digest(legacy_checkpoint) if legacy_checkpoint.exists() else None):
        raise RuntimeError("preserved original crop score checkpoint changed")
    legacy = json.loads(legacy_checkpoint.read_text()) if legacy_checkpoint.exists() else {"inputs": expected, "chunks": {}}
    if legacy["inputs"] != expected:
        raise RuntimeError("original crop score checkpoint population changed")
    pool = json.loads((previous / "descriptor_pool.json").read_text())["entries"]
    shards = defaultdict(list)
    for destination, row_index in enumerate(eligible):
        row = gallery.iloc[int(row_index)]
        entry = pool[row.id]
        if entry["image_sha256"] != row.file_sha256:
            raise RuntimeError("reference descriptor image identity changed")
        shards[entry["path"]].append((destination, entry["row"]))
    if not set(legacy["chunks"]) <= set(shards):
        raise RuntimeError("original checkpoint contains unexpected references")
    directory = out / "score_parts"
    directory.mkdir(exist_ok=True)
    score_shape = (len(queries), len(eligible))
    # Only score arrays, not a second100k×8448 descriptor pool, live in RAM.
    scores = np.empty(score_shape, np.float32)
    old_matrix = None
    committed = {"inputs": expected, "chunks": {}}
    filenames = sorted(shards)
    # Migrate already committed columns first, then release the legacy matrix.
    ordered = sorted(filenames, key=lambda name: name not in legacy["chunks"])
    for completed, filename in enumerate(ordered, 1):
        positions = np.asarray(shards[filename], np.int64)
        source = {"sha256": digest(filename), "positions_sha256": hashlib.sha256(positions.tobytes()).hexdigest()}
        number = filenames.index(filename)
        path = directory / f"part-{number:04d}.npy"
        receipt_path = path.with_suffix(".json")
        part_inputs = {"inputs": expected, "source": source, "source_path": filename}
        if receipt_path.exists():
            saved = json.loads(receipt_path.read_text())
            if saved["inputs"] != part_inputs or digest(path) != saved["sha256"]:
                raise RuntimeError("committed sequential crop score part changed")
            part = np.load(path, allow_pickle=False)
        elif filename in legacy["chunks"]:
            saved = legacy["chunks"][filename]
            if saved["source"] != source:
                raise RuntimeError("original committed reference shard changed")
            if old_matrix is None:
                # A sequential read avoids thousands of individual FUSE faults.
                old_matrix = np.load(legacy_scores, allow_pickle=False)
                if old_matrix.shape != score_shape or old_matrix.dtype != np.float32:
                    raise RuntimeError("original score matrix shape changed")
            if original.score_columns_hash(old_matrix, positions[:, 0]) != saved["score_sha256"]:
                raise RuntimeError("original committed crop score columns changed")
            part = np.ascontiguousarray(old_matrix[:, positions[:, 0]])
            atomic_array(path, part)
            save(receipt_path, {"inputs": part_inputs, "sha256": digest(path), "reused_original_columns": True})
        else:
            old_matrix = None
            part = np.empty((len(queries), len(positions)), np.float32)
            mapped = np.load(filename, mmap_mode="r", allow_pickle=False)
            try:
                for start in range(0, len(positions), 256):
                    block = mapped[positions[start:start + 256, 1]]
                    if (block.shape != (len(block), 8448) or block.dtype != np.float32 or not np.isfinite(block).all()
                            or not np.allclose(np.linalg.norm(block, axis=1), 1, atol=1e-5)):
                        raise RuntimeError("invalid original reference descriptors")
                    part[:, start:start + len(block)] = queries @ block.T
            finally:
                mapped._mmap.close()
            atomic_array(path, part)
            save(receipt_path, {"inputs": part_inputs, "sha256": digest(path), "reused_original_columns": False})
        if part.shape != (len(queries), len(positions)) or part.dtype != np.float32 or not np.isfinite(part).all():
            raise RuntimeError("invalid crop score part")
        scores[:, positions[:, 0]] = part
        committed["chunks"][filename] = {"source": source,
            "score_sha256": original.score_columns_hash(part, np.arange(part.shape[1]))}
        trial.state(out, "crop_exact_scores_sequential", completed=completed, total=len(shards))
    old_matrix = None
    if not np.isfinite(scores).all():
        raise RuntimeError("incomplete crop score matrix")
    atomic_array(out / "scores.npy", scores)
    save(checkpoint, committed)
    save(out / "scores.done.json", {"scores_sha256": digest(out / "scores.npy"), "inputs": expected,
        "score_chunks_sha256": digest(checkpoint), "shape": list(score_shape)})
    save(out / "io_runtime_repair.completed.json", {"repair_sha256": digest(repair_path),
        "original_committed_shards_reused": len(legacy["chunks"]), "sequential_parts": len(shards),
        "part_receipts": {p.name: digest(p) for p in directory.glob("part-*.json")}})


def run():
    # This entry point can only continue an already fully encoded trial.
    # It never reserves the GPU while validating or reading cached vectors.
    *_, night = trial.evaluator.paths()
    out = night / trial.PHASE
    if os.getpriority(os.PRIO_PROCESS, 0) < 10:
        os.nice(10 - os.getpriority(os.PRIO_PROCESS, 0))
    with (trial.evaluator.LOCAL / "query_square_crop.lock").open("a") as lock, threadpool_limits(limits=1):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        data = trial.load()
        _, gc, source, night, queries, q322, gallery, before, _, _, contract = data
        marker = out / f"{trial.PHASE}.started.json"
        if not marker.exists() or json.loads(marker.read_text())["contract"] != contract:
            raise RuntimeError("I/O resume requires the original registered crop experiment")
        if not (out / "descriptors.done.json").exists():
            raise RuntimeError("finish image encoding before using CPU-only I/O resume")
        for start in range(0, len(queries), trial.SETUP["chunk_queries"]):
            if not (out / "descriptors" / f"queries-{start:04d}.json").exists():
                raise RuntimeError("crop descriptor checkpoint incomplete; no silent image re-encoding")
        previous_scorer = original.compute_scores
        previous_model = trial.gallery_scale.create_model

        def no_model(*args, **kwargs):
            raise RuntimeError("CPU-only I/O continuation cannot re-encode any image")

        original.compute_scores = sequential_scores
        trial.gallery_scale.create_model = no_model
        try:
            if not (out / "done.json").exists():
                vectors = trial.encode(gc, queries, q322, out, contract)
                prefixes = trial.infer(source, gallery, before, vectors, out, contract)
                trial.evaluate(data, prefixes, out)
            trial.publish(night, out, contract)
        finally:
            original.compute_scores = previous_scorer
            trial.gallery_scale.create_model = previous_model


if __name__ == "__main__":
    run()
