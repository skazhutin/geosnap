"""One auxiliary 2x2 collage trial; never modifies the main development board."""
from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import time
from collections import defaultdict
from pathlib import Path

import h3
import numpy as np
import pandas as pd
from PIL import Image
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale, night_multiview
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.metrics import paired_group_bootstrap, raw_metrics
from ml.research.night_place import atomic_npz
from ml.research.night_v7 import LOCAL, allowed_to_start, paths
from ml.research.vector_evaluation import distances

SETUP = {"layout": "2x2 row-major, cyclic yaw order starting with case yaw",
         "yaws": [0, 90, 180, 270], "perspective_size": 322, "collage_size": 644,
         "model_input_size": 322, "model": "SAGE ViT-L No-Encoder", "device": "mps", "batch_size": 2,
         "cpu_threads": 1, "primary_comparator": "place_four", "secondary_comparator": "single",
         "physical_parents": 43, "orientation_cases": 172, "eligible_references": 99919,
         "estimator": "exact cosine, top1 reference coordinate", "no_abstention": True,
         "auxiliary_only": True, "calibration_final_access": False}


def state(out, phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), auxiliary_only=True, **extra)
    save(out / "status.json", value)
    save(LOCAL / "collage_status.json", value)
    print(json.dumps(value), flush=True)


def hash_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def make_collage(views, start):
    """No resizing here: the unmodified official transform handles the644 canvas."""
    size = SETUP["perspective_size"]
    if len(views) != 4 or start not in range(4) or any(v.size != (size, size) or v.mode != "RGB" for v in views):
        raise RuntimeError("collage requires the four fixed RGB322 perspective views")
    canvas = Image.new("RGB", (2 * size, 2 * size))
    for position in range(4):
        canvas.paste(views[(start + position) % 4], ((position % 2) * size, (position // 2) * size))
    return canvas


def validate_population(gallery, parents, eligible, records, prepared):
    if (parents.shape != (SETUP["physical_parents"],) or not np.issubdtype(parents.dtype, np.integer)
            or len(set(parents)) != len(parents) or (parents < 0).any() or (parents >= len(gallery)).any()):
        raise RuntimeError("auxiliary physical-parent population changed")
    expected = np.flatnonzero(~gallery.sequence_key.isin(prepared["forbidden_sequences"]))
    if (not np.issubdtype(eligible.dtype, np.integer) or not np.array_equal(eligible, expected)
            or len(eligible) != SETUP["eligible_references"]):
        raise RuntimeError("collage must retain exactly the existing auxiliary gallery exclusions")
    if (set(parents) & set(eligible) or set(gallery.sequence_key.iloc[parents]) & set(gallery.sequence_key.iloc[eligible])):
        raise RuntimeError("auxiliary parent or provider sequence leaked into reference search")
    actual = [{"gallery_row": int(j), "id": gallery.id.iloc[j], "sequence_key": gallery.sequence_key.iloc[j],
               "sha256": gallery.file_sha256.iloc[j],
               "h3_group": h3.latlng_to_cell(float(gallery.lat.iloc[j]), float(gallery.lon.iloc[j]), 6)} for j in parents]
    if records != actual:
        raise RuntimeError("auxiliary parent identity, order, source SHA or geographic group changed")
    if (prepared["parents"] != len(parents) or prepared["direction_queries"] != 4 * len(parents)
            or prepared["remaining_references"] != len(eligible)):
        raise RuntimeError("auxiliary prepared population counts changed")


def load_inputs(cfg, gc, previous, night):
    aux = night / "aux_multiview"
    prepared = json.loads((aux / "prepared.json").read_text())
    fixed = json.loads((night / "input_contract.json").read_text())
    descriptor = json.loads((previous / "descriptor_contract.json").read_text())
    for path, expected in ((previous / "manifests/G2_smart.parquet", fixed["gallery_sha256"]),
                           (previous / "descriptor_contract.json", fixed["descriptor_contract_sha256"]),
                           (previous / "descriptor_pool.json", fixed["pool_sha256"]),
                           (aux / "queries.npz", prepared["query_vectors_sha256"]),
                           (aux / "parents.json", prepared["groups_sha256"])):
        if digest(path) != expected:
            raise RuntimeError("collage source manifest or prepared auxiliary inputs changed")
    original = prepared["contract"]
    if (original["source_sha256"] != digest(Path(night_multiview.__file__)) or original["setup"] != night_multiview.SETUP
            or original["input_contract_sha256"] != digest(night / "input_contract.json") or original["config"] != cfg):
        raise RuntimeError("existing auxiliary source/night protocol changed")
    done = json.loads((aux / "done.json").read_text())
    verified = json.loads((aux / "verification.json").read_text())
    if (verified.get("verified") is not True or verified["report_sha256"] != digest(aux / "report.json")
            or verified["rows_sha256"] != digest(aux / "rows.json") or done["report_sha256"] != verified["report_sha256"]):
        raise RuntimeError("existing auxiliary comparator results are incomplete or changed")
    with np.load(aux / "queries.npz", allow_pickle=False) as cached:
        parents, eligible = cached["parents"], cached["eligible"]
    g = pd.read_parquet(previous / "manifests/G2_smart.parquet")
    records = json.loads((aux / "parents.json").read_text())
    validate_population(g, parents, eligible, records, prepared)
    comparisons = json.loads((aux / "rows.json").read_text())
    if [(row["parent"], row["yaw"]) for row in comparisons] != [(int(j), yaw) for j in parents for yaw in SETUP["yaws"]]:
        raise RuntimeError("auxiliary comparator parent/yaw order changed")
    pinned = [aux / name for name in ("prepared.json", "queries.npz", "parents.json", "rows.json", "report.json", "verification.json")]
    sources = [Path(__file__), Path(night_multiview.__file__), Path(gallery_scale.__file__),
               WORKSPACE / "ml/research/night_v7.py", WORKSPACE / "ml/research/retrievers.py",
               WORKSPACE / "ml/retrieval/sage.py", WORKSPACE / "ml/retrieval/torch_hub.py"]
    contract = {"setup": SETUP, "config": cfg, "gallery_config": gc,
                "night_input_sha256": digest(night / "input_contract.json"),
                "gallery_sha256": fixed["gallery_sha256"], "pool_sha256": fixed["pool_sha256"],
                "descriptor_contract_sha256": fixed["descriptor_contract_sha256"], "retriever": descriptor["retriever"],
                "auxiliary_inputs": {p.name: digest(p) for p in pinned},
                "source_sha256": {p.name: digest(p) for p in sources}}
    return g, parents, eligible, records, comparisons, contract


def validate_vectors(vectors):
    if (vectors.shape != (4, 8448) or vectors.dtype != np.float32 or not np.isfinite(vectors).all()
            or not np.allclose(np.linalg.norm(vectors, axis=1), 1, atol=1e-5)):
        raise RuntimeError("invalid collage unit descriptors")
    return vectors


def descriptor_chunk(path, receipt, expected):
    saved = json.loads(receipt.read_text())
    if saved["inputs"] != expected or digest(path) != saved["sha256"]:
        raise RuntimeError("committed collage descriptor chunk changed")
    with np.load(path, allow_pickle=False) as cached:
        return validate_vectors(cached["vectors"])


def encode(gc, g, parents, out, contract):
    directory = out / "descriptors"
    directory.mkdir(exist_ok=True)
    model, pieces = None, []
    try:
        for i, j in enumerate(parents):
            row = g.iloc[int(j)]
            expected = {"contract_sha256": hash_json(contract), "parent": int(j), "parent_id": row.id,
                        "image_sha256": row.file_sha256, "yaws": SETUP["yaws"]}
            target = directory / f"parent-{i:03d}.npz"
            receipt = target.with_suffix(".json")
            # Validate the actual physical parent even when its vectors are reused.
            payload = Path(row.image_path).read_bytes()
            if hashlib.sha256(payload).hexdigest() != row.file_sha256:
                raise RuntimeError("physical panorama parent changed")
            if not receipt.exists():
                with Image.open(io.BytesIO(payload)) as opened:
                    image = opened.convert("RGB")
                    if abs(image.width / image.height - 2) > .08:
                        raise RuntimeError("collage parent is no longer verified equirectangular")
                    views = [gallery_scale.perspective(image, yaw, 322) for yaw in SETUP["yaws"]]
                collages = [make_collage(views, start) for start in range(4)]
                if model is None:
                    model = gallery_scale.create_model(dict(gc, device="mps", batch_size=2, torch_threads=1))
                    if model.metadata.to_dict() != contract["retriever"] or tuple(model.image_size) != (322, 322):
                        raise RuntimeError("collage must use the frozen SAGE-L322 preprocessing and checkpoint")
                vectors = validate_vectors(model.embed_batch(collages))
                atomic_npz(target, vectors=vectors)
                save(receipt, {"inputs": expected, "sha256": digest(target)})
            pieces.append(descriptor_chunk(target, receipt, expected))
            state(out, "encoding_collages", completed=i + 1, total=len(parents))
    finally:
        if model is not None:
            model.close()
    vectors = np.concatenate(pieces)
    done = out / "descriptors.done.json"
    chunks = {p.name: digest(p) for p in sorted(directory.glob("*.npz"))}
    if done.exists():
        saved = json.loads(done.read_text())
        if (saved["query_sha256"] != digest(out / "queries.npz") or saved["contract_sha256"] != hash_json(contract)
                or saved["chunks"] != chunks):
            raise RuntimeError("completed collage descriptors changed")
        with np.load(out / "queries.npz", allow_pickle=False) as cached:
            if not np.array_equal(cached["vectors"], vectors) or not np.array_equal(cached["parents"], parents):
                raise RuntimeError("collage descriptor matrix differs from its committed parent chunks")
        return vectors
    atomic_npz(out / "queries.npz", vectors=vectors, parents=parents)
    save(done, {"query_sha256": digest(out / "queries.npz"),
         "contract_sha256": hash_json(contract), "parents": len(parents), "cases": len(vectors),
         "chunks": chunks})
    return vectors


def score_columns_hash(scores, positions):
    checksum = hashlib.sha256()
    for start in range(0, len(positions), 256):
        checksum.update(np.ascontiguousarray(scores[:, positions[start:start + 256]]).tobytes())
    return checksum.hexdigest()


def compute_scores(previous, g, eligible, qv, out, contract):
    pool = json.loads((previous / "descriptor_pool.json").read_text())["entries"]
    shards = defaultdict(list)
    for destination, j in enumerate(eligible):
        row = g.iloc[int(j)]
        entry = pool[row.id]
        if entry["image_sha256"] != row.file_sha256:
            raise RuntimeError("collage reference descriptor identity changed")
        shards[entry["path"]].append((destination, entry["row"]))
    path, checkpoint = out / "scores.npy", out / "score_chunks.json"
    if checkpoint.exists() and not path.exists():
        raise RuntimeError("committed collage scores lost their matrix")
    expected = {"contract_sha256": hash_json(contract), "query_sha256": digest(out / "queries.npz")}
    committed = json.loads(checkpoint.read_text()) if checkpoint.exists() else {"inputs": expected, "chunks": {}}
    if committed["inputs"] != expected or not set(committed["chunks"]) <= set(shards):
        raise RuntimeError("collage score checkpoint input contract changed")
    scores = np.lib.format.open_memmap(path, mode="r+" if path.exists() else "w+", dtype=np.float32,
                                       shape=(len(qv), len(eligible)))
    try:
        if scores.shape != (len(qv), len(eligible)) or scores.dtype != np.float32:
            raise RuntimeError("collage score matrix shape/dtype changed")
        for number, (filename, rows) in enumerate(sorted(shards.items()), 1):
            positions = np.asarray(rows, dtype=np.int64)
            source_sha = digest(filename)
            source = {"sha256": source_sha, "positions_sha256": hashlib.sha256(positions.tobytes()).hexdigest()}
            if filename in committed["chunks"]:
                saved = committed["chunks"][filename]
                if saved["source"] != source or saved["score_sha256"] != score_columns_hash(scores, positions[:, 0]):
                    raise RuntimeError("committed collage score columns or source descriptors changed")
                continue
            mapped = np.load(filename, mmap_mode="r", allow_pickle=False)
            try:
                for start in range(0, len(rows), 256):
                    dest, indices = positions[start:start + 256].T
                    block = mapped[indices]
                    if (block.shape != (len(dest), 8448) or block.dtype != np.float32 or not np.isfinite(block).all()
                            or not np.allclose(np.linalg.norm(block, axis=1), 1, atol=1e-5)):
                        raise RuntimeError("invalid cached collage reference descriptors")
                    scores[:, dest] = qv @ block.T
            finally:
                mapped._mmap.close()
            scores.flush()
            committed["chunks"][filename] = {"source": source, "score_sha256": score_columns_hash(scores, positions[:, 0])}
            save(checkpoint, committed)
            state(out, "collage_exact_scores", completed=number, total=len(shards))
    finally:
        scores._mmap.close()
    save(out / "scores.done.json", {"scores_sha256": digest(path), "inputs": expected,
         "score_chunks_sha256": digest(checkpoint), "shape": [len(qv), len(eligible)]})


def verify_rows(g, parents, eligible, rows, scores, comparisons):
    """Independently reconcile emitted coordinates, ranks and exact candidate order."""
    allowed = set(map(int, eligible))
    lat, lon = np.radians(g.lat.to_numpy(float)), np.radians(g.lon.to_numpy(float))
    if len(rows) != len(parents) * 4 or len(comparisons) != len(rows):
        raise RuntimeError("collage orientation denominator changed")
    errors, ranks, groups = [], [], []
    for i, (row, before) in enumerate(zip(rows, comparisons, strict=True)):
        parent, yaw = int(parents[i // 4]), SETUP["yaws"][i % 4]
        if (row["parent"], row["yaw"]) != (parent, yaw) or (before["parent"], before["yaw"]) != (parent, yaw):
            raise RuntimeError("collage and comparator parent/yaw order differ")
        indices = np.asarray(row["top100_gallery_rows"])
        if (indices.shape != (100,) or not np.issubdtype(indices.dtype, np.integer)
                or len(set(indices)) != 100 or not set(indices) <= allowed):
            raise RuntimeError("collage result contains an excluded or invalid reference")
        if not np.array_equal(indices, eligible[np.argsort(-scores[i], kind="stable")[:100]]):
            raise RuntimeError("collage prediction differs from exact cosine")
        truth = g.iloc[parent]
        d = distances(truth.lat, truth.lon, lat[indices], lon[indices])
        positive = np.flatnonzero(d <= 100)
        first = int(positive[0]) + 1 if len(positive) else None
        if first != row["positive_rank_through100"] or not np.isclose(d[0], row["error_m"], atol=1e-7, rtol=0):
            raise RuntimeError("collage saved error or positive rank differs from coordinates")
        # Recheck the two already-pinned comparators against the identical gallery.
        for name in ("place_four", "single"):
            candidate = before["methods"][name]
            original = np.asarray(candidate["top100_gallery_rows"])
            if (original.shape != (100,) or not np.issubdtype(original.dtype, np.integer)
                    or len(set(original)) != 100 or not set(original) <= allowed):
                raise RuntimeError("auxiliary comparator has invalid or differently excluded references")
            cd = distances(truth.lat, truth.lon, lat[original], lon[original])
            positive = np.flatnonzero(cd <= 100)
            comparator_rank = int(positive[0]) + 1 if len(positive) else None
            if (not np.isclose(cd[0], candidate["error_m"], atol=1e-7, rtol=0)
                    or candidate["positive_rank_through100"] != comparator_rank):
                raise RuntimeError("auxiliary comparator error or rank changed")
        errors.append(float(d[0]))
        ranks.append(first or np.inf)
        groups.append(h3.latlng_to_cell(float(truth.lat), float(truth.lon), 6))
    return errors, ranks, groups


def evaluate(g, parents, eligible, comparisons, out, contract):
    done = json.loads((out / "scores.done.json").read_text())
    if (done["inputs"]["contract_sha256"] != hash_json(contract)
            or done["inputs"]["query_sha256"] != digest(out / "queries.npz")
            or done["score_chunks_sha256"] != digest(out / "score_chunks.json")
            or done["scores_sha256"] != digest(out / "scores.npy")):
        raise RuntimeError("collage completed score contract changed")
    scores = np.load(out / "scores.npy", mmap_mode="r", allow_pickle=False)
    rows = []
    lat, lon = np.radians(g.lat.to_numpy(float)), np.radians(g.lon.to_numpy(float))
    try:
        for i, values in enumerate(scores):
            parent = int(parents[i // 4])
            order = eligible[np.argsort(-values, kind="stable")[:100]]
            truth = g.iloc[parent]
            d = distances(truth.lat, truth.lon, lat[order], lon[order])
            positive = np.flatnonzero(d <= 100)
            rows.append({"parent": parent, "yaw": SETUP["yaws"][i % 4], "error_m": float(d[0]),
                         "positive_rank_through100": int(positive[0]) + 1 if len(positive) else None,
                         "top100_gallery_rows": order.tolist()})
        errors, ranks, groups = verify_rows(g, parents, eligible, rows, scores, comparisons)
    finally:
        scores._mmap.close()
    paired = {name: paired_group_bootstrap([r["methods"][name]["error_m"] for r in comparisons], errors, groups)
              for name in ("place_four", "single")}
    comparators = {name: {"raw": raw_metrics([r["methods"][name]["error_m"] for r in comparisons]),
                          "R_at": {str(k): float(np.mean([(r["methods"][name]["positive_rank_through100"] or np.inf) <= k
                                                          for r in comparisons])) for k in (1, 5, 10, 20, 50, 100)}}
                   for name in ("place_four", "single")}
    report = {"setup": SETUP, "contract_sha256": hash_json(contract), "raw": raw_metrics(errors),
              "R_at": {str(k): float(np.mean(np.asarray(ranks) <= k)) for k in (1, 5, 10, 20, 50, 100)},
              "paired_vs": paired, "comparators": comparators, "physical_places": len(parents), "orientation_anchored_cases": len(rows),
              "h3_r6_blocks": len(set(groups)), "unchanged_main_development": True,
              "limitations": ["synthetic old reference-side panoramas; not real-user or main-development accuracy",
                  "four yaw cases per parent are dependent; only six geographic blocks",
                  "collage uses one322 forward per four-photo case, about161 pixels per tile axis; consensus uses four322 forwards",
                  "644 canvas is resized by official normalize-then-resize transform; no layout or resolution sweep",
                  "same frozen parent/sequence/duplicate exclusions; unknown aliases and partial-image duplicates remain uncertified",
                  "small exploratory sample; no final or production claim"]}
    save(out / "rows.json", rows)
    save(out / "report.json", report)
    save(out / "verification.json", {"verified": True, "report_sha256": digest(out / "report.json"),
         "rows_sha256": digest(out / "rows.json"), "scores_sha256": done["scores_sha256"],
         "exact_cosine_top100_verified": True, "same_parent_yaw_order_and_eligible_gallery": True,
         "contract_sha256": hash_json(contract), "physical_places": len(parents), "dependent_cases": len(rows)})
    metric = report["raw"]
    text = ("# Auxiliary four-photo collage\n\n43 synthetic panorama locations,172 dependent yaw cases,6 geographic blocks. "
            "No main-development or real-user accuracy claim.\n\n"
            f"Raw≤100m: {100 * metric['accuracy_100m']:.3f}%; "
            f"median/p90: {metric['median_error_m']:.0f}/{metric['p90_error_m']:.0f}m; "
            f">500m: {100 * metric['catastrophic_gt500m_rate']:.3f}%.\n\n")
    for name, result in paired.items():
        text += f"Compared with {name}: {result['gain_pp']:+.3f}pp; grouped95% interval {result['ci95_pp']}.\n\n"
    text += "Collage: one322 encode per bundle, about161×161 per tile. Individual consensus: four322 encodes. Fixed setup; no tuning.\n"
    (out / "report.md").write_text(text)
    save(out / "done.json", {"completed": time.time(), "contract_sha256": hash_json(contract),
         "files": {name: digest(out / name) for name in ("rows.json", "report.json", "report.md", "verification.json", "scores.done.json", "descriptors.done.json")}})
    state(out, "collage_completed", raw=metric, paired_vs=paired)


def run():
    cfg, gc, _, previous, night = paths()
    out = night / "aux_multiview_collage"
    out.mkdir(exist_ok=True)
    if os.getpriority(os.PRIO_PROCESS, 0) < 10:
        os.nice(10 - os.getpriority(os.PRIO_PROCESS, 0))
    with (LOCAL / "collage.lock").open("a") as own, threadpool_limits(limits=1):
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (out / "closed.json").exists():
            return
        try:
            # Match the existing queue order; never take controller.lock while
            # waiting for an earlier GPU job which itself needs that lock.
            state(out, "collage_waiting_for_registered_gpu_queue")
            with (LOCAL / "acquisition_queue.lock").open("a") as acquisition, (LOCAL / "followthrough.lock").open("a") as follow, (LOCAL / "controller.lock").open("a") as controller:
                for lock in (acquisition, follow, controller):
                    fcntl.flock(lock, fcntl.LOCK_EX)
                if not allowed_to_start(cfg, "collage", out):
                    save(out / "closed.json", {"reason": "06:00 cutoff reached while waiting; no collage encoding started"})
                    return
                g, parents, eligible, records, comparisons, contract = load_inputs(cfg, gc, previous, night)
                marker = out / "collage.started.json"
                if not allowed_to_start(cfg, "collage", out):
                    save(out / "closed.json", {"reason": "06:00 cutoff reached during input validation; no collage encoding started"})
                    return
                if marker.exists():
                    if json.loads(marker.read_text())["contract"] != contract:
                        raise RuntimeError("registered collage input/source contract changed")
                else:
                    save(marker, {"started": time.time(), "contract": contract,
                         "actual_start_after_gpu_queues": True, "no_new_main_development_trial": True})
                if (out / "done.json").exists():
                    completed = json.loads((out / "done.json").read_text())
                    if completed["contract_sha256"] != hash_json(contract) or any(digest(out / name) != sha for name, sha in completed["files"].items()):
                        raise RuntimeError("completed collage artifacts changed")
                    return
                qv = encode(gc, g, parents, out, contract)
            # The GPU and main controller are free during this CPU-only pass.
            if not (out / "scores.done.json").exists():
                compute_scores(previous, g, eligible, qv, out, contract)
            evaluate(g, parents, eligible, comparisons, out, contract)
        except BaseException as exc:
            save(out / "failure.json", {"failed": time.time(), "error": repr(exc), "pid": os.getpid()})
            state(out, "collage_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    run()
