"""One fixed query geometry trial on the first completed reference expansion."""
from __future__ import annotations

import fcntl
import hashlib
import inspect
import json
import os
import time
from pathlib import Path

import numpy as np
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale
from ml.research import night_multiview_collage as scoring
from ml.research import night_pipeline_added as pipeline
from ml.research import night_scale_context as context
from ml.research import night_v7 as evaluator
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.metrics import paired_group_bootstrap
from ml.research.night_place import atomic_npz
from ml.research.night_v7_verify import gallery_truth, reconcile_result, validate_prefix
from ml.retrieval.image_io import load_rgb_image

PHASE = "query_square_crop"
ARMS = ("query_square_crop_only", "query_square_crop_mean3")
SETUP = {"queries": 1184, "references": 102944, "dimension": 8448,
         "crop": "largest centered square after EXIF/RGB; floor integer left/top; no earlier resize",
         "preprocessing": "unchanged normalize-then-bilinear-antialias-resize322",
         "batch_size": 2, "device": "mps", "cpu_threads": 1, "chunk_queries": 32,
         "mean_score": "float32 (2*committed mean322_504 score + crop cosine)/3",
         "baseline": "scale_mean_added", "raw100_gate_pp": .3, "recall100_gate_pp": 1.,
         "conditional_context": "only if gate passes: existing context30/mixing0.5 in a separately registered continuation",
         "candidate_selection": "both complete-query inference arms remain eligible; crop-only is the single-view ablation",
         "parameter_sweep": False, "new_training": False, "calibration_final_access": False}


def state(out, phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), **extra)
    save(out / "status.json", value)
    save(evaluator.LOCAL / "query_square_crop_status.json", value)
    print(json.dumps(value), flush=True)


def centered_square(image):
    if image.mode != "RGB" or min(image.size) < 1:
        raise RuntimeError("crop requires a decoded EXIF-normalized RGB image")
    width, height = image.size
    side = min(width, height)
    left, top = (width - side) // 2, (height - side) // 2
    return image.crop((left, top, left + side, top + side))


def combine_scores(pair, crop):
    if pair.shape != crop.shape or pair.dtype != np.float32 or crop.dtype != np.float32:
        raise RuntimeError("three-view scores must share float32 shape")
    # Preserve the completed rounded pair score literally; do not reconstruct
    # its individual components or renormalize the mean query for this ranking.
    return (np.float32(2) * pair + crop) / np.float32(3)


def load():
    data, source, gallery, pool, original_contract = pipeline.load("gallery_addition")
    cfg, previous, night, queries, q322, *_ = data
    before = night / "cpu_pipeline_added/gallery_addition"
    done = json.loads((before / "done.json").read_text())
    published = json.loads((before / "published.json").read_text())
    if done["contract"] != original_contract or published["done_sha256"] != digest(before / "done.json"):
        raise RuntimeError("completed first-gallery mean pipeline contract changed")
    name = SETUP["baseline"]
    record = next(r for r in done["records"] if r["name"] == name)
    comparator_paths = (before / f"results/{name}.json", before / f"results/{name}_rows.json")
    for path, key in zip(comparator_paths, ("summary_sha256", "rows_sha256"), strict=True):
        if digest(path) != record[key] or digest(night / "results" / path.name) != record[key]:
            raise RuntimeError("published same-gallery mean comparator changed")
    summary, rows = [json.loads(p.read_text()) for p in comparator_paths]
    if rows["query_ids"] != queries.id.tolist() or len(queries) != SETUP["queries"] or len(gallery) != SETUP["references"]:
        raise RuntimeError("query crop population or comparator identity changed")
    receipt = json.loads((before / "mean_scores.done.json").read_text())
    if (receipt["contract_sha256"] != context.signature(original_contract)
            or receipt["scores_sha256"] != digest(before / "mean_scores.npy")
            or receipt["prefix_sha256"] != digest(before / "prefix.npz")):
        raise RuntimeError("completed pair mean scores or prefix changed")
    with np.load(before / "prefix.npz", allow_pickle=False) as saved:
        prefix = validate_prefix(saved["indices"], len(queries), len(gallery))
        if not np.array_equal(prefix, rows["top100_gallery_rows"]):
            raise RuntimeError("pair prefix differs from its completed development result")
    retriever = json.loads((previous / "descriptor_contract.json").read_text())["retriever"]
    paths = [Path(__file__), Path(pipeline.__file__), Path(context.__file__), Path(scoring.__file__),
             Path(gallery_scale.__file__), Path(evaluator.__file__), WORKSPACE / "ml/retrieval/torch_hub.py",
             WORKSPACE / "ml/retrieval/sage.py", WORKSPACE / "ml/retrieval/image_io.py",
             WORKSPACE / "ml/research/retrievers.py"]
    contract = {"setup": SETUP, "source_pipeline": original_contract, "retriever": retriever,
                "gallery_sha256": original_contract["gallery_sha256"], "sources": {str(p): digest(p) for p in paths},
                "checkpoint_sha256": retriever["extra"]["checkpoint_sha256"],
                "pair_mean_scores_sha256": receipt["scores_sha256"], "pair_prefix_sha256": receipt["prefix_sha256"],
                "pair_done_sha256": digest(before / "done.json"), "pair_published_sha256": digest(before / "published.json"),
                "baseline_summary_sha256": record["summary_sha256"], "baseline_rows_sha256": record["rows_sha256"],
                "query_ids": queries.id.tolist(), "image_sha256": queries.file_sha256.tolist(),
                "verifier_helpers": {f.__name__: hashlib.sha256(inspect.getsource(f).encode()).hexdigest()
                                     for f in (gallery_truth, reconcile_result, validate_prefix)}}
    return cfg, evaluator.paths()[1], source, night, queries, q322, gallery, before, summary, rows, contract


def encode(gc, queries, original, out, contract):
    """Commit 32 queries at a time; square inputs reuse verified original vectors."""
    context.unit_vectors(original)
    if len(original) != len(queries):
        raise RuntimeError("original descriptor/query alignment changed")
    directory = out / "descriptors"
    directory.mkdir(exist_ok=True)
    pieces, shapes, reuse, receipts, model = [], [], [], {}, None
    try:
        for start in range(0, len(queries), SETUP["chunk_queries"]):
            batch = queries.iloc[start:start + SETUP["chunk_queries"]]
            path = directory / f"queries-{start:04d}.npz"
            receipt = path.with_suffix(".json")
            expected = {"contract_sha256": context.signature(contract), "ids": batch.id.tolist(),
                        "image_sha256": batch.file_sha256.tolist(),
                        "original_vectors_sha256": hashlib.sha256(original[start:start + len(batch)].tobytes()).hexdigest()}
            # Check actual bytes even on resume; cached geometry describes these
            # exact bytes, so a committed query never requires another decode.
            for row in batch.itertuples():
                if digest(row.image_path) != row.file_sha256:
                    raise RuntimeError("fixed development image bytes changed")
            if receipt.exists():
                saved = json.loads(receipt.read_text())
                if saved["inputs"] != expected or saved["sha256"] != digest(path):
                    raise RuntimeError("committed crop descriptor checkpoint changed")
                with np.load(path, allow_pickle=False) as cached:
                    vectors, geometry, reused = cached["vectors"], cached["geometry"], cached["reused_square"]
                    if cached["ids"].tolist() != batch.id.tolist():
                        raise RuntimeError("crop query identities changed")
            else:
                vectors = np.empty((len(batch), SETUP["dimension"]), np.float32)
                geometry, reused, pending, pending_images = [], [], [], []
                for offset, row in enumerate(batch.itertuples()):
                    image = load_rgb_image(row.image_path)
                    crop = centered_square(image)
                    square = image.width == image.height
                    geometry.append(image.size)
                    reused.append(square)
                    if square:
                        if crop.size != image.size or crop.tobytes() != image.tobytes():
                            raise RuntimeError("square crop changes pixels; cached original cannot be reused")
                        vectors[offset] = original[start + offset]
                    else:
                        pending.append(offset)
                        pending_images.append(crop)
                    if len(pending_images) == SETUP["batch_size"] or offset == len(batch) - 1:
                        if pending_images:
                            if model is None:
                                model = gallery_scale.create_model(gc | {"device": "mps", "batch_size": 2, "torch_threads": 1})
                                if model.metadata.to_dict() != contract["retriever"] or model.image_size != (322, 322):
                                    raise RuntimeError("crop encoder differs from frozen SAGE-L322")
                            vectors[pending] = model.embed_batch(pending_images)
                            pending, pending_images = [], []
                geometry, reused = np.asarray(geometry, np.int32), np.asarray(reused, bool)
                context.unit_vectors(vectors)
                atomic_npz(path, vectors=vectors, geometry=geometry, reused_square=reused, ids=np.asarray(batch.id.tolist()))
                save(receipt, {"inputs": expected, "sha256": digest(path)})
            context.unit_vectors(vectors)
            if (vectors.shape != (len(batch), SETUP["dimension"]) or geometry.shape != (len(batch), 2)
                    or reused.shape != (len(batch),) or reused.dtype != bool or (geometry <= 0).any()
                    or not np.array_equal(reused, geometry[:, 0] == geometry[:, 1])
                    or not np.array_equal(vectors[reused], original[start:start + len(batch)][reused])):
                raise RuntimeError("committed crop geometry or square descriptor reuse differs")
            pieces.append(vectors)
            shapes.append(geometry)
            reuse.append(reused)
            receipts[path.name] = {"npz_sha256": digest(path), "receipt_sha256": digest(receipt)}
            state(out, "crop_encoding", completed=start + len(batch), total=len(queries), reused_square=sum(int(v.sum()) for v in reuse))
    finally:
        if model is not None:
            model.close()
    vectors, geometry, reused = np.concatenate(pieces), np.concatenate(shapes), np.concatenate(reuse)
    target, marker = out / "queries.npz", out / "descriptors.done.json"
    expected_done = {"contract_sha256": context.signature(contract), "chunks": receipts,
                     "square_reused": int(reused.sum()), "new_encodes": int((~reused).sum())}
    if marker.exists():
        done = json.loads(marker.read_text())
        if any(done[k] != value for k, value in expected_done.items()) or digest(target) != done["queries_sha256"]:
            raise RuntimeError("completed crop descriptor receipt changed")
        with np.load(target, allow_pickle=False) as cached:
            if (not np.array_equal(cached["vectors"], vectors) or not np.array_equal(cached["geometry"], geometry)
                    or not np.array_equal(cached["reused_square"], reused) or cached["ids"].tolist() != queries.id.tolist()):
                raise RuntimeError("crop descriptor matrix differs from committed chunks")
    else:
        atomic_npz(target, vectors=vectors, ids=np.asarray(queries.id.tolist()), geometry=geometry, reused_square=reused)
        save(marker, expected_done | {"queries_sha256": digest(target)})
    return vectors


def infer(source, gallery, before, vectors, out, contract):
    original_state = scoring.state
    scoring.state = lambda root, phase, **extra: state(root, phase.replace("collage", "crop"), **extra)
    try:
        # Existing streamed scorer: one mmap reference shard, 256 selected
        # vectors, per-shard content hashes and resumable score columns.
        scoring.compute_scores(source, gallery, np.arange(len(gallery)), vectors, out, contract)
    finally:
        scoring.state = original_state
    pair = np.load(before / "mean_scores.npy", mmap_mode="r", allow_pickle=False)
    crop = np.load(out / "scores.npy", mmap_mode="r", allow_pickle=False)
    with np.load(before / "prefix.npz", allow_pickle=False) as saved:
        expected_prefix = saved["indices"]
    try:
        if pair.shape != crop.shape or pair.dtype != np.float32 or pair.shape != (len(vectors), len(gallery)):
            raise RuntimeError("mean/crop score populations differ")
        prefixes = {name: np.empty((len(vectors), 100), np.int32) for name in ARMS}
        for i in range(len(vectors)):
            if not np.isfinite(pair[i]).all() or not np.isfinite(crop[i]).all():
                raise RuntimeError("nonfinite pair/crop scores")
            if not np.array_equal(np.argsort(-pair[i], kind="stable")[:100], expected_prefix[i]):
                raise RuntimeError("pair mean cache fails exact published prefix replay")
            prefixes[ARMS[0]][i] = np.argsort(-crop[i], kind="stable")[:100]
            prefixes[ARMS[1]][i] = np.argsort(-combine_scores(pair[i], crop[i]), kind="stable")[:100]
    finally:
        pair._mmap.close()
        crop._mmap.close()
    target = out / "predictions.npz"
    if target.exists():
        with np.load(target, allow_pickle=False) as saved:
            if any(not np.array_equal(saved[name], values) for name, values in prefixes.items()):
                raise RuntimeError("committed crop predictions changed")
    else:
        atomic_npz(target, **prefixes)
    return prefixes


def context_gate(baseline, result):
    raw_gain = 100 * (result["raw"]["accuracy_100m"] - baseline["raw"]["accuracy_100m"])
    recall_gain = 100 * (result["recall_at"]["100"] - baseline["recall_at"]["100"])
    eligible = raw_gain >= SETUP["raw100_gate_pp"] or recall_gain >= SETUP["recall100_gate_pp"]
    return {"raw100_gain_pp": raw_gain, "recall100_gain_pp": recall_gain, "eligible_for_fixed_context": eligible,
            "thresholds": {"raw100_pp": SETUP["raw100_gate_pp"], "recall100_pp": SETUP["recall100_gate_pp"]},
            "context_implemented_or_started": False, "decision": "await root review" if eligible else "close query-crop family"}


def evaluate(data, prefixes, out):
    cfg, _, source, _, queries, q322, gallery, _, baseline, baseline_rows, contract = data
    truth = gallery_truth(queries, gallery)  # Evaluation-only, after inference is committed.
    reconcile_result(baseline, baseline_rows, queries, gallery, truth)
    original_inputs, original_paths, original_local = evaluator.inputs, evaluator.paths, evaluator.LOCAL
    previous = original_paths()[3]
    base = original_inputs()[-1]
    augmented = dict(base, positive_count_100m=truth["positive_count"].tolist())
    evaluator.inputs = lambda: (cfg, None, previous, out, queries, q322, gallery, augmented)
    evaluator.paths = lambda: (*original_paths()[:4], out)
    evaluator.LOCAL = out
    records, results = [], {}
    try:
        for name in ARMS:
            validate_prefix(prefixes[name], len(queries), len(gallery))
            result = evaluator.record_result(name, prefixes[name], note=contract,
                extra={"gallery_manifest": str(source / "gallery.parquet"), "gallery_sha256": contract["gallery_sha256"],
                       "single_view_ablation": name == ARMS[0]})
            rows_path = out / f"results/{name}_rows.json"
            rows = json.loads(rows_path.read_text())
            result["paired_vs_scale_mean_added"] = paired_group_bootstrap(
                baseline_rows["errors_m"], rows["errors_m"], queries.h3_coarse.astype(str).tolist())
            verification = reconcile_result(result, rows, queries, gallery, truth)
            save(out / f"results/{name}.json", result)
            results[name] = result
            records.append({"name": name, "summary_sha256": digest(out / f"results/{name}.json"),
                            "rows_sha256": digest(rows_path), "verification": verification})
    finally:
        evaluator.inputs, evaluator.paths, evaluator.LOCAL = original_inputs, original_paths, original_local
    gate = context_gate(baseline, results[ARMS[1]])
    save(out / "gate.json", gate)
    if not gate["eligible_for_fixed_context"]:
        save(out / "closed.json", {"reason": "fixed crop family continuation gate failed", "gate": gate})
    save(out / "done.json", {"completed": time.time(), "contract": contract, "records": records,
         "predictions_sha256": digest(out / "predictions.npz"), "gate_sha256": digest(out / "gate.json"),
         "descriptors_done_sha256": digest(out / "descriptors.done.json"), "scores_done_sha256": digest(out / "scores.done.json"),
         "score_chunks_sha256": digest(out / "score_chunks.json"), "queries_sha256": digest(out / "queries.npz"),
         "scores_sha256": digest(out / "scores.npy"), "pair_top100_replayed": True})


def publish(night, out, contract):
    done = json.loads((out / "done.json").read_text())
    if done["contract"] != contract:
        raise RuntimeError("completed crop source contract changed")
    for filename, key in (("predictions.npz", "predictions_sha256"), ("gate.json", "gate_sha256"),
                          ("descriptors.done.json", "descriptors_done_sha256"), ("scores.done.json", "scores_done_sha256"),
                          ("score_chunks.json", "score_chunks_sha256"), ("queries.npz", "queries_sha256"), ("scores.npy", "scores_sha256")):
        if digest(out / filename) != done[key]:
            raise RuntimeError("completed crop artifact changed before publication")
    chunks = json.loads((out / "descriptors.done.json").read_text())["chunks"]
    for name, record in chunks.items():
        path = out / "descriptors" / name
        if digest(path) != record["npz_sha256"] or digest(path.with_suffix(".json")) != record["receipt_sha256"]:
            raise RuntimeError("crop descriptor chunk changed before publication")
    state(out, "crop_waiting_to_publish")
    with (evaluator.LOCAL / "controller.lock").open("a") as main:
        fcntl.flock(main, fcntl.LOCK_EX)
        for record in done["records"]:
            for suffix, key in ((".json", "summary_sha256"), ("_rows.json", "rows_sha256")):
                source = out / "results" / (record["name"] + suffix)
                target = night / "results" / source.name
                if digest(source) != record[key] or target.exists() and digest(target) != record[key]:
                    raise RuntimeError("crop result changed before publication")
                if not target.exists():
                    save(target, json.loads(source.read_text()))
        save(out / "published.json", {"published": time.time(), "done_sha256": digest(out / "done.json")})
        evaluator.report()
    state(out, "crop_completed")


def run():
    *_, night = evaluator.paths()
    out = night / PHASE
    out.mkdir(exist_ok=True)
    if os.getpriority(os.PRIO_PROCESS, 0) < 10:
        os.nice(10 - os.getpriority(os.PRIO_PROCESS, 0))
    with (evaluator.LOCAL / "query_square_crop.lock").open("a") as own, threadpool_limits(limits=1):
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            data = load()
            cfg, gc, source, night, queries, q322, gallery, before, _, _, contract = data
            marker = out / f"{PHASE}.started.json"
            if marker.exists() and json.loads(marker.read_text())["contract"] != contract:
                raise RuntimeError("registered query geometry experiment changed")
            if not (out / "done.json").exists():
                # Wait for active GPU work without holding its lock during the
                # later CPU score pass. Check cutoff AFTER the wait/validation.
                with (evaluator.LOCAL / "controller.lock").open("a") as gpu:
                    fcntl.flock(gpu, fcntl.LOCK_EX)
                    if not evaluator.allowed_to_start(cfg, PHASE, out):
                        save(out / "closed.json", {"reason": "06:00 cutoff; crop trial never started"})
                        return
                    if not marker.exists():
                        save(marker, {"started": time.time(), "contract": contract})
                    vectors = encode(gc, queries, q322, out, contract)
                prefixes = infer(source, gallery, before, vectors, out, contract)
                evaluate(data, prefixes, out)
            publish(night, out, contract)
        except BaseException as exc:
            save(out / "failure.json", {"time": time.time(), "error": repr(exc)})
            state(out, "crop_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    run()
