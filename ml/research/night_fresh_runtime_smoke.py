"""Two preregistered development images: fresh dual-scale query/search/context parity."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import resource
import socket
import sys
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale
from ml.research import night_quarantine_audit as audit
from ml.research import night_quarantine_eval as comparison
from ml.research import night_scale_context as context
from ml.research import night_v7 as evaluator
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.night_place import atomic_npz

SETUP = {"fixed_development_positions": [17, 941], "reference_scope": "common-quarantine expanded2",
         "scales": [322, 504], "device": "mps", "context_depth": 30, "context_mixing": .5,
         "descriptor_max_abs_tolerance": 1e-5, "descriptor_l2_tolerance": 1e-3,
         "score_max_abs_tolerance": 2e-6, "invariance_tolerance": 1e-5,
         "query_selection_uses_accuracy": False, "cpu_fresh_encoding": False,
         "new_reference_encoding": False, "reference_block_rows": 256,
         "calibration_final_access": False, "production_changes": False,
         "purpose": "research runtime reproducibility/resources only; not ML selection or deployment approval"}
PHASE = "smoke"


def stage():
    cfg, gc, _, previous, night = evaluator.paths()
    return cfg, gc, previous, night, night / "fresh_runtime_smoke"


def peak_rss_bytes():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def state(out, phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), peak_rss_bytes=peak_rss_bytes(), **extra)
    save(out / "status.json", value)
    save(evaluator.LOCAL / "fresh_runtime_smoke_status.json", value)
    print(json.dumps(value), flush=True)


def prepare():
    _, gc, _, _, out = stage()
    out.mkdir(exist_ok=True)
    path = WORKSPACE / gc["query_manifest"]
    if digest(path) != gc["query_sha256"]:
        raise RuntimeError("frozen development population changed")
    identities = pd.read_parquet(path, columns=["id", "file_sha256", "image_path"])
    if len(identities) != 1184:
        raise RuntimeError("fixed development denominator changed")
    selected = identities.iloc[SETUP["fixed_development_positions"]]
    value = {"setup": SETUP, "source_sha256": digest(Path(__file__)), "query_sha256": gc["query_sha256"],
             "query_ids": selected.id.tolist(), "image_sha256": selected.file_sha256.tolist(),
             "preparation_is_not_experiment_start": True}
    path = out / "intent.json"
    if path.exists():
        if json.loads(path.read_text()) != value:
            raise RuntimeError("preregistered smoke population/source changed")
    else:
        save(path, value)


def load():
    cfg, gc, previous, night, out = stage()
    _, _, _, parent, _, _, _, gallery = audit.verify_audited()
    source = parent / "evaluation/expanded2"
    completed = json.loads((source / "done.json").read_text())
    contract = completed["contract"]
    comparison.verify_done(source, contract)
    if (contract["source_sha256"] != digest(Path(comparison.__file__))
            or contract["audit_sha256"] != digest(parent / "audit.done.json")
            or contract["manifests"]["gallery.parquet"] != digest(parent / "gallery.parquet")):
        raise RuntimeError("completed comparison source/gallery changed")
    queries, all322 = gallery_scale.query_vectors(gc)
    if hashlib.sha256(all322.tobytes()).hexdigest() != contract["component"]["query322_values_sha256"]:
        raise RuntimeError("committed SAGE322 query vectors differ")
    positions = SETUP["fixed_development_positions"]
    selected = queries.iloc[positions].reset_index(drop=True)
    q322 = all322[positions].copy()
    del all322
    intent = json.loads((out / "intent.json").read_text())
    if selected.id.tolist() != intent["query_ids"] or selected.file_sha256.tolist() != intent["image_sha256"]:
        raise RuntimeError("two selected development identities changed")
    q504 = np.empty_like(q322)
    expected = {"query_sha256": gc["query_sha256"], "reference_contract_sha256": digest(previous / "descriptor_contract.json"),
                "query_size": [504, 504], "reference_size": [322, 322], "device": "mps", "batch": 2}
    chunk_hashes = {}
    for i, position in enumerate(positions):
        path = night / "query504" / f"chunk-{position // 32:04d}.npz"
        chunk_hashes[str(path)] = digest(path)
        if chunk_hashes[str(path)] != contract["component"]["query504_chunk_sha256"][path.name]:
            raise RuntimeError("committed SAGE504 query chunk changed")
        with np.load(path, allow_pickle=False) as saved:
            if (json.loads(str(saved["contract"])) != expected
                    or saved["ids"][position % 32] != selected.id.iloc[i]):
                raise RuntimeError("SAGE504 cached descriptor identity/preprocessing differs")
            q504[i] = saved["vectors"][position % 32]
    context.unit_vectors(q322)
    context.unit_vectors(q504)
    model_contract = json.loads((previous / "descriptor_contract.json").read_text())
    checkpoint = WORKSPACE / "data/models/research_v5/sage_context_encoder.pth"
    if digest(checkpoint) != evaluator.ENCODER_SHA or gc["device"] != "mps":
        raise RuntimeError("pinned MPS/context configuration changed")
    pool_path = parent / "descriptor_pool.json"
    pool = json.loads(pool_path.read_text())
    encoded = json.loads((parent / "encode.done.json").read_text())
    if digest(pool_path) != encoded["descriptor_pool_sha256"] or pool["contract_sha256"] != expected["reference_contract_sha256"]:
        raise RuntimeError("expanded source descriptor pool changed")
    paths = [out / "intent.json", parent / "audit.done.json", parent / "encode.done.json", pool_path,
             source / "done.json", source / "top1_scores.json", source / "mean_scores.json",
             source / "top1_rows.json", source / "mean_rows.json", source / "context_rows.json",
             source / "context_evidence.json", source / "context_evidence.npz", checkpoint,
             previous / "descriptor_contract.json"]
    registered = {"setup": SETUP, "files": {str(p): digest(p) for p in paths} | chunk_hashes,
        "sources": {p.name: digest(p) for p in (Path(__file__), Path(audit.__file__), Path(comparison.__file__),
                     Path(context.__file__), Path(gallery_scale.__file__), Path(evaluator.__file__))},
        "query_ids": selected.id.tolist(), "image_sha256": selected.file_sha256.tolist(),
        "gallery_sha256": digest(parent / "gallery.parquet"), "source_contract": contract,
        "retriever": model_contract["retriever"], "encoding_runtime": {k: gc[k] for k in ("device", "batch_size", "torch_threads")}}
    return dict(cfg=cfg, gc=gc, previous=previous, night=night, out=out, parent=parent, source=source,
                q=selected, positions=positions, cached={322: q322, 504: q504}, g=gallery,
                pool=pool["entries"], checkpoint=checkpoint, contract=registered)


def vector_parity(actual, cached, reverse, singles):
    for values in (actual, cached, reverse, singles):
        context.unit_vectors(values)
    value = {"cached_max_abs": float(np.max(np.abs(actual - cached))),
             "cached_max_l2": float(np.max(np.linalg.norm(actual - cached, axis=1))),
             "reverse_batch_max_abs": float(np.max(np.abs(actual - reverse))),
             "single_max_abs": float(np.max(np.abs(actual - singles)))}
    value["verified"] = (value["cached_max_abs"] <= SETUP["descriptor_max_abs_tolerance"]
        and value["cached_max_l2"] <= SETUP["descriptor_l2_tolerance"]
        and value["reverse_batch_max_abs"] <= SETUP["invariance_tolerance"]
        and value["single_max_abs"] <= SETUP["invariance_tolerance"])
    return value


def fresh_queries(data):
    out = data["out"]
    path = out / "fresh_queries.npz"
    signature = context.signature(data["contract"])
    if path.with_suffix(".json").exists():
        saved = json.loads(path.with_suffix(".json").read_text())
        if saved["contract_sha256"] != signature or digest(path) != saved["sha256"]:
            raise RuntimeError("fresh query validation checkpoint changed")
        with np.load(path, allow_pickle=False) as stored:
            return {size: stored[f"q{size}"] for size in SETUP["scales"]}, saved["measurements"]
    for row in data["q"].itertuples():
        if digest(Path(row.image_path)) != row.file_sha256:
            raise RuntimeError("selected development photo changed before fresh inference")
    def no_network(*args, **kwargs):
        raise RuntimeError("fresh runtime smoke forbids network access")
    connect, model = socket.socket.connect, None
    socket.socket.connect = no_network
    measurements = {"peak_rss_before_model_bytes": peak_rss_bytes(), "scales": {}}
    vectors = {}
    try:
        start = time.perf_counter()
        model = gallery_scale.create_model(data["gc"])
        torch.mps.synchronize()
        measurements["cold_model_load_seconds"] = time.perf_counter() - start
        if model.metadata.to_dict() != data["contract"]["retriever"]:
            raise RuntimeError("fresh query checkpoint/preprocessing differs from fixed gallery encoder")
        paths = data["q"].image_path.tolist()
        for size in SETUP["scales"]:
            model.image_size = (size, size)
            model.batch_size = data["gc"]["batch_size"] if size == 322 else 2
            model._transform = model._build_transform()
            def encode(images):
                torch.mps.synchronize()
                before = time.perf_counter()
                result = model.embed_batch(images)
                torch.mps.synchronize()
                return result, time.perf_counter() - before
            actual, elapsed = encode(paths)
            reverse, reversed_seconds = encode(list(reversed(paths)))
            singles, times = zip(*(encode([path]) for path in paths), strict=True)
            singles = np.concatenate(singles)
            vectors[size] = actual
            measurements["scales"][str(size)] = vector_parity(actual, data["cached"][size], reverse[::-1], singles) | {
                "two_image_batch_seconds": elapsed, "reverse_batch_seconds": reversed_seconds,
                "single_image_seconds": list(times), "batch_size_configuration": model.batch_size}
            state(out, "fresh_query_scale_verified", scale=size, verified=measurements["scales"][str(size)]["verified"])
        measurements["peak_rss_with_model_bytes"] = peak_rss_bytes()
        measurements["mps_driver_allocated_bytes"] = int(torch.mps.driver_allocated_memory())
    finally:
        if model is not None:
            model.close()
            del model
        torch.mps.empty_cache()
        socket.socket.connect = connect
    atomic_npz(path, **{f"q{size}": value for size, value in vectors.items()})
    save(path.with_suffix(".json"), {"contract_sha256": signature, "sha256": digest(path), "measurements": measurements,
         "reference_encoding_count": 0, "unique_query_photos_reencoded": 2, "validation_exception_not_descriptor_pool": True})
    return vectors, measurements


def streamed_scores(gallery, queries, pool):
    """One tiny query-score matrix and one mapped reference shard, CPU1."""
    groups = defaultdict(list)
    for column, row in enumerate(gallery.itertuples()):
        entry = pool[row.id]
        if entry["image_sha256"] != row.file_sha256:
            raise RuntimeError("runtime descriptor identity changed")
        groups[entry["path"]].append((column, entry["row"]))
    scores = np.empty((len(queries), len(gallery)), np.float32)
    shard_checks = []
    for filename, pairs in groups.items():
        pairs.sort(key=lambda value: value[1])
        path = Path(filename)
        before = path.stat()
        block = np.load(path, mmap_mode="r", allow_pickle=False)
        try:
            if block.ndim != 2 or block.shape[1] != 8448 or block.dtype != np.float32:
                raise RuntimeError("runtime descriptor shard has invalid dtype/dimension")
            for start in range(0, len(pairs), SETUP["reference_block_rows"]):
                columns, positions = np.asarray(pairs[start:start + SETUP["reference_block_rows"]]).T
                subset = block[positions]
                context.unit_vectors(subset)
                scores[:, columns] = queries @ subset.T
        finally:
            block._mmap.close()
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise RuntimeError("runtime reference shard changed during read")
        shard_checks.append({"path": filename, "size": before.st_size, "mtime_ns": before.st_mtime_ns})
    if not np.isfinite(scores).all():
        raise RuntimeError("nonfinite fresh query scores")
    return scores, shard_checks


def ranking_parity(actual, expected, reference_ids):
    fresh = np.argsort(-actual, axis=1, kind="stable")[:, :100]
    cached = np.argsort(-expected, axis=1, kind="stable")[:, :100]
    delta = float(np.max(np.abs(actual - expected)))
    ids = np.asarray(reference_ids)
    return fresh, {"max_abs_score_difference": delta,
        "scores_within_tolerance": delta <= SETUP["score_max_abs_tolerance"],
        "same_top100_order": np.all(fresh == cached, axis=1).tolist(),
        "same_top100_set": [set(a) == set(b) for a, b in zip(fresh, cached, strict=True)],
        "same_top1": (fresh[:, 0] == cached[:, 0]).tolist(),
        "fresh_top1_reference_ids": ids[fresh[:, 0]].tolist(), "cached_top1_reference_ids": ids[cached[:, 0]].tolist()}


def verify_done(data):
    out = data["out"]
    done = json.loads((out / "done.json").read_text())
    if done["contract"] != data["contract"] or any(digest(out / name) != sha for name, sha in done["artifacts"].items()):
        raise RuntimeError("completed fresh smoke artifact/input changed")


def score_and_context(data, vectors, measurements):
    out = data["out"]
    if (out / "done.json").exists():
        verify_done(data)
        return
    torch.set_num_threads(1)
    start = time.perf_counter()
    scores, shards = streamed_scores(data["g"], np.concatenate([vectors[322], vectors[504]]), data["pool"])
    search_seconds = time.perf_counter() - start
    mean = .5 * (scores[:2] + scores[2:])
    cached_scores = {}
    for arm in ("top1", "mean"):
        source = np.load(data["source"] / f"{arm}_scores.npy", mmap_mode="r", allow_pickle=False)
        try:
            cached_scores[arm] = source[data["positions"]].copy()
        finally:
            source._mmap.close()
    prefix322, parity322 = ranking_parity(scores[:2], cached_scores["top1"], data["g"].id)
    prefix, paritymean = ranking_parity(mean, cached_scores["mean"], data["g"].id)
    chosen = prefix[:, :SETUP["context_depth"]]
    unique, remap = np.unique(chosen, return_inverse=True)
    references = evaluator.load_vectors(data["g"], unique, data["parent"])
    remap = remap.reshape(chosen.shape)
    start = time.perf_counter()
    encoder = torch.nn.TransformerEncoder(torch.nn.TransformerEncoderLayer(
        d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=.1, batch_first=False), 2)
    encoder.load_state_dict(torch.load(data["checkpoint"], map_location="cpu", weights_only=True), strict=True)
    encoder.eval()
    means = context.mean_queries(vectors[322], vectors[504])
    contextual = []
    for i in range(2):
        values, direct = context.one_query(encoder, means[i], vectors[322][i], vectors[504][i], references[remap[i]])
        np.testing.assert_allclose(direct, mean[i, chosen[i]], atol=2e-6, rtol=1e-5)
        contextual.append(values)
    ranked = context.rank_prefix(prefix, np.asarray(contextual), np.take_along_axis(mean, chosen, axis=1))
    context_seconds = time.perf_counter() - start
    del encoder, references
    original = json.loads((data["source"] / "context_rows.json").read_text())
    if [original["query_ids"][i] for i in data["positions"]] != data["q"].id.tolist():
        raise RuntimeError("committed context comparator query order differs")
    expected = np.asarray(original["top100_gallery_rows"], np.int32)[data["positions"]]
    context_parity = {"same_top100_order": np.all(ranked == expected, axis=1).tolist(),
        "same_top1": (ranked[:, 0] == expected[:, 0]).tolist(),
        "fresh_top1_reference_ids": data["g"].id.iloc[ranked[:, 0]].tolist(),
        "cached_top1_reference_ids": data["g"].id.iloc[expected[:, 0]].tolist(),
        "unchanged_tail70": bool(np.array_equal(ranked[:, 30:], prefix[:, 30:]))}
    verified = (all(v["verified"] for v in measurements["scales"].values())
        and all(p["scores_within_tolerance"] and all(p["same_top100_order"]) for p in (parity322, paritymean))
        and all(context_parity["same_top100_order"]) and context_parity["unchanged_tail70"])
    atomic_npz(out / "retrieval_evidence.npz", query_ids=np.asarray(data["q"].id),
               fresh322_scores=scores[:2], fresh_mean_scores=mean, prefix322=prefix322,
               mean_prefix=prefix, context_prefix=ranked)
    save(out / "reference_shard_reads.json", {"shards": shards, "source_pool_sha256": digest(data["parent"] / "descriptor_pool.json"),
         "reference_vectors_not_reencoded": True, "score_matrix_shape": list(scores.shape)})
    save(out / "report.json", {"setup": SETUP, "query_ids": data["q"].id.tolist(), "gallery_count": len(data["g"]),
         "fresh_query": measurements, "top1_parity": parity322, "mean_parity": paritymean, "context_parity": context_parity,
         "four_vector_streamed_search_seconds": search_seconds, "two_query_context_load_and_inference_seconds": context_seconds,
         "peak_process_rss_bytes": peak_rss_bytes(), "cpu_fresh_encoding_checked": False,
         "limitations": "Two fixed open-development images; existing external research gallery. Fresh MPS encoding and CPU cosine/context only. "
                        "No package/release/serving concurrency/FAISS child-process certification, no broad CPU/platform check, no accuracy selection.",
         "verified": verified, "new_model_or_candidate": False})
    save(out / "verification.json", {"verified": verified, "fixed_development_positions": SETUP["fixed_development_positions"],
         "queries": 2, "fresh_mps_scales": [322, 504], "reference_encodes": 0,
         "no_query_truth_used_for_inference_or_selection": True, "production_mutations": False,
         "calibration_final_access": False})
    files = [p for p in out.iterdir() if p.suffix in {".json", ".npz"}
             and p.name not in {"status.json", "failure.json", "done.json"} and not p.name.startswith("._")]
    save(out / "done.json", {"completed": time.time(), "contract": data["contract"],
         "verified": verified, "artifacts": {p.name: digest(p) for p in files}})
    verify_done(data)
    state(out, "fresh_smoke_completed", verified=verified)


def run(wait=False):
    os.nice(max(0, 10 - os.getpriority(os.PRIO_PROCESS, 0)))
    cfg, _, _, night, out = stage()
    out.mkdir(exist_ok=True)
    with (evaluator.LOCAL / "fresh_runtime_smoke.lock").open("a") as own, threadpool_limits(limits=1):
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        prepare()
        prerequisite = night / "live_expansion2_quarantine/evaluation/expanded2/done.json"
        try:
            while not prerequisite.exists():
                if not evaluator.allowed_to_start(cfg, PHASE, out):
                    save(out / "closed.json", {"reason": "06:00 cutoff before prerequisite"})
                    return
                if not wait:
                    raise RuntimeError("complete common expanded2 comparison first")
                state(out, "fresh_smoke_waiting_expanded2")
                time.sleep(30)
            data = load()
            with (evaluator.LOCAL / "controller.lock").open("a") as gpu:
                fcntl.flock(gpu, fcntl.LOCK_EX)
                marker = out / "smoke.started.json"
                if marker.exists():
                    if json.loads(marker.read_text())["contract"] != data["contract"]:
                        raise RuntimeError("fresh smoke input/source contract changed")
                else:
                    if not evaluator.allowed_to_start(cfg, PHASE, out):
                        save(out / "closed.json", {"reason": "06:00 cutoff after GPU queue, before actual start"})
                        return
                    save(marker, {"started": time.time(), "contract": data["contract"]})
                vectors, measured = fresh_queries(data)
            state(out, "fresh_smoke_cpu_search")
            score_and_context(data, vectors, measured)
        except BaseException as exc:
            save(out / "failure.json", {"failed": time.time(), "pid": os.getpid(), "error": repr(exc)})
            state(out, "fresh_smoke_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait", action="store_true")
    run(parser.parse_args().wait)
