"""One fixed cached-query scale/context interaction; CPU only, no image encoding."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np
import torch
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale
from ml.research import night_v7 as evaluator
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.metrics import paired_group_bootstrap
from ml.research.night_place import atomic_npz

LOCAL = evaluator.LOCAL
NAME = "scale_mean_context"
SETUP = {"query_count": 1184, "gallery_count": 100000, "dimension": 8448,
         "context_depth": 30, "context_mixing": .5, "cpu_threads": 1, "nice": 10,
         "checkpoint_queries": 50, "prefix": "existing query322_504_mean top100",
         "encoder_query": "unit-normalized arithmetic mean of cached unit query322 and query504",
         "blend_original_score": "0.5*(cos(query322,reference)+cos(query504,reference))",
         "tail": "preserve original positions30:100 exactly",
         "parameter_sweep": False, "new_training": False, "image_encoding": False,
         "calibration_final_access": False}


def state(out, phase, **extra):
    value = dict(phase=phase, updated=time.time(), pid=os.getpid(), cpu_only=True, **extra)
    save(out / "status.json", value)
    save(LOCAL / "scale_context_status.json", value)
    print(json.dumps(value), flush=True)


def signature(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def unit_vectors(values):
    if (values.ndim != 2 or values.shape[1] != SETUP["dimension"] or values.dtype != np.float32
            or not np.isfinite(values).all() or not np.allclose(np.linalg.norm(values, axis=1), 1, atol=1e-5)):
        raise RuntimeError("expected finite cached unit SAGE-L float32 vectors")
    return values


def mean_queries(q322, q504):
    unit_vectors(q322)
    unit_vectors(q504)
    if q322.shape != q504.shape:
        raise RuntimeError("two scales must describe the same ordered query population")
    mean = .5 * (q322 + q504)
    norms = np.linalg.norm(mean, axis=1, keepdims=True)
    if (norms <= 1e-6).any():
        raise RuntimeError("opposing query descriptors cannot form a stable normalized mean")
    return mean / norms


def one_query(encoder, normalized_mean, q322, q504, references):
    """Only this query and its30 references enter attention; no labels/GPS."""
    features = np.concatenate([normalized_mean[None, :], references])
    with torch.inference_mode():
        output = encoder(torch.from_numpy(features).view(len(features), 11, 768))
        output = torch.nn.functional.normalize(output.flatten(1), dim=1)
        contextual = (output[0] @ output[1:].T).numpy()
    original = .5 * (q322 @ references.T + q504 @ references.T)
    if not np.isfinite(contextual).all() or not np.isfinite(original).all():
        raise RuntimeError("non-finite scale/context evidence")
    return contextual.astype(np.float32), original.astype(np.float32)


def rank_prefix(prefix, contextual, original):
    depth = SETUP["context_depth"]
    if contextual.shape != prefix[:, :depth].shape or original.shape != contextual.shape:
        raise RuntimeError("context evidence must align with the unchanged first30 references")
    values = evaluator.align_blend(original, contextual, SETUP["context_mixing"])
    ranked = prefix.copy()
    ranked[:, :depth] = np.take_along_axis(prefix[:, :depth], np.argsort(-values, axis=1, kind="stable"), axis=1)
    if not np.array_equal(ranked[:, depth:], prefix[:, depth:]):
        raise RuntimeError("context interaction changed the frozen mean-retrieval tail")
    return ranked


def load_inputs():
    cfg, gc, previous, night, q, q322, g, _ = evaluator.inputs()
    if cfg["context_depth"] != SETUP["context_depth"] or cfg["context_mixing"] != SETUP["context_mixing"]:
        raise RuntimeError("existing context settings differ from the single fixed interaction")
    if len(q) != SETUP["query_count"] or len(g) != SETUP["gallery_count"]:
        raise RuntimeError("fixed scale/context population changed")
    if not (night / "query_scale504.done.json").exists():
        raise RuntimeError("finish the registered query504 trial before this interaction")
    fixed = json.loads((night / "input_contract.json").read_text())
    for path, expected in ((previous / "manifests/G2_smart.parquet", fixed["gallery_sha256"]),
                           (previous / "descriptor_pool.json", fixed["pool_sha256"]),
                           (previous / "descriptor_contract.json", fixed["descriptor_contract_sha256"])):
        if digest(path) != expected:
            raise RuntimeError("frozen SAGE reference inputs changed")
    comparisons = {name: json.loads((night / "results" / f"{name}_rows.json").read_text())
                   for name in ("query322_504_mean", "context")}
    if any(rows["query_ids"] != q.id.tolist() for rows in comparisons.values()):
        raise RuntimeError("scale/context comparator query identities differ")
    prefix = np.asarray(comparisons["query322_504_mean"]["top100_gallery_rows"])
    if (prefix.shape != (len(q), 100) or not np.issubdtype(prefix.dtype, np.integer)
            or (prefix < 0).any() or (prefix >= len(g)).any() or any(len(set(row)) != 100 for row in prefix)):
        raise RuntimeError("invalid committed mean-query retrieval prefix")
    expected = {"query_sha256": gc["query_sha256"], "reference_contract_sha256": fixed["descriptor_contract_sha256"],
                "query_size": [504, 504], "reference_size": [322, 322], "device": "mps", "batch": 2}
    pieces, hashes = [], {}
    for start in range(0, len(q), 32):
        path = night / "query504" / f"chunk-{start // 32:04d}.npz"
        with np.load(path, allow_pickle=False) as chunk:
            if chunk["ids"].tolist() != q.id.iloc[start:start + 32].tolist() or json.loads(str(chunk["contract"])) != expected:
                raise RuntimeError("query504 cache identities or preprocessing changed")
            pieces.append(unit_vectors(chunk["vectors"]))
        hashes[path.name] = digest(path)
    q504 = np.concatenate(pieces)
    normalized_mean = mean_queries(q322, q504)
    checkpoint = WORKSPACE / "data/models/research_v5/sage_context_encoder.pth"
    if digest(checkpoint) != evaluator.ENCODER_SHA:
        raise RuntimeError("pinned existing SAGE context encoder changed")
    sources = [Path(__file__), Path(evaluator.__file__), Path(gallery_scale.__file__),
               WORKSPACE / "ml/research/night_query_scale.py"]
    contract = {"setup": SETUP, "config": cfg, "night_input_sha256": digest(night / "input_contract.json"),
                "checkpoint_sha256": evaluator.ENCODER_SHA,
                "query322_values_sha256": hashlib.sha256(q322.tobytes()).hexdigest(),
                "query504_chunk_sha256": hashes,
                "mean_rows_sha256": digest(night / "results/query322_504_mean_rows.json"),
                "context_rows_sha256": digest(night / "results/context_rows.json"),
                "source_sha256": {p.name: digest(p) for p in sources},
                "selected_reference_count": int(len(np.unique(prefix[:, :SETUP["context_depth"]])))}
    return cfg, previous, night, q, q322, q504, normalized_mean, g, prefix, comparisons, checkpoint, contract


def context_values(encoder, qids, q322, q504, means, vectors, remap, chosen, out, contract):
    """Immutable50-query commits; a crash can only repeat the uncommitted block."""
    blocks = out / "blocks"
    blocks.mkdir(exist_ok=True)
    values = np.empty(chosen.shape, np.float32)
    original = np.empty(chosen.shape, np.float32)
    for start in range(0, len(qids), SETUP["checkpoint_queries"]):
        stop = min(start + SETUP["checkpoint_queries"], len(qids))
        path = blocks / f"queries-{start:04d}.npz"
        receipt = path.with_suffix(".json")
        expected = {"contract_sha256": signature(contract), "query_ids": list(qids[start:stop]),
                    "indices_sha256": hashlib.sha256(chosen[start:stop].tobytes()).hexdigest()}
        if receipt.exists():
            saved = json.loads(receipt.read_text())
            if saved["inputs"] != expected or digest(path) != saved["sha256"]:
                raise RuntimeError("scale/context checkpoint identities, source or values changed")
            with np.load(path, allow_pickle=False) as cached:
                block_values, block_original = cached["contextual"], cached["original"]
                if not np.array_equal(cached["indices"], chosen[start:stop]):
                    raise RuntimeError("scale/context committed reference membership changed")
        else:
            block_values, block_original = [], []
            for i in range(start, stop):
                contextual, before = one_query(encoder, means[i], q322[i], q504[i], vectors[remap[i]])
                block_values.append(contextual)
                block_original.append(before)
            block_values = np.asarray(block_values, dtype=np.float32)
            block_original = np.asarray(block_original, dtype=np.float32)
            atomic_npz(path, contextual=block_values, original=block_original, indices=chosen[start:stop])
            save(receipt, {"inputs": expected, "sha256": digest(path)})
        if (block_values.shape != (stop - start, chosen.shape[1]) or block_original.shape != block_values.shape
                or block_values.dtype != np.float32 or block_original.dtype != np.float32
                or not np.isfinite(block_values).all() or not np.isfinite(block_original).all()):
            raise RuntimeError("invalid committed scale/context score block")
        values[start:stop], original[start:stop] = block_values, block_original
        state(out, "scale_context_queries", completed=stop, total=len(qids))
    return values, original


def compute(data, out):
    _, previous, _, q, q322, q504, means, g, prefix, comparisons, checkpoint, contract = data
    chosen = prefix[:, :SETUP["context_depth"]]
    unique, remap = np.unique(chosen, return_inverse=True)
    state(out, "scale_context_loading_reference_union", references=len(unique), bytes=len(unique) * 8448 * 4)
    vectors = evaluator.load_vectors(g, unique, previous)
    remap = remap.reshape(chosen.shape)
    encoder = torch.nn.TransformerEncoder(torch.nn.TransformerEncoderLayer(
        d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=0.1, batch_first=False), 2)
    encoder.load_state_dict(torch.load(checkpoint, map_location="cpu", weights_only=True), strict=True)
    encoder.eval()
    values, original = context_values(encoder, q.id.tolist(), q322, q504, means, vectors, remap, chosen, out, contract)
    del encoder, vectors
    ranked = rank_prefix(prefix, values, original)
    atomic_npz(out / "predictions.npz", indices=ranked, contextual=values, original=original)
    original_paths, original_local = evaluator.paths, evaluator.LOCAL
    evaluator.paths = lambda: (*original_paths()[:4], out)
    evaluator.LOCAL = out
    try:
        result = evaluator.record_result(NAME, ranked, note=contract | {
            "context_tensor": "one query plus30references,31x11x768; no cross-query attention",
            "encoder": "same pinned existing SAGE context weights; CPU inference only",
            "query_truth_used_in_prediction": False, "raw_estimator": "top1 reference coordinates"})
    finally:
        evaluator.paths, evaluator.LOCAL = original_paths, original_local
    rows = json.loads((out / f"results/{NAME}_rows.json").read_text())
    result["paired_vs_mean"] = paired_group_bootstrap(comparisons["query322_504_mean"]["errors_m"], rows["errors_m"], q.h3_coarse.astype(str).tolist())
    result["paired_vs_context"] = paired_group_bootstrap(comparisons["context"]["errors_m"], rows["errors_m"], q.h3_coarse.astype(str).tolist())
    save(out / f"results/{NAME}.json", result)
    if result["paired_vs_mean"]["gain_pp"] <= 0 or result["paired_vs_context"]["gain_pp"] <= 0:
        save(out / "closed.json", {"reason": "no raw100 gain over both fixed components; no further scale/context parameters",
             "paired_vs_mean": result["paired_vs_mean"], "paired_vs_context": result["paired_vs_context"]})
    save(out / "done.json", {"completed": time.time(), "contract": contract,
         "predictions_sha256": digest(out / "predictions.npz"),
         "result_sha256": digest(out / f"results/{NAME}.json"), "rows_sha256": digest(out / f"results/{NAME}_rows.json"),
         "tail70_unchanged": True, "full_score_matrix_recomputed": False,
         "blocks": {p.name: digest(p) for p in sorted((out / "blocks").glob("*.npz"))}})


def publish(night, out, contract):
    done = json.loads((out / "done.json").read_text())
    if done["contract"] != contract:
        raise RuntimeError("scale/context completed source contract changed")
    for name, key in (("predictions.npz", "predictions_sha256"), (f"results/{NAME}.json", "result_sha256"),
                      (f"results/{NAME}_rows.json", "rows_sha256")):
        if digest(out / name) != done[key]:
            raise RuntimeError("scale/context artifacts changed before publication")
    if any(digest(out / "blocks" / name) != sha for name, sha in done["blocks"].items()):
        raise RuntimeError("committed scale/context score blocks changed before publication")
    state(out, "scale_context_waiting_to_publish")
    with (LOCAL / "controller.lock").open("a") as main:
        fcntl.flock(main, fcntl.LOCK_EX)
        for suffix in (".json", "_rows.json"):
            source, target = out / f"results/{NAME}{suffix}", night / f"results/{NAME}{suffix}"
            if target.exists() and digest(target) != digest(source):
                raise RuntimeError("a different scale/context result is already published")
            if not target.exists():
                save(target, json.loads(source.read_text()))
        evaluator.report()
        save(out / "published.json", {"published": time.time(), "done_sha256": digest(out / "done.json")})
    state(out, "scale_context_completed")


def run():
    *_, night = evaluator.paths()
    out = night / "cpu_scale_context"
    out.mkdir(exist_ok=True)
    if os.getpriority(os.PRIO_PROCESS, 0) < SETUP["nice"]:
        os.nice(SETUP["nice"] - os.getpriority(os.PRIO_PROCESS, 0))
    torch.set_num_threads(1)
    with (LOCAL / "scale_context.lock").open("a") as own, threadpool_limits(limits=1):
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            data = load_inputs()
            cfg, contract = data[0], data[-1]
            marker = out / "scale_context.started.json"
            if marker.exists():
                if json.loads(marker.read_text())["contract"] != contract:
                    raise RuntimeError("registered scale/context protocol changed")
            else:
                if not evaluator.allowed_to_start(cfg, "scale_context", out):
                    save(out / "closed.json", {"reason": "06:00 cutoff; interaction never started"})
                    return
                save(marker, {"started": time.time(), "contract": contract,
                     "reference_vector_bytes": contract["selected_reference_count"] * 8448 * 4})
            if not (out / "done.json").exists():
                compute(data, out)
            publish(night, out, contract)
        except BaseException as exc:
            save(out / "failure.json", {"failed": time.time(), "pid": os.getpid(), "error": repr(exc)})
            state(out, "scale_context_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    run()
