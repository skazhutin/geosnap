"""Transfer the fixed two-scale/context pipeline to a completed gallery addition."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from ml.research import night_scale_context as context
from ml.research import night_v7 as evaluator
from ml.research.gallery_scale import exact_scores
from ml.research.gallery_scale_storage import digest, save
from ml.research.metrics import paired_group_bootstrap
from ml.research.night_place import atomic_npz
from ml.research.night_v7_verify import development_identity_checks, gallery_truth, reconcile_result

STAGES = {"gallery_addition": "added", "live_expansion2": "expanded2"}


def state(out, phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), cpu_only=True, **extra)
    save(out / "status.json", value)
    save(evaluator.LOCAL / f"pipeline_{out.name}_status.json", value)
    print(json.dumps(value), flush=True)


def load(stage):
    data = context.load_inputs()
    cfg, previous, night, q, q322, q504, means, original, _, comparisons, checkpoint, component = data
    source = night / stage
    if not (source / "published.json").exists():
        raise RuntimeError("finish and publish the source gallery experiment first")
    audit = json.loads((source / "audit.done.json").read_text())
    done = json.loads((source / "done.json").read_text())
    manifest = source / "gallery.parquet"
    if digest(manifest) != audit["gallery_sha256"] or digest(source / "scores.npy") != done["scores_sha256"]:
        raise RuntimeError("completed gallery or its exact SAGE322 scores changed")
    gallery = pd.read_parquet(manifest)
    columns = ["id", "file_sha256", "lat", "lon"]
    pd.testing.assert_frame_equal(gallery.iloc[:len(original)][columns].reset_index(drop=True),
                                  original[columns].reset_index(drop=True))
    if gallery.id.duplicated().any() or not gallery.production_compatible.iloc[len(original):].all():
        raise RuntimeError("expanded gallery has duplicate IDs or incompatible additional references")
    development_identity_checks(q, gallery)
    pool_path = source / "descriptor_pool.json"
    pool = json.loads(pool_path.read_text())
    if pool["contract_sha256"] != digest(previous / "descriptor_contract.json"):
        raise RuntimeError("added descriptors use a different SAGE fingerprint")
    contract = {"stage": stage, "component": component, "gallery_sha256": audit["gallery_sha256"],
                "pool_sha256": digest(pool_path), "sage322_scores_sha256": done["scores_sha256"],
                "source_sha256": digest(Path(__file__)), "context_source_sha256": digest(Path(context.__file__)),
                "hypothesis": "transfer the fixed winning mean-query/context30 pipeline to the newly expanded gallery",
                "new_encoding": False, "new_training": False, "parameter_search": False,
                "same_queries": len(q), "references": len(gallery), "calibration_final_access": False}
    return data, source, gallery, pool, contract


def mean_scores(data, source, gallery, pool, out, contract):
    _, _, night, _, _, q504, _, original, old_prefix, _, _, _ = data
    target, receipt = out / "mean_scores.npy", out / "mean_scores.done.json"
    expected = context.signature(contract)
    if receipt.exists():
        saved = json.loads(receipt.read_text())
        if saved["contract_sha256"] != expected or digest(target) != saved["scores_sha256"]:
            raise RuntimeError("committed mean-score cache changed")
        with np.load(out / "prefix.npz", allow_pickle=False) as cached:
            prefix = cached["indices"]
        if digest(out / "prefix.npz") != saved["prefix_sha256"]:
            raise RuntimeError("committed expanded mean prefix changed")
        return prefix
    state(out, "query504_exact_scores", references=len(gallery), new_image_descriptors=0)
    values = exact_scores(gallery, q504, pool["entries"])
    base = np.load(source / "scores.npy", mmap_mode="r", allow_pickle=False)
    if values.shape != base.shape:
        raise RuntimeError("two query scales have different gallery populations")
    # The original SAGE322 score matrix is preserved, including its old prefix.
    for start in range(0, len(values), 4):
        values[start:start + 4] += base[start:start + 4]
        values[start:start + 4] *= .5
    base._mmap.close()
    if not np.isfinite(values).all():
        raise RuntimeError("nonfinite exact mean-query scores")
    prefix = np.empty((len(values), 100), np.int32)
    for i, row in enumerate(values):
        # Same descriptors and arithmetic must replay the completed old-gallery
        # retrieval before adding references. No threshold/gap is adjusted.
        replay = np.argsort(-row[:len(original)], kind="stable")[:100]
        if not np.array_equal(replay, old_prefix[i]):
            raise RuntimeError("expanded mean scoring fails frozen old-gallery top100 parity")
        prefix[i] = np.argsort(-row, kind="stable")[:100]
    temporary = target.with_suffix(".writing")
    with temporary.open("wb") as handle:
        np.save(handle, values, allow_pickle=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)
    del values
    atomic_npz(out / "prefix.npz", indices=prefix)
    save(receipt, {"contract_sha256": expected, "scores_sha256": digest(target),
         "prefix_sha256": digest(out / "prefix.npz"), "original_G2_mean_top100_parity": True,
         "mean_component_sha256": digest(night / "results/query322_504_mean_rows.json")})
    return prefix


def compute(data, source, gallery, pool, out, contract):
    cfg, previous, night, q, q322, q504, means, _, _, _, checkpoint, _ = data
    prefix = mean_scores(data, source, gallery, pool, out, contract)
    chosen = prefix[:, :context.SETUP["context_depth"]]
    unique, remap = np.unique(chosen, return_inverse=True)
    state(out, "loading_context_references", references=len(unique))
    # load_vectors consumes a pool file in its supplied directory, independently
    # of the unchanged model fingerprint in previous/descriptor_contract.json.
    vectors = evaluator.load_vectors(gallery, unique, source)
    encoder = torch.nn.TransformerEncoder(torch.nn.TransformerEncoderLayer(
        d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=.1, batch_first=False), 2)
    encoder.load_state_dict(torch.load(checkpoint, map_location="cpu", weights_only=True), strict=True)
    encoder.eval()
    original_state = context.state
    context.state = state
    try:
        values, direct = context.context_values(encoder, q.id.tolist(), q322, q504, means,
            vectors, remap.reshape(chosen.shape), chosen, out, contract)
    finally:
        context.state = original_state
    del vectors, encoder
    scores = np.load(out / "mean_scores.npy", mmap_mode="r", allow_pickle=False)
    global_values = np.take_along_axis(scores, chosen, axis=1)
    np.testing.assert_allclose(global_values, direct, atol=2e-6, rtol=1e-5)
    scores._mmap.close()
    ranked = context.rank_prefix(prefix, values, global_values)
    atomic_npz(out / "predictions.npz", mean=prefix, context=ranked)
    # Query truth is consumed only after every inference output is committed.
    truth = gallery_truth(q, gallery)
    original_inputs, original_paths, original_local = evaluator.inputs, evaluator.paths, evaluator.LOCAL
    base = original_inputs()[-1]
    augmented_truth = dict(base, positive_count_100m=truth["positive_count"].tolist())
    fixed_rows = json.loads((night / "cpu_scale_context/results/scale_mean_context_rows.json").read_text())
    evaluator.inputs = lambda: (cfg, None, previous, out, q, q322, gallery, augmented_truth)
    evaluator.paths = lambda: (*original_paths()[:4], out)
    evaluator.LOCAL = out
    records = []
    try:
        for stem, order in (("scale_mean", prefix), ("scale_mean_context", ranked)):
            name = f"{stem}_{STAGES[contract['stage']]}"
            result = evaluator.record_result(name, order, note=contract,
                extra={"gallery_manifest": str(source / "gallery.parquet"), "gallery_sha256": contract["gallery_sha256"]})
            rows_path = out / "results" / f"{name}_rows.json"
            rows = json.loads(rows_path.read_text())
            result["paired_vs_fixed_gallery_pipeline"] = paired_group_bootstrap(
                fixed_rows["errors_m"], rows["errors_m"], q.h3_coarse.astype(str).tolist())
            verification = reconcile_result(result, rows, q, gallery, truth)
            save(out / "results" / f"{name}.json", result)
            records.append({"name": name, "summary_sha256": digest(out / "results" / f"{name}.json"),
                            "rows_sha256": digest(rows_path), "verification": verification})
    finally:
        evaluator.inputs, evaluator.paths, evaluator.LOCAL = original_inputs, original_paths, original_local
    save(out / "done.json", {"completed": time.time(), "contract": contract, "records": records,
         "predictions_sha256": digest(out / "predictions.npz"), "no_new_image_descriptors": True})


def run(stage):
    *_, night = evaluator.paths()
    out = night / "cpu_pipeline_added" / stage
    out.mkdir(parents=True, exist_ok=True)
    if os.getpriority(os.PRIO_PROCESS, 0) < 10:
        os.nice(10 - os.getpriority(os.PRIO_PROCESS, 0))
    torch.set_num_threads(1)
    with (evaluator.LOCAL / f"pipeline_{stage}.lock").open("a") as lock, threadpool_limits(limits=1):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        data, source, gallery, pool, contract = load(stage)
        marker = out / "pipeline.started.json"
        if marker.exists():
            if json.loads(marker.read_text())["contract"] != contract:
                raise RuntimeError("registered pipeline transfer changed")
        else:
            if not evaluator.allowed_to_start(data[0], "pipeline", out):
                save(out / "closed.json", {"reason": "06:00 cutoff; transfer not started"})
                return
            save(marker, {"started": time.time(), "contract": contract})
        if not (out / "done.json").exists():
            compute(data, source, gallery, pool, out, contract)
        done = json.loads((out / "done.json").read_text())
        if done["contract"] != contract or digest(out / "predictions.npz") != done["predictions_sha256"]:
            raise RuntimeError("completed pipeline transfer changed")
        state(out, "pipeline_waiting_to_publish")
        with (evaluator.LOCAL / "controller.lock").open("a") as main:
            fcntl.flock(main, fcntl.LOCK_EX)
            for record in done["records"]:
                for suffix, key in ((".json", "summary_sha256"), ("_rows.json", "rows_sha256")):
                    path = out / "results" / (record["name"] + suffix)
                    target = night / "results" / path.name
                    if digest(path) != record[key] or target.exists() and digest(target) != record[key]:
                        raise RuntimeError("pipeline result changed before publication")
                    if not target.exists():
                        save(target, json.loads(path.read_text()))
            save(out / "published.json", {"published": time.time(), "done_sha256": digest(out / "done.json")})
            evaluator.report()
        state(out, "pipeline_completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=STAGES)
    run(parser.parse_args().stage)
