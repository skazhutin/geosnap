"""Encode only audited second-tranche references, then evaluate exact raw top1."""
from __future__ import annotations

import fcntl
import json
import os
import time
from pathlib import Path

import numpy as np
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale
from ml.research import night_live_expansion as acquisition
from ml.research.gallery_scale_storage import digest, save
from ml.research.metrics import paired_group_bootstrap
from ml.research.night_gallery_addition import atomic_array
from ml.research.night_v7 import LOCAL, report

NAME = "live_expansion2"


def inputs():
    cfg, gc, store, previous, night, out = acquisition.stage()
    started = json.loads((out / "live_expansion.started.json").read_text())
    registered = acquisition.verify_prepared(out, cfg, gc, previous, night, started["limit"])
    gallery, added = acquisition.verify_audited(out, previous, night, registered)
    first = night / "gallery_addition"
    fixed = json.loads((night / "input_contract.json").read_text())
    first_done = json.loads((first / "done.json").read_text())
    pool = json.loads((first / "descriptor_pool.json").read_text())
    if pool["contract_sha256"] != fixed["descriptor_contract_sha256"]:
        raise RuntimeError("first expanded descriptor pool uses a different SAGE contract")
    contract = {"source_sha256": digest(Path(__file__)), "acquisition_source_sha256": digest(Path(acquisition.__file__)),
        "registration_sha256": digest(out / "live_expansion.started.json"), "audit_sha256": digest(out / "audit.done.json"),
        "gallery_sha256": digest(out / "gallery.parquet"), "addition_sha256": digest(out / "addition.parquet"),
        "baseline_pool_sha256": digest(first / "descriptor_pool.json"), "baseline_scores_sha256": first_done["scores_sha256"],
        "first_rows_sha256": digest(first / "results/gallery_addition_rows.json"),
        "query_sha256": gc["query_sha256"], "descriptor_contract_sha256": fixed["descriptor_contract_sha256"],
        "retriever": json.loads((previous / "descriptor_contract.json").read_text())["retriever"],
        "estimator": "same SAGE-L322 exact float32 cosine; all1184queries; original top1 reference coordinate",
        "calibration_final_access": False, "production_changes": False}
    return gc, previous, night, out, first, gallery, added, pool, contract


def missing_references(added, entries):
    for row in added.itertuples():
        if row.id in entries and entries[row.id]["image_sha256"] != row.file_sha256:
            raise RuntimeError("existing descriptor identity differs from new audited photo")
    return added[~added.id.isin(entries)].copy()


def encode(gc, out, added, pool, contract):
    marker = out / "encoder.started.json"
    if marker.exists():
        if json.loads(marker.read_text())["contract"] != contract:
            raise RuntimeError("second-tranche encoder/source contract changed")
    else:
        save(marker, {"started": time.time(), "contract": contract, "continuation_of_registered_reference_tranche": True})
    if (out / "encode.done.json").exists():
        done = json.loads((out / "encode.done.json").read_text())
        if done["contract"] != contract or digest(out / "descriptor_pool.json") != done["descriptor_pool_sha256"]:
            raise RuntimeError("completed second-tranche descriptor pool changed")
        if any(digest(out / "descriptors" / name) != sha for name, sha in done["chunks"].items()):
            raise RuntimeError("completed second-tranche descriptor bytes changed")
        return
    entries = dict(pool["entries"])
    missing = missing_references(added, entries)
    directory = out / "descriptors"
    directory.mkdir(exist_ok=True)
    model = None
    try:
        for start in range(0, len(missing), 64):
            batch = missing.iloc[start:start + 64]
            path = directory / f"chunk-{start // 64:04d}.npy"
            receipt = path.with_suffix(".json")
            expected = {"ids": batch.id.tolist(), "image_sha256": batch.file_sha256.tolist(),
                        "descriptor_contract_sha256": contract["descriptor_contract_sha256"]}
            if receipt.exists():
                saved = json.loads(receipt.read_text())
                if saved["inputs"] != expected or saved["sha256"] != digest(path):
                    raise RuntimeError("second-tranche committed descriptor chunk changed")
            else:
                for row in batch.itertuples():
                    if digest(row.image_path) != row.file_sha256:
                        raise RuntimeError("new photo changed after identity audit")
                if model is None:
                    model = gallery_scale.create_model(gc)
                    if model.metadata.to_dict() != contract["retriever"]:
                        raise RuntimeError("new references must use the exact existing SAGE fingerprint")
                values = model.embed_batch(batch.image_path.tolist())
                if (values.shape != (len(batch), 8448) or values.dtype != np.float32 or not np.isfinite(values).all()
                        or not np.allclose(np.linalg.norm(values, axis=1), 1, atol=1e-5)):
                    raise RuntimeError("invalid second-tranche unit descriptors")
                atomic_array(path, values)
                save(receipt, {"inputs": expected, "sha256": digest(path)})
            for i, row in enumerate(batch.itertuples()):
                entries[row.id] = {"path": str(path), "row": i, "image_sha256": row.file_sha256}
            acquisition.state(out, "live_expansion_encoding", start + len(batch), len(missing))
    finally:
        if model is not None:
            model.close()
    save(out / "descriptor_pool.json", {"contract_sha256": pool["contract_sha256"], "entries": entries,
         "existing_first_expanded_pool_reused": True, "newly_encoded": len(missing)})
    save(out / "encode.done.json", {"completed": time.time(), "contract": contract,
         "descriptor_pool_sha256": digest(out / "descriptor_pool.json"), "newly_encoded": len(missing),
         "chunks": {p.name: digest(p) for p in directory.glob("*.npy")},
         "already_compatible_added_descriptors_reused": len(added) - len(missing)})


def merge_scores(base, new, target):
    if base.shape[0] != new.shape[0] or base.dtype != np.float32 or new.dtype != np.float32:
        raise RuntimeError("exact score rows/dtypes differ between reference tranches")
    merged = np.lib.format.open_memmap(target, mode="w+", dtype=np.float32,
                                      shape=(base.shape[0], base.shape[1] + new.shape[1]))
    try:
        for start in range(0, len(base), 4):
            merged[start:start + 4, :base.shape[1]] = base[start:start + 4]
            merged[start:start + 4, base.shape[1]:] = new[start:start + 4]
        merged.flush()
    finally:
        merged._mmap.close()


def evaluate(gc, previous, night, out, first, gallery, added, contract):
    result_path = out / f"results/{NAME}.json"
    if (out / "done.json").exists():
        return
    queries, qv = gallery_scale.query_vectors(gc)
    pool = json.loads((out / "descriptor_pool.json").read_text())
    if pool["contract_sha256"] != contract["descriptor_contract_sha256"]:
        raise RuntimeError("second-tranche scoring uses incompatible descriptor pool")
    new_path, new_receipt = out / "new_scores.npy", out / "new_scores.done.json"
    score_inputs = {"pool_sha256": digest(out / "descriptor_pool.json"), "query_sha256": gc["query_sha256"],
                    "addition_sha256": contract["addition_sha256"]}
    if new_receipt.exists():
        saved = json.loads(new_receipt.read_text())
        if saved["inputs"] != score_inputs or digest(new_path) != saved["sha256"]:
            raise RuntimeError("cached second-tranche exact scores changed")
    else:
        original_limits = gallery_scale.threadpool_limits
        gallery_scale.threadpool_limits = lambda *args, **kwargs: threadpool_limits(limits=1)
        try:
            values = gallery_scale.exact_scores(added, qv, pool["entries"])
        finally:
            gallery_scale.threadpool_limits = original_limits
        atomic_array(new_path, values)
        del values
        save(new_receipt, {"inputs": score_inputs, "sha256": digest(new_path)})
    if digest(first / "scores.npy") != contract["baseline_scores_sha256"]:
        raise RuntimeError("first expanded exact baseline scores changed")
    base = np.load(first / "scores.npy", mmap_mode="r", allow_pickle=False)
    new = np.load(new_path, mmap_mode="r", allow_pickle=False)
    try:
        if base.shape[1] != acquisition.SETUP["baseline_count"] or new.shape != (len(queries), len(added)) or len(gallery) != base.shape[1] + new.shape[1]:
            raise RuntimeError("second expanded score/gallery membership changed")
        merge_scores(base, new, out / "scores.npy")
    finally:
        base._mmap.close()
        new._mmap.close()
    scores = np.load(out / "scores.npy", mmap_mode="r", allow_pickle=False)
    try:
        result, rows = gallery_scale.summarize(gallery, queries, scores)
    finally:
        scores._mmap.close()
    baseline = json.loads((previous / "results/G2_smart_rows.json").read_text())
    first_rows = json.loads((first / "results/gallery_addition_rows.json").read_text())
    if rows["query_ids"] != baseline["query_ids"] or rows["query_ids"] != first_rows["query_ids"]:
        raise RuntimeError("development denominator or query order changed")
    ranks = np.array([np.inf if r is None else r for r in rows["positive_ranks"]])
    groups = queries.h3_coarse.astype(str).tolist()
    result.update({"name": NAME, "query_count": len(queries), "gallery_count": len(gallery),
         "gallery_manifest": str(out / "gallery.parquet"), "gallery_sha256": contract["gallery_sha256"],
         "query_sha256": gc["query_sha256"], "recall_at": {str(k): float(np.mean(ranks <= k)) for k in (1, 5, 10, 20, 50, 100)},
         "paired_vs_baseline": paired_group_bootstrap(baseline["errors_m"], rows["errors_m"], groups),
         "paired_vs_first_addition": paired_group_bootstrap(first_rows["errors_m"], rows["errors_m"], groups),
         "abstention": False, "final_or_calibration_access": False,
         "rank_reporting": "exact full-gallery positive ranks; raw top1 coordinates",
         "note": "original100k+first2944+second audited modern tranche; same SAGE-L322; reference-only smart sampling",
         "limitations": ["adaptive expansion after development gain; no independent final claim",
                         "inherited MSLS remains research-only; historical v2 overlap is not independent evaluation"]})
    save(out / f"results/{NAME}_rows.json", rows)
    save(result_path, result)
    save(out / "done.json", {"completed": time.time(), "contract": contract,
         "scores_sha256": digest(out / "scores.npy"), "result_sha256": digest(result_path),
         "rows_sha256": digest(out / f"results/{NAME}_rows.json"), "gallery_sha256": contract["gallery_sha256"],
         "descriptor_pool_sha256": digest(out / "descriptor_pool.json")})
    acquisition.state(out, "live_expansion_evaluated", len(added), len(added), raw=result["raw"])


def publish(night, out, contract):
    done = json.loads((out / "done.json").read_text())
    if done["contract"] != contract:
        raise RuntimeError("completed second-tranche evaluation contract changed")
    for name, field in ((f"results/{NAME}.json", "result_sha256"), (f"results/{NAME}_rows.json", "rows_sha256"),
                         ("scores.npy", "scores_sha256"), ("descriptor_pool.json", "descriptor_pool_sha256"),
                         ("gallery.parquet", "gallery_sha256")):
        if digest(out / name) != done[field]:
            raise RuntimeError("second-tranche result artifacts changed before publication")
    with (LOCAL / "controller.lock").open("a") as main:
        fcntl.flock(main, fcntl.LOCK_EX)
        for suffix in (".json", "_rows.json"):
            source, target = out / f"results/{NAME}{suffix}", night / f"results/{NAME}{suffix}"
            if target.exists() and digest(target) != digest(source):
                raise RuntimeError("different second-tranche result already published")
            if not target.exists():
                save(target, json.loads(source.read_text()))
        report()
        save(out / "published.json", {"published": time.time(), "done_sha256": digest(out / "done.json")})


def run():
    *_, out = acquisition.stage()
    if os.getpriority(os.PRIO_PROCESS, 0) < 10:
        os.nice(10 - os.getpriority(os.PRIO_PROCESS, 0))
    with (LOCAL / "live_expansion2.lock").open("a") as own, threadpool_limits(limits=1):
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (out / "closed.json").exists():
            return
        try:
            data = inputs()
            gc, previous, night, out, first, gallery, added, pool, contract = data
            if not (out / "done.json").exists():
                acquisition.state(out, "live_expansion_waiting_for_collage_then_gpu")
                with (LOCAL / "collage.lock").open("a") as collage, (LOCAL / "controller.lock").open("a") as main:
                    fcntl.flock(collage, fcntl.LOCK_EX)
                    fcntl.flock(main, fcntl.LOCK_EX)
                    encode(gc, out, added, pool, contract)
                evaluate(gc, previous, night, out, first, gallery, added, contract)
            publish(night, out, contract)
            acquisition.state(out, "live_expansion_completed")
        except BaseException as exc:
            save(out / "evaluation.failure.json", {"failed": time.time(), "pid": os.getpid(), "error": repr(exc)})
            acquisition.state(out, "live_expansion_evaluation_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    run()
