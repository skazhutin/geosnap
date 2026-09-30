"""One common whole-sequence quarantine across cached baselines and new data."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale, night_live_expansion_eval
from ml.research import night_scale_context as context
from ml.research import night_v7 as evaluator
from ml.research.gallery_scale_storage import digest, save
from ml.research.metrics import paired_group_bootstrap, raw_metrics
from ml.research.night_gallery_addition import atomic_array
from ml.research.night_place import atomic_npz
from ml.research.night_v7_verify import gallery_truth, reconcile_result, validate_prefix
from ml.research.vector_evaluation import distances

PHASE = "quarantine_comparison"


def state(out, phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), **extra)
    save(out / "status.json", value)
    save(evaluator.LOCAL / f"quarantine_{out.name}_status.json", value)
    print(json.dumps(value), flush=True)


def subset_mapping(original, selected):
    if original.id.duplicated().any() or selected.id.duplicated().any():
        raise RuntimeError("reference identities must be unique")
    positions = pd.Index(original.id).get_indexer(selected.id)
    if (positions < 0).any() or (np.diff(positions) <= 0).any():
        raise RuntimeError("quarantine must preserve the original relative reference order")
    columns = ["id", "file_sha256", "lat", "lon"]
    pd.testing.assert_frame_equal(original.iloc[positions][columns].reset_index(drop=True),
                                  selected[columns].reset_index(drop=True))
    return positions


def compose_scores(original, positions, new=None):
    """Copy literal committed columns in RAM; no sparse external memmap writes."""
    if original.dtype != np.float32 or original.ndim != 2:
        raise RuntimeError("expected original exact float32 scores")
    if new is not None and (new.dtype != np.float32 or new.ndim != 2 or len(new) != len(original)):
        raise RuntimeError("new exact scores differ in dtype or query population")
    values = np.empty((len(original), len(positions) + (0 if new is None else new.shape[1])), np.float32)
    for start in range(0, len(original), 4):
        values[start:start + 4, :len(positions)] = original[start:start + 4, positions]
    if new is not None:
        values[:, len(positions):] = new
    if not np.isfinite(values).all():
        raise RuntimeError("nonfinite score matrix after common reference quarantine")
    return values


def load():
    from ml.research import night_quarantine_audit as audit

    gc, previous, night, stage, g2, first, added, expanded = audit.verify_audited()
    data = context.load_inputs()
    cfg, _, _, queries, q322, q504, means, original_g2, _, _, checkpoint, component = data
    old = night / "gallery_addition"
    original_first = pd.read_parquet(old / "gallery.parquet")
    first_done = json.loads((old / "done.json").read_text())
    pipeline = night / "cpu_pipeline_added/gallery_addition"
    pair_done = json.loads((pipeline / "mean_scores.done.json").read_text())
    for path, expected in ((old / "scores.npy", first_done["scores_sha256"]),
                           (pipeline / "mean_scores.npy", pair_done["scores_sha256"])):
        if digest(path) != expected:
            raise RuntimeError("completed source scores changed")
    maps = {"G2": subset_mapping(original_first, g2), "first": subset_mapping(original_first, first)}
    subset_mapping(original_g2, g2)
    pd.testing.assert_frame_equal(expanded.iloc[:len(first)].reset_index(drop=True), first.reset_index(drop=True))
    pool = json.loads((old / "descriptor_pool.json").read_text())
    contract = {"source_sha256": digest(Path(__file__)), "audit_source_sha256": digest(Path(audit.__file__)),
        "audit_sha256": digest(stage / "audit.done.json"), "quarantine_receipt_sha256": digest(stage / "quarantine.receipt.json"),
        "component": component, "baseline_pool_sha256": digest(old / "descriptor_pool.json"),
        "context_reuse_sources": {str(p): digest(p) for root in (pipeline, night / "cpu_scale_context")
            for p in [root / "done.json", *sorted((root / "blocks").glob("queries-*.json"))]},
        "baseline_scores_sha256": first_done["scores_sha256"], "pair_scores_sha256": pair_done["scores_sha256"],
        "sources": {p.name: digest(p) for p in (Path(night_live_expansion_eval.__file__), Path(context.__file__),
            Path(gallery_scale.__file__), Path(evaluator.__file__))},
        "descriptor_contract_sha256": digest(previous / "descriptor_contract.json"),
        "encoding_runtime": {key: gc[key] for key in ("device", "batch_size", "torch_threads")},
        "retriever": json.loads((previous / "descriptor_contract.json").read_text())["retriever"],
        "manifests": {name: digest(stage / name) for name in ("clean_G2.parquet", "clean_first.parquet", "addition.parquet", "gallery.parquet")},
        "query_sha256": gc["query_sha256"], "same_query_count": len(queries), "common_reference_quarantine": True,
        "inference": "fixed SAGE-L322; exact cosine; fixed mean322/504; context30/mix0.5",
        "new_model_or_training": False, "query_truth_used_for_inference": False,
        "calibration_final_access": False, "production_changes": False}
    return dict(gc=gc, cfg=cfg, previous=previous, night=night, stage=stage, q=queries, q322=q322,
        q504=q504, means=means, checkpoint=checkpoint, original_first=original_first, original_g2=original_g2,
        galleries={"G2": g2, "first": first, "expanded2": expanded}, added=added,
        maps=maps, pool=pool, contract=contract, old=old, pipeline=pipeline)


def saved_context(directory, queries):
    marker = json.loads((directory / ("pipeline.started.json" if (directory / "pipeline.started.json").exists()
                                     else "scale_context.started.json")).read_text())
    contract = marker["contract"]
    chosen, values = [], []
    for start in range(0, len(queries), context.SETUP["checkpoint_queries"]):
        path = directory / "blocks" / f"queries-{start:04d}.npz"
        receipt = json.loads(path.with_suffix(".json").read_text())
        with np.load(path, allow_pickle=False) as block:
            indices = block["indices"]
            expected = {"contract_sha256": context.signature(contract),
                        "query_ids": queries.id.iloc[start:start + len(indices)].tolist(),
                        "indices_sha256": hashlib.sha256(indices.tobytes()).hexdigest()}
            if receipt["inputs"] != expected or digest(path) != receipt["sha256"]:
                raise RuntimeError("original context evidence changed")
            chosen.append(indices)
            values.append(block["contextual"])
    return np.concatenate(chosen), np.concatenate(values)


def reuse_mask(gallery, chosen, previous_gallery, previous_chosen):
    if chosen.shape != previous_chosen.shape:
        raise RuntimeError("context query/depth population changed")
    return np.all(gallery.id.to_numpy()[chosen] == previous_gallery.id.to_numpy()[previous_chosen], axis=1)


def contextualize(data, gallery, prefix, scores, out, source):
    contract = data["contract"]
    target = out / "context_evidence.npz"
    receipt_path = out / "context_evidence.json"
    expected = {"contract_sha256": context.signature(contract), "prefix_sha256": digest(out / "mean_prefix.npz")}
    if receipt_path.exists():
        receipt = json.loads(receipt_path.read_text())
        if receipt["inputs"] != expected or digest(target) != receipt["sha256"]:
            raise RuntimeError("quarantined context checkpoint changed")
        with np.load(target, allow_pickle=False) as stored:
            return stored["ranked"]
    chosen = prefix[:, :context.SETUP["context_depth"]]
    directory, old_gallery = source
    if (directory / "context_evidence.json").exists():
        old_receipt = json.loads((directory / "context_evidence.json").read_text())
        if (digest(directory / "context_evidence.npz") != old_receipt["sha256"]
                or old_receipt["inputs"]["contract_sha256"] != context.signature(contract)):
            raise RuntimeError("preceding quarantine context evidence changed")
        with np.load(directory / "context_evidence.npz", allow_pickle=False) as stored:
            old_chosen, old_values = stored["chosen"], stored["contextual"]
    else:
        old_chosen, old_values = saved_context(directory, data["q"])
    reused = reuse_mask(gallery, chosen, old_gallery, old_chosen)
    values = np.empty(chosen.shape, np.float32)
    values[reused] = old_values[reused]
    pending = np.flatnonzero(~reused)
    encoder = vectors = None
    if len(pending):
        unique, remap = np.unique(chosen[pending], return_inverse=True)
        remap = remap.reshape(len(pending), chosen.shape[1])
        state(out, "context_loading_changed_references", queries=len(pending), references=len(unique))
        pool_root = data["stage"] if out.name == "expanded2" else data["old"]
        vectors = evaluator.load_vectors(gallery, unique, pool_root)
        encoder = torch.nn.TransformerEncoder(torch.nn.TransformerEncoderLayer(
            d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=.1, batch_first=False), 2)
        encoder.load_state_dict(torch.load(data["checkpoint"], map_location="cpu", weights_only=True), strict=True)
        encoder.eval()
        directory = out / "changed_context"
        directory.mkdir(exist_ok=True)
        for offset, i in enumerate(pending):
            path = directory / f"query-{i:04d}.npz"
            receipt = path.with_suffix(".json")
            key = expected | {"query_id": data["q"].id.iloc[i], "reference_ids": gallery.id.iloc[chosen[i]].tolist()}
            if receipt.exists():
                saved = json.loads(receipt.read_text())
                if saved["inputs"] != key or digest(path) != saved["sha256"]:
                    raise RuntimeError("changed context query checkpoint differs")
                with np.load(path, allow_pickle=False) as cached:
                    values[i], direct = cached["contextual"], cached["direct"]
            else:
                values[i], direct = context.one_query(encoder, data["means"][i], data["q322"][i], data["q504"][i], vectors[remap[offset]])
                atomic_npz(path, contextual=values[i], direct=direct)
                save(receipt, {"inputs": key, "sha256": digest(path)})
            np.testing.assert_allclose(direct, scores[i, chosen[i]], atol=2e-6, rtol=1e-5)
            if offset % 25 == 0 or offset + 1 == len(pending):
                state(out, "context_changed_queries", completed=offset + 1, total=len(pending), reused=int(reused.sum()))
    del encoder, vectors
    ranked = context.rank_prefix(prefix, values, np.take_along_axis(scores, chosen, axis=1))
    atomic_npz(target, chosen=chosen, contextual=values, ranked=ranked, reused=reused)
    save(receipt_path, {"inputs": expected, "sha256": digest(target), "reused_queries": int(reused.sum()),
                       "computed_queries": len(pending), "source_evidence_directory": str(source[0])})
    return ranked


def record(data, key, arm, gallery, out, scores=None, prefix=None, mean_summary=None):
    name = f"quarantine_{key}_{arm}"
    summary_path, rows_path = out / f"{arm}.json", out / f"{arm}_rows.json"
    q = data["q"]
    if scores is not None:
        summary, rows = gallery_scale.summarize(gallery, q, scores)
        ranks = np.asarray([np.inf if x is None else x for x in rows["positive_ranks"]])
    else:
        validate_prefix(prefix, len(q), len(gallery))
        truth = gallery_truth(q, gallery)
        errors, saved_ranks = [], []
        for i, query in enumerate(q.itertuples()):
            d = distances(query.lat, query.lon, truth["latitude"][prefix[i]], truth["longitude"][prefix[i]])
            errors.append(float(d[0]))
            positives = np.flatnonzero(d <= 100)
            saved_ranks.append(int(positives[0]) + 1 if len(positives) else None)
        ranks = np.asarray([np.inf if r is None else r for r in saved_ranks])
        coverage = truth["positive_count"] > 0
        summary = {"raw": raw_metrics(errors), "gallery": mean_summary["gallery"], "coverage": mean_summary["coverage"],
            "diagnosis": {"no_coverage": int((~coverage).sum()), "retrieval_miss_top100": int((coverage & ~np.isfinite(ranks)).sum()),
                "wrong_top1_positive_in_top100": int(((ranks > 1) & np.isfinite(ranks)).sum()), "correct_top1": int((ranks == 1).sum())}}
        rows = {"query_ids": q.id.tolist(), "errors_m": errors, "positive_ranks_through100": saved_ranks,
                "top100_gallery_rows": prefix.tolist()}
    manifest = data["stage"] / {"G2": "clean_G2.parquet", "first": "clean_first.parquet", "expanded2": "gallery.parquet"}[key]
    summary.update(name=name, query_count=len(q), gallery_count=len(gallery), gallery_manifest=str(manifest),
        gallery_sha256=digest(manifest), query_sha256=data["gc"]["query_sha256"],
        recall_at={str(k): float(np.mean(ranks <= k)) for k in (1, 5, 10, 20, 50, 100)},
        common_quarantine=True, abstention=False, contract_sha256=context.signature(data["contract"]))
    truth = gallery_truth(q, gallery)
    reconcile_result(summary, rows, q, gallery, truth)
    save(rows_path, rows)
    save(summary_path, summary)
    state(out, "result", name=name, raw100=summary["raw"]["accuracy_100m"])
    return summary, rows


def evaluate(data, key):
    out = data["stage"] / "evaluation" / key
    out.mkdir(parents=True, exist_ok=True)
    if (out / "done.json").exists():
        verify_done(out, data["contract"])
        return
    gallery = data["galleries"][key]
    positions = data["maps"]["first" if key == "expanded2" else key]
    new322 = newmean = None
    if key == "expanded2":
        pool = json.loads((data["stage"] / "descriptor_pool.json").read_text())
        for stem, queries in (("new322", data["q322"]), ("new504", data["q504"])):
            path = out / f"{stem}.npy"
            receipt = path.with_suffix(".json")
            expected = {"contract_sha256": context.signature(data["contract"]), "pool_sha256": digest(data["stage"] / "descriptor_pool.json")}
            if receipt.exists():
                saved = json.loads(receipt.read_text())
                if saved["inputs"] != expected or digest(path) != saved["sha256"]:
                    raise RuntimeError("new exact scores changed")
            else:
                state(out, f"{stem}_scores", references=len(data["added"]))
                values = gallery_scale.exact_scores(data["added"], queries, pool["entries"])
                atomic_array(path, values)
                save(receipt, {"inputs": expected, "sha256": digest(path)})
                del values
        new322 = np.load(out / "new322.npy", allow_pickle=False)
        newmean = .5 * (np.load(out / "new504.npy", allow_pickle=False) + new322)
    for arm, original_path, new in (("top1", data["old"] / "scores.npy", new322),
                                   ("mean", data["pipeline"] / "mean_scores.npy", newmean)):
        state(out, f"{arm}_copying_exact_scores", references=len(gallery))
        original = np.load(original_path, mmap_mode="r", allow_pickle=False)
        try:
            scores = compose_scores(original, positions, new)
        finally:
            original._mmap.close()
        # Sequential writes are intentional for the external NTFS driver.
        score_path = out / f"{arm}_scores.npy"
        receipt = score_path.with_suffix(".json")
        if receipt.exists():
            saved = json.loads(receipt.read_text())
            if saved["contract_sha256"] != context.signature(data["contract"]) or digest(score_path) != saved["sha256"]:
                raise RuntimeError("committed quarantine exact scores changed")
            old_scores = np.load(score_path, mmap_mode="r", allow_pickle=False)
            try:
                if not np.array_equal(old_scores, scores):
                    raise RuntimeError("literal old-score reuse or new-score arithmetic changed")
            finally:
                old_scores._mmap.close()
        else:
            atomic_array(score_path, scores)
            save(receipt, {"contract_sha256": context.signature(data["contract"]), "sha256": digest(score_path)})
        summary, rows = record(data, key, arm, gallery, out, scores=scores)
        if arm == "mean":
            prefix = np.asarray(rows["top100_gallery_rows"], np.int32)
            atomic_npz(out / "mean_prefix.npz", indices=prefix)
            source = {"G2": (data["night"] / "cpu_scale_context", data["original_g2"]),
                "first": (data["pipeline"], data["original_first"]),
                "expanded2": (data["stage"] / "evaluation/first", data["galleries"]["first"])}[key]
            ranked = contextualize(data, gallery, prefix, scores, out, source)
            record(data, key, "context", gallery, out, prefix=ranked, mean_summary=summary)
        del scores
    files = [p for p in out.iterdir() if p.suffix in (".json", ".npz") and p.name not in ("status.json", "done.json") and not p.name.startswith("._")]
    save(out / "done.json", {"contract": data["contract"], "completed": time.time(), "artifacts": {p.name: digest(p) for p in files}})
    state(out, "completed")


def verify_done(out, contract):
    done = json.loads((out / "done.json").read_text())
    if done["contract"] != contract or any(digest(out / name) != sha for name, sha in done["artifacts"].items()):
        raise RuntimeError("completed common-quarantine comparison changed")
    for arm in ("top1", "mean"):
        receipt = json.loads((out / f"{arm}_scores.json").read_text())
        if digest(out / f"{arm}_scores.npy") != receipt["sha256"]:
            raise RuntimeError("committed common-quarantine score bytes changed")


def board(data):
    records, errors = [], {}
    for key in ("G2", "first", "expanded2"):
        directory = data["stage"] / "evaluation" / key
        if not (directory / "done.json").exists():
            continue
        verify_done(directory, data["contract"])
        for arm in ("top1", "mean", "context"):
            record = json.loads((directory / f"{arm}.json").read_text())
            rows = json.loads((directory / f"{arm}_rows.json").read_text())
            records.append(record)
            errors[record["name"]] = rows["errors_m"]
    groups = data["q"].h3_coarse.astype(str).tolist()
    comparisons = {}
    for key in ("first", "expanded2"):
        for arm in ("top1", "mean", "context"):
            name = f"quarantine_{key}_{arm}"
            before = f"quarantine_{'G2' if key == 'first' else 'first'}_{arm}"
            if name in errors and before in errors:
                comparisons[name] = paired_group_bootstrap(errors[before], errors[name], groups)
    records.sort(key=lambda r: (-r["raw"]["accuracy_100m"], -r["raw"]["accuracy_50m"],
                               -r["raw"]["accuracy_25m"], r["raw"]["catastrophic_gt500m_rate"]))
    save(data["stage"] / "leaderboard.json", {"records": records, "paired_same_architecture_data_gain": comparisons,
        "scope": "common newly-implicated sequence quarantine, same1184development; original results preserved separately"})


def run(action):
    os.nice(max(0, 10 - os.getpriority(os.PRIO_PROCESS, 0)))
    torch.set_num_threads(1)
    with (evaluator.LOCAL / f"quarantine_{action}.lock").open("a") as own, threadpool_limits(limits=1):
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        data = load()
        stage = data["stage"]
        marker = stage / f"{action}.started.json"
        if marker.exists() and json.loads(marker.read_text())["contract"] != data["contract"]:
            raise RuntimeError("registered common-quarantine source/input changed")
        if not marker.exists():
            if not evaluator.allowed_to_start(data["cfg"], action, stage):
                save(stage / f"{action}.closed.json", {"reason": "06:00 cutoff"})
                return
            save(marker, {"started": time.time(), "contract": data["contract"]})
        if action == "encode":
            with (evaluator.LOCAL / "controller.lock").open("a") as gpu:
                fcntl.flock(gpu, fcntl.LOCK_EX)
                old_state = night_live_expansion_eval.acquisition.state
                night_live_expansion_eval.acquisition.state = lambda out, phase, completed, total: state(out, phase, completed=completed, total=total)
                try:
                    night_live_expansion_eval.encode(data["gc"], stage, data["added"], data["pool"], data["contract"])
                finally:
                    night_live_expansion_eval.acquisition.state = old_state
        else:
            keys = ("G2", "first") if action == "baseline" else ("expanded2",)
            if action == "expanded" and not (stage / "encode.done.json").exists():
                raise RuntimeError("complete and verify new reference encoding before evaluation")
            if action == "expanded":
                verify_done(stage / "evaluation/first", data["contract"])
                encoded = json.loads((stage / "encode.done.json").read_text())
                if (encoded["contract"] != data["contract"]
                        or digest(stage / "descriptor_pool.json") != encoded["descriptor_pool_sha256"]
                        or any(digest(stage / "descriptors" / name) != sha for name, sha in encoded["chunks"].items())):
                    raise RuntimeError("new encoded descriptor pool changed before expanded evaluation")
            for key in keys:
                evaluate(data, key)
            board(data)
            save(stage / f"{action}.done.json", {"contract_sha256": context.signature(data["contract"]), "completed": time.time()})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("baseline", "encode", "expanded"))
    run(parser.parse_args().action)
