"""One gated data continuation of the fixed mean/context pipeline; no fourth tranche."""
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
from ml.research import night_quarantine_eval as common
from ml.research import night_scale_context as context
from ml.research import night_v7 as evaluator
from ml.research.gallery_scale_storage import digest, save
from ml.research.metrics import paired_group_bootstrap, raw_metrics
from ml.research.night_gallery_addition import atomic_array
from ml.research.night_place import atomic_npz
from ml.research.night_v7_verify import gallery_truth, reconcile_result, validate_prefix
from ml.research.vector_evaluation import distances


def state(out, phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), **extra)
    save(out / "evaluation_status.json", value)
    save(evaluator.LOCAL / "tranche3_evaluation_status.json", value)
    print(json.dumps(value), flush=True)


def load():
    from ml.research import night_reference_tranche3 as audit

    gc, previous, night, out, baseline, added, gallery = audit.verify_audited()
    parent = night / "live_expansion2_quarantine"
    source = parent / "evaluation/expanded2"
    parent_done = json.loads((source / "done.json").read_text())
    parent_contract = parent_done["contract"]
    common.verify_done(source, parent_contract)
    data = context.load_inputs()
    cfg, _, _, q, q322, q504, means, _, _, _, checkpoint, component = data
    if (parent_contract["component"] != component or parent_contract["source_sha256"] != digest(Path(common.__file__))
            or parent_contract["manifests"]["gallery.parquet"] != digest(parent / "gallery.parquet")):
        raise RuntimeError("third tranche differs from completed parent query/context/gallery contract")
    pd.testing.assert_frame_equal(gallery.iloc[:len(baseline)].reset_index(drop=True), baseline.reset_index(drop=True), check_dtype=False)
    if (len(q) != 1184 or len(gallery) != len(baseline) + len(added) or gallery.id.duplicated().any()
            or not added.production_compatible.all()):
        raise RuntimeError("third-tranche denominator, membership, or source compatibility changed")
    encoded = json.loads((parent / "encode.done.json").read_text())
    if encoded["contract"] != parent_contract or digest(parent / "descriptor_pool.json") != encoded["descriptor_pool_sha256"]:
        raise RuntimeError("completed parent descriptor pool changed")
    pool = json.loads((parent / "descriptor_pool.json").read_text())
    contract = {"source_sha256": digest(Path(__file__)), "audit_source_sha256": digest(Path(audit.__file__)),
        "helper_sources": {p.name: digest(p) for p in (Path(common.__file__), Path(context.__file__),
            Path(night_live_expansion_eval.__file__), Path(gallery_scale.__file__), Path(evaluator.__file__))},
        "audit_sha256": digest(out / "audit.done.json"), "gallery_sha256": digest(out / "gallery.parquet"),
        "addition_sha256": digest(out / "addition.parquet"), "parent_contract": parent_contract,
        "parent_completion_sha256": digest(source / "done.json"), "parent_pool_sha256": digest(parent / "descriptor_pool.json"),
        "parent_context_receipt_sha256": digest(source / "context_evidence.json"),
        "component": component, "query_sha256": gc["query_sha256"], "query_count": len(q),
        "descriptor_contract_sha256": digest(previous / "descriptor_contract.json"),
        "retriever": json.loads((previous / "descriptor_contract.json").read_text())["retriever"],
        "encoding_runtime": {key: gc[key] for key in ("device", "batch_size", "torch_threads")},
        "inference": "same322; literal parent mean322/504 prefix; mean only new references; context30/mix0.5",
        "no_fourth_tranche": True, "calibration_final_access": False, "production_changes": False,
        "query_coordinates_used_for_inference_or_acquisition": False}
    return dict(gc=gc, cfg=cfg, previous=previous, night=night, out=out, parent=parent, source=source,
        q=q, q322=q322, q504=q504, means=means, checkpoint=checkpoint, baseline=baseline,
        added=added, gallery=gallery, pool=pool, contract=contract)


def context_ranking(data, prefix, scores):
    out, gallery = data["out"], data["gallery"]
    target = out / "context_evidence.npz"
    expected = {"contract_sha256": context.signature(data["contract"]),
                "mean_prefix_sha256": hashlib.sha256(prefix.tobytes()).hexdigest()}
    if target.with_suffix(".json").exists():
        saved = json.loads(target.with_suffix(".json").read_text())
        if saved["inputs"] != expected or digest(target) != saved["sha256"]:
            raise RuntimeError("third-tranche context evidence changed")
        with np.load(target, allow_pickle=False) as cached:
            return cached["ranked"]
    source = data["source"] / "context_evidence.npz"
    receipt = json.loads(source.with_suffix(".json").read_text())
    if (receipt["inputs"]["contract_sha256"] != context.signature(data["contract"]["parent_contract"])
            or digest(source) != receipt["sha256"]):
        raise RuntimeError("verified parent context evidence changed")
    with np.load(source, allow_pickle=False) as cached:
        old_choices, old_values = cached["chosen"], cached["contextual"]
    chosen = prefix[:, :30]
    reuse = common.reuse_mask(gallery, chosen, data["baseline"], old_choices)
    values = np.empty(chosen.shape, np.float32)
    values[reuse] = old_values[reuse]
    pending = np.flatnonzero(~reuse)
    vectors = encoder = None
    if len(pending):
        unique, remap = np.unique(chosen[pending], return_inverse=True)
        remap = remap.reshape(len(pending), 30)
        state(out, "tranche3_loading_context", queries=len(pending), references=len(unique), reused=int(reuse.sum()))
        vectors = evaluator.load_vectors(gallery, unique, out)
        encoder = torch.nn.TransformerEncoder(torch.nn.TransformerEncoderLayer(
            d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=.1, batch_first=False), 2)
        encoder.load_state_dict(torch.load(data["checkpoint"], map_location="cpu", weights_only=True), strict=True)
        encoder.eval()
        directory = out / "changed_context"
        directory.mkdir(exist_ok=True)
        for position, i in enumerate(pending):
            path = directory / f"query-{i:04d}.npz"
            marker = path.with_suffix(".json")
            inputs = expected | {"query_id": data["q"].id.iloc[i], "reference_ids": gallery.id.iloc[chosen[i]].tolist()}
            if marker.exists():
                saved = json.loads(marker.read_text())
                if saved["inputs"] != inputs or digest(path) != saved["sha256"]:
                    raise RuntimeError("third-tranche changed-query context checkpoint changed")
                with np.load(path, allow_pickle=False) as cached:
                    values[i], direct = cached["contextual"], cached["direct"]
            else:
                values[i], direct = context.one_query(encoder, data["means"][i], data["q322"][i], data["q504"][i], vectors[remap[position]])
                atomic_npz(path, contextual=values[i], direct=direct)
                save(marker, {"inputs": inputs, "sha256": digest(path)})
            np.testing.assert_allclose(direct, scores[i, chosen[i]], atol=2e-6, rtol=1e-5)
            if position % 25 == 0 or position + 1 == len(pending):
                state(out, "tranche3_context_queries", completed=position + 1, total=len(pending))
    del vectors, encoder
    ranked = context.rank_prefix(prefix, values, np.take_along_axis(scores, chosen, axis=1))
    atomic_npz(target, chosen=chosen, contextual=values, ranked=ranked, reused=reuse)
    save(target.with_suffix(".json"), {"inputs": expected, "sha256": digest(target),
         "reused_queries": int(reuse.sum()), "computed_queries": len(pending)})
    return ranked


def record(data, arm, scores=None, prefix=None, mean_summary=None):
    out, q, g = data["out"], data["q"], data["gallery"]
    truth = gallery_truth(q, g)
    if scores is not None:
        summary, rows = gallery_scale.summarize(g, q, scores)
        ranks = np.array([np.inf if r is None else r for r in rows["positive_ranks"]])
    else:
        validate_prefix(prefix, len(q), len(g))
        errors, positive = [], []
        for i, query in enumerate(q.itertuples()):
            d = distances(query.lat, query.lon, truth["latitude"][prefix[i]], truth["longitude"][prefix[i]])
            errors.append(float(d[0]))
            found = np.flatnonzero(d <= 100)
            positive.append(int(found[0]) + 1 if len(found) else None)
        ranks = np.array([np.inf if r is None else r for r in positive])
        covered = truth["positive_count"] > 0
        summary = {"raw": raw_metrics(errors), "gallery": mean_summary["gallery"], "coverage": mean_summary["coverage"],
            "diagnosis": {"no_coverage": int((~covered).sum()), "retrieval_miss_top100": int((covered & ~np.isfinite(ranks)).sum()),
                "wrong_top1_positive_in_top100": int(((ranks > 1) & np.isfinite(ranks)).sum()), "correct_top1": int((ranks == 1).sum())}}
        rows = {"query_ids": q.id.tolist(), "errors_m": errors, "positive_ranks_through100": positive,
                "top100_gallery_rows": prefix.tolist()}
    baseline = json.loads((data["source"] / f"{arm}_rows.json").read_text())
    if baseline["query_ids"] != q.id.tolist():
        raise RuntimeError("parent comparison query order differs")
    summary.update(name=f"tranche3_{arm}", query_count=len(q), gallery_count=len(g),
        gallery_manifest=str(out / "gallery.parquet"), gallery_sha256=data["contract"]["gallery_sha256"],
        query_sha256=data["gc"]["query_sha256"], abstention=False, common_quarantine=True,
        recall_at={str(k): float(np.mean(ranks <= k)) for k in (1, 5, 10, 20, 50, 100)},
        paired_vs_parent=paired_group_bootstrap(baseline["errors_m"], rows["errors_m"], q.h3_coarse.tolist()),
        contract_sha256=context.signature(data["contract"]))
    checked = reconcile_result(summary, rows, q, g, truth)
    save(out / f"{arm}_rows.json", rows)
    save(out / f"{arm}.json", summary)
    state(out, "tranche3_result", arm=arm, raw100=summary["raw"]["accuracy_100m"])
    return summary, rows, checked


def evaluate(data):
    out = data["out"]
    if (out / "evaluation.done.json").exists():
        verify_done(data)
        return
    pool = json.loads((out / "descriptor_pool.json").read_text())
    encoded = json.loads((out / "encode.done.json").read_text())
    if (encoded["contract"] != data["contract"] or digest(out / "descriptor_pool.json") != encoded["descriptor_pool_sha256"]
            or any(digest(out / "descriptors" / name) != sha for name, sha in encoded["chunks"].items())):
        raise RuntimeError("third-tranche encoded descriptor pool changed")
    for stem, vectors in (("new322", data["q322"]), ("new504", data["q504"])):
        path = out / f"{stem}.npy"
        marker = path.with_suffix(".json")
        expected = {"contract_sha256": context.signature(data["contract"]), "pool_sha256": digest(out / "descriptor_pool.json")}
        if marker.exists():
            saved = json.loads(marker.read_text())
            if saved["inputs"] != expected or digest(path) != saved["sha256"]:
                raise RuntimeError("third-tranche new exact scores changed")
        else:
            values = gallery_scale.exact_scores(data["added"], vectors, pool["entries"])
            atomic_array(path, values)
            save(marker, {"inputs": expected, "sha256": digest(path)})
            del values
    new322 = np.load(out / "new322.npy", allow_pickle=False)
    new_mean = .5 * (np.load(out / "new504.npy", allow_pickle=False) + new322)
    checks, records = {}, {}
    for arm, added in (("top1", new322), ("mean", new_mean)):
        path = out / f"{arm}_scores.npy"
        marker = path.with_suffix(".json")
        expected = {"contract_sha256": context.signature(data["contract"])}
        if marker.exists():
            saved = json.loads(marker.read_text())
            if saved["inputs"] != expected or digest(path) != saved["sha256"]:
                raise RuntimeError("third-tranche completed score matrix changed")
            values = np.load(path, allow_pickle=False)
        else:
            baseline = np.load(data["source"] / f"{arm}_scores.npy", mmap_mode="r", allow_pickle=False)
            try:
                values = common.compose_scores(baseline, np.arange(len(data["baseline"])), added)
            finally:
                baseline._mmap.close()
            atomic_array(path, values)
            save(marker, {"inputs": expected, "sha256": digest(path), "literal_parent_prefix": True})
        records[arm], rows, checks[arm] = record(data, arm, scores=values)
        if arm == "mean":
            prefix = np.asarray(rows["top100_gallery_rows"], np.int32)
            ranked = context_ranking(data, prefix, values)
            records["context"], _, checks["context"] = record(data, "context", prefix=ranked, mean_summary=records[arm])
        del values
    save(out / "report.json", {"records": records, "checks": checks, "fourth_tranche_will_not_start": True,
        "scope": "one gated reference-only continuation, same development, no abstention, no heldout or production change"})
    files = [p for p in out.iterdir() if p.suffix in (".json", ".npz", ".parquet")
             and p.name not in ("status.json", "evaluation_status.json", "failure.json", "evaluation.failure.json", "evaluation.done.json")
             and not p.name.startswith("._")]
    save(out / "evaluation.done.json", {"completed": time.time(), "contract": data["contract"],
        "artifacts": {p.name: digest(p) for p in files}})
    verify_done(data)
    state(out, "tranche3_completed")


def verify_done(data):
    out = data["out"]
    done = json.loads((out / "evaluation.done.json").read_text())
    if done["contract"] != data["contract"] or any(digest(out / name) != sha for name, sha in done["artifacts"].items()):
        raise RuntimeError("completed third-tranche evidence changed")
    for arm in ("top1", "mean"):
        if digest(out / f"{arm}_scores.npy") != json.loads((out / f"{arm}_scores.json").read_text())["sha256"]:
            raise RuntimeError("completed third-tranche score bytes changed")


def run(action):
    os.nice(max(0, 10 - os.getpriority(os.PRIO_PROCESS, 0)))
    torch.set_num_threads(1)
    with (evaluator.LOCAL / "tranche3_evaluation.lock").open("a") as own, threadpool_limits(limits=1):
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        data = load()
        out = data["out"]
        marker = out / "evaluation.started.json"
        if marker.exists() and json.loads(marker.read_text())["contract"] != data["contract"]:
            raise RuntimeError("third-tranche evaluation source/input contract changed")
        if not marker.exists():
            if not evaluator.allowed_to_start(data["cfg"], "evaluation", out):
                save(out / "evaluation.closed.json", {"reason": "06:00 cutoff"})
                return
            save(marker, {"started": time.time(), "contract": data["contract"]})
        if action == "run":
            with (evaluator.LOCAL / "controller.lock").open("a") as gpu:
                fcntl.flock(gpu, fcntl.LOCK_EX)
                old_state = night_live_expansion_eval.acquisition.state
                night_live_expansion_eval.acquisition.state = lambda root, phase, completed, total: state(root, phase, completed=completed, total=total)
                try:
                    night_live_expansion_eval.encode(data["gc"], out, data["added"], data["pool"], data["contract"])
                finally:
                    night_live_expansion_eval.acquisition.state = old_state
            evaluate(data)
        else:
            verify_done(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("run", "verify"))
    run(parser.parse_args().action)
