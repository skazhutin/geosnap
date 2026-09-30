"""One fixed source-compatible transfer to the third gallery, using cached scores only."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from threadpoolctl import threadpool_limits

from ml.research import night_licensed_pipeline as licensed
from ml.research import night_reference_tranche3 as acquisition
from ml.research import night_tranche3_eval as third
from ml.research.gallery_scale_storage import WORKSPACE, digest, save

evaluator = licensed.evaluator
context = licensed.context
PHASE = "licensed_third"
SETUP = {**licensed.SETUP, "gallery": "reference_tranche3 source-compatible subset",
         "parent_licensed_references": 40581, "new_compatible_references": 4414,
         "selected_references": 44995, "result_prefix": "licensed_third",
         "context_reuse": "previous licensed evidence iff ordered physical first30 identities match",
         "reference_pool": "explicit reference_tranche3 descriptor_pool.json"}


def stage():
    cfg, gc, store, previous, night = evaluator.paths()
    return cfg, gc, store, previous, night, night / PHASE


def state(out, phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), **extra)
    save(out / "status.json", value)
    save(evaluator.LOCAL / "licensed_third_status.json", value)
    print(json.dumps(value), flush=True)


def registration_inputs(night, gc):
    files = [night / "reference_tranche3" / name for name in ("audit.done.json", "gallery.parquet", "addition.parquet")]
    files += [night / "licensed_pipeline" / name for name in ("done.json", "gallery.parquet")]
    files += [WORKSPACE / gc["query_manifest"], WORKSPACE / gc["aoi"]]
    return {str(path): digest(path) for path in files}


def prepare():
    _, gc, _, _, night, out = stage()
    out.mkdir(exist_ok=True)
    fixed = {"setup": SETUP, "inputs": registration_inputs(night, gc),
             "selection_uses_reference_metadata_only": True,
             "new_image_encoding": False, "new_candidate_family": False,
             "preparation_does_not_count_as_started_experiment": True}
    path = out / "intent.json"
    if path.exists():
        existing = json.loads(path.read_text())
        if any(existing[key] != value for key, value in fixed.items()):
            raise RuntimeError("licensed third preregistration or audited inputs changed")
    else:
        save(path, fixed | {"registered_at": time.time(),
            "main_third_result_files_present_at_registration": [name for name in ("top1.json", "mean.json", "context.json")
                if (night / "reference_tranche3" / name).exists()]})
    return json.loads(path.read_text())


def source_contract():
    modules = (Path(__file__), Path(licensed.__file__), Path(acquisition.__file__), Path(third.__file__),
               Path(licensed.audit.__file__), Path(licensed.previous_eval.__file__), Path(context.__file__),
               Path(licensed.gallery_scale.__file__), Path(evaluator.__file__))
    return {str(path.resolve()): digest(path) for path in modules}


def assert_sources(expected):
    if source_contract() != expected:
        raise RuntimeError("licensed third implementation changed after registration")


def register_implementation(out):
    value = {"sources": source_contract()}
    target = out / "implementation.json"
    if target.exists():
        if json.loads(target.read_text()) != value:
            raise RuntimeError("licensed third frozen implementation changed")
    else:
        save(target, value)
    return value["sources"]


def selection(full, old):
    selected = licensed.selected_gallery(full)
    if len(old) != SETUP["parent_licensed_references"] or len(selected) != SETUP["selected_references"]:
        raise RuntimeError("registered licensed third population changed")
    pd.testing.assert_frame_equal(selected.iloc[:len(old)].reset_index(drop=True), old.reset_index(drop=True), check_dtype=False)
    if len(selected) - len(old) != SETUP["new_compatible_references"]:
        raise RuntimeError("registered compatible reference addition changed")
    return selected, licensed.previous_eval.subset_mapping(full, selected)


def load():
    cfg, gc, _, previous, night, out = stage()
    prepare()
    data = third.load()
    third.verify_done(data)
    source = night / "reference_tranche3"
    parent = night / "licensed_pipeline"
    completed = json.loads((parent / "done.json").read_text())
    old_contract = completed["contract"]
    licensed.verify({"out": parent, "contract": old_contract})
    if (data["out"] != source or data["night"] != night
            or old_contract["component"] != data["contract"]["component"]
            or old_contract["setup"] != licensed.SETUP
            or old_contract["query_sha256"] != gc["query_sha256"]):
        raise RuntimeError("third and prior licensed pipeline contracts differ")
    for path, sha in old_contract["files"].items():
        if digest(Path(path)) != sha:
            raise RuntimeError("previous licensed source/input changed")
    for filename, sha in old_contract["source_sha256"].items():
        if digest(WORKSPACE / "ml/research" / filename) != sha:
            raise RuntimeError("previous licensed implementation changed")
    full = data["gallery"]
    old = pd.read_parquet(parent / "gallery.parquet")
    selected, positions = selection(full, old)
    queries = data["q"]
    licensed.audit.development_checks(queries, selected)
    boundary = licensed.audit.acquisition.load_aoi_boundary(WORKSPACE / gc["aoi"])
    if not licensed.shapely.covers(boundary.geometry, licensed.shapely.points(selected.lon, selected.lat)).all():
        raise RuntimeError("licensed third reference outside exact Moscow AOI")
    encoded = json.loads((source / "encode.done.json").read_text())
    pool_path = source / "descriptor_pool.json"
    pool = json.loads(pool_path.read_text())
    if (encoded["contract"] != data["contract"] or digest(pool_path) != encoded["descriptor_pool_sha256"]
            or pool["contract_sha256"] != digest(previous / "descriptor_contract.json")
            or any(pool["entries"].get(row.id, {}).get("image_sha256") != row.file_sha256 for row in selected.itertuples())):
        raise RuntimeError("explicit third descriptor pool identity changed")
    production_path = night / "production_baseline/k30_rows.json"
    production = json.loads(production_path.read_text())
    if production["query_ids"] != queries.id.tolist() or len(queries) != SETUP["queries"]:
        raise RuntimeError("licensed third query population changed")
    files = [out / "intent.json", out / "implementation.json", source / "evaluation.done.json",
        pool_path, source / "encode.done.json", parent / "done.json", parent / "gallery.parquet",
        parent / "context_evidence.json", parent / "context_evidence.npz", production_path]
    files += [source / name for name in ("top1_scores.json", "mean_scores.json", "top1_rows.json", "mean_rows.json", "context_rows.json")]
    contract = {"setup": SETUP, "sources": source_contract(), "files": {str(path): digest(path) for path in files},
        "third_contract": data["contract"], "source_contract": old_contract,
        "query_sha256": gc["query_sha256"], "component": data["contract"]["component"],
        "selected_reference_ids_sha256": hashlib.sha256(json.dumps(selected.id.tolist()).encode()).hexdigest(),
        "gallery_count": len(selected), "full_gallery_count": len(full)}
    return dict(cfg=cfg, gc=gc, out=out, previous=previous, night=night, source=source,
        parent=parent, context_gallery=old, full=full, g=selected, positions=positions,
        q=queries, q322=data["q322"], q504=data["q504"], means=data["means"], checkpoint=data["checkpoint"],
        production=production, contract=contract)


@contextmanager
def helper_state():
    """Only redirect private process status; source helper code and settings stay frozen."""
    original = licensed.state
    licensed.state = state
    try:
        yield
    finally:
        licensed.state = original


def context_ranking(data, prefix, scores):
    # The previous licensed gallery supplies contextual evidence, whereas the
    # new third stage supplies reference descriptors. These roots are explicit.
    borrowed = data | {"source": data["parent"], "full": data["context_gallery"], "parent": data["source"]}
    with helper_state():
        return licensed.context_ranking(borrowed, prefix, scores)


def result(data, arm, truth, **kwargs):
    with helper_state():
        summary, rows, checked = licensed.result(data, arm, truth, **kwargs)
    summary["name"] = f"licensed_third_{arm}"
    summary["common_quarantine"] = True
    save(data["out"] / f"{arm}.json", summary)
    return summary, rows, checked


def evaluate(data):
    out, q, g = data["out"], data["q"], data["g"]
    if (out / "done.json").exists():
        verify(data)
        return
    licensed.audit.write_view(g, out / "gallery.parquet")
    save(out / "mapping.json", {"selected_to_third": data["positions"].tolist(), "full_count": len(data["full"]),
        "selected_count": len(g), "unchanged_prior_licensed_prefix": len(data["context_gallery"])})
    truth = licensed.gallery_truth(q, g)
    records, rows, checks = {}, {}, {}
    for arm in ("top1", "mean"):
        path = out / f"{arm}_scores.npy"
        expected = {"contract_sha256": context.signature(data["contract"])}
        marker = path.with_suffix(".json")
        if marker.exists():
            saved = json.loads(marker.read_text())
            if saved["inputs"] != expected or digest(path) != saved["sha256"]:
                raise RuntimeError("licensed third literal score checkpoint changed")
            scores = np.load(path, allow_pickle=False)
        else:
            full = np.load(data["source"] / f"{arm}_scores.npy", mmap_mode="r", allow_pickle=False)
            try:
                scores = licensed.previous_eval.compose_scores(full, data["positions"])
            finally:
                full._mmap.close()
            licensed.atomic_array(path, scores)
            save(marker, {"inputs": expected, "sha256": digest(path), "literal_third_columns": True})
        records[arm], rows[arm], checks[arm] = result(data, arm, truth, scores=scores)
        if arm == "mean":
            prefix = np.asarray(rows[arm]["top100_gallery_rows"], np.int32)
            ranked = context_ranking(data, prefix, scores)
            records["context"], rows["context"], checks["context"] = result(
                data, "context", truth, prefix=ranked, mean_summary=records[arm])
        del scores
    comparisons = {}
    for arm, values in rows.items():
        contrasts = {"vs_previous_licensed": data["parent"] / f"{arm}_rows.json",
                     "vs_full_third": data["source"] / f"{arm}_rows.json"}
        comparisons[arm] = {}
        for label, source in contrasts.items():
            before = json.loads(source.read_text())
            if before["query_ids"] != q.id.tolist():
                raise RuntimeError("licensed third comparator query order changed")
            comparisons[arm][label] = licensed.paired_group_bootstrap(before["errors_m"], values["errors_m"], q.h3_coarse.tolist())
        for label in ("top1", "geo"):
            comparisons[arm][f"vs_actual_production_raw_{label}"] = licensed.paired_group_bootstrap(
                data["production"][f"raw_{label}_errors_m"], values["errors_m"], q.h3_coarse.tolist())
    save(out / "report.json", {"setup": SETUP, "records": records, "paired_comparisons": comparisons,
        "by_source": g.source.value_counts().to_dict(), "all_queries_retained": len(q),
        "main_leaderboard_modified": False, "source_compatible": True, "production_approval": False,
        "limitation": "Reused open development only; source compatibility does not certify deployment rights or runtime. "
            "No query/reference image encoding, new model family, source sweep, confidence fitting or heldout access."})
    save(out / "verification.json", {"checks": checks, "same_ordered1184_queries": True,
        "source_compatible_reference_gate": True, "common_whole_sequence_quarantine": True,
        "literal_score_columns": True, "query_coordinates_only_in_evaluation": True,
        "context_pool_root": str(data["source"]), "context_evidence_root": str(data["parent"])})
    files = [path for path in out.iterdir() if path.suffix in {".json", ".npz", ".parquet"}
        and not path.name.startswith("._") and path.name not in {"status.json", "failure.json", "done.json"}]
    save(out / "done.json", {"completed": time.time(), "contract": data["contract"],
        "artifacts": {path.name: digest(path) for path in files}})
    verify(data)
    state(out, "licensed_third_completed", primary_raw100=records["context"]["raw"]["accuracy_100m"], references=len(g))


def verify(data):
    licensed.verify(data)


def run(wait=False):
    os.nice(max(0, 10 - os.getpriority(os.PRIO_PROCESS, 0)))
    torch.set_num_threads(1)
    cfg, _, _, _, night, out = stage()
    out.mkdir(exist_ok=True)
    with (evaluator.LOCAL / "licensed_third.lock").open("a") as own, threadpool_limits(limits=1):
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        prepare()
        sources = register_implementation(out)
        try:
            if (out / "closed.json").exists():
                return
            while not (night / "reference_tranche3/evaluation.done.json").exists():
                assert_sources(sources)
                if any((night / "reference_tranche3" / name).exists() for name in (
                    "failure.json", "evaluation.failure.json", "evaluation_queue/failure.json")):
                    raise RuntimeError("third parent failed; explicit review required")
                if not evaluator.allowed_to_start(cfg, PHASE, out):
                    save(out / "closed.json", {"reason": "06:00 cutoff before prerequisite completed"})
                    state(out, "licensed_third_closed_cutoff")
                    return
                if not wait:
                    raise RuntimeError("finish full third pipeline before the licensed transfer")
                state(out, "licensed_third_waiting_parent")
                time.sleep(30)
            assert_sources(sources)
            data = load()
            marker = out / f"{PHASE}.started.json"
            if marker.exists():
                if json.loads(marker.read_text())["contract"] != data["contract"]:
                    raise RuntimeError("licensed third resumed contract changed")
            else:
                if not evaluator.allowed_to_start(cfg, PHASE, out):
                    save(out / "closed.json", {"reason": "06:00 cutoff after validation before compute"})
                    state(out, "licensed_third_closed_cutoff")
                    return
                save(marker, {"started": time.time(), "contract": data["contract"], "cpu_only": True})
            evaluate(data)
            assert_sources(sources)
        except BaseException as exc:
            save(out / "failure.json", {"failed": time.time(), "pid": os.getpid(), "error": repr(exc)})
            state(out, "licensed_third_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--wait", action="store_true")
    options = parser.parse_args()
    prepare() if options.prepare else run(wait=options.wait)
