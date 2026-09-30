"""Fixed source-compatible scope assessment of the completed best cached pipeline."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np
import shapely
import torch
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale
from ml.research import night_quarantine_audit as audit
from ml.research import night_quarantine_eval as previous_eval
from ml.research import night_scale_context as context
from ml.research import night_v7 as evaluator
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.metrics import paired_group_bootstrap, raw_metrics
from ml.research.night_gallery_addition import atomic_array
from ml.research.night_place import atomic_npz
from ml.research.night_v7_verify import gallery_truth, reconcile_result, validate_prefix
from ml.research.vector_evaluation import distances

SETUP = {"sources": ["mapillary", "kartaview"], "license": "CC BY-SA 4.0",
         "required_production_compatible": True, "gallery": "common-quarantine expanded2",
         "queries": 1184, "query_views": [322, 504], "query_mean_weight": .5,
         "context_depth": 30, "context_mixing": .5, "cpu_threads": 1,
         "primary": "fixed mean322/504 plus context30/mix0.5",
         "ablation_arms": ["top1", "mean"], "source_or_parameter_sweeps": False,
         "confidence_filtering": False, "new_image_encoding": False,
         "calibration_final_access": False, "production_changes": False,
         "scope": "source-compatible candidate assessment, not full deployment approval"}
PHASE = "licensed_pipeline"


def stage():
    cfg, gc, store, previous, night = evaluator.paths()
    return cfg, gc, store, previous, night, night / "licensed_pipeline"


def state(out, phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), **extra)
    save(out / "status.json", value)
    save(evaluator.LOCAL / "licensed_pipeline_status.json", value)
    print(json.dumps(value), flush=True)


def selected_gallery(frame):
    """One fixed source/license filter; missing provenance fails, never tunes membership."""
    compatible = frame.production_compatible.fillna(False).eq(True)
    allowed = frame.source.isin(SETUP["sources"]) & frame.license.eq(SETUP["license"])
    if (compatible & ~allowed).any():
        raise RuntimeError("production-compatible flag contradicts allowed source/license")
    result = frame[compatible & allowed].reset_index(drop=True).copy()
    required = ["id", "source", "source_image_id", "sequence_id", "license", "attribution", "source_url", "image_path", "file_sha256"]
    if result.empty or result[required].isna().any().any() or any(result[k].astype(str).str.strip().eq("").any() for k in required):
        raise RuntimeError("source-compatible gallery has missing provenance or identity")
    if (result.id.duplicated().any() or len(audit.expansion.identities(result)) != len(result)
            or set(audit.sequence_keys(result)) & audit.QUARANTINE):
        raise RuntimeError("source-compatible references repeat physical identities or quarantined sequences")
    return result


def prepare():
    *_, out = stage()
    out.mkdir(exist_ok=True)
    value = {"setup": SETUP, "source_sha256": digest(Path(__file__)),
             "selection_uses_reference_metadata_only": True,
             "registration_precedes_this_assessment_outcomes": True,
             "preparation_does_not_count_as_started_experiment": True}
    target = out / "intent.json"
    if target.exists():
        if json.loads(target.read_text()) != value:
            raise RuntimeError("fixed source-scope intent changed")
    else:
        save(target, value)


def load():
    cfg, gc, store, previous, night, out = stage()
    _, _, _, parent, _, _, _, full = audit.verify_audited()
    source = parent / "evaluation/expanded2"
    completed = json.loads((source / "done.json").read_text())
    original_contract = completed["contract"]
    previous_eval.verify_done(source, original_contract)
    data = context.load_inputs()
    _, _, _, queries, q322, q504, means, _, _, _, checkpoint, component = data
    if (original_contract["component"] != component
            or original_contract["source_sha256"] != digest(Path(previous_eval.__file__))
            or original_contract["audit_source_sha256"] != digest(Path(audit.__file__))
            or original_contract["audit_sha256"] != digest(parent / "audit.done.json")
            or original_contract["manifests"]["gallery.parquet"] != digest(parent / "gallery.parquet")):
        raise RuntimeError("expanded pipeline input/source identity changed")
    encoded = json.loads((parent / "encode.done.json").read_text())
    if encoded["contract"] != original_contract or digest(parent / "descriptor_pool.json") != encoded["descriptor_pool_sha256"]:
        raise RuntimeError("expanded descriptor pool contract changed")
    if (len(queries) != SETUP["queries"]
            or component["setup"]["context_depth"] != SETUP["context_depth"]
            or component["setup"]["context_mixing"] != SETUP["context_mixing"]):
        raise RuntimeError("fixed development population or context setup changed")
    selected = selected_gallery(full)
    positions = previous_eval.subset_mapping(full, selected)
    audit.development_checks(queries, selected)
    boundary = audit.acquisition.load_aoi_boundary(WORKSPACE / gc["aoi"])
    if not shapely.covers(boundary.geometry, shapely.points(selected.lon, selected.lat)).all():
        raise RuntimeError("source-compatible reference outside exact Moscow AOI")
    production = night / "production_baseline"
    prod_done = json.loads((production / "k30.done.json").read_text())
    prod_verified = json.loads((production / "k30.verification.json").read_text())
    if (digest(production / "k30_rows.json") != prod_done["rows_sha256"]
            or digest(production / "k30_report.json") != prod_done["report_sha256"]
            or prod_verified["k30_done_sha256"] != digest(production / "k30.done.json")
            or prod_verified["production_files_unchanged"] != 42):
        raise RuntimeError("actual production baseline receipt changed")
    prod_rows = json.loads((production / "k30_rows.json").read_text())
    if prod_rows["query_ids"] != queries.id.tolist():
        raise RuntimeError("actual production comparator query order differs")
    files = [out / "intent.json", parent / "audit.done.json", parent / "quarantine.receipt.json",
        parent / "gallery.parquet", parent / "descriptor_pool.json", parent / "encode.done.json",
        source / "done.json", source / "context_evidence.json", source / "context_evidence.npz",
        production / "k30_rows.json", production / "k30_report.json", production / "k30.done.json",
        production / "k30.verification.json", WORKSPACE / gc["aoi"]]
    files += [source / name for name in ("top1_scores.json", "mean_scores.json", "top1_rows.json", "mean_rows.json", "context_rows.json")]
    contract = {"setup": SETUP, "files": {str(p): digest(p) for p in files},
        "source_sha256": {p.name: digest(p) for p in (Path(__file__), Path(audit.__file__), Path(previous_eval.__file__),
            Path(context.__file__), Path(gallery_scale.__file__), Path(evaluator.__file__))},
        "source_contract": original_contract, "query_sha256": gc["query_sha256"],
        "selected_reference_ids_sha256": hashlib.sha256(json.dumps(selected.id.tolist()).encode()).hexdigest(),
        "component": component, "gallery_count": len(selected), "full_gallery_count": len(full)}
    return dict(cfg=cfg, gc=gc, out=out, previous=previous, night=night, parent=parent, source=source,
        q=queries, q322=q322, q504=q504, means=means, checkpoint=checkpoint, full=full, g=selected,
        positions=positions, production=prod_rows, contract=contract)


def context_ranking(data, prefix, scores):
    out, selected, full = data["out"], data["g"], data["full"]
    signature = context.signature(data["contract"])
    expected = {"contract_sha256": signature, "prefix_sha256": hashlib.sha256(prefix.tobytes()).hexdigest()}
    path = out / "context_evidence.npz"
    if path.with_suffix(".json").exists():
        receipt = json.loads(path.with_suffix(".json").read_text())
        if receipt["inputs"] != expected or digest(path) != receipt["sha256"]:
            raise RuntimeError("licensed context checkpoint changed")
        with np.load(path, allow_pickle=False) as saved:
            return saved["ranked"]
    original_path = data["source"] / "context_evidence.npz"
    receipt = json.loads(original_path.with_suffix(".json").read_text())
    if (receipt["inputs"]["contract_sha256"] != context.signature(data["contract"]["source_contract"])
            or digest(original_path) != receipt["sha256"]):
        raise RuntimeError("source context evidence contract differs")
    with np.load(original_path, allow_pickle=False) as stored:
        old_chosen, old_values = stored["chosen"], stored["contextual"]
    chosen = prefix[:, :SETUP["context_depth"]]
    reused = previous_eval.reuse_mask(selected, chosen, full, old_chosen)
    values = np.empty(chosen.shape, np.float32)
    values[reused] = old_values[reused]
    pending = np.flatnonzero(~reused)
    vectors = encoder = None
    if len(pending):
        unique, remap = np.unique(chosen[pending], return_inverse=True)
        remap = remap.reshape(len(pending), chosen.shape[1])
        state(out, "licensed_loading_changed_context", references=len(unique), queries=len(pending), reused=int(reused.sum()))
        vectors = evaluator.load_vectors(selected, unique, data["parent"])
        encoder = torch.nn.TransformerEncoder(torch.nn.TransformerEncoderLayer(
            d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=.1, batch_first=False), 2)
        encoder.load_state_dict(torch.load(data["checkpoint"], map_location="cpu", weights_only=True), strict=True)
        encoder.eval()
        directory = out / "changed_context"
        directory.mkdir(exist_ok=True)
        for offset, i in enumerate(pending):
            target = directory / f"query-{i:04d}.npz"
            marker = target.with_suffix(".json")
            inputs = expected | {"query_id": data["q"].id.iloc[i], "reference_ids": selected.id.iloc[chosen[i]].tolist()}
            if marker.exists():
                saved = json.loads(marker.read_text())
                if saved["inputs"] != inputs or digest(target) != saved["sha256"]:
                    raise RuntimeError("licensed changed-query context checkpoint differs")
                with np.load(target, allow_pickle=False) as cached:
                    values[i], direct = cached["contextual"], cached["direct"]
            else:
                values[i], direct = context.one_query(encoder, data["means"][i], data["q322"][i], data["q504"][i], vectors[remap[offset]])
                atomic_npz(target, contextual=values[i], direct=direct)
                save(marker, {"inputs": inputs, "sha256": digest(target)})
            np.testing.assert_allclose(direct, scores[i, chosen[i]], atol=2e-6, rtol=1e-5)
            if offset % 25 == 0 or offset + 1 == len(pending):
                state(out, "licensed_context_queries", completed=offset + 1, total=len(pending), reused=int(reused.sum()))
    del encoder, vectors
    ranked = context.rank_prefix(prefix, values, np.take_along_axis(scores, chosen, axis=1))
    atomic_npz(path, chosen=chosen, contextual=values, ranked=ranked, reused=reused)
    save(path.with_suffix(".json"), {"inputs": expected, "sha256": digest(path),
         "reused_queries": int(reused.sum()), "computed_queries": len(pending)})
    return ranked


def result(data, arm, truth, scores=None, prefix=None, mean_summary=None):
    q, g, out = data["q"], data["g"], data["out"]
    if scores is not None:
        summary, rows = gallery_scale.summarize(g, q, scores)
        ranks = np.asarray([np.inf if r is None else r for r in rows["positive_ranks"]])
    else:
        validate_prefix(prefix, len(q), len(g))
        errors, positive = [], []
        for i, query in enumerate(q.itertuples()):
            d = distances(query.lat, query.lon, truth["latitude"][prefix[i]], truth["longitude"][prefix[i]])
            errors.append(float(d[0]))
            found = np.flatnonzero(d <= 100)
            positive.append(int(found[0]) + 1 if len(found) else None)
        ranks = np.asarray([np.inf if r is None else r for r in positive])
        covered = truth["positive_count"] > 0
        summary = {"raw": raw_metrics(errors), "gallery": mean_summary["gallery"], "coverage": mean_summary["coverage"],
            "diagnosis": {"no_coverage": int((~covered).sum()), "retrieval_miss_top100": int((covered & ~np.isfinite(ranks)).sum()),
                "wrong_top1_positive_in_top100": int(((ranks > 1) & np.isfinite(ranks)).sum()), "correct_top1": int((ranks == 1).sum())}}
        rows = {"query_ids": q.id.tolist(), "errors_m": errors, "positive_ranks_through100": positive,
                "top100_gallery_rows": prefix.tolist()}
    summary.update(name=f"licensed_{arm}", query_count=len(q), gallery_count=len(g),
        gallery_manifest=str(out / "gallery.parquet"), gallery_sha256=digest(out / "gallery.parquet"),
        query_sha256=data["gc"]["query_sha256"], source_compatible=True, production_approval=False, abstention=False,
        recall_at={str(k): float(np.mean(ranks <= k)) for k in (1, 5, 10, 20, 50, 100)},
        contract_sha256=context.signature(data["contract"]))
    verified = reconcile_result(summary, rows, q, g, truth)
    save(out / f"{arm}.json", summary)
    save(out / f"{arm}_rows.json", rows)
    state(out, "licensed_result", arm=arm, raw100=summary["raw"]["accuracy_100m"])
    return summary, rows, verified


def evaluate(data):
    out, queries, gallery = data["out"], data["q"], data["g"]
    if (out / "done.json").exists():
        verify(data)
        return
    audit.write_view(gallery, out / "gallery.parquet")
    save(out / "mapping.json", {"selected_to_expanded": data["positions"].tolist(),
         "original_count": len(data["full"]), "selected_count": len(gallery),
         "excluded_by_source": data["full"].loc[~data["full"].id.isin(gallery.id), "source"].value_counts().to_dict()})
    truth = gallery_truth(queries, gallery)
    records, rows_by_arm, checks = {}, {}, {}
    for arm in ("top1", "mean"):
        score_path = out / f"{arm}_scores.npy"
        marker = score_path.with_suffix(".json")
        expected = {"contract_sha256": context.signature(data["contract"])}
        if marker.exists():
            receipt = json.loads(marker.read_text())
            if receipt["inputs"] != expected or digest(score_path) != receipt["sha256"]:
                raise RuntimeError("licensed literal score subset changed")
            scores = np.load(score_path, allow_pickle=False)
        else:
            source = np.load(data["source"] / f"{arm}_scores.npy", mmap_mode="r", allow_pickle=False)
            try:
                scores = previous_eval.compose_scores(source, data["positions"])
            finally:
                source._mmap.close()
            atomic_array(score_path, scores)
            save(marker, {"inputs": expected, "sha256": digest(score_path), "literal_expanded_columns": True})
        records[arm], rows_by_arm[arm], checks[arm] = result(data, arm, truth, scores=scores)
        if arm == "mean":
            prefix = np.asarray(rows_by_arm[arm]["top100_gallery_rows"], np.int32)
            ranked = context_ranking(data, prefix, scores)
            records["context"], rows_by_arm["context"], checks["context"] = result(
                data, "context", truth, prefix=ranked, mean_summary=records[arm])
        del scores
    groups = queries.h3_coarse.astype(str).tolist()
    comparisons = {}
    for arm, rows in rows_by_arm.items():
        original_rows = json.loads((data["source"] / f"{arm}_rows.json").read_text())
        if original_rows["query_ids"] != queries.id.tolist():
            raise RuntimeError("expanded source-scope comparator query order differs")
        comparisons[arm] = {"vs_same_expanded_pipeline": paired_group_bootstrap(original_rows["errors_m"], rows["errors_m"], groups),
            "vs_actual_production_raw_top1": paired_group_bootstrap(data["production"]["raw_top1_errors_m"], rows["errors_m"], groups),
            "vs_actual_production_raw_geographic": paired_group_bootstrap(data["production"]["raw_geo_errors_m"], rows["errors_m"], groups)}
    save(out / "report.json", {"setup": SETUP, "records": records, "paired_comparisons": comparisons,
        "actual_production_raw_top1": raw_metrics(data["production"]["raw_top1_errors_m"]),
        "actual_production_raw_geographic": raw_metrics(data["production"]["raw_geo_errors_m"]),
        "by_source": gallery.source.value_counts().to_dict(), "all_queries_retained": len(queries),
        "limitation": "Open-development assessment and source-license compatibility only; checkpoint/deployment rights, "
                      "historical protected-sequence uncertainty and production operating point are not newly certified. "
                      "No heldout evaluation, source sweeps, confidence filtering, model training or production change.",
        "main_leaderboard_modified": False})
    save(out / "verification.json", {"checks": checks, "same_ordered1184_queries": True,
         "source_compatible_reference_gate": True, "common_whole_sequence_quarantine": True,
         "literal_score_columns": True, "query_coordinates_only_in_evaluation": True})
    files = [p for p in out.iterdir() if p.suffix in {".json", ".npz", ".parquet"}
             and p.name not in {"status.json", "failure.json", "done.json"} and not p.name.startswith("._")]
    save(out / "done.json", {"completed": time.time(), "contract": data["contract"],
         "artifacts": {p.name: digest(p) for p in files}})
    verify(data)
    state(out, "licensed_completed", primary_raw100=records["context"]["raw"]["accuracy_100m"], references=len(gallery))


def verify(data):
    out = data["out"]
    done = json.loads((out / "done.json").read_text())
    if done["contract"] != data["contract"]:
        raise RuntimeError("licensed completed input contract changed")
    for name, sha in done["artifacts"].items():
        if Path(name).name != name or digest(out / name) != sha:
            raise RuntimeError("licensed completed artifact changed")
    for arm in ("top1", "mean"):
        receipt = json.loads((out / f"{arm}_scores.json").read_text())
        if digest(out / f"{arm}_scores.npy") != receipt["sha256"]:
            raise RuntimeError("licensed completed score subset changed")


def run(wait=False):
    os.nice(max(0, 10 - os.getpriority(os.PRIO_PROCESS, 0)))
    torch.set_num_threads(1)
    cfg, _, _, _, night, out = stage()
    out.mkdir(exist_ok=True)
    with (evaluator.LOCAL / "licensed_pipeline.lock").open("a") as lock, threadpool_limits(limits=1):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        prepare()
        source_done = night / "live_expansion2_quarantine/evaluation/expanded2/done.json"
        try:
            while not source_done.exists():
                if not evaluator.allowed_to_start(cfg, PHASE, out):
                    save(out / "closed.json", {"reason": "06:00 cutoff before prerequisite completed"})
                    state(out, "licensed_closed_cutoff")
                    return
                if not wait:
                    raise RuntimeError("finish common-quarantine expanded2 pipeline before this assessment")
                state(out, "licensed_waiting_expanded2")
                time.sleep(30)
            data = load()
            marker = out / f"{PHASE}.started.json"
            if marker.exists():
                if json.loads(marker.read_text())["contract"] != data["contract"]:
                    raise RuntimeError("licensed pipeline source/input contract changed")
            else:
                if not evaluator.allowed_to_start(cfg, PHASE, out):
                    save(out / "closed.json", {"reason": "06:00 cutoff after input validation, before actual start"})
                    state(out, "licensed_closed_cutoff")
                    return
                save(marker, {"started": time.time(), "contract": data["contract"], "cpu_only": True})
            evaluate(data)
        except BaseException as exc:
            save(out / "failure.json", {"failed": time.time(), "pid": os.getpid(), "error": repr(exc)})
            state(out, "licensed_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait", action="store_true")
    run(parser.parse_args().wait)
