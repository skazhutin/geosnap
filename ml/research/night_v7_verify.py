"""Reconcile committed development results and freeze one research candidate."""
from __future__ import annotations

import fcntl
import importlib.metadata
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.metrics import raw_metrics
from ml.research.night_v7 import LOCAL, inputs, report
from ml.research.vector_evaluation import distances


def validate_prefix(values, query_count, gallery_count):
    prefix = np.asarray(values)
    if prefix.shape != (query_count, 100) or not np.issubdtype(prefix.dtype, np.integer):
        raise RuntimeError("top100 must contain integer gallery indices for every development query")
    if (prefix < 0).any() or (prefix >= gallery_count).any():
        raise RuntimeError("top100 gallery index is outside the reference manifest")
    if any(len(set(row)) != 100 for row in prefix):
        raise RuntimeError("top100 contains repeated reference identities")
    return prefix


def gallery_truth(queries, reference):
    """Evaluation-only geometry, computed once per distinct frozen gallery."""
    lat, lon = np.radians(reference.lat.to_numpy(float)), np.radians(reference.lon.to_numpy(float))
    positives, nearest = [], []
    for row in queries.itertuples():
        d = distances(row.lat, row.lon, lat, lon)
        if not np.isfinite(d).all():
            raise RuntimeError("non-finite evaluation coordinates")
        positives.append(np.flatnonzero(d <= 100))
        nearest.append(float(d.min()))
    return {"latitude": lat, "longitude": lon, "positive_indices": positives,
            "positive_count": np.array([len(p) for p in positives]), "nearest_m": nearest}


def rank_values(saved, query_count, gallery_count):
    if len(saved) != query_count or any(
        value is not None and (isinstance(value, bool) or not isinstance(value, (int, np.integer))
                               or not 1 <= value <= gallery_count) for value in saved
    ):
        raise RuntimeError("saved positive ranks must be valid one-based integers or null")
    return np.array([np.inf if value is None else value for value in saved])


def verify_full_ranks(rows, truth, scores):
    """Reconstruct complete-gallery positive ranks with the original stable ties."""
    count = len(truth["positive_indices"])
    if scores.shape != (count, len(truth["latitude"])):
        raise RuntimeError("full-rank score cache shape differs from the frozen population")
    saved = rank_values(rows["positive_ranks"], count, scores.shape[1])
    actual = np.full(count, np.inf)
    for i, positives in enumerate(truth["positive_indices"]):
        score = scores[i]
        if not np.isfinite(score).all():
            raise RuntimeError("full-rank score cache contains invalid values")
        if len(positives):
            # np.flatnonzero gives ascending indices, so argmax chooses the same
            # first tied positive as argsort(kind='stable') in the evaluator.
            best = int(positives[np.argmax(score[positives])])
            actual[i] = 1 + np.count_nonzero(score > score[best]) + np.count_nonzero(score[:best] == score[best])
    if not np.array_equal(saved, actual):
        raise RuntimeError("saved full-gallery positive ranks disagree with exact scores and geometry")
    if "positive_count_100m" in rows and not np.array_equal(rows["positive_count_100m"], truth["positive_count"]):
        raise RuntimeError("saved spatial positive counts disagree with the reference gallery")
    if "nearest_reference_m" in rows:
        np.testing.assert_allclose(rows["nearest_reference_m"], truth["nearest_m"], rtol=0, atol=1e-7)
    return actual


def reconcile_result(result, rows, queries, reference, truth):
    if rows["query_ids"] != queries.id.tolist():
        raise RuntimeError("result query identities or order changed")
    prefix = validate_prefix(rows["top100_gallery_rows"], len(queries), len(reference))
    errors, first = [], []
    for i, row in enumerate(queries.itertuples()):
        d = distances(row.lat, row.lon, truth["latitude"][prefix[i]], truth["longitude"][prefix[i]])
        errors.append(float(d[0]))
        positive = np.flatnonzero(d <= 100)
        first.append(int(positive[0]) + 1 if len(positive) else np.inf)
    first = np.array(first)
    fields = [name for name in ("positive_ranks_through100", "positive_ranks") if name in rows]
    if not fields:
        raise RuntimeError("result lacks saved positive ranks")
    for name in fields:
        saved = rank_values(rows[name], len(queries), 100 if name.endswith("through100") else len(reference))
        if name == "positive_ranks":
            saved = np.where(saved <= 100, saved, np.inf)
        if not np.array_equal(saved, first):
            raise RuntimeError(f"saved {name} disagree with the actual top100 prefix")
    np.testing.assert_allclose(errors, rows["errors_m"], rtol=0, atol=1e-7)
    if raw_metrics(errors) != result["raw"]:
        raise RuntimeError("saved raw metrics disagree with top1 coordinates")
    coverage = truth["positive_count"] > 0
    if (np.isfinite(first) & ~coverage).any():
        raise RuntimeError("retrieved spatial positive without gallery coverage")
    diagnosis = {"no_coverage": int((~coverage).sum()),
                 "retrieval_miss_top100": int((coverage & ~np.isfinite(first)).sum()),
                 "wrong_top1_positive_in_top100": int(((first > 1) & np.isfinite(first)).sum()),
                 "correct_top1": int((first == 1).sum())}
    if result["diagnosis"] != diagnosis:
        raise RuntimeError("saved error decomposition disagrees with coverage and retrieval")
    recall = {str(k): float(np.mean(first <= k)) for k in (1, 5, 10, 20, 50, 100)}
    if result["recall_at"] != recall:
        raise RuntimeError("saved recall disagrees with the actual top100 prefix")
    return {"positive_rank_fields_verified": fields, "diagnosis_verified": diagnosis,
            "recall_verified": recall, "raw_verified": True}


def development_identity_checks(queries, gallery):
    def identities(frame):
        providers = ["mapillary" if r.source == "msls" else r.source for r in frame.itertuples()]
        return ({(source, str(r.source_image_id).removeprefix("msls:")) for source, r in zip(providers, frame.itertuples(), strict=True)},
                {f"{source}::{str(r.sequence_id).removeprefix('msls:')}"
                 for source, r in zip(providers, frame.itertuples(), strict=True)})

    query_ids, query_sequences = identities(queries)
    reference_ids, reference_sequences = identities(gallery)
    counts = {"stable_image_ids": len(set(queries.id) & set(gallery.id)),
              "normalized_provider_image_ids": len(query_ids & reference_ids),
              "normalized_provider_sequences": len(query_sequences & reference_sequences),
              "file_sha256": len(set(queries.file_sha256) & set(gallery.file_sha256))}
    if any(counts.values()):
        raise RuntimeError("frozen gallery intersects current development identities or sequences")
    return counts


def verify_production_audit(out, queries, candidate_rows):
    """Reconcile the completed raw production replay without opening its config."""
    from ml.research.metrics import paired_group_bootstrap

    stage = out / "production_baseline"
    if not (stage / "audit.started.json").exists():
        return None
    for prefix, directory, data_key, receipt_key in (
        ("", "chunks", "data_sha256", "done_sha256"),
        ("k30.", "k30_chunks", "npz_sha256", "k30_done_sha256"),
    ):
        done_path = stage / f"{prefix}done.json"
        done = json.loads(done_path.read_text())
        verified = json.loads((stage / f"{prefix}verification.json").read_text())
        if verified[receipt_key] != digest(done_path) or verified["production_files_unchanged"] != 42:
            raise RuntimeError("production audit completion or protected-file receipt changed")
        stem = "k30_" if prefix else ""
        for filename, key in ((f"{stem}rows.json", "rows_sha256"), (f"{stem}report.json", "report_sha256")):
            if digest(stage / filename) != done[key]:
                raise RuntimeError("completed production audit rows/report changed")
        for name, record in done["chunks"].items():
            path = stage / directory / name
            if digest(path) != record[data_key] or digest(path.with_suffix(".json")) != record["receipt_sha256"]:
                raise RuntimeError("completed production search evidence changed")
    rows = json.loads((stage / "k30_rows.json").read_text())
    report = json.loads((stage / "k30_report.json").read_text())
    if rows["query_ids"] != queries.id.tolist() or candidate_rows["query_ids"] != queries.id.tolist():
        raise RuntimeError("production comparison differs from the fixed development population")
    for kind in ("geo", "top1"):
        if raw_metrics(rows[f"raw_{kind}_errors_m"]) != report[f"canonical_production_raw_{kind}"]:
            raise RuntimeError("production raw metrics differ from committed errors")
    groups = queries.h3_coarse.astype(str).tolist()
    return {"kind": "actual_production_before_confidence_filtering", "query_count": len(queries),
            "gallery_count": report["gallery_count"], "search_k": 30,
            "raw_geographic": report["canonical_production_raw_geo"],
            "raw_top1": report["canonical_production_raw_top1"],
            "selected_vs_production_geographic": paired_group_bootstrap(
                rows["raw_geo_errors_m"], candidate_rows["errors_m"], groups),
            "selected_vs_production_top1": paired_group_bootstrap(
                rows["raw_top1_errors_m"], candidate_rows["errors_m"], groups),
            "operating_point_evaluated": False}


def verify_quarantined_comparison(out, queries):
    """Select only among complete results with the identical new quarantine."""
    from ml.research import night_quarantine_eval as comparison

    data = comparison.load()
    if data["night"] != out or data["q"].id.tolist() != queries.id.tolist():
        raise RuntimeError("quarantine comparison changed the fixed night population")
    stage = data["stage"]
    board = json.loads((stage / "leaderboard.json").read_text())
    evidence = []
    results_by_name = {record["name"]: record for record in board["records"]}
    for key in ("G2", "first", "expanded2"):
        directory = stage / "evaluation" / key
        comparison.verify_done(directory, data["contract"])
        gallery = data["galleries"][key]
        truth = gallery_truth(queries, gallery)
        identity = development_identity_checks(queries, gallery)
        for arm in ("top1", "mean", "context"):
            summary_path, rows_path = directory / f"{arm}.json", directory / f"{arm}_rows.json"
            record = json.loads(summary_path.read_text())
            rows = json.loads(rows_path.read_text())
            if results_by_name.get(record["name"]) != record:
                raise RuntimeError("quarantine leaderboard differs from committed results")
            checked = reconcile_result(record, rows, queries, gallery, truth)
            if arm != "context":
                scores = np.load(directory / f"{arm}_scores.npy", mmap_mode="r", allow_pickle=False)
                try:
                    verify_full_ranks(rows, truth, scores)
                finally:
                    scores._mmap.close()
            evidence.append({"name": record["name"], "rows_sha256": digest(rows_path),
                "summary_sha256": digest(summary_path), "identity": identity, **checked})
    if len(results_by_name) != len(evidence):
        raise RuntimeError("unexpected results in quarantined candidate selection")
    actual = sorted(board["records"], key=lambda r: (-r["raw"]["accuracy_100m"], -r["raw"]["accuracy_50m"],
                    -r["raw"]["accuracy_25m"], r["raw"]["catastrophic_gt500m_rate"]))
    if actual != board["records"]:
        raise RuntimeError("quarantine selection order differs from the fixed rule")
    best = actual[0]
    chosen = next(directory / f"{arm}_rows.json" for key in ("G2", "first", "expanded2")
                  for directory in [stage / "evaluation" / key] for arm in ("top1", "mean", "context")
                  if f"quarantine_{key}_{arm}" == best["name"])
    return best, json.loads(chosen.read_text()), {"results": evidence, "audit_sha256": digest(stage / "audit.done.json"),
        "quarantine_sha256": digest(stage / "quarantine.receipt.json"), "leaderboard_sha256": digest(stage / "leaderboard.json"),
        "scope": "original exploratory results retained; selected candidate obeys common newly-implicated whole-sequence quarantine"}


def verify_licensed_comparison(out, queries, licensed=None):
    if licensed is None:
        from ml.research import night_licensed_pipeline as licensed

    data = licensed.load()
    licensed.verify(data)
    if data["night"] != out or data["q"].id.tolist() != queries.id.tolist():
        raise RuntimeError("licensed comparison changed fixed development population")
    stage, gallery = data["out"], data["g"]
    truth = gallery_truth(queries, gallery)
    identity = development_identity_checks(queries, gallery)
    evidence, records, saved_rows = [], [], {}
    committed = json.loads((stage / "report.json").read_text())["records"]
    for arm in ("top1", "mean", "context"):
        record = json.loads((stage / f"{arm}.json").read_text())
        if committed[arm] != record:
            raise RuntimeError("source-compatible report differs from committed arm")
        rows_path = stage / f"{arm}_rows.json"
        rows = json.loads(rows_path.read_text())
        checked = reconcile_result(record, rows, queries, gallery, truth)
        if arm != "context":
            scores = np.load(stage / f"{arm}_scores.npy", mmap_mode="r", allow_pickle=False)
            try:
                verify_full_ranks(rows, truth, scores)
            finally:
                scores._mmap.close()
        evidence.append({"name": record["name"], "rows_sha256": digest(rows_path), "identity": identity, **checked})
        records.append(record)
        saved_rows[record["name"]] = rows
    records.sort(key=lambda r: (-r["raw"]["accuracy_100m"], -r["raw"]["accuracy_50m"],
                 -r["raw"]["accuracy_25m"], r["raw"]["catastrophic_gt500m_rate"]))
    return records[0], saved_rows[records[0]["name"]], {"records": evidence, "done_sha256": digest(stage / "done.json")}


def selection_key(record):
    raw = record["raw"]
    return (-raw["accuracy_100m"], -raw["accuracy_50m"], -raw["accuracy_25m"], raw["catastrophic_gt500m_rate"])


def verify_third_comparison(out, queries):
    from ml.research import night_tranche3_eval as third

    data = third.load()
    third.verify_done(data)
    if data["night"] != out or data["q"].id.tolist() != queries.id.tolist():
        raise RuntimeError("third comparison changed fixed development population")
    stage, gallery = data["out"], data["gallery"]
    truth = gallery_truth(queries, gallery)
    identity = development_identity_checks(queries, gallery)
    report = json.loads((stage / "report.json").read_text())
    evidence, records, saved_rows = [], [], {}
    for arm in ("top1", "mean", "context"):
        record = json.loads((stage / f"{arm}.json").read_text())
        if report["records"][arm] != record:
            raise RuntimeError("third comparison report differs from committed result")
        rows_path = stage / f"{arm}_rows.json"
        rows = json.loads(rows_path.read_text())
        checked = reconcile_result(record, rows, queries, gallery, truth)
        if arm != "context":
            scores = np.load(stage / f"{arm}_scores.npy", mmap_mode="r", allow_pickle=False)
            try:
                verify_full_ranks(rows, truth, scores)
            finally:
                scores._mmap.close()
        evidence.append({"name": record["name"], "rows_sha256": digest(rows_path), "identity": identity, **checked})
        records.append(record)
        saved_rows[record["name"]] = rows
    best = min(records, key=selection_key)
    return best, saved_rows[best["name"]], {"records": evidence,
        "done_sha256": digest(stage / "evaluation.done.json"), "parent_prefix_unchanged": True}


def run():
    cfg, gc, _, out, q, _, g, base = inputs()
    for marker in out.glob("*.started.json"):
        phase = marker.name.removesuffix(".started.json")
        if not (out / f"{phase}.done.json").exists() and not (out / f"{phase}.closed.json").exists():
            raise RuntimeError(f"finish or explicitly close the started {phase} experiment before freezing")
    place = out / "cpu_place"
    if (place / "place.started.json").exists() and not (place / "published.json").exists():
        raise RuntimeError("finish and publish the parallel place trial before freezing")
    multiview = out / "aux_multiview"
    if (multiview / "multiview.started.json").exists() and not (multiview / "done.json").exists():
        raise RuntimeError("finish the auxiliary multi-photo trial before the final night report")
    collage = out / "aux_multiview_collage"
    if (collage / "collage.started.json").exists() and not (collage / "done.json").exists():
        raise RuntimeError("finish the registered auxiliary collage comparison before freezing")
    license_scope = out / "license_scope"
    if (license_scope / "license_scope.started.json").exists() and not (license_scope / "done.json").exists():
        raise RuntimeError("finish the registered license-defined gallery diagnostic before freezing")
    licensed = out / "licensed_pipeline"
    if (licensed / "intent.json").exists() and not any((licensed / name).exists() for name in ("done.json", "closed.json")):
        raise RuntimeError("finish or explicitly close the registered source-compatible pipeline assessment before freezing")
    licensed_third = out / "licensed_third"
    if (licensed_third / "intent.json").exists() and not any((licensed_third / name).exists() for name in ("done.json", "closed.json")):
        raise RuntimeError("finish or explicitly close the registered third source-compatible assessment before freezing")
    third = out / "reference_tranche3"
    third_queue = third / "evaluation_queue"
    audited_but_cutoff = (third / "audit.done.json").exists() and (third_queue / "closed.json").exists()
    if (third / "queue.started.json").exists() and not any(
        (third / name).exists() for name in ("evaluation.done.json", "evaluation.closed.json", "closed.json")
    ) and not audited_but_cutoff:
        raise RuntimeError("finish or explicitly close the third registered reference tranche before freezing")
    if (third_queue / "started.json").exists() and not any(
        (third_queue / name).exists() for name in ("done.json", "closed.json")
    ):
        raise RuntimeError("finish the third evaluation queue's final verification before freezing")
    fresh_smoke = out / "fresh_runtime_smoke"
    if (fresh_smoke / "intent.json").exists() and not any((fresh_smoke / name).exists() for name in ("done.json", "closed.json")):
        raise RuntimeError("finish or explicitly close the registered fresh inference verification before freezing")
    centering = out / "cpu_centering"
    if (centering / "centering.started.json").exists() and not (centering / "published.json").exists():
        raise RuntimeError("finish and publish the registered centering ablation before freezing")
    density = out / "cpu_density"
    if (density / "density.started.json").exists() and not (density / "published.json").exists():
        raise RuntimeError("finish and publish the registered reference-density correction before freezing")
    scale_context = out / "cpu_scale_context"
    if (scale_context / "scale_context.started.json").exists() and not (scale_context / "published.json").exists():
        raise RuntimeError("finish and publish the registered scale/context interaction before freezing")
    query_crop = out / "query_square_crop"
    if (query_crop / "query_square_crop.started.json").exists() and not (query_crop / "published.json").exists():
        raise RuntimeError("finish and publish the registered query-geometry trial before freezing")
    production_audit = out / "production_baseline"
    if (production_audit / "audit.started.json").exists() and not all(
        (production_audit / name).exists() for name in (
            "done.json", "verification.json", "k30.done.json", "k30.verification.json")
    ):
        raise RuntimeError("finish both full-rank and actual K30 production baseline audits before freezing")
    addition = out / "gallery_addition"
    gallery_directories = {str(addition / "gallery.parquet"): addition,
                           str(out / "live_expansion2/gallery.parquet"): out / "live_expansion2"}
    for directory in (out / "cpu_pipeline_added").glob("*"):
        if (directory / "pipeline.started.json").exists() and not (directory / "published.json").exists():
            raise RuntimeError("finish and publish the started pipeline transfer before freezing")
    acquisition = out / "reference_acquisition"
    if (acquisition / "acquisition.started.json").exists() and not (acquisition / "download.closed.json").exists():
        if not (addition / "published.json").exists() and not (addition / "closed.json").exists():
            raise RuntimeError("finish the bounded reference acquisition and evaluation before freezing")
    expanded = out / "live_expansion2"
    quarantined = out / "live_expansion2_quarantine"
    quarantine_complete = all((quarantined / name).exists() for name in ("baseline.done.json", "expanded.done.json", "expansion3_gate.json"))
    if quarantined.exists() and not quarantine_complete:
        raise RuntimeError("finish the common-quarantine baselines, expanded gallery, and data-growth gate before freezing")
    if quarantine_complete:
        gate = json.loads((quarantined / "expansion3_gate.json").read_text())
        if gate["eligible"] and not (quarantined / "data_growth_decision.json").exists():
            raise RuntimeError("resolve the positive next-tranche growth gate before freezing")
    if (expanded / "live_expansion.started.json").exists():
        if not any((expanded / name).exists() for name in ("published.json", "download.closed.json", "closed.json")):
            raise RuntimeError("finish the second registered reference tranche before freezing")
    if (expanded / "encoder.started.json").exists() and not (expanded / "published.json").exists():
        raise RuntimeError("finish and publish the second tranche encoder/evaluator before freezing")
    queue_status = LOCAL / "live_expansion2_queue_status.json"
    if (queue_status.exists() and json.loads(queue_status.read_text())["phase"] not in (
        "completed", "closed", "closed_failed_network_smoke"
    ) and not (quarantine_complete and (expanded / "closed.json").exists())):
        raise RuntimeError("finish the second tranche queue, including its fixed pipeline transfer, before freezing")
    with (LOCAL / "controller.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        report()
        if (multiview / "done.json").exists():
            from ml.research.night_aux_verify import run as verify_auxiliary
            verify_auxiliary()
        full_smoke = out / "fol_cpu_full_pair_smoke.json"
        if full_smoke.exists():
            smoke = json.loads(full_smoke.read_text())
            assert digest(out / "fol_plan.json") == smoke["plan_sha256"]
            with np.load(out / "fol_pair_evidence.npz", allow_pickle=False) as cached:
                evidence = cached["evidence"]
            for pair in smoke["records"]:
                actual = evidence[pair["query_index"], pair["candidate_position"], :3]
                assert actual[0] == pair["cpu_evidence"][0], "CPU/MPS full mutual-match counts differ"
                np.testing.assert_allclose(actual[1:], pair["cpu_evidence"][1:], atol=2e-5, rtol=1e-5)
            save(out / "fol_cpu_mps_full_pair_verified.json", {"verified": True, "pairs": len(smoke["records"]),
                 "cpu_receipt_sha256": digest(full_smoke), "mps_evidence_sha256": digest(out / "fol_pair_evidence.npz")})
        board = json.loads((out / "leaderboard.json").read_text())
        fixed = json.loads((out / "input_contract.json").read_text())
        base_truth = gallery_truth(q, g)
        identity_checks = {"baseline": development_identity_checks(q, g)}
        if digest(out / "base_scores.npy") != fixed["scores_sha256"]:
            raise RuntimeError("frozen baseline exact scores changed")
        baseline_scores = np.load(out / "base_scores.npy", mmap_mode="r", allow_pickle=False)
        verify_full_ranks(base, base_truth, baseline_scores)
        del baseline_scores
        gallery_cache = {fixed["gallery_sha256"]: (g, base_truth)}
        scope_path = addition / "baseline_identity_scope.json"
        if scope_path.exists():
            scope = json.loads(scope_path.read_text())
            if (scope["baseline_sha256"] != fixed["gallery_sha256"]
                    or scope["current_development_sequence_id_sha_overlap"] != 0
                    or scope.get("baseline_unchanged") is not True):
                raise RuntimeError("baseline identity scope differs from the verified current-development population")
            audit_path = addition / "audit.done.json"
            if audit_path.exists() and json.loads(audit_path.read_text())["baseline_identity_scope_sha256"] != digest(scope_path):
                raise RuntimeError("audited baseline identity-scope receipt changed")
        evidence = []
        for result in board["records"]:
            reference, truth = g, base_truth
            gallery_directory = None
            if result.get("gallery_manifest"):
                manifest = Path(result["gallery_manifest"])
                gallery_directory = gallery_directories.get(str(manifest))
                if gallery_directory is None or digest(manifest) != result["gallery_sha256"]:
                    raise RuntimeError("unexpected or changed expanded gallery manifest")
                if result["gallery_sha256"] not in gallery_cache:
                    reference = pd.read_parquet(manifest)
                    columns = ["id", "file_sha256", "lat", "lon"]
                    pd.testing.assert_frame_equal(reference.iloc[:len(g)][columns].reset_index(drop=True), g[columns].reset_index(drop=True))
                    gallery_cache[result["gallery_sha256"]] = (reference, gallery_truth(q, reference))
                    identity_checks[gallery_directory.name] = development_identity_checks(q, reference)
                reference, truth = gallery_cache[result["gallery_sha256"]]
            path = out / "results" / f"{result['name']}_rows.json"
            rows = json.loads(path.read_text())
            reconciliation = reconcile_result(result, rows, q, reference, truth)
            if "baseline_positive_ranks" in rows and rows["baseline_positive_ranks"] != base["positive_ranks"]:
                raise RuntimeError("result carries changed baseline positive ranks")
            if "positive_ranks" in rows:
                score_path = gallery_directory / "scores.npy" if gallery_directory else out / "base_scores.npy"
                expected = json.loads((gallery_directory / "done.json").read_text())["scores_sha256"] if gallery_directory else fixed["scores_sha256"]
                if digest(score_path) != expected:
                    raise RuntimeError("full-rank exact score cache changed")
                scores = np.load(score_path, mmap_mode="r", allow_pickle=False)
                verify_full_ranks(rows, truth, scores)
                del scores
                reconciliation["full_gallery_positive_ranks_verified"] = True
            summary = path.with_name(result["name"] + ".json")
            if json.loads(summary.read_text()) != result:
                raise RuntimeError("leaderboard differs from its committed result summary")
            evidence.append({"name": result["name"], "rows_sha256": digest(path),
                             "summary_sha256": digest(summary), **reconciliation})
        protected = json.loads((WORKSPACE / "data/evaluation/moscow_research_v5/baseline_snapshot.json").read_text())["files"]
        changed = [p for p, sha in protected.items() if digest(WORKSPACE / p) != sha]
        if changed:
            raise RuntimeError(f"frozen production changed: {changed}")
        best = board["records"][0]
        best_rows = json.loads((out / "results" / f"{best['name']}_rows.json").read_text())
        quarantine_verification = None
        if quarantine_complete:
            best, best_rows, quarantine_verification = verify_quarantined_comparison(out, q)
        licensed_verification = None
        if (licensed / "done.json").exists():
            licensed_best, licensed_rows, licensed_verification = verify_licensed_comparison(out, q)
            if selection_key(licensed_best) < selection_key(best):
                best, best_rows = licensed_best, licensed_rows
        third_verification = None
        if (third / "evaluation.done.json").exists():
            third_best, third_rows, third_verification = verify_third_comparison(out, q)
            if selection_key(third_best) < selection_key(best):
                best, best_rows = third_best, third_rows
        licensed_third_verification = None
        if (licensed_third / "done.json").exists():
            from ml.research import night_licensed_third
            compatible_best, compatible_rows, licensed_third_verification = verify_licensed_comparison(out, q, night_licensed_third)
            if selection_key(compatible_best) < selection_key(best):
                best, best_rows = compatible_best, compatible_rows
        production_comparison = verify_production_audit(out, q, best_rows)
        fresh_verification = None
        if (fresh_smoke / "done.json").exists():
            from ml.research import night_fresh_runtime_smoke
            smoke_data = night_fresh_runtime_smoke.load()
            night_fresh_runtime_smoke.verify_done(smoke_data)
            fresh_verification = json.loads((fresh_smoke / "verification.json").read_text())
        from ml.research.night_family_uncertainty import run as family_uncertainty
        from ml.research.night_gain_diagnosis import STAGES as diagnostic_stages
        from ml.research.night_gain_diagnosis import run as gain_diagnosis
        family_uncertainty()
        if best["name"] in diagnostic_stages:
            gain_diagnosis(best["name"])
        if quarantine_complete:
            from ml.research.night_quarantine_report import run as quarantine_report
            final_evidence = quarantine_report()
            if final_evidence["selected"]["name"] != best["name"]:
                raise RuntimeError("verified candidate and final evidence selection disagree")
        sources = [WORKSPACE / "ml/research" / f for f in ("night_v7.py", "night_fol.py", "night_query_scale.py",
            "night_fol_extend.py", "night_place.py", "night_multiview.py", "night_centering.py", "night_v7_verify.py",
            "night_aux_verify.py", "night_followthrough.py", "night_diagnosis.py", "gallery_scale.py", "gallery_scale_storage.py",
            "night_acquisition.py", "night_acquisition_queue.py", "night_gallery_addition.py", "night_density.py",
            "night_license_scope.py", "night_multiview_collage.py", "night_scale_context.py", "night_pipeline_added.py",
            "night_live_expansion.py", "night_live_expansion_eval.py", "night_live_expansion_queue.py",
            "night_gain_diagnosis.py", "night_family_uncertainty.py",
            "night_query_square_crop.py", "night_query_crop_io_resume.py", "night_expansion_gate.py",
            "night_production_baseline.py", "night_production_k30.py",
            "night_quarantine_audit.py", "night_quarantine_eval.py", "night_quarantine_queue.py",
            "night_quarantine_report.py", "night_licensed_pipeline.py",
            "night_fresh_runtime_smoke.py",
            "night_reference_tranche3.py", "night_tranche3_eval.py", "night_tranche3_queue.py",
            "night_report_artifacts.py", "night_finish_queue.py",
            "night_licensed_third.py",
            "metrics.py", "confidence_experiment.py", "vector_evaluation.py", "retrievers.py")]
        artifacts = [out / "input_contract.json", out / "fol_source_audit.json", out / "fol_smoke.json",
                     out / "fol_plan.json", out / "fol_feature_index.json", out / "fol_ranker.json",
                     out / "fol100.closed.json", out / "fol_family_decision.json",
                     out / "query504_fol_pair_scores.json", out / "query504_fol_pair_scores.npz",
                     full_smoke, out / "fol_cpu_mps_full_pair_verified.json",
                     out / "fol100/plan.json", out / "fol100/fol_ranker.json",
                     place / "place.started.json", place / "groups.json", place / "norms.npz",
                     place / "group_inventory.json", place / "coherence.json", place / "smoke.json",
                     place / "predictions.npz", place / "done.json", multiview / "prepared.json",
                     multiview / "parents.json", multiview / "scores.done.json", multiview / "report.json",
                     multiview / "cohort.json", multiview / "verification.json",
                     centering / "centering.started.json", centering / "reference_projection.npz",
                     centering / "smoke.json", centering / "predictions.npz", centering / "done.json",
                     centering / "runtime_repair.json", centering / "closed.json",
                     acquisition / "acquisition.started.json", acquisition / "prepare.done.json",
                     acquisition / "selection_inventory.json",
                     acquisition / "smoke.done.json", acquisition / "download.done.json", acquisition / "download.closed.json",
                     addition / "gallery_addition.started.json", addition / "audit.done.json", addition / "closed.json",
                     addition / "gallery.parquet", addition / "addition.parquet", addition / "descriptor_pool.json",
                     addition / "done.json", addition / "runtime_repair.json", addition / "source_before_path_repair.py",
                     addition / "identity_incident.json", addition / "baseline_identity_scope.json",
                     addition / "source_before_identity_repair.py",
                     density / "density.started.json", density / "landmarks.json",
                     density / "density.npz", density / "density.done.json", density / "predictions.npz",
                     density / "predictions.checkpoint.json", density / "done.json", density / "closed.json",
                     license_scope / "license_scope.started.json", license_scope / "gallery.parquet",
                     license_scope / "selection.json", license_scope / "report.json", license_scope / "rows.json",
                     license_scope / "verification.json", license_scope / "done.json",
                     collage / "collage.started.json", collage / "scores.done.json", collage / "report.json",
                     collage / "rows.json", collage / "verification.json", collage / "done.json", collage / "closed.json",
                     scale_context / "scale_context.started.json", scale_context / "predictions.npz",
                     scale_context / "done.json", scale_context / "published.json", scale_context / "closed.json",
                     out / "context_scale_complementarity.json", out / "legacy_document_scope_disclosure.json",
                     out / "family_uncertainty.json", out / "legacy_document_scope_disclosure.before_production_config.json",
                     out / "expansion3_gate.plan.json", out / "expansion3_gate.json",
                     out / "expansion3_gate.closed.json",
                     out / "expansion3_gate.plan_before_format.json", out / "expansion3_gate.format_repair.json",
                     out / "expansion3_gate.source_before_format.py"]
        artifacts.extend(p for p in (out / "gain_diagnosis").glob("*.json") if not p.name.startswith("._"))
        artifacts.extend(query_crop / name for name in ("query_square_crop.started.json", "descriptors.done.json",
            "queries.npz", "scores.done.json", "score_chunks.json", "predictions.npz", "gate.json",
            "done.json", "published.json", "closed.json", "io_pause.json", "io_stall_sample.txt",
            "io_runtime_repair.json", "io_runtime_repair.initialized.json", "io_runtime_repair.completed.json",
            "score_chunks.before_io_repair.json"))
        artifacts.extend(production_audit / name for name in ("audit.started.json", "report.json", "rows.json",
            "done.json", "verification.json", "k30.started.json", "k30_report.json", "k30_rows.json",
            "k30.done.json", "k30.verification.json"))
        artifacts.extend(expanded / name for name in ("live_expansion.started.json", "prepare.done.json",
            "selected.parquet", "selection_inventory.json", "smoke.done.json", "download.done.json", "download.closed.json", "closed.json", "identity_incident.json",
            "query_identity_guard.receipt.json", "baseline_identity_scope.json", "audit.done.json",
            "addition.parquet", "gallery.parquet", "encoder.started.json", "encode.done.json",
            "descriptor_pool.json", "new_scores.done.json", "done.json", "published.json"))
        for directory in (out / "cpu_pipeline_added").glob("*"):
            artifacts.extend(directory / name for name in ("pipeline.started.json", "mean_scores.done.json",
                "prefix.npz", "predictions.npz", "done.json", "published.json", "closed.json"))
        if quarantine_complete:
            artifacts.extend(p for p in quarantined.rglob("*.json") if not p.name.startswith("._"))
            artifacts.extend(quarantined / name for name in ("clean_G2.parquet", "clean_first.parquet", "addition.parquet", "gallery.parquet"))
        artifacts.extend(p for p in licensed.glob("*.json") if not p.name.startswith("._"))
        artifacts.extend(licensed / name for name in ("gallery.parquet", "context_evidence.npz"))
        artifacts.extend(p for p in licensed_third.glob("*.json") if not p.name.startswith("._") and not p.name.endswith("status.json"))
        artifacts.extend(licensed_third / name for name in ("gallery.parquet", "context_evidence.npz"))
        artifacts.extend(p for p in fresh_smoke.glob("*") if p.suffix in (".json", ".npz") and not p.name.startswith("._"))
        artifacts.extend((out / "model_gallery_attribution.json", out / "finish_queue/licensed_third_extension.registration.json"))
        artifacts.extend(p for p in third.rglob("*.json") if not p.name.startswith("._") and not p.name.endswith("status.json"))
        artifacts.extend(third / name for name in ("selected.parquet", "local_reuse.parquet", "network_selected.parquet",
            "addition.parquet", "gallery.parquet", "context_evidence.npz"))
        payload = {"selected_at": time.time(), "kind": "development_research_candidate_not_production_release",
            "candidate": best, "config": cfg, "query_sha256": gc["query_sha256"],
            "source_hashes": {str(p.relative_to(WORKSPACE)): digest(p) for p in sources},
            "artifact_hashes": {str(p.relative_to(out)): digest(p) for p in artifacts if p.exists()},
            "results_verified": evidence, "production_frozen_files_verified": len(protected),
            "actual_production_baseline_comparison": production_comparison,
            "common_quarantine_verification": quarantine_verification,
            "source_compatible_pipeline_verification": licensed_verification,
            "third_reference_tranche_verification": third_verification,
            "third_source_compatible_verification": licensed_third_verification,
            "fresh_inference_verification": fresh_verification,
            "baseline_full_gallery_positive_ranks_verified": True,
            "current_development_identity_overlap_counts": identity_checks,
            "calibration_opened_this_night": False, "final_opened_this_night": False,
            "historical_final_population_status": "prior v5 status mentions final claims; historical populations are not certified untouched; see legacy document exposure receipt",
            "runtime_versions": {name: importlib.metadata.version(name) for name in ("torch", "numpy", "scikit-learn", "pandas")},
            "selection_rule": cfg["selection"], "production_compatible": False,
            "selected_gallery_sources_compatible": bool(best.get("source_compatible", False)),
            "limitations": ["1184 reused development queries; model selection uncertainty remains",
                "MSLS-containing gallery is research-only; no production promotion",
                "conditional operating point not optimized; raw improvement cannot arise from abstention",
                "positive ranks measured exactly through100 for every reranked prefix",
                "immutable baseline retains historical v2 protected-sequence overlap; those historical populations are not independent evaluation",
                "legacy audits accidentally exposed adjacent historical summary/calibration prose and embedded historical final_evaluation config values; receipt records scope; current heldout files and frozen-test section were not opened",
                "current development has zero checked stable-ID/provider-ID/sequence/SHA overlaps; unknown provider aliases and partial-image duplicates remain uncertified",
                "OOF metrics, if selected, describe held-out geographic folds rather than full-development training accuracy"]}
        save(out / "candidate_frozen.json", payload)
        save(LOCAL / "candidate_frozen.json", payload)
        save(LOCAL / "verification.json", {"verified": True, "results": len(evidence),
             "production_files": len(protected), "candidate": best["name"], "raw100": best["raw"]["accuracy_100m"]})
        print(json.dumps({"candidate": best["name"], "raw": best["raw"], "results_verified": len(evidence)}))


if __name__ == "__main__":
    run()
