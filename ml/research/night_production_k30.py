"""Verify actual production K30 search against the completed full-K audit."""
from __future__ import annotations

import fcntl
import json
import os
import time
from pathlib import Path

import faiss
import numpy as np
from threadpoolctl import threadpool_limits

from ml.research import night_production_baseline as baseline
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.metrics import paired_group_bootstrap, raw_metrics
from ml.research.night_v7 import LOCAL, paths, save_npz
from ml.research.vector_evaluation import distances


def compare_orders(actual, full, scores, full_scores):
    if (actual.shape != full.shape or actual.shape != scores.shape or scores.shape != full_scores.shape
            or actual.ndim != 2 or actual.shape[1] != 30 or not np.isfinite(scores).all()
            or not np.isfinite(full_scores).all()):
        raise RuntimeError("K30 parity inputs must contain aligned finite30-reference rows")
    same = np.all(actual == full, axis=1)
    return {"identical_order_rows": int(same.sum()), "different_order_rows": int((~same).sum()),
            "different_top1_rows": int((actual[:, 0] != full[:, 0]).sum()),
            "same_members_different_order_rows": sum(not equal and set(a) == set(b)
                for equal, a, b in zip(same, actual, full, strict=True)),
            "max_score_difference_same_order": float(np.abs(scores[same] - full_scores[same]).max()) if same.any() else 0.}


def compute(data, out, contract):
    queries, qvectors, gallery, metadata, index_root, _, _, candidate, _ = data
    directory = out / "k30_chunks"
    directory.mkdir(exist_ok=True)
    configuration = baseline.policy(contract["full_audit_contract"]["configuration"]["localization"])
    ids = gallery.id.to_numpy()
    index, blocks, hashes = None, [], {}
    try:
        for start in range(0, len(queries), 16):
            batch = queries.iloc[start:start + 16]
            path = directory / f"queries-{start:04d}.npz"
            receipt = path.with_suffix(".json")
            expected = {"contract_sha256": baseline.signature(contract), "query_ids": batch.id.tolist()}
            if receipt.exists():
                saved = json.loads(receipt.read_text())
                if saved["inputs"] != expected or digest(path) != saved["sha256"]:
                    raise RuntimeError("committed K30 replay changed")
                with np.load(path, allow_pickle=False) as cached:
                    block = {key: cached[key] for key in cached.files}
            else:
                if index is None:
                    index = faiss.read_index(str(index_root / "index.faiss"))
                    if not isinstance(index, faiss.IndexFlatIP) or index.ntotal != len(gallery) or index.d != 8448:
                        raise RuntimeError("production K30 index differs")
                score, order = index.search(np.ascontiguousarray(qvectors[start:start + len(batch)]), 30)
                if (order < 0).any() or (order >= len(gallery)).any() or any(len(set(row)) != 30 for row in order):
                    raise RuntimeError("production K30 returned invalid reference identities")
                predictions, errors, top1_errors = [], [], []
                for i, query in enumerate(batch.itertuples()):
                    prediction = baseline.geographic_prediction(order[i], score[i], metadata, configuration)
                    predictions.append(prediction)
                    errors.append(float(distances(query.lat, query.lon, *np.radians(prediction))))
                    row = gallery.iloc[int(order[i, 0])]
                    top1_errors.append(float(distances(query.lat, query.lon, *np.radians([row.lat, row.lon]))))
                block = {"order": order, "scores": score, "geo": np.asarray(predictions),
                         "geo_error": np.asarray(errors), "top1_error": np.asarray(top1_errors)}
                save_npz(path, **block)
                save(receipt, {"inputs": expected, "sha256": digest(path)})
            blocks.append(block)
            hashes[path.name] = {"npz_sha256": digest(path), "receipt_sha256": digest(receipt)}
            baseline.status(out, "production_K30_replay", completed=start + len(batch), total=len(queries))
    finally:
        del index
    joined = {key: np.concatenate([block[key] for block in blocks]) for key in blocks[0]}
    full_order, full_scores = [], []
    done = json.loads((out / "done.json").read_text())
    for start in range(0, len(queries), 16):
        name = f"queries-{start:04d}.npz"
        path = out / "chunks" / name
        receipt = done["chunks"][name]
        if digest(path) != receipt["data_sha256"] or digest(path.with_suffix(".json")) != receipt["receipt_sha256"]:
            raise RuntimeError("original full-K evidence changed")
        with np.load(path, allow_pickle=False) as block:
            full_order.append(block["top100"][:, :30])
            full_scores.append(block["scores100"][:, :30])
    parity = compare_orders(joined["order"], np.concatenate(full_order), joined["scores"], np.concatenate(full_scores))
    original = json.loads((out / "rows.json").read_text())
    original_coordinates = np.asarray(original["geo_predictions"])
    parity["changed_geographic_coordinate_rows"] = int(np.any(joined["geo"] != original_coordinates, axis=1).sum())
    parity["max_raw_geo_error_difference_m"] = float(np.abs(joined["geo_error"] - original["raw_geo_errors_m"]).max())
    groups = queries.h3_coarse.astype(str).tolist()
    report = {"kind": "actual_frozen_production_K30_verification", "query_count": len(queries), "gallery_count": len(gallery),
        "search_k": 30, "canonical_production_raw_geo": raw_metrics(joined["geo_error"]),
        "canonical_production_raw_top1": raw_metrics(joined["top1_error"]), "full_K_parity": parity,
        "paired_candidate_vs_production_geographic": paired_group_bootstrap(joined["geo_error"], candidate["errors_m"], groups),
        "paired_candidate_vs_production_top1": paired_group_bootstrap(joined["top1_error"], candidate["errors_m"], groups),
        "candidate": baseline.COMPARATOR, "all_queries_retained": True, "confidence_filtering_or_fitting": False,
        "calibration_final_access": False, "production_mutations": False, "night_leaderboard_modified": False,
        "scope": "This actual-K30 geographic replay is canonical; completed full-K retrieval/rank evidence is preserved separately.",
        "limitations": ["raw coordinates before production acceptance; conditional operating point not evaluated",
            "development-selected research-only candidate; no independent final or production-promotion claim"]}
    rows = {"query_ids": queries.id.tolist(), "reference_ids": ids[joined["order"]].tolist(),
            "scores": joined["scores"].tolist(), "geo_predictions": joined["geo"].tolist(),
            "raw_geo_errors_m": joined["geo_error"].tolist(), "raw_top1_errors_m": joined["top1_error"].tolist()}
    save(out / "k30_rows.json", rows)
    save(out / "k30_report.json", report)
    save(out / "k30.done.json", {"completed": time.time(), "contract": contract, "chunks": hashes,
        "rows_sha256": digest(out / "k30_rows.json"), "report_sha256": digest(out / "k30_report.json")})
    return report


def run():
    out = paths()[-1] / "production_baseline"
    if os.getpriority(os.PRIO_PROCESS, 0) < 10:
        os.nice(10 - os.getpriority(os.PRIO_PROCESS, 0))
    data = None
    with (LOCAL / "production_baseline.lock").open("a") as lock, threadpool_limits(limits=2):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        faiss.omp_set_num_threads(2)
        try:
            done = json.loads((out / "done.json").read_text())
            verification = json.loads((out / "verification.json").read_text())
            if (verification["done_sha256"] != digest(out / "done.json") or verification["production_files_unchanged"] != 42
                    or digest(out / "rows.json") != done["rows_sha256"] or digest(out / "report.json") != done["report_sha256"]):
                raise RuntimeError("complete verified full-K audit required before actual K30 check")
            data = baseline.load(out)
            if data[-1] != done["contract"]:
                raise RuntimeError("production K30 and full-K input/source contracts differ")
            contract = {"full_audit_contract": data[-1], "done_sha256": digest(out / "done.json"),
                        "verification_sha256": digest(out / "verification.json"), "source_sha256": digest(Path(__file__)),
                        "search_k": 30, "same_cpu_threads": 2}
            marker = out / "k30.started.json"
            if marker.exists() and json.loads(marker.read_text())["contract"] != contract:
                raise RuntimeError("registered K30 parity source/input changed")
            if not marker.exists():
                save(marker, {"started": time.time(), "contract": contract})
            if (out / "k30.done.json").exists():
                saved = json.loads((out / "k30.done.json").read_text())
                if (saved["contract"] != contract or digest(out / "k30_report.json") != saved["report_sha256"]
                        or digest(out / "k30_rows.json") != saved["rows_sha256"]):
                    raise RuntimeError("completed K30 report changed")
                for name, value in saved["chunks"].items():
                    path = out / "k30_chunks" / name
                    if digest(path) != value["npz_sha256"] or digest(path.with_suffix(".json")) != value["receipt_sha256"]:
                        raise RuntimeError("completed K30 evidence changed")
                report = json.loads((out / "k30_report.json").read_text())
            else:
                report = compute(data, out, contract)
            if any(digest(WORKSPACE / name) != sha for name, sha in data[-1]["production_files"].items()):
                raise RuntimeError("production frozen artifacts changed during K30 replay")
            receipt = {"k30_done_sha256": digest(out / "k30.done.json"), "production_files_unchanged": 42,
                       "canonical_rawgeo_report": str(out / "k30_report.json"), "full_K_parity": report["full_K_parity"]}
            target = out / "k30.verification.json"
            if target.exists() and json.loads(target.read_text()) != receipt:
                raise RuntimeError("committed K30 verification changed")
            if not target.exists():
                save(target, receipt)
            baseline.status(out, "production_K30_verified", **report["full_K_parity"])
        except BaseException as exc:
            save(out / "k30.failure.json", {"time": time.time(), "error": repr(exc)})
            raise
        finally:
            if data is not None:
                data[1]._mmap.close()


if __name__ == "__main__":
    run()
