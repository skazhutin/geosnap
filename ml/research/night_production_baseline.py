"""Audit the actual frozen production baseline on open development only.

No model/image inference, confidence filtering/fitting, or night leaderboard writes.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import time
from pathlib import Path

import faiss
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from ml.localization.models import Candidate
from ml.localization.product_policy import AggregationStrategy, ProductAggregationConfig, aggregate_geographic_modes
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.metrics import paired_group_bootstrap, raw_metrics, retrieval_metrics
from ml.research.night_v7 import LOCAL, paths, save_npz
from ml.research.night_v7_verify import development_identity_checks
from ml.research.vector_evaluation import distances

DEV_SHA = "6bcc0b4a618682e05e15305dc76b9f02af635e73573e06ea2567d6094ad3e7d9"
COMPARATOR = "scale_mean_context_added"
REFERENCE_FIELDS = ("id", "lat", "lon", "source", "source_image_id", "sequence_id", "file_sha256")
MODE_FIELDS = ("source", "sequence_id", "heading", "captured_at", "local_gallery_density_25m",
               "local_gallery_density_50m", "local_gallery_density_100m")


def status(out, phase, **extra):
    value = {"phase": phase, "updated": time.time(), "pid": os.getpid(), **extra}
    save(out / "status.json", value)
    save(LOCAL / "production_baseline_status.json", value)
    print(json.dumps(value), flush=True)


def signature(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def safe_config(path):
    # Never emit, return, or inspect legacy outcome sections embedded elsewhere
    # in this configuration. Only explicit frozen runtime identifiers/settings.
    document = json.loads(path.read_text())
    allowed = {
        "dataset": ("gallery_manifest", "gallery_sha256"),
        "retriever": ("name", "checkpoint_sha256", "preprocessing", "descriptor_dimension", "normalized"),
        "index": ("directory", "index_faiss_sha256", "id_mapping_sha256", "reference_metadata_sha256", "gallery_size", "artifact_generation"),
        "retrieval": ("index_type", "similarity", "top_k", "query_aggregation", "reranker"),
        "localization": ("aggregation", "cluster_radius_m", "max_cluster_diameter_m", "estimator", "score_temperature", "rank_decay_exponent"),
    }
    result = {section: {key: document[section][key] for key in keys} for section, keys in allowed.items()}
    if (result["retriever"]["name"] != "sage-vitb" or result["index"]["gallery_size"] != 20487
            or result["retrieval"]["top_k"] != 30 or result["retrieval"]["query_aggregation"] != "single"
            or result["retrieval"]["reranker"] is not None or result["retrieval"]["index_type"] != "faiss.IndexFlatIP"
            or result["localization"]["estimator"] != "weighted_medoid"):
        raise RuntimeError("unsupported frozen production configuration")
    return result


def policy(settings):
    keys = ("cluster_radius_m", "max_cluster_diameter_m", "score_temperature", "rank_decay_exponent")
    return ProductAggregationConfig(strategy=AggregationStrategy(settings["aggregation"]), **{k: settings[k] for k in keys})


def references(index_root, gallery, config):
    mapping = json.loads((index_root / "id_mapping.json").read_text())
    if ([row["row"] for row in mapping] != list(range(len(gallery)))
            or any(row["artifact_generation"] != config["index"]["artifact_generation"] for row in mapping)):
        raise RuntimeError("production row mapping or generation changed")
    ids = [row["reference_id"] for row in mapping]
    if len(set(ids)) != len(gallery) or set(ids) != set(gallery.id):
        raise RuntimeError("production index and gallery identities differ")
    indexed = gallery.set_index("id").loc[ids].reset_index()
    compact = {}
    with (index_root / "reference_metadata.jsonl").open() as handle:
        for line in handle:
            record = json.loads(line)
            identity, metadata = record["reference_id"], record["metadata"]
            if identity in compact or identity != metadata["id"] or record["artifact_generation"] != config["index"]["artifact_generation"]:
                raise RuntimeError("duplicate or inconsistent production reference metadata")
            compact[identity] = {name: metadata.get(name) for name in (*REFERENCE_FIELDS, *MODE_FIELDS)}
    for row in indexed.itertuples():
        metadata = compact[row.id]
        if any(metadata[key] != getattr(row, key) for key in REFERENCE_FIELDS):
            raise RuntimeError("production indexed coordinates/identity differ from frozen gallery")
        if any(not isinstance(metadata[f"local_gallery_density_{radius}m"], (int, float))
               or metadata[f"local_gallery_density_{radius}m"] < 1 for radius in (25, 50, 100)):
            raise RuntimeError("production density metadata missing")
    if set(compact) != set(ids):
        raise RuntimeError("extra/missing production reference metadata")
    return indexed, [compact[identity] for identity in ids]


def geographic_prediction(order, scores, metadata, configuration, depth=30):
    if len(order) < depth or len(scores) < depth or len(set(map(int, order[:depth]))) != depth:
        raise RuntimeError("frozen geographic policy requires30 distinct ordered references")
    candidates = []
    for rank, (position, score) in enumerate(zip(order[:depth], scores[:depth], strict=True), 1):
        if not 0 <= int(position) < len(metadata):
            raise RuntimeError("production candidate row out of range")
        row = metadata[int(position)]
        candidates.append(Candidate(reference_id=row["id"], lat=float(row["lat"]), lon=float(row["lon"]),
            retrieval_score=float(score), rank=rank, source=row["source"], metadata=row))
    # No true coordinate, query metadata, confidence model, or acceptance gate.
    value = aggregate_geographic_modes(candidates, config=configuration)
    return [float(value.lat), float(value.lon)]


def load(out):
    _, gc, _, _, night = paths()
    snapshot = WORKSPACE / "data/evaluation/moscow_research_v5/baseline_snapshot.json"
    protected = json.loads(snapshot.read_text())["files"]
    if len(protected) != 42:
        raise RuntimeError("expected all42 production frozen hashes")
    observed = {name: digest(WORKSPACE / name) for name in protected}
    if observed != protected:
        raise RuntimeError("frozen production hashes changed")
    config_path = WORKSPACE / "configs/moscow_production_frozen.json"
    config = safe_config(config_path)
    dev = WORKSPACE / gc["query_manifest"]
    if digest(dev) != DEV_SHA:
        raise RuntimeError("only the fixed1184 development manifest is permitted")
    queries = pd.read_parquet(dev, columns=[*REFERENCE_FIELDS, "h3_coarse"])
    root = WORKSPACE / "data/evaluation/moscow_research_v5/development"
    cache = root / "global_sage-vitb_new"
    cached = json.loads((cache / "query_contract.json").read_text())
    expected = cached["contract"]
    retriever = expected["retriever"]
    if (expected["manifest_sha256"] != DEV_SHA or expected["image_sha256"] != queries.file_sha256.tolist()
            or cached["signature"] != signature(expected) or digest(cache / "queries.npy") != cached["descriptors_sha256"]
            or retriever["extra"]["checkpoint_sha256"] != config["retriever"]["checkpoint_sha256"]
            or retriever["preprocessing"] != config["retriever"]["preprocessing"]):
        raise RuntimeError("production query cache fingerprint differs")
    query_vectors = np.load(cache / "queries.npy", mmap_mode="r", allow_pickle=False)
    if query_vectors.shape != (1184, 8448) or query_vectors.dtype != np.float32 or len(queries) != 1184:
        raise RuntimeError("production query population or vectors changed")
    gallery_path = WORKSPACE / config["dataset"]["gallery_manifest"]
    if digest(gallery_path) != config["dataset"]["gallery_sha256"]:
        raise RuntimeError("production gallery changed")
    gallery = pd.read_parquet(gallery_path, columns=REFERENCE_FIELDS)
    checks = development_identity_checks(queries, gallery)
    index_root = WORKSPACE / config["index"]["directory"]
    for filename, field in (("index.faiss", "index_faiss_sha256"), ("id_mapping.json", "id_mapping_sha256"),
                            ("reference_metadata.jsonl", "reference_metadata_sha256")):
        if observed.get(str((index_root / filename).relative_to(WORKSPACE))) != config["index"][field]:
            raise RuntimeError("production snapshot does not verify the configured index")
    gallery, metadata = references(index_root, gallery, config)
    stream_path = root / "exact_faiss_baseline_new/retrieval.json"
    summary_path = root / "exact_faiss_baseline_new/summary.json"
    stream, summary = json.loads(stream_path.read_text()), json.loads(summary_path.read_text())
    if (stream["query_ids"] != queries.id.tolist() or summary["query_manifest_sha256"] != DEV_SHA
            or summary["provenance"]["index_sha256"] != config["index"]["index_faiss_sha256"]):
        raise RuntimeError("cached exact production retrieval uses another population/index")
    candidate_rows_path = night / f"results/{COMPARATOR}_rows.json"
    candidate_summary_path = night / f"results/{COMPARATOR}.json"
    candidate_rows, candidate_summary = json.loads(candidate_rows_path.read_text()), json.loads(candidate_summary_path.read_text())
    if candidate_rows["query_ids"] != queries.id.tolist() or raw_metrics(candidate_rows["errors_m"]) != candidate_summary["raw"]:
        raise RuntimeError("current development comparator identities or metrics changed")
    source_paths = [Path(__file__), *[WORKSPACE / "ml/localization" / f"{name}.py"
                    for name in ("product_policy", "product_runtime", "clustering", "estimators", "geo", "models")],
                    WORKSPACE / "ml/research/metrics.py", WORKSPACE / "ml/research/vector_evaluation.py"]
    contract = {"configuration": config, "configuration_sha256": digest(config_path), "query_sha256": DEV_SHA,
        "query_cache_sha256": cached["descriptors_sha256"], "query_contract_sha256": digest(cache / "query_contract.json"),
        "production_snapshot_sha256": digest(snapshot), "production_files": observed,
        "gallery_sha256": config["dataset"]["gallery_sha256"], "cached_stream_sha256": digest(stream_path),
        "cached_summary_sha256": digest(summary_path), "comparator": COMPARATOR,
        "comparator_rows_sha256": digest(candidate_rows_path), "comparator_summary_sha256": digest(candidate_summary_path),
        "source_sha256": {str(p.relative_to(WORKSPACE)): digest(p) for p in source_paths},
        "faiss_version": faiss.__version__, "cpu_threads": 2, "search_batch": 16,
        "index_search_k": len(gallery), "prediction_top_k": 30, "query_gallery_identity_checks": checks,
        "confidence_filtering_or_fitting": False, "calibration_final_access": False}
    return queries, query_vectors, gallery, metadata, index_root, stream["methods"]["sage"], summary["metrics"]["sage"], candidate_rows, contract


def compute(data, out):
    queries, qvectors, gallery, metadata, index_root, cached, cached_summary, candidate, contract = data
    directory = out / "chunks"
    directory.mkdir(exist_ok=True)
    config = policy(contract["configuration"]["localization"])
    latitude, longitude = np.radians(gallery.lat.to_numpy(float)), np.radians(gallery.lon.to_numpy(float))
    ids = gallery.id.to_numpy()
    index, blocks, hashes = None, [], {}
    try:
        for start in range(0, len(queries), 16):
            batch = queries.iloc[start:start + 16]
            path = directory / f"queries-{start:04d}.npz"
            receipt = path.with_suffix(".json")
            expected = {"contract_sha256": signature(contract), "query_ids": batch.id.tolist()}
            if receipt.exists():
                saved = json.loads(receipt.read_text())
                if saved["inputs"] != expected or saved["sha256"] != digest(path):
                    raise RuntimeError("production replay chunk changed")
                with np.load(path, allow_pickle=False) as chunk:
                    values = {key: chunk[key] for key in chunk.files}
            else:
                if index is None:
                    index = faiss.read_index(str(index_root / "index.faiss"))
                    if not isinstance(index, faiss.IndexFlatIP) or index.ntotal != len(gallery) or index.d != 8448:
                        raise RuntimeError("actual production index type/population changed")
                vectors = np.ascontiguousarray(qvectors[start:start + len(batch)])
                if not np.isfinite(vectors).all() or not np.allclose(np.linalg.norm(vectors, axis=1), 1, atol=1e-5):
                    raise RuntimeError("invalid production query unit descriptors")
                similarities, order = index.search(vectors, len(gallery))
                records = {name: [] for name in ("top100", "scores100", "rank", "error", "geo", "geo_error", "positive_count", "nearest")}
                for offset, query in enumerate(batch.itertuples()):
                    position = start + offset
                    current, score = order[offset], similarities[offset]
                    prefix = current[:100]
                    if not np.array_equal(ids[prefix], cached["top100"][position]["ids"]):
                        raise RuntimeError("production FAISS top100 differs from cached exact stream")
                    np.testing.assert_allclose(score[:100], cached["top100"][position]["scores"], atol=2e-6, rtol=0)
                    d = distances(query.lat, query.lon, latitude, longitude)
                    positives = np.flatnonzero(d[current] <= 100)
                    rank = int(positives[0] + 1) if len(positives) else 0
                    if (rank or None) != cached["ranks"][position] or not np.isclose(d[current[0]], cached["errors"][position], atol=1e-7, rtol=0):
                        raise RuntimeError("production cached full positive rank or raw error differs")
                    geo = geographic_prediction(prefix, score[:100], metadata, config)
                    geo_error = distances(query.lat, query.lon, np.radians(geo[0]), np.radians(geo[1]))
                    for key, value in {"top100": prefix, "scores100": score[:100], "rank": rank, "error": float(d[current[0]]),
                                       "geo": geo, "geo_error": float(geo_error), "positive_count": int((d <= 100).sum()), "nearest": float(d.min())}.items():
                        records[key].append(value)
                values = {key: np.asarray(value) for key, value in records.items()}
                save_npz(path, **values)
                save(receipt, {"inputs": expected, "sha256": digest(path)})
            if len(values["rank"]) != len(batch) or values["top100"].shape != (len(batch), 100):
                raise RuntimeError("production replay chunk population changed")
            blocks.append(values)
            hashes[path.name] = {"data_sha256": digest(path), "receipt_sha256": digest(receipt)}
            status(out, "production_exact_replay", completed=start + len(batch), total=len(queries))
    finally:
        del index
    joined = {key: np.concatenate([block[key] for block in blocks]) for key in blocks[0]}
    ranks = [int(value) or None for value in joined["rank"]]
    raw, geo = raw_metrics(joined["error"]), raw_metrics(joined["geo_error"])
    if raw != cached_summary["raw_top1"]:
        raise RuntimeError("production replay raw summary differs from cached exact evaluation")
    retrieval = retrieval_metrics(ranks, gallery_size=len(gallery), positive_counts=joined["positive_count"].tolist())
    groups = queries.h3_coarse.astype(str).tolist()
    result = {"kind": "actual_frozen_production_open_development_baseline", "query_count": len(queries),
        "gallery_count": len(gallery), "query_manifest_sha256": DEV_SHA, "raw_top1": raw, "raw_geographic_policy": geo,
        "retrieval": retrieval, "coverage": {str(m): float(np.mean(joined["nearest"] <= m)) for m in (25, 50, 100)},
        "paired_candidate_vs_production_top1": paired_group_bootstrap(joined["error"], candidate["errors_m"], groups),
        "paired_candidate_vs_production_geographic": paired_group_bootstrap(joined["geo_error"], candidate["errors_m"], groups),
        "candidate": COMPARATOR, "candidate_raw": raw_metrics(candidate["errors_m"]),
        "all_queries_retained": True, "frozen_faiss_top100_and_full_positive_ranks_replayed": True,
        "pure_frozen_geographic_function": True, "confidence_filtering_or_fitting": False,
        "calibration_final_access": False, "night_leaderboard_modified": False,
        "limitations": ["raw geographic coordinates precede the existing production acceptance policy; product operating point is not evaluated",
            "candidate differs in model and gallery and contains research-only MSLS; this does not establish a production-compatible replacement",
            "same open development cohort used to select the candidate; not independent final accuracy",
            "historical production config embeds prior legacy outcomes; earlier exposure disclosed separately, not used in this audit"]}
    rows = {"query_ids": queries.id.tolist(), "top100_reference_ids": ids[joined["top100"]].tolist(),
            "raw_top1_errors_m": joined["error"].tolist(), "raw_geo_errors_m": joined["geo_error"].tolist(),
            "geo_predictions": joined["geo"].tolist(), "positive_ranks": ranks,
            "positive_count_100m": joined["positive_count"].tolist(), "nearest_reference_m": joined["nearest"].tolist()}
    save(out / "rows.json", rows)
    save(out / "report.json", result)
    save(out / "done.json", {"completed": time.time(), "contract": contract, "report_sha256": digest(out / "report.json"),
         "rows_sha256": digest(out / "rows.json"), "chunks": hashes, "production_frozen_files_verified": 42})
    return result


def run():
    out = paths()[-1] / "production_baseline"
    out.mkdir(exist_ok=True)
    if os.getpriority(os.PRIO_PROCESS, 0) < 10:
        os.nice(10 - os.getpriority(os.PRIO_PROCESS, 0))
    with (LOCAL / "production_baseline.lock").open("a") as lock, threadpool_limits(limits=2):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        faiss.omp_set_num_threads(2)
        data = None
        try:
            data = load(out)
            contract = data[-1]
            marker = out / "audit.started.json"
            if marker.exists() and json.loads(marker.read_text())["contract"] != contract:
                raise RuntimeError("production baseline audit inputs changed")
            if not marker.exists():
                save(marker, {"started": time.time(), "contract": contract})
            if (out / "done.json").exists():
                done = json.loads((out / "done.json").read_text())
                if done["contract"] != contract or digest(out / "report.json") != done["report_sha256"] or digest(out / "rows.json") != done["rows_sha256"]:
                    raise RuntimeError("completed production baseline audit changed")
                for name, record in done["chunks"].items():
                    path = out / "chunks" / name
                    if digest(path) != record["data_sha256"] or digest(path.with_suffix(".json")) != record["receipt_sha256"]:
                        raise RuntimeError("completed production replay evidence changed")
                verification = out / "verification.json"
                if verification.exists() and json.loads(verification.read_text())["done_sha256"] != digest(out / "done.json"):
                    raise RuntimeError("production verification points at another completed audit")
                if not verification.exists():
                    save(verification, {"done_sha256": digest(out / "done.json"), "production_files_unchanged": 42,
                         "completed": time.time(), "current_development_identity_checks": contract["query_gallery_identity_checks"]})
                return
            result = compute(data, out)
            # Check42 immutable files after the read-only replay as well.
            if any(digest(WORKSPACE / name) != sha for name, sha in contract["production_files"].items()):
                raise RuntimeError("frozen production changed during read-only replay")
            save(out / "verification.json", {"done_sha256": digest(out / "done.json"), "production_files_unchanged": 42,
                 "completed": time.time(), "current_development_identity_checks": contract["query_gallery_identity_checks"]})
            status(out, "production_baseline_completed", raw_top1=result["raw_top1"], raw_geo=result["raw_geographic_policy"])
        except BaseException as exc:
            save(out / "failure.json", {"time": time.time(), "error": repr(exc)})
            status(out, "production_baseline_failed", error=repr(exc))
            raise
        finally:
            if data is not None:
                data[1]._mmap.close()


if __name__ == "__main__":
    run()
