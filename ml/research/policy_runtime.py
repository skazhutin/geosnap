"""Offline fixed-policy inference, shared by development preflight and sealed runs.

Encoding and exact FAISS search are separate processes to avoid macOS OpenMP
conflicts. This module never opens a sealed stage, trains a model, selects a
threshold, or substitutes a fallback model after an inference failure.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from ml.localization.confidence_model import ConfidenceModel
from ml.localization.geo import haversine_m
from ml.localization.models import Candidate
from ml.localization.product_policy import AggregationStrategy, ProductAggregationConfig, aggregate_geographic_modes
from ml.research.metrics import geographic_intervals, product_metrics, raw_metrics, retrieval_metrics
from ml.research.seal import SealError, _relative, _validate_stage, _verify_registry, sha256, write_once
from ml.research.vector_evaluation import development_frame, distances


def verify_runtime(bundle, policy):
    contract = json.loads(_relative(bundle, policy["runtime_contract"]).read_text())
    if contract["python"] != sys.version:
        raise SealError("Python runtime differs from preflight")
    workspace = Path(__file__).resolve().parents[2]
    if "ml/research/policy_runtime.py" not in contract["workspace_sources"]:
        raise SealError("executing runtime is not bound to the source contract")
    for name, digest in contract["workspace_sources"].items():
        path = _relative(workspace, name)
        if not path.is_file() or sha256(path) != digest:
            raise SealError(f"executing source differs from the frozen runtime: {name}")
    for package, version in contract["dependencies"].items():
        if importlib.metadata.version(package) != version:
            raise SealError(f"runtime dependency differs from preflight: {package}")


def authorize(bundle, policy_path, query_path, output):
    """Allow only registered public development or an already claimed stage."""
    bundle, policy_path, query_path, output = (p.resolve() for p in (bundle, policy_path, query_path, output))
    if not policy_path.is_relative_to(bundle):
        raise SealError("policy must be inside the evaluation bundle")
    # This function checks fixed public hashes before reading development rows.
    try:
        frame = development_frame(query_path)
        public = True
    except ValueError:
        frame, public = None, False
    if not public:
        matched = None
        for cohort in [bundle, bundle.parent / "recent_commons"]:
            seal_path = cohort / "seal.json"
            if not seal_path.is_file():
                continue
            seal = json.loads(seal_path.read_text())
            for stage, entry in seal.get("splits", {}).items():
                if stage not in {"calibration", "final"} or _relative(cohort, entry["path"]) != query_path:
                    continue
                _, frozen, freeze_path, _, _ = _validate_stage(bundle, stage)
                ledger_path = cohort / f".ledger/{stage}.opened.json"
                if not ledger_path.is_file():
                    raise SealError("inference is forbidden before the one-shot stage opening")
                ledger = json.loads(ledger_path.read_text())
                if ledger.get("freeze_sha256") != sha256(freeze_path) or ledger.get("query_sha256") != sha256(
                    query_path
                ):
                    raise SealError("claimed transaction does not match this policy and manifest")
                if cohort != bundle:
                    registry = _verify_registry(bundle, frozen)
                    if (
                        registry is None
                        or _relative(bundle.parent, registry["cohorts"]["supplementary_recent"]["seal_path"])
                        != seal_path
                    ):
                        raise SealError("unregistered supplementary inference")
                    primary_ledger = bundle / ".ledger/final.opened.json"
                    if ledger.get("primary_final_ledger_sha256") != sha256(primary_ledger):
                        raise SealError("supplementary transaction differs from primary")
                policies = frozen.get("policies", {})
                if not any(
                    _relative(bundle, item["path"]) == policy_path and item["sha256"] == sha256(policy_path)
                    for item in policies.values()
                ):
                    raise SealError("only the two concrete frozen policies may infer this stage")
                if not output.is_relative_to(cohort / "private/evaluation" / stage):
                    raise SealError("held-out outputs must stay in their fixed private stage directory")
                matched = cohort, stage
        if matched is None:
            raise SealError("query manifest is neither registered development nor an opened sealed stage")
        frame = pd.read_parquet(query_path)
    policy = json.loads(policy_path.read_text())
    verify_runtime(bundle, policy)
    if frame.id.duplicated().any() or not len(frame):
        raise ValueError("every query identity must occur exactly once")
    output.mkdir(parents=True, exist_ok=True)
    return policy, frame


def encode(bundle, policy_path, query_path, output, model_name):
    policy, frame = authorize(bundle, policy_path, query_path, output)
    specification = policy["models"][model_name]
    # Import torch/retrievers only in the encoding worker, never in FAISS search.
    import torch

    from ml.research.retrievers import research_retriever

    destination = output / model_name
    destination.mkdir(parents=True, exist_ok=True)
    if (destination / "receipt.json").exists():
        raise SealError("completed query encoding is immutable")
    os.environ["GEOSNAP_SAGE_SOURCE_DIR"] = str(_relative(bundle, specification["source"]))
    os.environ["GEOSNAP_SAGE_CHECKPOINT"] = str(_relative(bundle, specification["checkpoint"]))
    os.environ["HF_HUB_OFFLINE"] = "1"
    torch.set_num_threads(2)
    model = research_retriever(model_name, device=policy["device"], allow_device_fallback=False, batch_size=4)
    model.load()  # A broken deployment is fatal; individual query failures remain in the denominator.
    vectors = np.zeros((len(frame), model.descriptor_dim), dtype=np.float32)
    valid = np.zeros(len(frame), dtype=bool)
    failures, times = [], []
    for start in range(0, len(frame), 4):
        positions = []
        for i in range(start, min(start + 4, len(frame))):
            row = frame.iloc[i]
            try:
                if sha256(Path(row.image_path)) != row.file_sha256:
                    raise ValueError("query image hash mismatch")
                positions.append(i)
            except (OSError, ValueError) as exc:
                failures.append({"id": row.id, "reason": str(exc), "phase": "image_integrity"})
        before = time.perf_counter()
        if positions:
            try:
                values = model.embed_batch(frame.iloc[positions].image_path.tolist())
                vectors[positions], valid[positions] = values, True
            except Exception:
                # Retry each image with exactly the same model. This prevents one
                # invalid image from discarding the other members of its batch.
                for i in positions:
                    try:
                        vectors[i] = model.embed_batch([frame.iloc[i].image_path])[0]
                        valid[i] = True
                    except Exception as exc:
                        failures.append({"id": frame.iloc[i].id, "reason": type(exc).__name__, "phase": "encoder"})
        times.append(time.perf_counter() - before)
        if start % 120 == 0:
            print("fixed-policy encoded queries", model_name, start, "/", len(frame), flush=True)
    model.close()
    np.save(destination / "queries.npy", vectors, allow_pickle=False)
    np.save(destination / "valid.npy", valid, allow_pickle=False)
    write_once(
        destination / "receipt.json",
        {
            "policy_sha256": sha256(policy_path),
            "manifest_sha256": sha256(query_path),
            "model": model_name,
            "query_ids": frame.id.tolist(),
            "failures": failures,
            "vectors_sha256": sha256(destination / "queries.npy"),
            "valid_sha256": sha256(destination / "valid.npy"),
            "batch_seconds": times,
            "device": policy["device"],
        },
    )


def load_queries(directory, frame, manifest_hash, policy_hash):
    receipt = json.loads((directory / "receipt.json").read_text())
    if (receipt["manifest_sha256"], receipt["policy_sha256"], receipt["query_ids"]) != (
        manifest_hash,
        policy_hash,
        frame.id.tolist(),
    ):
        raise SealError("query descriptors are not bound to this exact transaction")
    for filename, key in (("queries.npy", "vectors_sha256"), ("valid.npy", "valid_sha256")):
        if sha256(directory / filename) != receipt[key]:
            raise SealError("query descriptor artifact changed")
    return np.load(directory / "queries.npy", allow_pickle=False), np.load(directory / "valid.npy", allow_pickle=False)


def summarize_rows(rows, threshold):
    errors = rows["errors"]
    accepted = [
        e is not None and threshold is not None and s >= threshold for e, s in zip(errors, rows["scores"], strict=True)
    ]
    return {
        "raw": raw_metrics(errors),
        "retrieval": retrieval_metrics(
            rows["ranks"], gallery_size=rows["gallery_size"], positive_counts=rows["positive_counts"]
        ),
        "product": product_metrics(errors, accepted),
        "intervals": geographic_intervals(errors, accepted, rows["groups"]),
        "threshold": threshold,
        "threshold_optimized_on_this_set": False,
    }


def score(bundle, policy_path, query_path, output):
    policy, frame = authorize(bundle, policy_path, query_path, output)
    from ml.evaluation.product_recovery import _gallery_metadata_and_density

    gallery = _gallery_metadata_and_density(_relative(bundle, policy["gallery"]))
    ids = list(gallery)
    lookup = {identity: i for i, identity in enumerate(ids)}
    latitude = np.radians([gallery[i]["lat"] for i in ids])
    longitude = np.radians([gallery[i]["lon"] for i in ids])
    valid = np.ones(len(frame), dtype=bool)
    combined = np.zeros((len(frame), len(ids)), dtype=np.float32)
    context_queries, context_references = None, None
    for name, specification in policy["models"].items():
        queries, available = load_queries(output / name, frame, sha256(query_path), sha256(policy_path))
        valid &= available
        if policy["search"] == "exact_faiss_production":
            import faiss

            faiss.omp_set_num_threads(2)
            index = faiss.read_index(str(_relative(bundle, policy["index"])))
            mapping = json.loads(_relative(bundle, policy["index_ids"]).read_text())
            index_ids = [r["reference_id"] for r in mapping]
            if len(policy["models"]) != 1 or set(index_ids) != set(ids):
                raise ValueError("production FAISS must use its exact original gallery and one model")
            # Retain the actual FAISS order, including its tie behavior.
            index_positions = np.asarray([lookup[i] for i in index_ids])
            order = np.empty((len(frame), len(ids)), dtype=np.int64)
            for start in range(0, len(frame), 16):
                scores, positions = index.search(np.ascontiguousarray(queries[start : start + 16]), len(ids))
                order[start : start + len(scores)] = index_positions[positions]
                np.put_along_axis(combined[start : start + len(scores)], index_positions[positions], scores, axis=1)
        elif policy["search"] == "exact_cosine_fusion":
            reference_ids = json.loads(_relative(bundle, specification["descriptor_ids"]).read_text())
            if reference_ids != ids:
                raise ValueError("frozen descriptor rows must exactly match gallery order")
            vectors = np.load(_relative(bundle, specification["descriptors"]), mmap_mode="r", allow_pickle=False)
            if vectors.shape != (len(ids), queries.shape[1]):
                raise ValueError("descriptor dimensions differ")
            with threadpool_limits(limits=2):
                combined += np.float32(specification["weight"]) * (queries @ vectors.T)
            if policy.get("context", {}).get("descriptor_model") == name:
                context_queries, context_references = queries, vectors
        else:
            raise ValueError("unsupported frozen search backend")
    before_context = None
    if policy["search"] == "exact_cosine_fusion":
        if policy.get("context"):
            from ml.research.context_runtime import apply_context

            if context_queries is None:
                raise ValueError("context descriptor model is absent from the frozen retrievers")
            spec = policy["context"]
            if policy.get("confidence_features") == "base_and_context":
                before_context = combined.copy()
            # Failed query descriptors cannot enter contextual attention. They
            # remain failed for both raw and product metrics without fallback.
            combined[valid] = apply_context(
                combined[valid],
                context_queries[valid],
                context_references,
                checkpoint=_relative(bundle, spec["checkpoint"]),
                depth=spec["depth"],
                mixing=spec["mixing"],
            )
        order = np.argsort(-combined, axis=1, kind="stable")
    if not np.isfinite(combined).all():
        raise ValueError("search produced nonfinite similarities")
    confidence = json.loads(_relative(bundle, policy["confidence"]).read_text())
    if policy["confidence_kind"] == "production_logistic":
        estimator = ConfidenceModel.from_dict(confidence.get("model", confidence))

        def probability(features):
            return estimator.predict_proba(features)
    elif policy["confidence_kind"] == "portable_development":
        from ml.research.confidence_experiment import predict_portable

        def probability(features):
            return float(predict_portable(confidence, [features])[0])
    else:
        raise ValueError("unsupported frozen confidence model")
    rows = {
        key: []
        for key in (
            "errors",
            "ranks",
            "positive_counts",
            "nearest_reference_m",
            "predictions",
            "features",
            "scores",
            "top100",
        )
    }
    rows.update(query_ids=frame.id.tolist(), groups=frame.h3_coarse.astype(str).tolist(), gallery_size=len(ids))
    for i, row in enumerate(frame.itertuples()):
        distance = distances(row.lat, row.lon, latitude, longitude)
        rows["positive_counts"].append(int((distance <= 100).sum()))
        rows["nearest_reference_m"].append(float(distance.min()))
        if not valid[i]:
            for key in ("errors", "ranks", "predictions"):
                rows[key].append(None)
            rows["features"].append({})
            rows["scores"].append(0.0)
            rows["top100"].append({"ids": [], "scores": []})
            continue
        ranked = order[i]
        positive = np.flatnonzero(distance[ranked] <= 100)
        rows["ranks"].append(int(positive[0]) + 1 if len(positive) else None)
        candidates = [
            Candidate(
                reference_id=ids[j],
                lat=float(gallery[ids[j]]["lat"]),
                lon=float(gallery[ids[j]]["lon"]),
                retrieval_score=float(combined[i, j]),
                rank=rank,
                source=str(gallery[ids[j]]["source"]),
                metadata=gallery[ids[j]],
            )
            for rank, j in enumerate(ranked[:30], 1)
        ]
        localized = aggregate_geographic_modes(
            candidates, config=ProductAggregationConfig(strategy=AggregationStrategy.DENSITY_AWARE_MODE_VOTE)
        )
        if policy["coordinates"] == "geographic":
            lat, lon = localized.lat, localized.lon
        elif policy["coordinates"] == "top1":
            lat, lon = candidates[0].lat, candidates[0].lon
        else:
            raise ValueError("unsupported coordinate policy")
        features = dict(localized.features)
        if policy.get("confidence_features") == "base_and_context":
            from ml.research.transfer_confidence import transfer_features

            if before_context is None:
                raise ValueError("transfer confidence requires the original retrieval scores")
            base_order = np.argsort(-before_context[i], kind="stable")
            base_candidates = [
                Candidate(
                    reference_id=ids[j],
                    lat=float(gallery[ids[j]]["lat"]),
                    lon=float(gallery[ids[j]]["lon"]),
                    retrieval_score=float(before_context[i, j]),
                    rank=rank,
                    source=str(gallery[ids[j]]["source"]),
                    metadata=gallery[ids[j]],
                )
                for rank, j in enumerate(base_order[:30], 1)
            ]
            original = aggregate_geographic_modes(
                base_candidates, config=ProductAggregationConfig(strategy=AggregationStrategy.DENSITY_AWARE_MODE_VOTE)
            )
            original_rank = int(np.flatnonzero(base_order == ranked[0])[0]) + 1
            features = transfer_features(
                original.features, features, (base_candidates[0].lat, base_candidates[0].lon), (lat, lon), original_rank
            )
        rows["features"].append(features)
        rows["predictions"].append([lat, lon])
        rows["errors"].append(haversine_m(row.lat, row.lon, lat, lon))
        rows["scores"].append(probability(features))
        rows["top100"].append({"ids": [ids[j] for j in ranked[:100]], "scores": combined[i, ranked[:100]].tolist()})
    write_once(output / "rows.json", rows)
    write_once(
        output / "summary.json",
        {
            "policy_sha256": sha256(policy_path),
            "manifest_sha256": sha256(query_path),
            "rows_sha256": sha256(output / "rows.json"),
            **summarize_rows(rows, policy["threshold"]),
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["encode", "score"])
    for name in ("bundle", "policy", "queries", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--model")
    args = parser.parse_args()
    if args.action == "encode":
        if not args.model:
            parser.error("encoding requires --model")
        encode(args.bundle, args.policy, args.queries, args.output, args.model)
    else:
        score(args.bundle, args.policy, args.queries, args.output)
