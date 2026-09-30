"""Public-development parity and offline encoder smoke for a staged runtime."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

from ml.research.policy_runtime import verify_runtime
from ml.research.prepare_queries import ROOT
from ml.research.prepare_queries import score as stable_score
from ml.research.runtime_bundle import copy_verified
from ml.research.seal import SealError, _relative, sha256, write_once
from ml.research.vector_evaluation import development_frame


def seed_public_cache(bundle, policy_path, output):
    query_path = ROOT / "prospective/development.parquet"
    frame = development_frame(query_path)
    policy = json.loads(policy_path.read_text())
    verify_runtime(bundle, policy)
    for name, spec in policy["models"].items():
        source = ROOT / f"development/global_{name}_new"
        contract = json.loads((source / "query_contract.json").read_text())
        if contract["contract"]["manifest_sha256"] != sha256(query_path):
            raise SealError("preflight cache is not this public development cohort")
        retriever = contract["contract"]["retriever"]
        if retriever["extra"]["checkpoint_sha256"] != sha256(_relative(bundle, spec["checkpoint"])):
            raise SealError("preflight cache checkpoint differs from offline runtime")
        if contract["descriptors_sha256"] != sha256(source / "queries.npy"):
            raise SealError("development cache changed")
        destination = output / name
        destination.mkdir(parents=True, exist_ok=True)
        copy_verified(source / "queries.npy", destination / "queries.npy")
        np.save(destination / "valid.npy", np.ones(len(frame), dtype=bool), allow_pickle=False)
        write_once(
            destination / "receipt.json",
            {
                "policy_sha256": sha256(policy_path),
                "manifest_sha256": sha256(query_path),
                "query_ids": frame.id.tolist(),
                "model": name,
                "failures": [],
                "vectors_sha256": sha256(destination / "queries.npy"),
                "valid_sha256": sha256(destination / "valid.npy"),
                "kind": "verified_public_development_cache_replay",
                "source_contract_sha256": sha256(source / "query_contract.json"),
            },
        )


def smoke(bundle, device):
    policy_path = bundle / "candidate_architecture.json"
    policy = json.loads(policy_path.read_text())
    verify_runtime(bundle, policy)
    frame = development_frame(ROOT / "prospective/development.parquet")
    positions = sorted(range(len(frame)), key=lambda i: stable_score("offline-runtime-smoke", frame.iloc[i].id))[:8]
    images = frame.iloc[positions].image_path.tolist()
    import torch

    from ml.research.retrievers import research_retriever

    def no_network(*args, **kwargs):
        raise RuntimeError("offline runtime smoke forbids network access")

    original_connect = socket.socket.connect
    socket.socket.connect = no_network
    torch.set_num_threads(2)
    result = {"device": device, "public_development_ids": frame.iloc[positions].id.tolist(), "models": {}}
    try:
        for name, spec in policy["models"].items():
            os.environ["GEOSNAP_SAGE_SOURCE_DIR"] = str(_relative(bundle, spec["source"]))
            os.environ["GEOSNAP_SAGE_CHECKPOINT"] = str(_relative(bundle, spec["checkpoint"]))
            os.environ["HF_HUB_OFFLINE"] = "1"
            model = research_retriever(name, device=device, allow_device_fallback=False, batch_size=4)
            start = time.perf_counter()
            model.load()
            load_seconds = time.perf_counter() - start
            start = time.perf_counter()
            batched = model.embed_batch(images)
            batch_seconds = time.perf_counter() - start
            reversed_batch = model.embed_batch(list(reversed(images)))[::-1]
            singles, seconds = [], []
            for path in images:
                start = time.perf_counter()
                singles.append(model.embed_batch([path])[0])
                seconds.append(time.perf_counter() - start)
            cached = np.load(ROOT / f"development/global_{name}_new/queries.npy", allow_pickle=False)[positions]
            differences = {
                "batch_order": float(np.max(abs(batched - reversed_batch))),
                "batch_vs_single": float(np.max(abs(batched - np.asarray(singles)))),
                "cached_research_descriptor": float(np.max(abs(batched - cached))),
                "cached_research_l2_distance": float(np.linalg.norm(batched - cached, axis=1).max()),
            }
            # CPU and MPS reductions can differ in individual components. Check
            # independent-image invariance within a device separately from the
            # geometric displacement of the complete unit descriptor across devices.
            if (
                differences["batch_order"] > 1e-5
                or differences["batch_vs_single"] > 1e-5
                or differences["cached_research_l2_distance"] > 1e-3
            ):
                raise ValueError("offline encoder differs materially from independent-image research descriptors")
            result["models"][name] = {
                "max_descriptor_difference": differences,
                "load_seconds": load_seconds,
                "eight_image_batch_seconds": batch_seconds,
                "single_image_seconds": seconds,
                "checkpoint_sha256": sha256(_relative(bundle, spec["checkpoint"])),
            }
            model.close()
            if torch.backends.mps.is_available():
                torch.mps.empty_cache()
    finally:
        socket.socket.connect = original_connect
    write_once(bundle / f"offline_smoke_{device}.json", result)


def replay(bundle):
    query_path = ROOT / "prospective/development.parquet"
    evidence = {}
    for name, filename in (("baseline", "baseline.json"), ("candidate", "candidate_architecture.json")):
        policy = bundle / filename
        output = bundle / "development_parity" / name
        seed_public_cache(bundle, policy, output)
        started = time.perf_counter()
        with (output / "score.log").open("x") as log:
            subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "ml.research.policy_runtime",
                    "score",
                    "--bundle",
                    str(bundle),
                    "--policy",
                    str(policy),
                    "--queries",
                    str(query_path),
                    "--output",
                    str(output),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )
        result = json.loads((output / "rows.json").read_text())
        expected_path = ROOT / (
            "development/localized_exact_faiss_baseline_new/sage_rows.json"
            if name == "baseline"
            else (
                "development/hybrid_top1_new/hybrid_context30_mix0.5_rows.json"
                if json.loads(policy.read_text()).get("context")
                else "development/gallery_fusion_top1_new/union_larger_budget/fusion50_rows.json"
            )
        )
        expected = json.loads(expected_path.read_text())
        if result["query_ids"] != expected["query_ids"]:
            raise ValueError("preflight query ordering differs")
        maximum = float(np.max(abs(np.asarray(result["errors"]) - expected["errors"])))
        if maximum > 1e-6 or result["predictions"] != expected["predictions"]:
            raise ValueError("staged runtime changes development coordinates")
        if name == "baseline" and not np.allclose(
            result["scores"], expected["baseline_confidence_scores"], atol=1e-10, rtol=0
        ):
            raise ValueError("staged production confidence changed")
        evidence[name] = {
            "queries": len(result["query_ids"]),
            "coordinate_parity": True,
            "max_error_difference_m": maximum,
            "search_localization_seconds": time.perf_counter() - started,
            "expected_rows_sha256": sha256(expected_path),
            "actual_rows_sha256": sha256(output / "rows.json"),
            "encoding": "verified public development cache; offline fresh-encoding smoke reported separately",
        }
    write_once(bundle / "development_parity.json", evidence)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["replay", "smoke"])
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--device", choices=["mps", "cpu"], default="cpu")
    args = parser.parse_args()
    if args.action == "replay":
        replay(args.bundle)
    else:
        smoke(args.bundle, args.device)
