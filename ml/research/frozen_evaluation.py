"""One candidate, calibration-only threshold, then one paired untouched final.

Assembly, selection and runtime preflight must be completed before this lifecycle
can freeze an architecture. Final reporting has no threshold optimization path.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from ml.research.metrics import paired_group_bootstrap, select_threshold
from ml.research.policy_runtime import summarize_rows
from ml.research.prepare_queries import ROOT
from ml.research.runtime_bundle import copy_verified
from ml.research.seal import (
    SealError,
    _relative,
    _verify_artifacts,
    _verify_registry,
    open_stage,
    open_supplementary_final,
    sha256,
    write_once,
)


def detached_freeze(path, data):
    digest = write_once(path, data)
    # Exclusive detached digest: a later stage cannot silently rewrite a freeze.
    with path.with_suffix(".sha256").open("x") as stream:
        stream.write(digest + "\n")
    return digest


def architecture_freeze(bundle, prepared, selection_path, license_path, deployment_path):
    if (bundle / ".ledger").exists() or (bundle / "architecture_frozen.json").exists():
        raise SealError("architecture selection has already been frozen or calibration opened")
    assembly = json.loads((prepared / "assembly.json").read_text())
    _verify_artifacts(prepared, assembly["artifacts"])
    selection = json.loads(selection_path.read_text())
    licensing = json.loads(license_path.read_text())
    deployment = json.loads(deployment_path.read_text())
    candidate_digest = sha256(prepared / "candidate_architecture.json")
    if (
        selection.get("status") != "selected_on_development_only"
        or selection.get("candidate_policy_sha256") != candidate_digest
    ):
        raise SealError("a concrete development-only selection report is required")
    if not selection.get("search_complete") or not selection.get("confidence_family_frozen"):
        raise SealError("architecture search and confidence family selection must be complete")
    if licensing.get("status") != "production_eligible" or licensing.get("candidate_policy_sha256") != candidate_digest:
        raise SealError("production eligibility must cover the actual selected candidate")
    if deployment.get("status") != "preflight_passed" or deployment.get("candidate_policy_sha256") != candidate_digest:
        raise SealError("actual selected candidate must pass runtime preflight")
    snapshot_path = ROOT / "baseline_snapshot.json"
    for name, digest in json.loads(snapshot_path.read_text())["files"].items():
        if sha256(Path(name)) != digest:
            raise SealError("production baseline has changed")
    for name, digest in assembly["artifacts"].items():
        copy_verified(_relative(prepared, name), bundle / name)
        if sha256(bundle / name) != digest:
            raise SealError("assembled artifact changed during transfer")
    for source, name in (
        (selection_path, "selection.json"),
        (license_path, "licensing.json"),
        (deployment_path, "deployment_preflight.json"),
        (snapshot_path, "baseline_snapshot.json"),
        (Path("configs/moscow_research_v5_protocol.json"), "protocol.json"),
    ):
        copy_verified(source, bundle / name)
    registry_path = bundle.parent / "final_cohorts.json"
    if not registry_path.exists():
        raise SealError("both final cohorts must be sealed and registered before architecture freeze")
    payload = {
        "status": "architecture_frozen_before_calibration",
        "seal_sha256": sha256(bundle / "seal.json"),
        "benchmark_registry_sha256": sha256(registry_path),
        "policies": {
            name: {"path": filename, "sha256": sha256(bundle / filename)}
            for name, filename in (("baseline", "baseline.json"), ("candidate", "candidate_architecture.json"))
        },
        "artifacts": assembly["artifacts"]
        | {
            name: sha256(bundle / name)
            for name in (
                "selection.json",
                "licensing.json",
                "deployment_preflight.json",
                "baseline_snapshot.json",
                "protocol.json",
            )
        },
    }
    _verify_registry(bundle.resolve(), payload)
    detached_freeze(bundle / "architecture_frozen.json", payload)


def run_policy(bundle, policy, queries, destination):
    specification = json.loads(policy.read_text())
    destination.mkdir(parents=True, exist_ok=True)
    common = ["--bundle", str(bundle), "--policy", str(policy), "--queries", str(queries), "--output", str(destination)]
    for name in specification["models"]:
        with (destination / f"encode_{name}.log").open("x") as log:
            subprocess.run(
                [sys.executable, "-B", "-m", "ml.research.policy_runtime", "encode", *common, "--model", name],
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )
    with (destination / "score.log").open("x") as log:
        subprocess.run(
            [sys.executable, "-B", "-m", "ml.research.policy_runtime", "score", *common],
            stdout=log,
            stderr=subprocess.STDOUT,
            check=True,
        )


def calibrate(bundle):
    queries = open_stage(bundle, "calibration")
    architecture = json.loads((bundle / "architecture_frozen.json").read_text())
    for name, entry in architecture["policies"].items():
        print("fixed calibration policy", name, flush=True)
        run_policy(bundle, _relative(bundle, entry["path"]), queries, bundle / "private/evaluation/calibration" / name)
    directory = bundle / "private/evaluation/calibration/candidate"
    rows = json.loads((directory / "rows.json").read_text())
    operating = select_threshold(rows["errors"], rows["scores"], precision=0.9)
    threshold_report = {
        "kind": "calibration_only_numeric_threshold_selection",
        "architecture_sha256": sha256(bundle / "architecture_frozen.json"),
        "query_sha256": sha256(queries),
        "rows_sha256": sha256(directory / "rows.json"),
        "operating": operating,
        "ties": "inseparable",
        "confidence_refitted": False,
    }
    write_once(bundle / "calibration_selection.json", threshold_report)
    candidate = json.loads((bundle / "candidate_architecture.json").read_text())
    candidate["threshold"] = operating["threshold"]
    write_once(bundle / "candidate_policy.json", candidate)
    artifacts = architecture["artifacts"] | {
        name: sha256(bundle / name) for name in ("candidate_policy.json", "calibration_selection.json")
    }
    detached_freeze(
        bundle / "candidate_frozen.json",
        {
            "status": "policy_frozen_before_final",
            "seal_sha256": sha256(bundle / "seal.json"),
            "architecture_sha256": sha256(bundle / "architecture_frozen.json"),
            "benchmark_registry_sha256": architecture["benchmark_registry_sha256"],
            "policies": {
                "baseline": architecture["policies"]["baseline"],
                "candidate": {"path": "candidate_policy.json", "sha256": sha256(bundle / "candidate_policy.json")},
            },
            "artifacts": artifacts,
        },
    )
    print("Calibration completed; numeric threshold and both policies frozen", flush=True)


def promotion_gates(baseline, candidate, paired):
    bp, cp = baseline["product"], candidate["product"]
    return {
        "raw_gain_at_least_5pp": paired["gain_pp"] >= 5,
        "paired_geographic_interval_lower_above_zero": paired["ci95_pp"][0] > 0,
        "catastrophic_rate_not_worse": candidate["raw"]["catastrophic_gt500m_rate"]
        <= baseline["raw"]["catastrophic_gt500m_rate"],
        "nonzero_answers": cp["accepted_count"] > 0,
        "conditional_accuracy_at_least_90pct": cp["conditional_accuracy_100m"] is not None
        and cp["conditional_accuracy_100m"] >= 0.9,
        "answer_rate_not_worse": cp["answer_rate"] >= bp["answer_rate"],
    }


def final(bundle):
    # Claim both registered populations before either one's inference begins.
    queries = open_stage(bundle, "final")
    supplement = bundle.parent / "recent_commons"
    recent_queries = open_supplementary_final(bundle, supplement)
    freeze = json.loads((bundle / "candidate_frozen.json").read_text())
    results = {}
    for cohort, root, manifest in (("primary", bundle, queries), ("supplementary_recent", supplement, recent_queries)):
        policies, rows = {}, {}
        for name, entry in freeze["policies"].items():
            print("fixed paired final inference", cohort, name, flush=True)
            policy = _relative(bundle, entry["path"])
            destination = root / "private/evaluation/final" / name
            run_policy(bundle, policy, manifest, destination)
            rows[name] = json.loads((destination / "rows.json").read_text())
            policies[name] = summarize_rows(rows[name], json.loads(policy.read_text())["threshold"])
        if rows["baseline"]["query_ids"] != rows["candidate"]["query_ids"]:
            raise SealError("paired final query identities differ")
        paired = paired_group_bootstrap(
            rows["baseline"]["errors"], rows["candidate"]["errors"], rows["baseline"]["groups"]
        )
        results[cohort] = {
            "manifest_sha256": sha256(manifest),
            "policies": policies,
            "paired_raw_gain": paired,
            "row_artifacts": {
                name: sha256(root / "private/evaluation/final" / name / "rows.json") for name in policies
            },
            "thresholds": "unchanged primary calibration candidate threshold and original production baseline threshold",
        }
    primary = results["primary"]
    gates = promotion_gates(
        primary["policies"]["baseline"], primary["policies"]["candidate"], primary["paired_raw_gain"]
    )
    write_once(
        bundle.parent / "final_comparison.json",
        {
            "status": "single_paired_final_complete",
            "candidate_freeze_sha256": sha256(bundle / "candidate_frozen.json"),
            "cohorts": results,
            "primary_preregistered_promotion_gates": gates,
            "eligible_for_production_packaging": all(gates.values()),
            "production_changed": False,
            "supplementary_scope": "separate recent-source stress result; no pooling, retuning or alternate selection",
        },
    )
    print("Single paired final comparison completed; production remains intact", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["freeze-architecture", "calibrate", "final"])
    parser.add_argument("--bundle", type=Path, default=ROOT / "prospective")
    for name in ("prepared", "selection", "licensing", "deployment"):
        parser.add_argument("--" + name, type=Path)
    args = parser.parse_args()
    if args.action == "freeze-architecture":
        if any(getattr(args, name) is None for name in ("prepared", "selection", "licensing", "deployment")):
            parser.error("architecture freeze requires prepared assets, selection, licensing and deployment reports")
        architecture_freeze(args.bundle, args.prepared, args.selection, args.licensing, args.deployment)
    elif args.action == "calibrate":
        calibrate(args.bundle)
    else:
        final(args.bundle)
