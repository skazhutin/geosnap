"""Fixed-location, one-shot access to calibration and final query manifests.

This is a procedural guard for cooperating research code, not OS isolation.
No CLI argument can substitute a fresh state directory to reopen the same test.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class SealError(RuntimeError):
    """A benchmark stage is unavailable, already opened, or has changed."""


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_once(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = (json.dumps(payload, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise SealError(f"immutable artifact already exists: {path.name}") from exc
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    return hashlib.sha256(body).hexdigest()


def _relative(root: Path, value: str) -> Path:
    if Path(value).is_absolute():
        raise SealError("sealed paths must be relative to the bundle")
    path = (root / value).resolve()
    if not path.is_relative_to(root.resolve()):
        raise SealError("sealed path escapes the bundle")
    return path


def _verify_artifacts(root: Path, artifacts: dict[str, str]) -> None:
    if not artifacts:
        raise SealError("empty artifact binding")
    for name, expected in artifacts.items():
        path = _relative(root, name)
        if not path.is_file() or sha256(path) != expected:
            raise SealError(f"bound artifact changed or missing: {name}")


def register_final_cohorts(primary: Path, supplementary: Path) -> str:
    """Bind both already sealed populations before any calibration or freeze."""
    primary, supplementary = primary.resolve(), supplementary.resolve()
    parent = primary.parent
    if supplementary.parent != parent or supplementary == primary:
        raise SealError("final cohorts must be distinct sibling bundles")
    for root in (primary, supplementary):
        if any((root / name).exists() for name in ("architecture_frozen.json", "candidate_frozen.json", ".ledger")):
            raise SealError("cohort registration must precede every freeze and opening")
        if json.loads((root / "seal.json").read_text()).get("status") != "sealed_before_inference":
            raise SealError("both cohorts must already be sealed")
    if json.loads((supplementary / "seal.json").read_text()).get("original_prospective_seal_sha256") != sha256(
        primary / "seal.json"
    ):
        raise SealError("supplementary cohort does not bind the original primary seal")
    return write_once(
        parent / "final_cohorts.json",
        {
            "status": "registered_before_architecture_freeze",
            "cohorts": {
                name: {"seal_path": f"{root.name}/seal.json", "seal_sha256": sha256(root / "seal.json")}
                for name, root in (("primary", primary), ("supplementary_recent", supplementary))
            },
            "evaluation": "same baseline and single candidate; primary calibration thresholds transferred unchanged",
            "reporting": "separate denominators and results; no pooling, alternate winner or threshold search on final",
        },
    )


def _verify_registry(root: Path, freeze: dict[str, Any]) -> dict[str, Any] | None:
    path = root.parent / "final_cohorts.json"
    if not path.exists() and not freeze.get("benchmark_registry_sha256"):
        return None
    if not path.is_file() or freeze.get("benchmark_registry_sha256") != sha256(path):
        raise SealError("freeze does not bind the fixed final-cohort registry")
    registry = json.loads(path.read_text())
    if registry.get("status") != "registered_before_architecture_freeze" or set(registry.get("cohorts", {})) != {
        "primary",
        "supplementary_recent",
    }:
        raise SealError("invalid joint final-cohort registry")
    for entry in registry["cohorts"].values():
        seal_path = _relative(root.parent, entry["seal_path"])
        if not seal_path.is_file() or sha256(seal_path) != entry["seal_sha256"]:
            raise SealError("registered final cohort changed")
    if _relative(root.parent, registry["cohorts"]["primary"]["seal_path"]) != root / "seal.json":
        raise SealError("registry refers to a different primary benchmark")
    return registry


def open_stage(bundle: Path, stage: str) -> Path:
    """Claim access after architecture or full-policy freeze; return private path.

    The seal.json and freeze files are created once by the benchmark builder and
    freezer. They bind each other, all manifest bytes, code and learned artifacts.
    The caller must compute both final policies in this single transaction.
    """
    root = bundle.resolve()
    seal, freeze, freeze_path, path, entry = _validate_stage(root, stage)
    write_once(
        root / f".ledger/{stage}.opened.json",
        {
            "stage": stage,
            "opened_at": datetime.now(UTC).isoformat(),
            "freeze_sha256": sha256(freeze_path),
            "seal_sha256": sha256(root / "seal.json"),
            "query_sha256": entry["sha256"],
            "benchmark_registry_sha256": freeze.get("benchmark_registry_sha256"),
        },
    )
    return path


def _validate_stage(root: Path, stage: str):
    if stage not in {"calibration", "final"}:
        raise SealError("only calibration or final can be opened")
    seal_path = root / "seal.json"
    freeze_path = root / ("architecture_frozen.json" if stage == "calibration" else "candidate_frozen.json")
    try:
        seal = json.loads(seal_path.read_text())
        freeze = json.loads(freeze_path.read_text())
        detached = freeze_path.with_suffix(".sha256").read_text().strip()
    except (OSError, ValueError) as exc:
        raise SealError("seal or frozen architecture/policy is unavailable") from exc
    if detached != sha256(freeze_path):
        raise SealError("freeze hash mismatch")
    if freeze.get("seal_sha256") != sha256(seal_path):
        raise SealError("freeze does not bind this benchmark seal")
    required = "architecture_frozen_before_calibration" if stage == "calibration" else "policy_frozen_before_final"
    if freeze.get("status") != required or seal.get("status") != "sealed_before_inference":
        raise SealError("invalid freeze/seal state")
    _verify_registry(root, freeze)
    if stage == "final":
        if not (root / ".ledger/calibration.opened.json").is_file():
            raise SealError("calibration transaction is missing")
        if set(freeze.get("policies", {})) != {"baseline", "candidate"}:
            raise SealError("exact baseline and single candidate are required")
        for name, policy in freeze["policies"].items():
            if not isinstance(policy, dict) or not isinstance(policy.get("path"), str) or not policy.get("sha256"):
                raise SealError(f"{name} policy has no concrete frozen artifact")
            if freeze.get("artifacts", {}).get(policy["path"]) != policy["sha256"]:
                raise SealError(f"{name} policy is not bound to verified artifacts")
            _verify_artifacts(root, {policy["path"]: policy["sha256"]})
        architecture = root / "architecture_frozen.json"
        architecture_data = json.loads(architecture.read_text())
        _verify_artifacts(root, architecture_data.get("artifacts", {}))
        _verify_registry(root, architecture_data)
        # If the architecture binds concrete policies, calibration may change
        # only the candidate's numeric threshold. Rebinding a new coordinate
        # rule, confidence model or retriever after calibration is prohibited.
        if architecture_data.get("policies"):
            original = architecture_data["policies"]
            if freeze["policies"]["baseline"] != original["baseline"]:
                raise SealError("production baseline policy changed during calibration")
            before = json.loads(_relative(root, original["candidate"]["path"]).read_text())
            after = json.loads(_relative(root, freeze["policies"]["candidate"]["path"]).read_text())
            before.pop("threshold", None)
            threshold = after.pop("threshold", None)
            if before != after:
                raise SealError("calibration changed candidate architecture rather than threshold only")
            if threshold is not None and (
                isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or not 0 <= threshold <= 1
            ):
                raise SealError("candidate confidence threshold must be null or a probability")
        if freeze.get("architecture_sha256") != sha256(architecture):
            raise SealError("architecture changed after calibration")
        calibration_ledger = root / ".ledger/calibration.opened.json"
        if json.loads(calibration_ledger.read_text())["freeze_sha256"] != sha256(architecture):
            raise SealError("architecture differs from the calibration opening")
    _verify_artifacts(root, freeze.get("artifacts", {}))
    entry = seal.get("splits", {}).get(stage)
    if not isinstance(entry, dict):
        raise SealError("split missing from seal")
    path = _relative(root, entry["path"])
    if not path.is_file() or sha256(path) != entry["sha256"]:
        raise SealError("query manifest hash mismatch")
    return seal, freeze, freeze_path, path, entry


def open_supplementary_final(primary: Path, supplementary: Path) -> Path:
    """Use the exact frozen primary policies; never fit a second threshold."""
    root, supplement = primary.resolve(), supplementary.resolve()
    _, freeze, freeze_path, _, _ = _validate_stage(root, "final")
    registry = _verify_registry(root, freeze)
    if registry is None:
        raise SealError("supplementary evaluation requires preregistered joint cohorts")
    expected_seal = _relative(root.parent, registry["cohorts"]["supplementary_recent"]["seal_path"])
    if supplement / "seal.json" != expected_seal:
        raise SealError("unregistered supplementary bundle")
    primary_ledger = root / ".ledger/final.opened.json"
    if not primary_ledger.is_file():
        raise SealError("primary paired final transaction must be claimed first")
    ledger = json.loads(primary_ledger.read_text())
    if ledger.get("freeze_sha256") != sha256(freeze_path) or ledger.get("benchmark_registry_sha256") != sha256(
        root.parent / "final_cohorts.json"
    ):
        raise SealError("primary final ledger differs from frozen joint evaluation")
    seal = json.loads(expected_seal.read_text())
    if seal.get("status") != "sealed_before_inference":
        raise SealError("supplementary cohort is not sealed")
    entry = seal["splits"]["final"]
    path = _relative(supplement, entry["path"])
    if not path.is_file() or sha256(path) != entry["sha256"]:
        raise SealError("supplementary query manifest changed")
    write_once(
        supplement / ".ledger/final.opened.json",
        {
            "stage": "supplementary_final",
            "opened_at": datetime.now(UTC).isoformat(),
            "freeze_sha256": sha256(freeze_path),
            "seal_sha256": sha256(expected_seal),
            "query_sha256": entry["sha256"],
            "primary_final_ledger_sha256": sha256(primary_ledger),
            "benchmark_registry_sha256": sha256(root.parent / "final_cohorts.json"),
            "threshold_source": "unchanged primary calibration; no supplementary fitting",
        },
    )
    return path
