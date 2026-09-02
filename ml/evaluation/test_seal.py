"""One-shot v3 final-test seal enforced before any test manifest is read."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class TestSealError(RuntimeError):
    """The v3 test is still sealed, already opened, or inconsistent."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class TestOpening:
    bundle: Path
    frozen_config: Path
    frozen_sha256: str
    test_manifest: Path
    test_sha256: str
    marker: Path
    receipt: Path


def begin_test_opening(
    *,
    bundle: Path,
    frozen_config: Path,
    frozen_hash: Path,
    state_dir: Path,
) -> TestOpening:
    """Claim the sole final-test opening after all frozen hashes validate."""

    seal_path = bundle / "test_seal.json"
    try:
        seal = json.loads(seal_path.read_text(encoding="utf-8"))
        frozen = json.loads(frozen_config.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TestSealError("cannot read test seal or frozen configuration") from exc
    if seal.get("status") != "sealed_before_policy_tuning" or seal.get("maximum_runs") != 1:
        raise TestSealError("v3 test seal has an unsupported status or run allowance")
    if frozen.get("status") != "frozen_before_final_test":
        raise TestSealError("v3 runtime configuration is not frozen_before_final_test")
    actual_frozen_hash = sha256_file(frozen_config)
    try:
        declared_frozen_hash = frozen_hash.read_text(encoding="ascii").strip().split()[0]
    except (OSError, IndexError) as exc:
        raise TestSealError("frozen configuration hash file is missing or malformed") from exc
    if declared_frozen_hash != actual_frozen_hash:
        raise TestSealError("frozen configuration hash mismatch")
    test_manifest = bundle / str(seal["test_manifest"])
    actual_test_hash = sha256_file(test_manifest)
    expected_test_hash = str(seal["test_manifest_sha256"])
    frozen_test_hash = str(frozen.get("dataset", {}).get("sealed_test_sha256", ""))
    if actual_test_hash != expected_test_hash or frozen_test_hash != expected_test_hash:
        raise TestSealError("sealed test manifest hash mismatch")

    state_dir.mkdir(parents=True, exist_ok=True)
    marker = state_dir / "v3_final_test.opening.json"
    receipt = state_dir / "v3_final_test.receipt.json"
    if receipt.exists():
        raise TestSealError("v3 final test has already been opened and completed")
    marker_payload = {
        "schema_version": 1,
        "status": "opening_claimed",
        "claimed_at": datetime.now(UTC).isoformat(),
        "frozen_config": str(frozen_config),
        "frozen_config_sha256": actual_frozen_hash,
        "test_manifest_sha256": actual_test_hash,
    }
    try:
        descriptor = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError as exc:
        raise TestSealError("v3 final-test opening is already claimed") from exc
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(marker_payload, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    return TestOpening(
        bundle=bundle,
        frozen_config=frozen_config,
        frozen_sha256=actual_frozen_hash,
        test_manifest=test_manifest,
        test_sha256=actual_test_hash,
        marker=marker,
        receipt=receipt,
    )


def complete_test_opening(opening: TestOpening, reports: dict[str, Path]) -> Path:
    """Write the immutable receipt after both baseline and candidate reports exist."""

    if not opening.marker.is_file() or opening.receipt.exists():
        raise TestSealError("v3 final-test opening claim is missing or already completed")
    required = {"baseline", "candidate"}
    if set(reports) != required or any(not path.is_file() for path in reports.values()):
        raise TestSealError("both baseline and candidate final reports are required")
    payload: dict[str, Any] = {
        "schema_version": 1,
        "status": "completed_no_post_test_tuning",
        "completed_at": datetime.now(UTC).isoformat(),
        "frozen_config": str(opening.frozen_config),
        "frozen_config_sha256": opening.frozen_sha256,
        "test_manifest_sha256": opening.test_sha256,
        "reports": {
            name: {"path": str(path), "sha256": sha256_file(path)}
            for name, path in sorted(reports.items())
        },
    }
    temporary = opening.receipt.with_name(opening.receipt.name + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, opening.receipt)
    opening.marker.unlink()
    return opening.receipt
