from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from ml.evaluation.test_seal import TestSealError as SealError
from ml.evaluation.test_seal import begin_test_opening, complete_test_opening


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    bundle = tmp_path / "bundle"
    test = bundle / "test_queries.parquet"
    test.parent.mkdir(parents=True)
    test.write_bytes(b"sealed-test")
    test_hash = hashlib.sha256(test.read_bytes()).hexdigest()
    _write(
        bundle / "test_seal.json",
        {
            "status": "sealed_before_policy_tuning",
            "maximum_runs": 1,
            "test_manifest": test.name,
            "test_manifest_sha256": test_hash,
        },
    )
    frozen = tmp_path / "moscow_real_v3_frozen.json"
    _write(
        frozen,
        {
            "status": "frozen_before_final_test",
            "dataset": {"sealed_test_sha256": test_hash},
        },
    )
    frozen_hash = tmp_path / "moscow_real_v3_frozen.sha256"
    frozen_hash.write_text(
        hashlib.sha256(frozen.read_bytes()).hexdigest() + "  " + frozen.name + "\n",
        encoding="ascii",
    )
    return bundle, frozen, frozen_hash, tmp_path / "state"


def test_test_seal_allows_exactly_one_complete_opening(tmp_path: Path) -> None:
    bundle, frozen, frozen_hash, state = _fixture(tmp_path)
    opening = begin_test_opening(
        bundle=bundle,
        frozen_config=frozen,
        frozen_hash=frozen_hash,
        state_dir=state,
    )
    reports = {"baseline": tmp_path / "baseline.json", "candidate": tmp_path / "candidate.json"}
    for path in reports.values():
        path.write_text("{}\n", encoding="utf-8")

    receipt = complete_test_opening(opening, reports)

    assert receipt.is_file()
    with pytest.raises(SealError, match="already been opened"):
        begin_test_opening(
            bundle=bundle,
            frozen_config=frozen,
            frozen_hash=frozen_hash,
            state_dir=state,
        )


def test_test_seal_rejects_unhashed_frozen_change(tmp_path: Path) -> None:
    bundle, frozen, frozen_hash, state = _fixture(tmp_path)
    frozen.write_text(frozen.read_text(encoding="utf-8") + " ", encoding="utf-8")

    with pytest.raises(SealError, match="hash mismatch"):
        begin_test_opening(
            bundle=bundle,
            frozen_config=frozen,
            frozen_hash=frozen_hash,
            state_dir=state,
        )
