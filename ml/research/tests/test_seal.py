import json

import pytest

from ml.research.seal import SealError, open_stage, sha256, write_once


def prepare(tmp_path):
    (tmp_path / "private").mkdir()
    (tmp_path / "private/calibration.parquet").write_bytes(b"calibration")
    (tmp_path / "private/final.parquet").write_bytes(b"final")
    (tmp_path / "code.py").write_text("frozen implementation")
    write_once(
        tmp_path / "seal.json",
        {
            "status": "sealed_before_inference",
            "splits": {
                stage: {"path": f"private/{stage}.parquet", "sha256": sha256(tmp_path / f"private/{stage}.parquet")}
                for stage in ("calibration", "final")
            },
        },
    )
    freeze = {
        "status": "architecture_frozen_before_calibration",
        "seal_sha256": sha256(tmp_path / "seal.json"),
        "artifacts": {"code.py": sha256(tmp_path / "code.py")},
    }
    digest = write_once(tmp_path / "architecture_frozen.json", freeze)
    (tmp_path / "architecture_frozen.sha256").write_text(digest)
    return tmp_path


def test_cannot_open_final_without_full_freeze(tmp_path):
    root = prepare(tmp_path)
    with pytest.raises(SealError):
        open_stage(root, "final")
    assert not (root / ".ledger/final.opened.json").exists()


def test_calibration_is_one_shot_even_via_alias_path(tmp_path):
    root = prepare(tmp_path)
    assert open_stage(root, "calibration") == root / "private/calibration.parquet"
    with pytest.raises(SealError):
        open_stage(root / "private/..", "calibration")


def test_artifact_tamper_blocks_access(tmp_path):
    root = prepare(tmp_path)
    (root / "code.py").write_text("changed after freeze")
    with pytest.raises(SealError):
        open_stage(root, "calibration")


def test_full_final_transaction_and_reopening_block(tmp_path):
    root = prepare(tmp_path)
    open_stage(root, "calibration")
    for name in ("baseline", "candidate"):
        (root / f"{name}.json").write_text(json.dumps({"threshold": 0.9, "pipeline": name}))
    policies = {
        name: {"path": f"{name}.json", "sha256": sha256(root / f"{name}.json")} for name in ("baseline", "candidate")
    }
    freeze = {
        "status": "policy_frozen_before_final",
        "seal_sha256": sha256(root / "seal.json"),
        "architecture_sha256": sha256(root / "architecture_frozen.json"),
        "policies": policies,
        "artifacts": {"code.py": sha256(root / "code.py"), **{p["path"]: p["sha256"] for p in policies.values()}},
    }
    digest = write_once(root / "candidate_frozen.json", freeze)
    (root / "candidate_frozen.sha256").write_text(digest)
    assert open_stage(root, "final") == root / "private/final.parquet"
    with pytest.raises(SealError):
        open_stage(root, "final")


def test_empty_placeholder_policies_cannot_open_final(tmp_path):
    root = prepare(tmp_path)
    open_stage(root, "calibration")
    freeze = {
        "status": "policy_frozen_before_final",
        "seal_sha256": sha256(root / "seal.json"),
        "architecture_sha256": sha256(root / "architecture_frozen.json"),
        "policies": {"baseline": {}, "candidate": {}},
        "artifacts": {"code.py": sha256(root / "code.py")},
    }
    digest = write_once(root / "candidate_frozen.json", freeze)
    (root / "candidate_frozen.sha256").write_text(digest)
    with pytest.raises(SealError, match="concrete frozen artifact"):
        open_stage(root, "final")
    assert not (root / ".ledger/final.opened.json").exists()


def test_path_escape_is_rejected(tmp_path):
    root = prepare(tmp_path)
    seal = json.loads((root / "seal.json").read_text())
    seal["splits"]["calibration"]["path"] = "../outside.parquet"
    (root / "seal.json").write_text(json.dumps(seal))
    freeze = json.loads((root / "architecture_frozen.json").read_text())
    freeze["seal_sha256"] = sha256(root / "seal.json")
    (root / "architecture_frozen.json").write_text(json.dumps(freeze))
    (root / "architecture_frozen.sha256").write_text(sha256(root / "architecture_frozen.json"))
    with pytest.raises(SealError):
        open_stage(root, "calibration")
