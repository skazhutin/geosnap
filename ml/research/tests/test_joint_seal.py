import json

import pytest

from ml.research.seal import (
    SealError,
    open_stage,
    open_supplementary_final,
    register_final_cohorts,
    sha256,
    write_once,
)


def freeze(root, filename, payload):
    digest = write_once(root / filename, payload)
    (root / filename).with_suffix(".sha256").write_text(digest)


def setup(tmp_path):
    primary, supplement = tmp_path / "prospective", tmp_path / "recent"
    for root in (primary, supplement):
        root.mkdir()
        for stage in ("calibration", "final"):
            (root / f"{stage}.parquet").write_bytes(stage.encode())
        seal = {
            "status": "sealed_before_inference",
            "splits": {
                stage: {"path": f"{stage}.parquet", "sha256": sha256(root / f"{stage}.parquet")}
                for stage in ("calibration", "final")
            },
        }
        if root == supplement:
            seal["original_prospective_seal_sha256"] = sha256(primary / "seal.json")
        write_once(root / "seal.json", seal)
    registry = register_final_cohorts(primary, supplement)
    (primary / "code.py").write_text("immutable implementation")
    freeze(
        primary,
        "architecture_frozen.json",
        {
            "status": "architecture_frozen_before_calibration",
            "seal_sha256": sha256(primary / "seal.json"),
            "benchmark_registry_sha256": registry,
            "artifacts": {"code.py": sha256(primary / "code.py")},
        },
    )
    return primary, supplement


def policy_freeze(primary):
    for name in ("baseline", "candidate"):
        (primary / f"{name}.json").write_text(json.dumps({"threshold": 0.93, "pipeline": name}))
    policies = {
        name: {"path": f"{name}.json", "sha256": sha256(primary / f"{name}.json")} for name in ("baseline", "candidate")
    }
    freeze(
        primary,
        "candidate_frozen.json",
        {
            "status": "policy_frozen_before_final",
            "seal_sha256": sha256(primary / "seal.json"),
            "architecture_sha256": sha256(primary / "architecture_frozen.json"),
            "benchmark_registry_sha256": sha256(primary.parent / "final_cohorts.json"),
            "policies": policies,
            "artifacts": {p["path"]: p["sha256"] for p in policies.values()},
        },
    )


def test_joint_cohorts_require_same_frozen_transaction_and_are_one_shot(tmp_path):
    primary, supplement = setup(tmp_path)
    with pytest.raises(SealError):
        open_supplementary_final(primary, supplement)
    open_stage(primary, "calibration")
    policy_freeze(primary)
    with pytest.raises(SealError, match="claimed first"):
        open_supplementary_final(primary, supplement)
    open_stage(primary, "final")
    assert open_supplementary_final(primary, supplement) == supplement / "final.parquet"
    with pytest.raises(SealError, match="immutable artifact"):
        open_supplementary_final(primary, supplement)


def test_registry_cannot_be_changed_after_architecture_freeze(tmp_path):
    primary, supplement = setup(tmp_path)
    (tmp_path / "final_cohorts.json").write_text("{}")
    with pytest.raises(SealError, match="registry"):
        open_stage(primary, "calibration")


def test_supplement_seal_tampering_blocks_primary_calibration(tmp_path):
    primary, supplement = setup(tmp_path)
    (supplement / "seal.json").write_text("{}")
    with pytest.raises(SealError, match="cohort changed"):
        open_stage(primary, "calibration")


def test_supplement_private_manifest_tampering_blocks_opening(tmp_path):
    primary, supplement = setup(tmp_path)
    open_stage(primary, "calibration")
    policy_freeze(primary)
    open_stage(primary, "final")
    (supplement / "final.parquet").write_bytes(b"replaced")
    with pytest.raises(SealError, match="manifest changed"):
        open_supplementary_final(primary, supplement)


def test_cohort_registration_after_freeze_is_forbidden(tmp_path):
    primary, supplement = setup(tmp_path)
    with pytest.raises(SealError, match="precede"):
        register_final_cohorts(primary, supplement)


def test_candidate_cannot_change_coordinates_after_calibration(tmp_path):
    primary, supplement = setup(tmp_path)
    architecture_path = primary / "architecture_frozen.json"
    architecture = json.loads(architecture_path.read_text())
    for name in ("baseline", "candidate"):
        (primary / f"{name}_architecture.json").write_text(json.dumps({"threshold": 0.93, "pipeline": name}))
    architecture["policies"] = {
        name: {"path": f"{name}_architecture.json", "sha256": sha256(primary / f"{name}_architecture.json")}
        for name in ("baseline", "candidate")
    }
    architecture["artifacts"].update({p["path"]: p["sha256"] for p in architecture["policies"].values()})
    architecture_path.write_text(json.dumps(architecture))
    architecture_path.with_suffix(".sha256").write_text(sha256(architecture_path))
    open_stage(primary, "calibration")
    policy_freeze(primary)
    frozen_path = primary / "candidate_frozen.json"
    frozen = json.loads(frozen_path.read_text())
    frozen["policies"]["baseline"] = architecture["policies"]["baseline"]
    frozen["artifacts"].update(architecture["artifacts"])
    (primary / "candidate.json").write_text(json.dumps({"threshold": 0.95, "pipeline": "new after calibration"}))
    frozen["policies"]["candidate"]["sha256"] = sha256(primary / "candidate.json")
    frozen["artifacts"]["candidate.json"] = sha256(primary / "candidate.json")
    frozen_path.write_text(json.dumps(frozen))
    frozen_path.with_suffix(".sha256").write_text(sha256(frozen_path))
    with pytest.raises(SealError, match="architecture rather than threshold"):
        open_stage(primary, "final")
