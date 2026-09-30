"""Verify frozen production and every completed v8 research input/artifact receipt."""
import json
from pathlib import Path

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import LOCAL, frames, guard


def verify_file(path, expected):
    if digest(path) != expected:
        raise RuntimeError(f"Artifact hash mismatch: {path}")


def run():
    production = guard("after")
    q, g = frames()
    stage = LOCAL / "staged"
    receipt = json.loads((stage / "complete.json").read_text())
    for item in receipt["fixed_inputs"].values():
        verify_file(item["path"], item["sha256"])
    verify_file(stage / "paths.json", receipt["mapping_sha256"])
    mapping = json.loads((stage / "paths.json").read_text())
    for item in mapping.values():
        verify_file(item["path"], item["sha256"])
    source_snapshots = sorted((LOCAL / "source_snapshots").glob("*/manifest.json"))
    source_snapshot_files = 0
    for manifest_path in source_snapshots:
        source_manifest = json.loads(manifest_path.read_text())
        for filename, sha in source_manifest["hashes"].items():
            verify_file(manifest_path.parent / filename, sha)
            source_snapshot_files += 1
    for path in sorted((LOCAL / "audit").glob("*_download.json")):
        model = json.loads(path.read_text())
        if "files" in model:
            for filename, sha in model["files"].items():
                verify_file(Path(model["path"]) / filename, sha)
        else:
            verify_file(model["path"], model["sha256"])
    for name in ("g3", "geoclip", "plonk", "osv5m"):
        completed = LOCAL / name / "complete.json"
        if not completed.exists():
            continue
        info = json.loads(completed.read_text())
        for filename, sha in info["hashes"].items():
            verify_file(LOCAL / name / filename, sha)
        if name == "geoclip":
            contract = info["contract"]
            base = LOCAL / f"hf_cache/models--openai--clip-vit-large-patch14/snapshots/{contract['base_revision']}"
            for filename, sha in contract["base_hashes"].items():
                verify_file(base / filename, sha)
            from ml.research.geographic_v8.common import ROOT
            bundle = ROOT / "sources/geoclip/geoclip/model/weights"
            for filename, sha in contract["bundle_hashes"].items():
                verify_file(bundle / filename, sha)
    for name, expected in (("baseline", 411), ("secondary_baseline", 380)):
        report = json.loads((LOCAL / name / "report.json").read_text())
        raw = report["raw"]["accuracy_100m"]
        if round(raw * len(q)) != expected:
            raise RuntimeError(f"{name} accuracy changed")
    pilot = LOCAL / "sage_adaptation/pilot_invalid.json"
    if pilot.exists():
        invalid = json.loads(pilot.read_text())
        verify_file(LOCAL / "sage_adaptation/epoch1_trainable.pth", invalid["preserved_checkpoint_sha256"])
    v2_invalid = LOCAL / "sage_adaptation_v2/v2_invalid_geography.json"
    if v2_invalid.exists():
        invalid = json.loads(v2_invalid.read_text())
        verify_file(LOCAL / "sage_adaptation_v2/epoch1_trainable.pth",
                    invalid["preserved_epoch1_checkpoint_sha256"])
    adapted = LOCAL / "sage_adaptation_v3/complete.json"
    if adapted.exists():
        info = json.loads(adapted.read_text())
        epoch = info["selected_epoch"]
        verify_file(LOCAL / f"sage_adaptation_v3/epoch{epoch}_trainable.pth",
                    info["selected_checkpoint_sha256"])
    output = {"verified": True, "production": production, "queries": len(q), "references": len(g),
        "staged_fixed_inputs": len(receipt["fixed_inputs"]), "staged_descriptor_shards": len(mapping),
        "source_snapshot_files": source_snapshot_files,
        "source_snapshots": len(source_snapshots),
        "model_download_receipts": len(list((LOCAL / "audit").glob("*_download.json"))),
        "adapted_checkpoint_present": adapted.exists()}
    save(LOCAL / "integrity_report.json", output)
    print(json.dumps(output), flush=True)


if __name__ == "__main__":
    run()
