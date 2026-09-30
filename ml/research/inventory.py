"""Snapshot production and audit source availability without model inference."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pandas as pd

from ml.ingestion.mapillary_citywide import load_aoi_boundary

ROOT = Path("data/evaluation/moscow_research_v5")


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run():
    ROOT.mkdir(parents=True, exist_ok=True)
    destination = ROOT / "baseline_snapshot.json"
    if destination.exists():
        snapshot = json.loads(destination.read_text())
        for path, digest in snapshot["files"].items():
            if sha(path) != digest:
                raise ValueError(f"baseline changed: {path}")
    else:
        config = json.loads(Path("configs/moscow_production_frozen.json").read_text())
        files = [
            "configs/moscow_production_frozen.json",
            "configs/moscow_real_v3_confidence_model.json",
            "configs/production_artifacts.json",
            config["dataset"]["gallery_manifest"],
        ]
        for root in [config["embedding"]["directory"], config["index"]["directory"]]:
            files += [str(p) for p in sorted(Path(root).iterdir()) if p.is_file()]
        files += [
            str(p)
            for root in ["ml/retrieval", "ml/localization", "ml/indexing"]
            for p in sorted(Path(root).glob("*.py"))
        ]
        files += ["ml/runtime_config.py"]
        snapshot = {
            "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
            "files": {p: sha(p) for p in files},
        }
        destination.write_text(json.dumps(snapshot, indent=2) + "\n")
    history_paths = sorted(Path("data/evaluation").glob("moscow_real*/*.parquet"))
    history = pd.concat([pd.read_parquet(p) for p in history_paths], ignore_index=True)
    sequences = set(zip(history.source.astype(str), history.sequence_id.astype(str), strict=True))
    boundary = load_aoi_boundary(Path("data/raw/moscow/moscow_admin_boundary.geojson"))
    physical = pd.read_parquet("data/processed/moscow/live_physical_manifest.parquet")
    candidates = []
    for row in physical.to_dict("records"):
        row["inventory_origin"] = "physical_download_before_v5"
        candidates.append(row)
    for source, filename in [
        ("mapillary", "mapillary_admin_outer_raw_v2.json"),
        ("mapillary", "mapillary_citywide_raw.json"),
        ("kartaview", "kartaview_sequences_raw.json"),
    ]:
        for row in json.loads((Path("data/raw/moscow") / filename).read_text()):
            candidates.append(dict(row, source=source, inventory_origin=filename))
    unique = {}
    for row in candidates:
        key = (str(row["source"]), str(row["source_image_id"]))
        # Prefer physical records, which contain canonical IDs and local paths.
        if key not in unique:
            unique[key] = row
    fresh = []
    exclusions = {"historically_exposed_sequence": 0, "missing_sequence": 0, "outside_exact_aoi": 0}
    for row in unique.values():
        source, sequence = str(row["source"]), str(row.get("sequence_id") or "")
        if not sequence or sequence in {"nan", "None", "<NA>"}:
            exclusions["missing_sequence"] += 1
            continue
        if (source, sequence) in sequences:
            exclusions["historically_exposed_sequence"] += 1
            continue
        if not boundary.covers(float(row["lon"]), float(row["lat"])):
            exclusions["outside_exact_aoi"] += 1
            continue
        fresh.append(row)
    frame = pd.DataFrame(fresh)
    frame.to_parquet(ROOT / "unassigned_fresh_source_pool.parquet", index=False)
    counts = {
        str(source): {
            "rows": len(group),
            "sequences": group.sequence_id.nunique(),
            "physical_images": sum(isinstance(p, str) and Path(p).is_file() for p in group.image_path),
        }
        for source, group in frame.groupby("source")
    }
    report = {
        "kind": "metadata_only_no_inference_no_final_split_yet",
        "baseline_snapshot_sha256": sha(destination),
        "protocol_sha256": sha("configs/moscow_research_v5_protocol.json"),
        "historical_manifest_hashes": {str(p): sha(p) for p in history_paths},
        "historical_sequences": len(sequences),
        "input_unique_rows": len(unique),
        "exclusions": exclusions,
        "fresh_pool": counts,
        "pool_sha256": sha(ROOT / "unassigned_fresh_source_pool.parquet"),
        "limitations": [
            "Unused physical pool includes historical cleaning rejections; do not mistake it for representative random sampling.",
            "Provider sequences do not prove photographer, scene, session, or pretrained-model independence.",
            "No newly sealed final test exists yet.",
        ],
    }
    (ROOT / "inventory.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "historical_manifest_hashes"}, indent=2))


if __name__ == "__main__":
    run()
