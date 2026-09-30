"""Verify protected artifacts, blind schema, raw coverage and write-once hashes."""
from __future__ import annotations

import argparse
import json

import pandas as pd

from .common import OUT, ROOT, json_once, now, sha256, verify_production

PROTECTED = {
    "v7_frozen_candidate": ROOT / "data/evaluation/moscow_night_v7/candidate_frozen.json",
    "v8_baseline_predictions": ROOT / "data/evaluation/geographic_v8_20260928/baseline/predictions.npz",
    "v9_integrity_receipt": ROOT / "data/evaluation/geographic_v9_20260928/integrity_report.json",
}
FORBIDDEN = {"lat", "lon", "gt_lat", "gt_lon", "predicted_lat", "predicted_lon",
             "baseline_correct", "failure_bucket", "localization_error_m", "retrieval_score",
             "candidate_rank", "top100_positive"}


def verify_manifest() -> tuple[list[dict], dict]:
    path = OUT / "annotation_input_manifest.jsonl"
    receipt = json.loads((OUT / "input_receipt.json").read_text())
    if sha256(path) != receipt["annotation_manifest_sha256"]:
        raise RuntimeError("Annotation input manifest changed")
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    if len(rows) != 1184 or len({r["query_id"] for r in rows}) != 1184:
        raise RuntimeError("Not 1,184 unique query IDs")
    if set(rows[0]) != {"query_id", "image_path", "image_sha256", "width", "height"}:
        raise RuntimeError("Annotation input contains an unexpected field")
    return rows, receipt


def run(before: bool) -> None:
    production = verify_production()
    rows, receipt = verify_manifest()
    protected = {key: sha256(path) for key, path in PROTECTED.items()}
    before_path = OUT / "protected_before.json"
    if before:
        if before_path.exists():
            old = json.loads(before_path.read_text())
            if old["hashes"] != protected:
                raise RuntimeError("Protected artifact mismatch")
        else:
            json_once(before_path, {"checked_at": now(), "hashes": protected,
                                    "production": production, "annotation_manifest_sha256": receipt["annotation_manifest_sha256"]})
        print(json.dumps({"protected_snapshot": str(before_path), "files": len(protected)}))
        return
    if not before_path.exists() or json.loads(before_path.read_text())["hashes"] != protected:
        raise RuntimeError("Protected artifact hashes changed during annotation")
    frozen = json.loads((OUT / "annotation_freeze.json").read_text())
    canonical = OUT / "geolocatability_annotations.jsonl"
    parquet = OUT / "geolocatability_annotations.parquet"
    if sha256(canonical) != frozen["canonical_jsonl_sha256"] or sha256(parquet) != frozen["canonical_parquet_sha256"]:
        raise RuntimeError("Canonical annotation changed")
    data = pd.read_parquet(parquet)
    if len(data) != 1184 or data.query_id.nunique() != 1184 or FORBIDDEN.intersection(data.columns):
        raise RuntimeError("Canonical annotation has wrong size, duplicate ID, or leaked outcome column")
    counts = data.annotation_status.value_counts().to_dict()
    for row in rows:
        folder = OUT / "annotations/raw_qwen35_full1024" / row["query_id"]
        if not (all((folder / f"pass{n}.json").is_file() for n in (1, 2, 3)) or
                (folder / "failure.json").is_file() or (folder / "recovery_failure.json").is_file()):
            raise RuntimeError(f"Unaccounted query: {row['query_id']}")
    if (OUT / "analysis_receipt.json").exists():
        analysis = json.loads((OUT / "analysis_receipt.json").read_text())
        if analysis["canonical_annotation_sha256"] != frozen["canonical_jsonl_sha256"]:
            raise RuntimeError("Outcome analysis points to another annotation version")
    json_once(OUT / "final_integrity.json", {"verified_at": now(), "production": production,
        "protected_hashes": protected, "canonical_sha256": sha256(canonical),
        "annotation_status_counts": counts, "input_count": len(rows),
        "outcome_columns_in_canonical": [], "raw_or_explicit_failure_count": len(rows)})
    print(json.dumps({"status": "verified", "canonical_rows": len(data), "counts": counts,
                      "production_guarded_files": production["guarded_files"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", action="store_true")
    run(parser.parse_args().before)
