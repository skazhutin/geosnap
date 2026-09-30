"""Verify the v9 research evidence and frozen production after the cycle."""
from __future__ import annotations

import json

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v9.common import OUT, V8, frames, production_guard


def run():
    guard = production_guard("after")
    q, g = frames()
    with np.load(V8 / "baseline/predictions.npz", allow_pickle=False) as z:
        if z["query_ids"].tolist() != q.id.tolist() or int((z["errors_m"] <= 100).sum()) != 411:
            raise RuntimeError("Baseline population or result changed")
    expected = {
        OUT / "hypotheses/inference_contract.json": {"features.npz": "features_sha256",
                                                   "hypotheses.json": "hypotheses_sha256"},
        OUT / "hypotheses_all/inference_contract.json": {"features.npz": "features_sha256",
                                                       "hypotheses.json": "hypotheses_sha256"},
        OUT / "hypotheses_visual30/inference_contract.json": {"features.npz": "features_sha256",
            "visual_scores.npz": "visual_scores_sha256"},
        OUT / "technical_quality/feature_contract.json": {"image_features.npz": "image_features_sha256"},
        OUT / "reference_pairwise_verifier/reference_feature_contract.json": {
            "reference_pair_scores.npz": "reference_pair_scores_sha256"},
    }
    checked = {}
    for receipt_path, artifacts in expected.items():
        receipt = json.loads(receipt_path.read_text())
        for filename, key in artifacts.items():
            path = receipt_path.parent / filename
            actual = digest(path)
            if actual != receipt[key]:
                raise RuntimeError(f"Research artifact changed: {path}")
            checked[str(path)] = actual
    for sample in ("", "quick20_"):
        folder = OUT / "annotations"
        private = folder / f"{sample}private_sample.json"
        receipt = json.loads((folder / f"{sample}sample_receipt.json").read_text())
        if digest(private) != receipt["private_sample_sha256"]:
            raise RuntimeError("Blind annotation sample changed")
        checked[str(private)] = digest(private)
    pilot = OUT / "annotations/partial_pilot_11_395eae3f7f77.json"
    if pilot.exists():
        snapshot = json.loads(pilot.read_text())
        if digest(OUT / "annotations/quick20_rater_reviewer1.jsonl") != snapshot["label_log_sha256"]:
            raise RuntimeError("Human pilot log changed after seal; create a new version if work resumed")
        checked[str(pilot)] = digest(pilot)
    report = {"status": "verified", "production_files": guard["files"],
        "query_count": len(q), "gallery_count": len(g), "baseline_raw100_count": 411,
        "checked_artifact_hashes": checked,
        "experiment_record_count": len(list((OUT / "experiments").glob("*.json"))),
        "no_production_change": True}
    path = OUT / "integrity_report.json"
    if path.exists():
        old = json.loads(path.read_text())
        if old != report:
            raise RuntimeError("Prior v9 integrity receipt differs")
    else:
        save(path, report)
    print(json.dumps({"status": "verified", "production_files": guard["files"],
                      "artifacts": len(checked), "experiments": report["experiment_record_count"]}))


if __name__ == "__main__":
    run()
