"""Freeze only a materially better research candidate; never touch production."""
import json
from datetime import datetime, timezone
from pathlib import Path

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import GALLERY, LOCAL, QUERY

OUT = LOCAL / "sage_adaptation_v3"


def run():
    complete = json.loads((OUT / "complete.json").read_text())
    epoch = complete["selected_epoch"]
    report_path = OUT / f"sage_asymmetric_v3_refonly_epoch{epoch}_primary/report.json"
    secondary_path = OUT / f"sage_asymmetric_v3_refonly_epoch{epoch}_licensed_secondary/report.json"
    primary = json.loads(report_path.read_text())
    secondary = json.loads(secondary_path.read_text())
    baseline = json.loads((LOCAL / "baseline/report.json").read_text())
    old, new = baseline["raw"], primary["raw"]
    count = round(new["accuracy_100m"] * 1184)
    interval = primary["paired_geographic_bootstrap"]["ci95_pp"]
    eligible = (count >= 423 and interval[0] > 0 and
                new["catastrophic_gt500m_rate"] <= old["catastrophic_gt500m_rate"])
    decision = {"status": "freeze" if eligible else "retain_frozen_prior_baseline",
        "adapted_raw100_count": count, "baseline_raw100_count": 411,
        "paired_gain_ci95_pp": interval,
        "adapted_gt500_rate": new["catastrophic_gt500m_rate"],
        "baseline_gt500_rate": old["catastrophic_gt500m_rate"],
        "precommitted_research_selection_bar": ">=1pp (at least 423/1184), CI lower bound >0, no increased >500m rate",
        "primary_report_sha256": digest(report_path), "secondary_report_sha256": digest(secondary_path),
        "note": "development-only selection; no independent final accuracy claim"}
    save(LOCAL / "candidate_decision.json", decision)
    if not eligible:
        print(json.dumps(decision), flush=True)
        return
    path = LOCAL / "candidate_frozen_v8.json"
    if path.exists():
        raise FileExistsError("Versioned research candidate is immutable")
    checkpoint = OUT / f"epoch{epoch}_trainable.pth"
    files = [GALLERY, QUERY, checkpoint, OUT / "complete.json", OUT / "strict_split_gate.json",
        OUT / "development_exclusion_gate.json", OUT / "mining_contract.json", OUT / "pairs.json",
        OUT / "reference_validation_comparison.json", OUT / "evaluation_contract.json",
        OUT / "evaluation_complete.json", report_path, secondary_path,
        LOCAL / "runtime_versions.json", LOCAL / "staged/descriptor_pool.json",
        LOCAL / "staged/paths.json", Path("data/models/research_v5/sage_context_encoder.pth"),
        Path(".cache/huggingface/hub/models--shunpeng--SAGE/blobs/31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc"),
        *(Path("ml/research") / name for name in ("retrievers.py", "gallery_scale.py",
            "night_scale_context.py", "night_v7.py", "gallery_scale_storage.py",
            "metrics.py", "vector_evaluation.py"))]
    files += sorted(Path("ml/research/geographic_v8").glob("*.py"))
    artifacts = {str(item.resolve()): digest(item) for item in files}
    staged = json.loads((LOCAL / "staged/paths.json").read_text())
    artifacts.update({item["path"]: item["sha256"] for item in staged.values()})
    candidate = {"id": f"geographic_v8_sage_v3_refonly_epoch{epoch}_20260928",
        "frozen_at": datetime.now(timezone.utc).isoformat(),
        "research_only": True, "production_modified": False,
        "production_suitability": "unassessed; MSLS references research-only",
        "source_model": "SAGE ViT-L No-Encoder, pinned official source/checkpoint",
        "adaptation": "reference-only asymmetric last ViT block and aggregator, all-role H3r6 embargoed sequence-disjoint train/validation",
        "selected_epoch": epoch, "selected_checkpoint_sha256": digest(checkpoint),
        "gallery_manifest": str(GALLERY), "gallery_sha256": digest(GALLERY), "gallery_count": 112163,
        "release_compatible_gallery_count": 44995,
        "inference": {"reference_size": 322, "query_sizes": [322, 504],
            "candidate_generation": "exact cosine mean of two unit query descriptors against fixed unit reference descriptors",
            "context": "frozen SAGE encoder top30, standardized 0.5 original/context blend",
            "coordinate_prediction": "first reference after context reranking", "confidence_filter": None,
            "query_ground_truth_feature": False},
        "development_results": {"primary_raw100": count, "primary_report_sha256": digest(report_path),
            "secondary_raw100": round(secondary["raw"]["accuracy_100m"]*1184),
            "secondary_report_sha256": digest(secondary_path),
            "paired_gain_ci95_pp": interval},
        "artifact_hashes": artifacts, "independent_validation_done": False}
    save(path, candidate)
    print(path, "sha256", digest(path), flush=True)


if __name__ == "__main__":
    run()
