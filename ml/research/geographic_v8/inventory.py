"""Inventory historical evidence without opening historical final populations."""
import json
import platform
import subprocess
from pathlib import Path

import torch

from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.geographic_v8.common import FROZEN, GALLERY, LOCAL, NIGHT, QUERY, ROOT, initialize


def run():
    initialize()
    registry = WORKSPACE / "docs/research_experiment_registry_20260910.md"
    sections, title, header = [], "", []
    for line in registry.read_text().splitlines():
        if line.startswith("#"):
            title, header = line.lstrip("# "), []
        elif line.startswith("|"):
            cells = [c.strip() for c in line.strip("| ").split("|")]
            if not header:
                header = cells
            elif not all(set(c) <= set("-: ") for c in cells):
                sections.append({"section": title, "values": dict(zip(header, cells, strict=False))})
    frozen = json.loads(FROZEN.read_text())
    historical = WORKSPACE / "docs/research_summary_20260910.md"
    text = historical.read_text()
    families = text.split("## 6.", 1)[1].split("## 7.", 1)[0]
    pool = GALLERY.parent / "descriptor_pool.json"
    data = json.loads(pool.read_text())
    files = sorted(set(v["path"] for v in data["entries"].values()))
    result = {
        "date": "2026-09-28", "head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "initial_worktree_status": subprocess.check_output(["git", "status", "--short"], text=True).splitlines(),
        "historical_registry": {"path": str(registry), "sha256": digest(registry), "rows": sections},
        "closed_families_source": {"path": str(historical), "sha256": digest(historical), "text": families},
        "models_tested": ["MegaLoc", "SALAD", "SAGE ViT-B No-Encoder", "SAGE ViT-L No-Encoder",
            "SAGE full context", "SelaVPR++ base/rerank", "EDTformer", "BoQ", "XFeat", "FoL-L"],
        "preprocessing_tested": ["official 322", "query504/reference322", "equal query322/504", "scene/pooled branch weighting",
            "crop/rotation", "square crop", "reference panorama4", "multi-photo separate descriptors", "2x2 collage"],
        "rerankers_and_fusions_tested": ["SAGE context30/100 mixtures", "diversity-packed context", "B/L cosine fusion",
            "EDTformer cosine/RRF", "XFeat geometry", "FoL global/MNN/blend/OOF", "geographic mode voting",
            "reference centering", "density/hub correction", "cross-sequence place prototypes", "DBA/AQE"],
        "learned_adapters": ["small linear/residual feature adapters", "rank64 reference adapter"],
        "backbone_finetuning_completed": False,
        "confidence": ["standardized logistic", "HGB", "geographic OOF/nested", "auxiliary transfer"],
        "galleries": {"primary": {"path": str(GALLERY), "sha256": digest(GALLERY), **frozen["candidate"]["gallery"]},
            "secondary": {"path": str(NIGHT / "licensed_third/gallery.parquet"), "count": 44995,
                "sources": ["mapillary", "kartaview"], "raw100": 380/1184},
            "historical_sizes": [20031, 20487, 31084, 60000, 100000, 99976, 102915, 107749]},
        "query_sets": {"current": {"path": str(QUERY), "sha256": digest(QUERY), "count": 1184,
            "status": "heavily reused development"}, "historical_development_counts": [602, 751],
            "auxiliary_multiphoto": {"panorama_parents": 43, "dependent_cases": 172, "geographic_blocks": 6,
                "product_accuracy_claim": False}, "historical_final": "previously exposed; never independent"},
        "descriptor_pool": {"path": str(pool), "sha256": digest(pool), "entries": len(data["entries"]),
            "unique_shards": len(files), "missing_shards": [p for p in files if not Path(p).exists()],
            "contract_sha256": data["contract_sha256"]},
        "provenance": {"msls": "CC BY-NC-SA 4.0; research-only", "mapillary_kartaview": "CC BY-SA 4.0 per saved row audit",
            "model_license_does_not_override_training_data": True,
            "pretraining_overlap": "not certified absent for externally pretrained models"},
        "leakage_exclusions": {"current": "common-quarantine manifest, whole provider sequences, stable/provider IDs, file SHA, decoded pixels, rotated/mirrored pHash<=4",
            "saved_identity_checks": frozen["current_development_identity_overlap_counts"],
            "limitations": "unknown provider aliases/partial-image duplicates not certified absent; historical protected populations not independent",
            "query_GT": "evaluation/controlled fold labels only; inference receives id,image_path,file_sha256 only"},
        "compute": {"platform": platform.platform(), "torch": torch.__version__, "cuda": torch.cuda.is_available(),
            "mps": torch.backends.mps.is_available(), "external_root": str(ROOT)},
        "research_protocol": {"frozen_gallery": True, "new_photographs": False, "all_queries_denominator": True,
            "new_final_claim": False, "confidence_last": True, "new_training": "geographic and sequence groups; OOF or nested only"},
    }
    save(LOCAL / "inventory.json", result)
    print(json.dumps({"historical_records": len(sections), "descriptor_entries": len(data["entries"]),
                      "shards": len(files), "missing_shards": len(result["descriptor_pool"]["missing_shards"])}))


if __name__ == "__main__":
    run()
