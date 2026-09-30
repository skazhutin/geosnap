"""Verify immutable v8 inputs and open an isolated v9 research workspace."""
import json

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v9.common import OUT, V8, frames, production_guard


def run():
    guard = production_guard("before")
    q, g = frames()
    paths = {"baseline": V8 / "baseline/predictions.npz",
        "licensed_baseline": V8 / "secondary_baseline/predictions.npz",
        "union": V8 / "sage_adaptation_v3/all_model_candidate_union_oracle.npz",
        "union_sources": V8 / "cross_model_union/all_verified_same_gallery.json",
        "adapted": V8 / "sage_adaptation_v3/sage_asymmetric_v3_refonly_epoch2_primary/predictions.npz",
        "quality_diagnostic": V8 / "query_quality_audit/per_query.npz"}
    with np.load(paths["baseline"], allow_pickle=False) as base, \
            np.load(paths["licensed_baseline"], allow_pickle=False) as licensed, \
            np.load(paths["union"], allow_pickle=False) as union:
        for name, source in (("baseline", base), ("licensed", licensed), ("union", union)):
            if source["query_ids"].tolist() != q.id.tolist():
                raise RuntimeError(f"v8 {name} query population/order changed")
        counts = {"baseline_raw100": int((base["errors_m"] <= 100).sum()),
            "licensed_raw100": int((licensed["errors_m"] <= 100).sum()),
            "union_oracle100": int(union["oracle100"].sum())}
    if counts != {"baseline_raw100": 411, "licensed_raw100": 380, "union_oracle100": 652}:
        raise RuntimeError(f"v8 accuracy/recall state changed: {counts}")
    receipt = json.loads((V8 / "integrity_report.json").read_text())
    if not receipt["verified"] or receipt["production"]["files"] != 85:
        raise RuntimeError("v8 provenance receipt invalid")
    result = {"status": "verified_v9_inputs", "queries": len(q), "references": len(g),
        "counts": counts, "production_files": guard["files"],
        "v8_integrity_report_sha256": digest(V8 / "integrity_report.json"),
        "input_hashes": {name: digest(path) for name, path in paths.items()}}
    save(OUT / "input_contract.json", result)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    run()
