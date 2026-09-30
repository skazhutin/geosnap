"""Zero-shot visual-context selection and failure transitions (development only)."""
from __future__ import annotations

import numpy as np

from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import compare, metrics
from ml.research.geographic_v9.common import OUT, frames, record

SOURCE = OUT / "hypotheses_visual30"


def run():
    q, g = frames()
    with np.load(SOURCE / "visual_scores.npz", allow_pickle=False) as z:
        if z["query_ids"].tolist() != q.id.tolist():
            raise RuntimeError("Visual score population changed")
        base = z["baseline_rows"].copy()
        challenger = z["challenger_rows"].copy()
        values = z["contextual_scores"].copy()
    rows = np.column_stack((base, challenger))
    selected = rows[np.arange(len(q)), np.argmax(values, axis=1)]
    result, errors, _ = metrics(q, g, selected[:, None])
    paired = compare(q, errors)
    report = {"status": "complete_negative_or_exploratory",
        "variant": "frozen_SAGE_visual_context_over_anchor_plus_30_hypotheses",
        "fitted": False, "query_gt_in_inference": False,
        "raw": result["raw"], "transitions": paired["transitions"],
        "original_failure_buckets": paired["buckets"],
        "paired_geographic_bootstrap": paired["paired_geographic_bootstrap"],
        "switches": int((selected != base).sum()),
        "source_visual_scores_sha256": digest(SOURCE / "visual_scores.npz"),
        "evaluation_note": "1184 repeatedly used development queries; no final claim"}
    np.savez_compressed(SOURCE / "zero_shot_visual_selection.npz",
        query_ids=q.id.to_numpy(str), baseline_rows=base,
        selected_rows=selected, errors_m=errors)
    save(SOURCE / "zero_shot_visual_selection.json", report)
    record("visual_context30_zero_shot_select_v1", {"status": "complete",
        "fitted": False, "result": report,
        "artifact_paths": [str(SOURCE / "zero_shot_visual_selection.npz"),
                           str(SOURCE / "zero_shot_visual_selection.json")],
        "artifact_hashes": {f: digest(SOURCE / f) for f in (
            "zero_shot_visual_selection.npz", "zero_shot_visual_selection.json")}})
    print({"correct": int((errors <= 100).sum()),
           "switches": report["switches"], "transitions": report["transitions"]})


if __name__ == "__main__":
    run()
