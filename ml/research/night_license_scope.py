"""One license-defined gallery ablation using already committed exact scores."""
from __future__ import annotations

import fcntl
import json
import time
from pathlib import Path

import numpy as np
from threadpoolctl import threadpool_limits

from ml.research.gallery_scale import summarize
from ml.research.gallery_scale_storage import digest, save
from ml.research.metrics import paired_group_bootstrap
from ml.research.night_v7 import LOCAL, allowed_to_start, inputs
from ml.research.night_v7_verify import gallery_truth, reconcile_result, verify_full_ranks


class SelectedColumns:
    def __init__(self, scores, selected):
        self.scores, self.selected = scores, selected
        self.shape = (len(scores), len(selected))

    def __getitem__(self, index):
        return self.scores[index][self.selected]


def run():
    cfg, _, previous, night, q, _, gallery, base = inputs()
    out = night / "license_scope"
    out.mkdir(exist_ok=True)
    with (LOCAL / "license_scope.lock").open("a") as lock, threadpool_limits(limits=1):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (out / "done.json").exists() or not allowed_to_start(cfg, "license_scope", out):
            return
        fixed = json.loads((night / "input_contract.json").read_text())
        contract = {"source_sha256": digest(Path(__file__)), "input_sha256": digest(night / "input_contract.json"),
                    "selection": "production_compatible == True; sources Mapillary/KartaView and CC BY-SA 4.0 only",
                    "query_coordinates_used_for_selection": False, "inference": "same cached SAGE-L322 exact cosine top1",
                    "scope": "auxiliary license-defined data ablation; no parameter or threshold search"}
        marker = out / "license_scope.started.json"
        if marker.exists() and json.loads(marker.read_text())["contract"] != contract:
            raise RuntimeError("license-scope diagnostic contract changed")
        if not marker.exists():
            save(marker, {"started": time.time(), "contract": contract})
        selected = np.flatnonzero(gallery.production_compatible.to_numpy(bool))
        reference = gallery.iloc[selected].reset_index(drop=True)
        if not reference.source.isin(["mapillary", "kartaview"]).all() or not reference.license.eq("CC BY-SA 4.0").all():
            raise RuntimeError("license-defined subset contains incompatible provenance")
        reference.to_parquet(out / "gallery.parquet", index=False)
        save(out / "selection.json", {"original_gallery_rows": selected.tolist(), "contract": contract,
             "gallery_sha256": digest(out / "gallery.parquet")})
        if digest(night / "base_scores.npy") != fixed["scores_sha256"]:
            raise RuntimeError("cached exact scores changed")
        scores = np.load(night / "base_scores.npy", mmap_mode="r", allow_pickle=False)
        subset = SelectedColumns(scores, selected)
        result, rows = summarize(reference, q, subset)
        ranks = np.array([np.inf if x is None else x for x in rows["positive_ranks"]])
        result.update(name="production_compatible_raw", query_count=len(q), gallery_count=len(reference),
            recall_at={str(k): float(np.mean(ranks <= k)) for k in (1, 5, 10, 20, 50, 100)},
            paired_vs_full_G2=paired_group_bootstrap(base["errors_m"], rows["errors_m"], q.h3_coarse.astype(str).tolist()),
            contract=contract, final_or_calibration_access=False, abstention=False)
        truth = gallery_truth(q, reference)
        verify_full_ranks(rows, truth, subset)
        verified = reconcile_result(result, rows, q, reference, truth)
        save(out / "rows.json", rows)
        save(out / "report.json", result)
        save(out / "verification.json", verified)
        scores._mmap.close()
        save(out / "done.json", {"completed": time.time(), "report_sha256": digest(out / "report.json"),
             "rows_sha256": digest(out / "rows.json"), "query_count": len(q), "new_image_descriptors": 0})
        print(json.dumps({"raw": result["raw"], "coverage": result["coverage"],
              "gallery": result["gallery"], "paired_vs_full_G2": result["paired_vs_full_G2"]}), flush=True)


if __name__ == "__main__":
    run()
