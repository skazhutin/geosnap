"""Conditional top100 extension of the one FoL checkpoint; reuse all local caches."""
from __future__ import annotations

import fcntl
import json
import time

import numpy as np
import torch
from threadpoolctl import threadpool_limits

from ml.research import night_fol as fol
from ml.research.gallery_scale_storage import save
from ml.research.metrics import paired_group_bootstrap
from ml.research.night_v7 import LOCAL, allowed_to_start, inputs, save_npz, status


def run():
    cfg, _, _, out, q, _, g, _ = inputs()
    phase = "fol100"
    with (LOCAL / "controller.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (out / f"{phase}.done.json").exists() or (out / f"{phase}.closed.json").exists():
            return
        if not (out / "fol.done.json").exists() or not allowed_to_start(cfg, phase, out):
            return
        old_plan = json.loads((out / "fol_plan.json").read_text())
        board = json.loads((out / "leaderboard.json").read_text())
        choices = [r for r in board["records"] if r["name"].startswith("fol_")]
        best = max(choices, key=lambda r: r["raw"]["accuracy_100m"])
        best_rows = json.loads((out / "results" / f"{best['name']}_rows.json").read_text())
        before = json.loads((out / "results" / f"{old_plan['base']}_rows.json").read_text())
        gain = paired_group_bootstrap(before["errors_m"], best_rows["errors_m"], q.h3_coarse.astype(str).tolist())
        if gain["gain_pp"] < 2 or gain["ci95_pp"][0] <= 0:
            save(out / f"{phase}.closed.json", {"reason": "top20 gain fails preregistered >=2pp and positive CI gate",
                                               "best_top20": best["name"], "paired_vs_base": gain})
            return
        if not (out / f"{phase}.started.json").exists():
            save(out / f"{phase}.started.json", {"started": time.time(), "phase": phase, "config": cfg,
                                                "gate": gain, "depth": 100, "no_new_checkpoint": True})
        stage = out / "fol100"
        stage.mkdir(exist_ok=True)
        source_scores = stage / "base_scores.npy"
        if not source_scores.exists():
            source_scores.symlink_to(out / "base_scores.npy")
        prefix = np.asarray(old_plan["prefix"], np.int32)
        records = []
        for role, frame in [("query", q), ("reference", g.iloc[np.unique(prefix)])]:
            for row in frame.itertuples():
                records.append({"id": row.id, "sha256": row.file_sha256, "path": row.image_path, "role": role})
        plan = dict(old_plan, depth=100, images=records)
        save(stage / "plan.json", plan)
        torch.set_num_threads(2)
        model, audit = fol.load_model(out)
        entries = fol.extract(model, audit, plan, out, cfg)
        del model
        torch.mps.empty_cache()
        cache = stage / "fol_pair_evidence.npz"
        if not cache.exists():
            with np.load(out / "fol_pair_evidence.npz", allow_pickle=False) as old:
                if not np.array_equal(old["prefix"], prefix):
                    raise RuntimeError("the original top20 prefix changed")
                evidence = np.full((len(q), 100, 4), np.nan, np.float32)
                evidence[:, :old_plan["depth"]] = old["evidence"]
            save_npz(cache, prefix=prefix, evidence=evidence)
        # Reuse the exact same scorer and five-fold learner. Namespace only its
        # reports; original top20 experiment files are never overwritten.
        recorder = fol.record_result

        def namespaced(name, ranked, **kwargs):
            return recorder(f"fol100_{name}", ranked, **kwargs)

        fol.record_result = namespaced
        try:
            with threadpool_limits(limits=2):
                fol.rerank(plan, entries, stage, cfg, q, g)
        finally:
            fol.record_result = recorder
        save(out / f"{phase}.done.json", {"completed": time.time(), "gate": gain})
        status(phase, state="completed")


if __name__ == "__main__":
    run()
