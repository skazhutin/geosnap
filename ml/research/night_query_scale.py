"""Contingency: one asymmetric query-resolution test; references stay at 322."""
from __future__ import annotations

import fcntl
import json
import time

import numpy as np

from ml.research.gallery_scale import create_model, exact_scores
from ml.research.gallery_scale_storage import digest, save
from ml.research.night_v7 import LOCAL, allowed_to_start, inputs, record_result, report, save_npz, status


def run():
    cfg, gc, previous, out, queries, _, gallery, _ = inputs()
    phase = "query_scale504"
    with (LOCAL / "controller.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (out / f"{phase}.done.json").exists() or not allowed_to_start(cfg, phase, out):
            return
        fol_finished = (out / "fol.done.json").exists() or (out / "fol.closed.json").exists()
        ready_path = out / "fol_cpu/ready.json"
        cpu_ready = False
        if ready_path.exists():
            ready = json.loads(ready_path.read_text())
            cpu_ready = (ready.get("extraction_complete") is True
                and ready.get("feature_index_sha256") == digest(out / "fol_feature_index.json")
                and ready.get("plan_sha256") == digest(out / "fol_plan.json"))
        if not fol_finished and not cpu_ready:
            raise RuntimeError("finish FoL or validate its CPU-only matching handoff before using the GPU")
        if not (out / f"{phase}.started.json").exists():
            save(out / f"{phase}.started.json", {"phase": phase, "started": time.time(), "config": cfg,
                 "hypothesis": "larger query resolution retains street detail; no query504 or asymmetric-scale trial exists in earlier negative crop/rotation work",
                 "settings": "query504/reference322, and equal cosine blend query322+504; same frozen SAGE-L weights"})
        cache = out / "query504"
        cache.mkdir(exist_ok=True)
        model, pieces = None, []
        contract = {"query_sha256": gc["query_sha256"], "reference_contract_sha256": digest(previous / "descriptor_contract.json"),
                    "query_size": [504, 504], "reference_size": [322, 322], "device": "mps", "batch": 2}
        try:
            for start in range(0, len(queries), 32):
                chunk = queries.iloc[start:start + 32]
                target = cache / f"chunk-{start // 32:04d}.npz"
                if target.exists():
                    with np.load(target, allow_pickle=False) as d:
                        if d["ids"].tolist() != chunk.id.tolist() or json.loads(str(d["contract"])) != contract:
                            raise RuntimeError("query504 cache changed")
                        vectors = d["vectors"]
                else:
                    if model is None:
                        # Instance-only preprocessing change; no production class/config edits.
                        model = create_model(gc)
                        model.image_size = (504, 504)
                        model.batch_size = 2
                        model._transform = model._build_transform()
                    for row in chunk.itertuples():
                        if digest(row.image_path) != row.file_sha256:
                            raise RuntimeError("development photo changed")
                    vectors = model.embed_batch(chunk.image_path.tolist())
                    if vectors.shape != (len(chunk), 8448) or not np.isfinite(vectors).all():
                        raise RuntimeError("invalid query504 vectors")
                    save_npz(target, ids=np.asarray(chunk.id.tolist()), vectors=vectors,
                             contract=np.asarray(json.dumps(contract, sort_keys=True)))
                pieces.append(vectors)
                status(phase, start + len(chunk), len(queries), state="encoding")
            if model is not None:
                model.close()
            model = None
            qv = np.concatenate(pieces)
            pool = json.loads((previous / "descriptor_pool.json").read_text())["entries"]
            scores = exact_scores(gallery, qv, pool)
            # Keep cheap inference-only evidence for a possible later fixed
            # interaction trial; do not read labels or refit any selector here.
            plan_path = out / "fol_plan.json"
            plan = json.loads(plan_path.read_text())
            pairs = np.asarray(plan["prefix"], dtype=np.int32)[:, :plan["depth"]]
            save_npz(out / "query504_fol_pair_scores.npz", query_ids=np.asarray(queries.id.tolist()),
                     prefix=pairs, scores=np.take_along_axis(scores, pairs, axis=1))
            save(out / "query504_fol_pair_scores.json", {"plan_sha256": digest(plan_path),
                 "scores_sha256": digest(out / "query504_fol_pair_scores.npz"), "contract": contract,
                 "query_truth_used": False, "interaction_experiment_started": False})
            for name in ("query504", "query322_504_mean"):
                if name.endswith("mean"):
                    scores += np.load(out / "base_scores.npy", mmap_mode="r", allow_pickle=False)
                    scores *= 0.5
                prefix = np.argsort(-scores, axis=1, kind="stable")[:, :100]
                record_result(name, prefix, note=contract | {"estimator": "top1 reference coordinate", "asymmetric_preprocessing_experiment": True})
            save(out / f"{phase}.done.json", {"completed": time.time()})
            status(phase, state="completed")
        except BaseException as exc:
            status(phase, state="failed", error=repr(exc))
            raise
        finally:
            if model is not None:
                model.close()
        report()


if __name__ == "__main__":
    run()
