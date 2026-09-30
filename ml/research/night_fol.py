"""One official FoL-L local reranking experiment with resumable chunk caches."""
from __future__ import annotations

import json
import subprocess
import sys
import time
from collections import OrderedDict
from pathlib import Path

import numpy as np
import torch
from PIL import Image
from threadpoolctl import threadpool_limits

from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.night_v7 import align_blend, inputs, record_result, save_npz, status


def load_model(out):
    """Keep upstream unchanged; suppress an unnecessary foundation download only."""
    source = out / "upstream/FoL"
    audit = json.loads((out / "fol_source_audit.json").read_text())
    revision = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    if revision != audit["code_revision"] or subprocess.run(["git", "-C", str(source), "diff", "--quiet", "HEAD"]).returncode:
        raise RuntimeError("official FoL source changed")
    path = out / "models/FoL_large.pth"
    if not path.is_file():
        raise RuntimeError("pinned FoL checkpoint download must finish before starting")
    checkpoint_sha = digest(path)
    if audit.get("checkpoint_sha256") not in (None, checkpoint_sha):
        raise RuntimeError("FoL checkpoint changed")
    sys.path.insert(0, str(source))
    from network_FoL import FoLNet

    local_dino = WORKSPACE / ".cache/torch/hub/facebookresearch_dinov2_7764ea0f912e53c92e82eb78a2a1631e92725fc8"
    original = torch.hub.load

    def local_foundation(repo, name, *args, **kwargs):
        if repo != "facebookresearch/dinov2":
            raise RuntimeError("unexpected upstream dependency")
        return original(str(local_dino), name, source="local", pretrained=False)

    torch.hub.load = local_foundation
    try:
        model = FoLNet(num_channels=1024, model_name="dinov2_vitl14", num_trainable_blocks=4)
    finally:
        torch.hub.load = original
    numpy_globals = [(np.core.multiarray._reconstruct, "numpy.core.multiarray._reconstruct"),
                     (np.core.multiarray.scalar, "numpy.core.multiarray.scalar"),
                     np.ndarray, np.dtype, type(np.dtype(np.float32)), type(np.dtype(np.float64))]
    with torch.serialization.safe_globals(numpy_globals):
        state = torch.load(path, map_location="cpu", weights_only=True, mmap=True)
    state = {k.removeprefix("module."): v for k, v in state["model_state_dict"].items()}
    # Released weights retain this historical scalar; current official forward
    # uses (1 - mask) instead. Preserve it for strict loading, never drop keys.
    extra = set(state) - set(model.state_dict())
    if extra == {"aggregator.dust_bin"} and state["aggregator.dust_bin"].numel() == 1:
        model.aggregator.register_parameter("dust_bin", torch.nn.Parameter(state["aggregator.dust_bin"].clone(), requires_grad=False))
        audit["checkpoint_compatibility"] = "preserve obsolete aggregator.dust_bin scalar; unused by current official forward; all other keys strict"
    model.load_state_dict(state, strict=True)
    del state
    if not torch.backends.mps.is_available():
        raise RuntimeError("MPS required; no silent CPU fallback for this timed experiment")
    model = model.eval().to("mps")
    audit.update(checkpoint_sha256=checkpoint_sha, strict_state_dict=True,
                 foundation_source=str(local_dino), foundation_pretrained_download=False,
                 device="mps", preprocessing="official RGB -> tensor -> ImageNet normalize -> bilinear resize322 antialias",
                 numerical_reproduction="research MPS/torch2.13; not paper faiss-gpu/torch2.0 environment")
    save(out / "fol_source_audit.json", audit)
    return model, audit


def preprocess(path, size):
    from torchvision.transforms import functional as tf
    with Image.open(path) as source:
        x = tf.to_tensor(source.convert("RGB"))
    return tf.resize(tf.normalize(x, [0.485, 0.456, 0.406], [0.229, 0.224, 0.225]), [size, size], antialias=True)


def forward(model, tensors):
    with torch.inference_mode():
        output = model(torch.stack(tensors).to("mps"), test=True)
        global_values = output[0].cpu().numpy()
        locals_ = output[1].cpu().numpy()
    # Removing zero padding cannot create a >0.7 cosine match. Keep float32.
    locals_ = [a[np.linalg.norm(a, axis=1) > 0.5].astype(np.float32) for a in locals_]
    if not np.isfinite(global_values).all() or any(not np.isfinite(a).all() for a in locals_):
        raise RuntimeError("nonfinite FoL output")
    return global_values, locals_


def match(a, b, device="mps"):
    """Official >0.7 mutual-nearest-neighbor count, plus continuous evidence.

    Explicitly handles the upstream squeeze/single-match edge case correctly.
    No global coordinate or query truth enters matching.
    """
    if len(a) == 0 or len(b) == 0:
        return [0., 0., 0.]
    with torch.inference_mode():
        left = torch.from_numpy(a).to(device)
        right = torch.from_numpy(b).to(device)
        similarity = left @ right.T
        neighbor = similarity.argmax(1)
        i = torch.arange(len(a), device=device)
        values = similarity[i, neighbor]
        valid = (similarity.argmax(0)[neighbor] == i) & (values > 0.7)
        count = int(valid.sum().item())
        average = float(values[valid].mean().item()) if count else 0.
        return [float(count), count / max(1, min(len(a), len(b))), average]


def prepare_plan(out, cfg, queries, gallery):
    destination = out / "fol_plan.json"
    if destination.exists():
        return json.loads(destination.read_text())
    leaderboard = json.loads((out / "leaderboard.json").read_text())
    eligible = [r for r in leaderboard["records"] if r["name"] in ("baseline", "context", "diverse_context")]
    best = max(eligible, key=lambda r: (r["raw"]["accuracy_100m"], r["raw"]["accuracy_50m"]))["name"]
    rows = json.loads((out / "results" / f"{best}_rows.json").read_text())
    prefix = np.asarray(rows["top100_gallery_rows"], dtype=np.int32)
    chosen = np.unique(prefix[:, :cfg["fol_depth"]])
    records = []
    for role, frame in [("query", queries), ("reference", gallery.iloc[chosen])]:
        for row in frame.itertuples():
            records.append({"id": row.id, "sha256": row.file_sha256, "path": row.image_path, "role": role})
    result = {"base": best, "depth": cfg["fol_depth"], "prefix": prefix.tolist(), "images": records,
              "selection_inputs": "fixed global/context scores; query ground truth used only for overall architecture selection",
              "coordinates_used_for_image_selection": False}
    save(destination, result)
    return result


def extract(model, audit, plan, out, cfg):
    directory = out / "fol_features"
    directory.mkdir(exist_ok=True)
    contract = {"checkpoint_sha256": audit["checkpoint_sha256"], "code_revision": audit["code_revision"],
                "device": "mps", "size": cfg["fol_size"], "dtype": "float32", "local_rule": "official225 regions; remove zero padding only"}
    committed = {}
    for receipt_path in sorted(directory.glob("chunk-*.json")):
        receipt = json.loads(receipt_path.read_text())
        if receipt["contract"] != contract:
            raise RuntimeError("FoL cache fingerprint mismatch")
        chunk = receipt_path.with_suffix(".npz")
        if digest(chunk) != receipt["sha256"]:
            raise RuntimeError("FoL committed chunk changed")
        for i, row in enumerate(receipt["images"]):
            committed[row["id"]] = {"path": str(chunk), "row": i, "image_sha256": row["sha256"]}
    missing = [r for r in plan["images"] if r["id"] not in committed]
    for row in plan["images"]:
        if row["id"] in committed and committed[row["id"]]["image_sha256"] != row["sha256"]:
            raise RuntimeError("FoL image content differs")
    serial = max([int(p.stem.split("-")[1]) for p in directory.glob("chunk-*.npz")], default=-1) + 1
    started = time.monotonic()
    for start in range(0, len(missing), 32):
        chunk_rows = missing[start:start + 32]
        local_values, global_values = [], []
        for offset in range(0, len(chunk_rows), 2):
            batch = chunk_rows[offset:offset + 2]
            tensors = []
            for row in batch:
                if digest(Path(row["path"])) != row["sha256"]:
                    raise RuntimeError("physical photo changed before FoL extraction")
                tensors.append(preprocess(row["path"], cfg["fol_size"]))
            gv, lv = forward(model, tensors)
            global_values.extend(gv)
            local_values.extend(lv)
        offsets = np.r_[0, np.cumsum([len(v) for v in local_values])]
        chunk = directory / f"chunk-{serial:06d}.npz"
        save_npz(chunk, local=np.concatenate(local_values), offsets=offsets,
                 global_vectors=np.asarray(global_values), ids=np.asarray([r["id"] for r in chunk_rows]))
        save(chunk.with_suffix(".json"), {"contract": contract, "images": chunk_rows, "sha256": digest(chunk)})
        for j, row in enumerate(chunk_rows):
            committed[row["id"]] = {"path": str(chunk), "row": j, "image_sha256": row["sha256"]}
        serial += 1
        status("fol_extract", len(committed), len(plan["images"]),
               new_completed=start + len(chunk_rows), new_total=len(missing), elapsed_seconds=time.monotonic() - started)
    save(out / "fol_feature_index.json", {"contract": contract, "entries": committed})
    return committed


class FeatureStore:
    def __init__(self, entries, max_chunks=4):
        self.entries, self.max_chunks = entries, max_chunks
        self.chunks = OrderedDict()

    def get(self, identity):
        entry = self.entries[identity]
        path = entry["path"]
        if path not in self.chunks:
            with np.load(path, allow_pickle=False) as values:
                self.chunks[path] = (values["local"], values["offsets"], values["global_vectors"])
            if len(self.chunks) > self.max_chunks:
                self.chunks.popitem(last=False)
        self.chunks.move_to_end(path)
        local, offsets, global_vectors = self.chunks[path]
        j = entry["row"]
        return local[offsets[j]:offsets[j + 1]], global_vectors[j]


def rerank(plan, entries, out, cfg, q, g):
    prefix = np.asarray(plan["prefix"], dtype=np.int32)
    depth = plan["depth"]
    cache = out / "fol_pair_evidence.npz"
    values = np.full((len(q), depth, 4), np.nan, dtype=np.float32)
    if cache.exists():
        with np.load(cache, allow_pickle=False) as old:
            if not np.array_equal(old["prefix"], prefix):
                raise RuntimeError("FoL pair membership changed")
            values = old["evidence"]
    store = FeatureStore(entries)
    started = time.monotonic()
    # Query locals are tiny compared to ref cache; reference-sorted matching avoids
    # rereading a 60MB NTFS chunk for every random pair.
    query_local = [store.get(r.id) for r in q.itertuples()]
    tasks = [(int(prefix[i, j]), i, j) for i in range(len(q)) for j in range(depth)
             if not np.isfinite(values[i, j]).all()]
    tasks.sort(key=lambda t: (entries[g.id.iloc[t[0]]]["path"], entries[g.id.iloc[t[0]]]["row"], t[1]))
    last_ref, ref = None, None
    for n, (ref_row, i, j) in enumerate(tasks, 1):
        if ref_row != last_ref:
            ref = store.get(g.id.iloc[ref_row])
            last_ref = ref_row
        query = query_local[i]
        values[i, j] = [*match(query[0], ref[0]), float(query[1] @ ref[1])]
        if n % 100 == 0 or n == len(tasks):
            save_npz(cache, prefix=prefix, evidence=values)
            status("fol_match", int(np.isfinite(values[:, :, 0]).sum()), len(q) * depth,
                   elapsed_seconds=time.monotonic() - started)
    for name, scores in [("fol_global_in_prefix", values[:, :, 3]), ("fol_mnn", values[:, :, 0]),
                         ("fol_blend", align_blend(-np.broadcast_to(np.log1p(np.arange(depth)), (len(q), depth)),
                                                   np.log1p(values[:, :, 0]), 0.5))]:
        order = prefix.copy()
        order[:, :depth] = np.take_along_axis(prefix[:, :depth], np.argsort(-scores, axis=1, kind="stable"), axis=1)
        record_result(name, order, note={"base": plan["base"], "depth": depth,
                      "official_mnn_cosine_threshold": 0.7, "tie_break": "original candidate order",
                      "local_descriptors": "official FoL-L float32, zero padding removed",
                      "blend": "half standardized log-count and negative log-rank" if name == "fol_blend" else None})
    learned_fusion(values, prefix, plan, out, q, g)


def learned_fusion(evidence, prefix, plan, out, q, g):
    """One fixed learner; geographically held-out predictions, never fitted scores."""
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.model_selection import GroupKFold

    from ml.research.confidence_experiment import portable, predict_portable
    from ml.research.vector_evaluation import distances

    depth = plan["depth"]
    base = np.load(out / "base_scores.npy", mmap_mode="r", allow_pickle=False)
    sage = np.take_along_axis(base, prefix[:, :depth], axis=1)
    features = np.stack([
        sage, sage - sage.max(1, keepdims=True),
        np.broadcast_to(np.log1p(np.arange(depth)), sage.shape),
        np.log1p(evidence[:, :, 0]), evidence[:, :, 1], evidence[:, :, 2],
        evidence[:, :, 3], evidence[:, :, 3] - evidence[:, :, 3].max(1, keepdims=True),
        np.log1p(evidence[:, :, 0]) - np.log1p(evidence[:, :, 0]).max(1, keepdims=True),
    ], axis=-1).astype(np.float64)
    names = ["sage_cosine", "sage_gap", "candidate_logrank", "mnn_logcount", "mnn_fraction",
             "mnn_mean_cosine", "fol_global_cosine", "fol_global_gap", "mnn_logcount_gap"]
    # Labels are constructed only after all inference-only features exist.
    lat, lon = np.radians(g.lat.to_numpy(float)), np.radians(g.lon.to_numpy(float))
    labels = np.array([distances(row.lat, row.lon, lat[prefix[i, :depth]], lon[prefix[i, :depth]]) <= 100
                       for i, row in enumerate(q.itertuples())])
    groups = q.h3_coarse.astype(str).to_numpy()
    predicted = np.full(labels.shape, np.nan)
    fold_records = []

    def model():
        return HistGradientBoostingClassifier(max_leaf_nodes=7, max_iter=100, learning_rate=0.06,
                    min_samples_leaf=40, l2_regularization=5, early_stopping=False, random_state=20260909)

    with threadpool_limits(limits=2):
        for fold, (train, test) in enumerate(GroupKFold(n_splits=5).split(features, groups=groups)):
            if set(groups[train]) & set(groups[test]):
                raise RuntimeError("geographic fold leakage")
            fitted = model().fit(features[train].reshape(-1, len(names)), labels[train].ravel())
            predicted[test] = fitted.predict_proba(features[test].reshape(-1, len(names)))[:, 1].reshape(len(test), depth)
            fold_records.append({"fold": fold, "train_query_count": len(train), "validation_query_ids": q.id.iloc[test].tolist(),
                                 "validation_groups": sorted(set(groups[test]))})
        if not np.isfinite(predicted).all():
            raise RuntimeError("missing held-out FoL ranking predictions")
        final_model = model().fit(features.reshape(-1, len(names)), labels.ravel())
        exported = portable(final_model, names, "hgb")
        check = features.reshape(-1, len(names))[::101]
        expected = final_model.predict_proba(check)[:, 1]
        actual = predict_portable(exported, [dict(zip(names, row, strict=True)) for row in check])
        if not np.allclose(actual, expected, atol=1e-10):
            raise RuntimeError("portable FoL ranking model differs from fitted estimator")
    save(out / "fol_ranker.json", exported)
    save(out / "fol_ranker_folds.json", {"folds": fold_records, "features": names,
         "training": "all development refit exported only; reported scores always held-out by H3r6 group",
         "hyperparameters": "single preregistered HGB7/100iterations; no sweep", "final_or_calibration_access": False})
    order = prefix.copy()
    order[:, :depth] = np.take_along_axis(prefix[:, :depth], np.argsort(-predicted, axis=1, kind="stable"), axis=1)
    record_result("fol_learned_oof", order, note={"base": plan["base"], "depth": depth,
         "validation": "five H3r6 geographic groups folds, one fixed family, all-query out-of-fold predictions",
         "new_evidence_vs_negative_XFeat": "FoL trained local and global descriptors",
         "deployed_full_fit_accuracy_not_measured": True}, extra={"evidence_kind": "geographically_grouped_oof"})


def run():
    cfg, _, _, out, q, _, g, _ = inputs()
    torch.set_num_threads(2)
    model, audit = load_model(out)
    smoke_path = out / "fol_smoke.json"
    if not smoke_path.exists():
        tensors = [preprocess(r.image_path, cfg["fol_size"]) for r in q.iloc[:2].itertuples()]
        started = time.monotonic()
        batch_global, batch_local = forward(model, tensors)
        single_global, single_local = forward(model, tensors[:1])
        if single_local[0].shape != batch_local[0].shape:
            raise RuntimeError("FoL changes selected region membership with batch")
        difference = float(np.max(np.abs(single_local[0] - batch_local[0])))
        if difference > 2e-4 or not np.allclose(single_global[0], batch_global[0], atol=2e-4):
            raise RuntimeError("FoL batch-invariance smoke failed")
        mps = match(batch_local[0][:128], batch_local[1][:128])
        cpu = match(batch_local[0][:128], batch_local[1][:128], device="cpu")
        if mps[0] != cpu[0]:
            raise RuntimeError("CPU/MPS mutual matching smoke mismatch")
        save(smoke_path, {"seconds_three_images": time.monotonic() - started, "batch_max_local_difference": difference,
            "local_shapes": [list(v.shape) for v in batch_local], "matching_cpu_mps_counts_equal": True,
            "checkpoint_sha256": audit["checkpoint_sha256"], "no_query_label_used": True})
    plan = prepare_plan(out, cfg, q, g)
    entries = extract(model, audit, plan, out, cfg)
    del model
    torch.mps.empty_cache()
    with threadpool_limits(limits=2):
        rerank(plan, entries, out, cfg, q, g)


if __name__ == "__main__":
    run()
