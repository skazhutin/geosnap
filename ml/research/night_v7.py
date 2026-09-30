"""Bounded raw-first night experiments; no calibration/final API or production writes."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from ml.research.gallery_scale import exact_scores, query_vectors
from ml.research.gallery_scale_storage import WORKSPACE, digest, save, settings
from ml.research.metrics import paired_group_bootstrap, raw_metrics
from ml.research.vector_evaluation import distances

LOCAL = WORKSPACE / "data/evaluation/moscow_night_v7"
ENCODER_SHA = "b3b49e7aa0e7c848c4c57b2d79660c3654a9946fb9f423f6f0e91681afc414b2"


def paths():
    gc, root, store, previous = settings()
    cfg = json.loads((WORKSPACE / "configs/moscow_night_v7.json").read_text())
    out = root / cfg["run_id"]
    out.mkdir(exist_ok=True)
    LOCAL.mkdir(exist_ok=True)
    return cfg, gc, store, previous, out


def status(phase, completed=0, total=0, **extra):
    _, _, _, _, out = paths()
    value = dict(phase=phase, completed=completed, total=total, pid=os.getpid(), updated=time.time(), **extra)
    save(out / "status.json", value)
    save(LOCAL / "status.json", value)
    print(json.dumps(value), flush=True)


def save_npz(path, **arrays):
    temporary = path.with_suffix(".writing")
    with temporary.open("wb") as f:
        np.savez(f, **arrays)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)


def allowed_to_start(cfg, phase, out):
    if (out / f"{phase}.started.json").exists():
        return True  # Finish or resume this already-started experiment.
    return datetime.now().astimezone() < datetime.fromisoformat(cfg["cutoff"])


def inputs():
    cfg, gc, _, prev, out = paths()
    q, qv = query_vectors(gc)
    gpath = prev / "manifests" / f"{cfg['gallery']}.parquet"
    g = pd.read_parquet(gpath)
    baseline = json.loads((prev / "results" / f"{cfg['gallery']}_rows.json").read_text())
    result = json.loads((prev / "results" / f"{cfg['gallery']}.json").read_text())
    if digest(gpath) != result["gallery_sha256"] or q.id.tolist() != baseline["query_ids"]:
        raise RuntimeError("fixed gallery/query identities changed")
    if len(q) != cfg["query_count"] or len(g) != 100000:
        raise RuntimeError("unexpected population size")
    return cfg, gc, prev, out, q, qv, g, baseline


def prepare():
    cfg, _, prev, out, q, qv, g, baseline = inputs()
    pool = json.loads((prev / "descriptor_pool.json").read_text())
    if pool["contract_sha256"] != digest(prev / "descriptor_contract.json"):
        raise RuntimeError("descriptor contract changed")
    scores = exact_scores(g, qv, pool["entries"])
    order = np.argsort(-scores, axis=1, kind="stable")[:, :cfg["retrieval_depth"]].astype(np.int32)
    if not np.array_equal(order[:, :100], baseline["top100_gallery_rows"]):
        raise RuntimeError("new score cache does not reproduce original v6 top100")
    save_npz(out / "retrieval.npz", indices=order, scores=np.take_along_axis(scores, order, axis=1))
    with (out / "base_scores.npy.writing").open("wb") as f:
        np.save(f, scores, allow_pickle=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(out / "base_scores.npy.writing", out / "base_scores.npy")
    save(out / "input_contract.json", {"gallery_sha256": digest(prev / "manifests/G2_smart.parquet"),
         "query_sha256": digest(WORKSPACE / "data/evaluation/moscow_research_v5/prospective/development.parquet"),
         "descriptor_contract_sha256": pool["contract_sha256"], "pool_sha256": digest(prev / "descriptor_pool.json"),
         "baseline_raw100": float(np.mean(np.asarray(baseline["errors_m"]) <= 100)),
         "top100_parity": True, "scores_sha256": digest(out / "base_scores.npy"),
         "source_scope": "research-only MSLS-containing fixed gallery", "final_or_calibration_access": False})
    record_result("baseline", order[:, :100], note="Existing G2 SAGE-L exact top1; identity-parity confirmed")


def load_vectors(gallery, selected, prev):
    """Gather only required existing descriptors once, without another disk copy."""
    pool = json.loads((prev / "descriptor_pool.json").read_text())["entries"]
    groups = defaultdict(list)
    selected = np.asarray(selected, dtype=np.int32)
    for pos, j in enumerate(selected):
        row = gallery.iloc[int(j)]
        entry = pool[row.id]
        if entry["image_sha256"] != row.file_sha256:
            raise RuntimeError("descriptor image identity mismatch")
        groups[entry["path"]].append((pos, entry["row"]))
    result = np.empty((len(selected), 8448), dtype=np.float32)
    for filename, positions in groups.items():
        block = np.load(filename, mmap_mode="r", allow_pickle=False)
        destination, source = np.asarray(positions).T
        result[destination] = block[source]
        del block
    if not np.isfinite(result).all() or not np.allclose(np.linalg.norm(result, axis=1), 1, atol=1e-5):
        raise RuntimeError("invalid cached descriptors")
    return result


def diverse_prefix(indices, gallery, depth, radius):
    lat, lon = np.radians(gallery.lat.to_numpy(float)), np.radians(gallery.lon.to_numpy(float))
    sequences = gallery.sequence_key.fillna(gallery.id).to_numpy()
    result = []
    for candidates in indices:
        selected, seen = [], set()
        for j in candidates:
            if sequences[j] in seen:
                continue
            if selected and (distances(gallery.lat.iloc[j], gallery.lon.iloc[j], lat[selected], lon[selected]) < radius).any():
                continue
            selected.append(j)
            seen.add(sequences[j])
            if len(selected) == depth:
                break
        if len(selected) < depth:
            selected.extend([j for j in candidates if j not in set(selected)][:depth - len(selected)])
        result.append(selected)
    return np.asarray(result, dtype=np.int32)


def context(diverse=False):
    import torch

    cfg, _, prev, out, q, qv, g, _ = inputs()
    data = np.load(out / "retrieval.npz", allow_pickle=False)
    prefix = data["indices"]
    depth = cfg["context_depth"]
    chosen = (diverse_prefix(prefix, g, depth, cfg["diverse_context_radius_m"])
              if diverse else prefix[:, :depth])
    unique, remap = np.unique(chosen, return_inverse=True)
    vectors = load_vectors(g, unique, prev)
    remap = remap.reshape(chosen.shape)
    checkpoint = WORKSPACE / "data/models/research_v5/sage_context_encoder.pth"
    if digest(checkpoint) != ENCODER_SHA:
        raise RuntimeError("context checkpoint changed")
    torch.set_num_threads(2)
    encoder = torch.nn.TransformerEncoder(torch.nn.TransformerEncoderLayer(
        d_model=768, nhead=16, dim_feedforward=1024, activation="gelu", dropout=0.1, batch_first=False), 2)
    encoder.load_state_dict(torch.load(checkpoint, map_location="cpu", weights_only=True), strict=True)
    encoder.eval()
    phase = "diverse_context" if diverse else "context"
    cache = out / f"{phase}_scores.npz"
    values = np.full(chosen.shape, np.nan, dtype=np.float32)
    if cache.exists():
        old = np.load(cache, allow_pickle=False)
        if not np.array_equal(old["indices"], chosen):
            raise RuntimeError("context cache membership changed")
        values = old["values"]
    started = time.monotonic()
    with torch.inference_mode(), threadpool_limits(limits=2):
        for i in range(len(q)):
            if np.isfinite(values[i]).all():
                continue
            features = np.concatenate([qv[i:i + 1], vectors[remap[i]]])
            output = torch.nn.functional.normalize(encoder(torch.from_numpy(features).view(depth + 1, 11, 768)).flatten(1), dim=1)
            values[i] = (output[0] @ output[1:].T).numpy()
            if (i + 1) % 50 == 0 or i + 1 == len(q):
                save_npz(cache, indices=chosen, values=values)
                status(phase, i + 1, len(q), elapsed_seconds=time.monotonic() - started)
    scores = np.load(out / "base_scores.npy", mmap_mode="r", allow_pickle=False)
    old = np.take_along_axis(scores, chosen, axis=1)
    new = align_blend(old, values, cfg["context_mixing"])
    ranked = np.take_along_axis(chosen, np.argsort(-new, axis=1, kind="stable"), axis=1)
    # A diversity-selected place shortlist is explicitly promoted; preserve tail order.
    full = []
    for i in range(len(q)):
        seen = set(ranked[i])
        full.append(list(ranked[i]) + [j for j in prefix[i] if j not in seen])
    record_result(phase, np.asarray(full)[:, :100], note={"encoder_sha256": ENCODER_SHA,
        "mixing": cfg["context_mixing"], "depth": depth, "diverse": diverse,
        "query_truth_used_in_prediction": False, "transfer_not_official_full_sage": True})


def align_blend(old, new, mixing):
    aligned = ((new - new.mean(axis=1, keepdims=True)) / np.maximum(new.std(axis=1, keepdims=True), 1e-6)
               * np.maximum(old.std(axis=1, keepdims=True), 1e-6) + old.mean(axis=1, keepdims=True))
    return (1 - mixing) * old + mixing * aligned


def panorama():
    from PIL import Image

    from ml.research.gallery_scale import create_model, perspective

    cfg, gc, prev, out, _, qv, g, _ = inputs()
    parents = []
    for i, row in g[g.is_pano.fillna(False)].iterrows():
        if digest(Path(row.image_path)) != row.file_sha256:
            raise RuntimeError("panorama identity changed")
        with Image.open(row.image_path) as image:
            if abs(image.width / image.height - 2) <= 0.08:
                parents.append(i)
    if not parents:
        save(out / "panorama_skipped.json", {"reason": "no verified equirectangular parents"})
        return
    directory = out / "panorama_descriptors"
    directory.mkdir(exist_ok=True)
    model = None
    pieces = []
    contract_sha = digest(prev / "descriptor_contract.json")
    for n, j in enumerate(parents):
        row = g.iloc[j]
        target = directory / f"{row.file_sha256}.npz"
        if target.exists():
            with np.load(target, allow_pickle=False) as data:
                if str(data["image_sha256"]) != row.file_sha256 or str(data["contract_sha256"]) != contract_sha:
                    raise RuntimeError("panorama descriptor fingerprint changed")
                views = data["vectors"]
        else:
            if model is None:
                model = create_model(gc)
                if model.metadata.to_dict() != json.loads((prev / "descriptor_contract.json").read_text())["retriever"]:
                    raise RuntimeError("panorama encoder differs from reference model")
            with Image.open(row.image_path) as image:
                views = model.embed_batch([perspective(image, yaw, 322) for yaw in (0, 90, 180, 270)])
            save_npz(target, vectors=views, image_sha256=np.asarray(row.file_sha256), contract_sha256=np.asarray(contract_sha))
        pieces.append(views)
        status("panorama", n + 1, len(parents))
    if model is not None:
        model.close()
    scores = np.array(np.load(out / "base_scores.npy", mmap_mode="r", allow_pickle=False))
    with threadpool_limits(limits=2):
        scores[:, parents] = (qv @ np.concatenate(pieces).T).reshape(len(qv), len(parents), 4).max(axis=2)
    ranked = np.argsort(-scores, axis=1, kind="stable")[:, :100]
    record_result("panorama4", ranked, note={"parents": len(parents), "perspective_views": len(parents) * 4,
                  "hfov": 90, "parent_score": "max four views, one identity/coordinate", "physical_photo_copies": 0,
                  "A": "baseline", "limited_AB_previously_skipped_not_negative": True})


def dispatch():
    """Run independently of an LLM; stage locks and receipts make resume safe."""
    cfg, _, _, _, out = paths()
    with (LOCAL / "dispatcher.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for phase in ("prepare", "context", "diverse_context", "panorama", "fol"):
            # A standalone phase might already be running from interactive setup.
            # Wait on its lock, not on stale PIDs, then recheck its completion.
            with (LOCAL / "controller.lock").open("a") as worker_lock:
                fcntl.flock(worker_lock, fcntl.LOCK_EX)
                if (out / f"{phase}.done.json").exists():
                    continue
                if not allowed_to_start(cfg, phase, out):
                    break
            with (LOCAL / f"{phase}.log").open("ab") as log:
                result = subprocess.run([sys.executable, "-u", "-m", "ml.research.night_v7", phase],
                                        cwd=WORKSPACE, stdout=log, stderr=subprocess.STDOUT)
            if result.returncode:
                save(out / f"{phase}.failure.json", {"time": time.time(), "returncode": result.returncode,
                                                   "log": str(LOCAL / f"{phase}.log")})
                if phase == "prepare":
                    break
        report()
        status("queue_finished", state="awaiting_review", cutoff=cfg["cutoff"])


def record_result(name, ranked, *, note, extra=None):
    _, _, _, out, q, _, g, baseline = inputs()
    lat, lon = np.radians(g.lat.to_numpy(float)), np.radians(g.lon.to_numpy(float))
    ranked = np.asarray(ranked, dtype=np.int32)
    if ranked.shape != (len(q), 100) or any(len(set(r)) != 100 for r in ranked):
        raise RuntimeError("every query needs 100 distinct reference identities")
    errors, ranks = [], []
    base_ranks = np.array([np.inf if r is None else r for r in baseline["positive_ranks"]])
    # R@k is exact for the emitted prefix. Tail ranks are intentionally not claimed
    # when a diverse shortlist promotes references from beyond rank100.
    for i, row in enumerate(q.itertuples()):
        d = distances(row.lat, row.lon, lat[ranked[i]], lon[ranked[i]])
        errors.append(float(d[0]))
        positive = np.flatnonzero(d <= 100)
        ranks.append(int(positive[0]) + 1 if len(positive) else None)
    truth_counts = baseline["positive_count_100m"]
    within = np.array([np.inf if r is None else r for r in ranks])
    result = {"name": name, "query_count": len(q), "gallery_count": len(g), "raw": raw_metrics(errors),
        "recall_at": {str(k): float(np.mean(within <= k)) for k in (1, 5, 10, 20, 50, 100)},
        "diagnosis": {"no_coverage": int(sum(p == 0 for p in truth_counts)),
                      "retrieval_miss_top100": int(sum(p > 0 and not np.isfinite(r) for p, r in zip(truth_counts, within, strict=True))),
                      "wrong_top1_positive_in_top100": int(((within > 1) & np.isfinite(within)).sum()),
                      "correct_top1": int((within == 1).sum())},
        "paired_vs_baseline": paired_group_bootstrap(baseline["errors_m"], errors, q.h3_coarse.astype(str).tolist()),
        "note": note, "abstention": False, "final_or_calibration_access": False,
        "rank_reporting": "exact through 100; positive ranks outside emitted prefix not measured"}
    if extra:
        result.update(extra)
    rows = {"query_ids": q.id.tolist(), "errors_m": errors, "positive_ranks_through100": ranks,
            "top100_gallery_rows": ranked.tolist(), "baseline_positive_ranks": [None if not np.isfinite(x) else int(x) for x in base_ranks]}
    save(out / "results" / f"{name}_rows.json", rows)
    save(out / "results" / f"{name}.json", result)
    print(name, json.dumps(result["raw"]), flush=True)
    report()
    return result


def report():
    cfg, _, _, _, out = paths()
    directory = out / "results"
    records = [json.loads(p.read_text()) for p in directory.glob("*.json")
               if not p.name.startswith("._") and not p.name.endswith("_rows.json")]
    if not records:
        return
    records.sort(key=lambda r: (-r["raw"]["accuracy_100m"], -r["raw"]["accuracy_50m"],
                              -r["raw"]["accuracy_25m"], r["raw"]["catastrophic_gt500m_rate"]))
    save(out / "leaderboard.json", {"candidate": records[0]["name"], "records": records,
         "development_only": True, "final_opened": False, "production_changed": False, "cutoff": cfg["cutoff"]})
    text = "# GeoSnap — ночные development-эксперименты\n\n1184 фиксированных запроса; без abstention. Final не открывался в этой работе. MSLS — research-only.\n\n"
    text += "| Вариант | ≤25 м | ≤50 м | ≤100 м | median, м | p90, м | >500 м | R@1/10/100 |\n|---|---:|---:|---:|---:|---:|---:|---|\n"
    for r in records:
        m, recall = r["raw"], r["recall_at"]
        text += f"| {r['name']} | {100*m['accuracy_25m']:.2f}% | {100*m['accuracy_50m']:.2f}% | {100*m['accuracy_100m']:.2f}% | {m['median_error_m']:.0f} | {m['p90_error_m']:.0f} | {100*m['catastrophic_gt500m_rate']:.2f}% | " + "/".join(f"{100*recall[str(k)]:.2f}" for k in (1,10,100)) + " |\n"
    text += "\nРезультаты выбора на development являются исследовательскими, не оценкой на независимом final.\n"
    (out / "report.md").write_text(text)
    (LOCAL / "report.md").write_text(text)
    save(LOCAL / "leaderboard.json", {"candidate": records[0]["name"], "records": records})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["prepare", "context", "diverse_context", "panorama", "fol", "run", "report"])
    args = parser.parse_args()
    cfg, _, _, _, out = paths()
    if args.phase == "run":
        dispatch()
        return
    with (LOCAL / "controller.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        phase = args.phase
        if phase == "report":
            report()
            return
        if (out / f"{phase}.done.json").exists():
            return
        if not allowed_to_start(cfg, phase, out):
            status("cutoff_no_new_experiments", state="waiting_for_report")
            return
        started = out / f"{phase}.started.json"
        if started.exists() and json.loads(started.read_text())["config"] != cfg:
            raise RuntimeError("cannot change configuration of a started experiment")
        if not started.exists():
            save(started, {"started": time.time(), "phase": phase, "config": cfg,
                           "source_sha256": digest(Path(__file__))})
        status(phase, state="running")
        try:
            if phase == "prepare":
                prepare()
            elif phase == "panorama":
                panorama()
            elif phase == "fol":
                from ml.research.night_fol import run
                run()
            else:
                context(diverse=phase == "diverse_context")
            save(out / f"{phase}.done.json", {"completed": time.time(), "phase": phase})
            status(phase, state="completed")
        except BaseException as exc:
            status(phase, state="failed", error=repr(exc))
            raise


if __name__ == "__main__":
    main()
