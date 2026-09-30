"""One small reference-only acquisition addition; preserve every G2 reference."""
from __future__ import annotations

import argparse
import fcntl
import io
import json
import os
import sqlite3
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import imagehash
import numpy as np
import pandas as pd
import shapely
from PIL import Image, ImageOps
from threadpoolctl import threadpool_limits

from ml.evaluation.moscow_split import _PerceptualHashIndex
from ml.ingestion.mapillary_citywide import load_aoi_boundary
from ml.research.gallery_scale import (
    create_model,
    decorate,
    exact_scores,
    fingerprint_bytes,
    fixed_development,
    gallery_role,
    query_guard,
    query_vectors,
    summarize,
)
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.metrics import paired_group_bootstrap
from ml.research.night_v7 import LOCAL, allowed_to_start, paths, report


def state(out, phase, **extra):
    value = dict(phase=phase, pid=os.getpid(), updated=time.time(), **extra)
    save(out / "status.json", value)
    save(LOCAL / "gallery_addition_status.json", value)
    print(json.dumps(value), flush=True)


def atomic_array(path, values):
    temporary = path.with_suffix(".writing")
    with temporary.open("wb") as f:
        np.save(f, values, allow_pickle=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)


def fingerprint_file(path):
    data = Path(path).read_bytes()
    fp = fingerprint_bytes(data)
    if fp["decode_ok"]:
        with Image.open(io.BytesIO(data)) as opened:
            original = ImageOps.exif_transpose(opened).convert("RGB")
            original.load()
        variants = []
        for rotation in (None, Image.Transpose.ROTATE_90, Image.Transpose.ROTATE_180, Image.Transpose.ROTATE_270):
            rotated = original if rotation is None else original.transpose(rotation)
            variants.extend(str(imagehash.phash(im)) for im in (rotated, ImageOps.mirror(rotated)))
        fp["phash_variants"] = variants
    return fp


def filter_fingerprints(candidates, fingerprints, baseline, guard):
    """Only identity gates; no query coordinates or outcome-driven selection."""
    exact, pixels, identities, forbidden, query_index = guard
    leaked = set(forbidden)
    newly_implicated = set()
    eligible, counts = [], Counter()
    for row in candidates.to_dict("records"):
        fp = fingerprints.get(row["id"], {"decode_ok": False})
        if not fp["decode_ok"]:
            counts["missing_or_undecodable"] += 1
            continue
        provider = "mapillary" if row["source"] == "msls" else row["source"]
        if ((provider, str(row["source_image_id"])) in identities or fp["file_sha256"] in exact
                or fp["pixel_sha256"] in pixels
                or any(query_index.matches(int(value, 16)) for value in fp["phash_variants"])):
            leaked.add(row["sequence_key"])
            newly_implicated.add(row["sequence_key"])
            counts["query_identity_or_phash"] += 1
            continue
        eligible.append(row | {k: v for k, v in fp.items() if k != "phash_variants"})
    # A newly discovered baseline leak requires review; never silently rewrite G2.
    if set(baseline.sequence_key) & newly_implicated:
        raise RuntimeError("new identity audit implicates an existing G2 sequence; baseline needs scientific review")
    ref_index = _PerceptualHashIndex(4)
    for i, value in enumerate(baseline.perceptual_hash):
        ref_index.add(i, int(value, 16))
    hashes, decoded = set(baseline.file_sha256), set(baseline.pixel_sha256.dropna())
    physical = {(('mapillary' if r.source == 'msls' else r.source), str(r.source_image_id)) for r in baseline.itertuples()}
    retained = []
    for row in eligible:
        if row["sequence_key"] in leaked:
            counts["prohibited_sequence"] += 1
            continue
        identity = (row["source"], str(row["source_image_id"]))
        if identity in physical or row["file_sha256"] in hashes or row["pixel_sha256"] in decoded:
            counts["exact_reference_duplicate"] += 1
            continue
        variants = fingerprints[row["id"]]["phash_variants"]
        if any(ref_index.matches(int(value, 16)) for value in variants):
            counts["reference_phash_duplicate"] += 1
            continue
        retained.append(row)
        hashes.add(row["file_sha256"])
        decoded.add(row["pixel_sha256"])
        physical.add(identity)
        ref_index.add(len(baseline) + len(retained), int(row["perceptual_hash"], 16))
    return retained, dict(counts)


def context_paths():
    cfg, gc, store, previous, night = paths()
    out, acquisition = night / "gallery_addition", night / "reference_acquisition"
    out.mkdir(exist_ok=True)
    return cfg, gc, store, previous, night, out, acquisition


def source_compatible(registered, out):
    current = digest(Path(__file__))
    if registered == current:
        return True
    repair_path = out / "runtime_repair.json"
    repair = json.loads(repair_path.read_text()) if repair_path.exists() else {}
    return repair.get("previous_source_sha256") == registered and repair.get("current_source_sha256") == current


def verify_canonical_paths(values, store):
    """Resolve each shared parent once; reject symlink leaves independently."""
    parents = {}
    for value in values:
        path = Path(value)
        if path.parent not in parents:
            parents[path.parent] = path.parent.resolve()
        if not parents[path.parent].is_relative_to(store) or path.is_symlink():
            raise RuntimeError("new physical images must live directly in canonical full_mapillary")


def audit():
    cfg, gc, store, previous, night, out, acquisition = context_paths()
    selected_path = acquisition / "selected.parquet"
    if not (acquisition / "download.done.json").exists():
        raise RuntimeError("reference-only download must complete first")
    selected_sha = digest(selected_path)
    for filename in ("acquisition.started.json", "download.done.json"):
        if json.loads((acquisition / filename).read_text())["selected_sha256"] != selected_sha:
            raise RuntimeError("downloaded population differs from the preregistered selection")
    fixed = json.loads((night / "input_contract.json").read_text())
    if (digest(previous / "manifests/G2_smart.parquet") != fixed["gallery_sha256"]
            or digest(previous / "descriptor_contract.json") != fixed["descriptor_contract_sha256"]):
        raise RuntimeError("frozen G2 or SAGE descriptor contract changed")
    contract = dict(source_sha256=digest(Path(__file__)), selected_sha256=digest(selected_path),
                    base_sha256=digest(previous / "manifests/G2_smart.parquet"),
                    descriptor_contract_sha256=digest(previous / "descriptor_contract.json"),
                    query_sha256=gc["query_sha256"], aoi_sha256=digest(WORKSPACE / gc["aoi"]))
    registered = out / "gallery_addition.started.json"
    if registered.exists():
        original = json.loads(registered.read_text())["contract"]
        if dict(original, source_sha256=contract["source_sha256"]) != contract or not source_compatible(original["source_sha256"], out):
            raise RuntimeError("gallery addition input/source contract changed")
    else:
        if not allowed_to_start(cfg, "gallery_addition", out):
            return
        save(registered, {"started": time.time(), "contract": contract,
             "hypothesis": "small missing Mapillary/KartaView reference-only smart acquisition adds useful viewpoints",
             "primary": "same SAGE-L322 exact cosine, top1 coordinates, unchanged1184queries",
             "selection_uses_query_coordinates": False, "calibration_or_final_access": False})
    if (out / "audit.done.json").exists():
        receipt = json.loads((out / "audit.done.json").read_text())
        if digest(out / "addition.parquet") != receipt["addition_sha256"] or digest(out / "gallery.parquet") != receipt["gallery_sha256"]:
            raise RuntimeError("audited manifests changed")
        return
    g = pd.read_parquet(previous / "manifests/G2_smart.parquet")
    selected = decorate(pd.read_parquet(selected_path), gc)
    required = ["source", "source_image_id", "sequence_id", "license", "attribution", "source_url"]
    if selected[required].isna().any().any() or any(selected[c].astype(str).str.strip().eq("").any() for c in required):
        raise RuntimeError("missing provider provenance")
    if not selected.production_compatible.all() or not all(gallery_role(r.source, r.sequence_id) for r in selected.itertuples()):
        raise RuntimeError("source license or sequence role not allowed")
    boundary = load_aoi_boundary(WORKSPACE / gc["aoi"])
    if not shapely.covers(boundary.geometry, shapely.points(selected.lon, selected.lat)).all():
        raise RuntimeError("reference outside exact Moscow administrative AOI")
    if selected.id.duplicated().any() or set(selected.id) & set(g.id):
        raise RuntimeError("new manifest contains repeated gallery identities")
    state(out, "validating_canonical_paths", total=len(selected))
    verify_canonical_paths(selected.image_path, store)
    state(out, "fingerprinting_new_references", total=len(selected))
    db = sqlite3.connect(out / "image_audit.sqlite")
    db.execute("CREATE TABLE IF NOT EXISTS images (id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL)")
    fps = {i: json.loads(v) for i, v in db.execute("SELECT id,fingerprint FROM images")}
    pending = [r for r in selected.itertuples() if r.id not in fps and Path(r.image_path).is_file()]
    with ThreadPoolExecutor(max_workers=2) as workers:
        for start in range(0, len(pending), 32):
            batch = pending[start:start + 32]
            for row, fp in zip(batch, workers.map(fingerprint_file, [r.image_path for r in batch]), strict=True):
                fps[row.id] = fp
                db.execute("INSERT INTO images VALUES (?,?)", (row.id, json.dumps(fp)))
            db.commit()
            state(out, "fingerprinting_new_references", completed=len(fps), total=len(selected))
    db.close()
    selected["is_pano"] = [bool(json.loads(r.metadata_json or "{}").get("is_pano", False))
         or str(json.loads(r.metadata_json or "{}").get("projection", "")).upper() == "SPHERE" for r in selected.itertuples()]
    state(out, "development_identity_guard")
    guard = query_guard(gc)
    development = fixed_development(gc)
    dev_sequences = {f"{'mapillary' if r.source == 'msls' else r.source}::{str(r.sequence_id).removeprefix('msls:')}"
                     for r in development.itertuples()}
    dev_ids = {('mapillary' if r.source == 'msls' else r.source, str(r.source_image_id))
               for r in development.itertuples()}
    base_ids = {('mapillary' if r.source == 'msls' else r.source, str(r.source_image_id)) for r in g.itertuples()}
    if set(g.sequence_key) & dev_sequences or base_ids & dev_ids or set(g.file_sha256) & set(development.file_sha256):
        raise RuntimeError("frozen baseline intersects the current development identities or sequences")
    # G0 predates the v6 development split and includes historical v2 query
    # sequences. Preserve the frozen baseline, disclose that old benchmark
    # limitation, and still exclude every protected sequence from additions.
    legacy_overlap = g[g.sequence_key.isin(guard[3])]
    save(out / "baseline_identity_scope.json", {
        "baseline_sha256": contract["base_sha256"],
        "preexisting_protected_sequences": sorted(legacy_overlap.sequence_key.unique()),
        "preexisting_protected_reference_count": len(legacy_overlap),
        "current_development_sequence_id_sha_overlap": 0,
        "claim": "current fixed development only; historical heldout independence is not certified",
        "baseline_unchanged": True, "new_evidence_implicating_baseline_still_fails": True,
        "all_protected_sequences_excluded_from_additions": True,
    })
    rows, counts = filter_fingerprints(selected, fps, g, guard)
    if not rows:
        save(out / "closed.json", {"reason": "no usable new images after download/identity gates", "exclusions": counts})
        return
    addition = pd.DataFrame(rows)
    addition.to_parquet(out / "addition.parquet", index=False)
    gallery = pd.concat([g, addition], ignore_index=True)
    gallery.to_parquet(out / "gallery.parquet", index=False)
    save(out / "audit.done.json", {"addition_sha256": digest(out / "addition.parquet"),
         "gallery_sha256": digest(out / "gallery.parquet"), "added": len(addition),
         "by_source": dict(Counter(addition.source)), "exclusions": counts,
         "original_G2_prefix_unchanged": True, "physical_images_in_canonical_store": True,
         "cross_source_checks": "normalized source IDs, provider sequences, SHA/pixel SHA, pHash4 rotations/mirrors",
         "heldout_access": "none; deterministic whole-query-role sequence exclusion and existing opaque identity ledger",
         "baseline_identity_scope_sha256": digest(out / "baseline_identity_scope.json"),
         "limitation": "unknown old-to-modern provider sequence aliases remain uncertified"})
    state(out, "audit_completed", usable=len(addition))


def verify_audited(out, previous, night):
    receipt = json.loads((out / "audit.done.json").read_text())
    registered = json.loads((out / "gallery_addition.started.json").read_text())["contract"]
    fixed = json.loads((night / "input_contract.json").read_text())
    if not source_compatible(registered["source_sha256"], out):
        raise RuntimeError("gallery addition source changed after registration")
    for name, expected in (("addition.parquet", receipt["addition_sha256"]), ("gallery.parquet", receipt["gallery_sha256"])):
        if digest(out / name) != expected:
            raise RuntimeError("audited addition/gallery manifest changed before encoding or scoring")
    if digest(previous / "manifests/G2_smart.parquet") != fixed["gallery_sha256"] or registered["base_sha256"] != fixed["gallery_sha256"]:
        raise RuntimeError("frozen G2 changed before addition encoding or scoring")
    if digest(previous / "descriptor_contract.json") != registered["descriptor_contract_sha256"]:
        raise RuntimeError("SAGE descriptor contract changed after image audit")
    columns = ["id", "file_sha256", "lat", "lon"]
    base = pd.read_parquet(previous / "manifests/G2_smart.parquet", columns=columns)
    gallery = pd.read_parquet(out / "gallery.parquet", columns=columns)
    addition = pd.read_parquet(out / "addition.parquet", columns=columns)
    pd.testing.assert_frame_equal(gallery.iloc[:len(base)].reset_index(drop=True), base.reset_index(drop=True))
    pd.testing.assert_frame_equal(gallery.iloc[len(base):].reset_index(drop=True), addition.reset_index(drop=True))


def embed():
    _, gc, _, previous, night, out, _ = context_paths()
    if not (out / "audit.done.json").exists():
        raise RuntimeError("complete the new-reference identity/license audit first")
    verify_audited(out, previous, night)
    added = pd.read_parquet(out / "addition.parquet")
    contract_sha = digest(previous / "descriptor_contract.json")
    expected = json.loads((previous / "descriptor_contract.json").read_text())["retriever"]
    cache = json.loads((previous / "descriptor_pool.json").read_text())["entries"]
    directory = out / "descriptors"
    directory.mkdir(exist_ok=True)
    model = None
    try:
        for start in range(0, len(added), 64):
            batch = added.iloc[start:start + 64]
            target = directory / f"chunk-{start // 64:04d}.npy"
            receipt_path = target.with_suffix(".json")
            contract = {"ids": batch.id.tolist(), "image_sha256": batch.file_sha256.tolist(), "contract_sha256": contract_sha}
            if receipt_path.exists():
                receipt = json.loads(receipt_path.read_text())
                if any(receipt[k] != v for k, v in contract.items()) or digest(target) != receipt["descriptors_sha256"]:
                    raise RuntimeError("new descriptor chunk fingerprint changed")
            else:
                if any(i in cache for i in batch.id):
                    raise RuntimeError("new reference already has a reusable descriptor; review before recomputing")
                for row in batch.itertuples():
                    if digest(row.image_path) != row.file_sha256:
                        raise RuntimeError("new photo changed before encoding")
                if model is None:
                    model = create_model(gc)
                    if model.metadata.to_dict() != expected:
                        raise RuntimeError("new references must use identical SAGE-L fingerprint")
                vectors = model.embed_batch(batch.image_path.tolist())
                if vectors.shape != (len(batch), 8448) or vectors.dtype != np.float32 or not np.isfinite(vectors).all() or not np.allclose(np.linalg.norm(vectors, axis=1), 1, atol=1e-5):
                    raise RuntimeError("invalid new reference descriptors")
                atomic_array(target, vectors)
                save(receipt_path, contract | {"descriptors_sha256": digest(target)})
            for i, row in enumerate(batch.itertuples()):
                cache[row.id] = {"path": str(target), "row": i, "image_sha256": row.file_sha256}
            state(out, "encoding_new_references", completed=start + len(batch), total=len(added))
    finally:
        if model is not None:
            model.close()
    save(out / "descriptor_pool.json", {"contract_sha256": contract_sha, "entries": cache,
         "note": "existing100k descriptors reused; only audited new identities encoded once"})


def evaluate():
    _, gc, _, previous, night, out, _ = context_paths()
    destination = out / "results/gallery_addition.json"
    if destination.exists():
        return
    verify_audited(out, previous, night)
    q, qv = query_vectors(gc)
    gallery, added = pd.read_parquet(out / "gallery.parquet"), pd.read_parquet(out / "addition.parquet")
    entries = json.loads((out / "descriptor_pool.json").read_text())["entries"]
    base = np.load(night / "base_scores.npy", mmap_mode="r", allow_pickle=False)
    input_contract = json.loads((night / "input_contract.json").read_text())
    if digest(night / "base_scores.npy") != input_contract["scores_sha256"] or len(gallery) != base.shape[1] + len(added):
        raise RuntimeError("original exact score cache or gallery prefix changed")
    new_scores = exact_scores(added, qv, entries)
    score_path = out / "scores.npy"
    scores = np.lib.format.open_memmap(score_path, mode="w+", dtype=np.float32, shape=(len(q), len(gallery)))
    for start in range(0, len(q), 4):
        scores[start:start + 4, :base.shape[1]] = base[start:start + 4]
        scores[start:start + 4, base.shape[1]:] = new_scores[start:start + 4]
    scores.flush()
    del base, new_scores
    result, rows = summarize(gallery, q, scores)
    del scores
    baseline = json.loads((previous / "results/G2_smart_rows.json").read_text())
    if rows["query_ids"] != baseline["query_ids"]:
        raise RuntimeError("development denominator changed")
    ranks = np.asarray([np.inf if r is None else r for r in rows["positive_ranks"]])
    result.update({"name": "gallery_addition", "query_count": len(q), "gallery_count": len(gallery),
        "gallery_manifest": str(out / "gallery.parquet"), "gallery_sha256": digest(out / "gallery.parquet"),
        "query_sha256": gc["query_sha256"], "recall_at": {str(k): float(np.mean(ranks <= k)) for k in (1, 5, 10, 20, 50, 100)},
        "paired_vs_baseline": paired_group_bootstrap(baseline["errors_m"], rows["errors_m"], q.h3_coarse.astype(str).tolist()),
        "abstention": False, "final_or_calibration_access": False,
        "note": "original G2 plus bounded reference-only smart Mapillary/KartaView acquisition; same SAGE-L322 top1",
        "rank_reporting": "exact full-gallery positive ranks; no geographic reranking"})
    save(out / "results/gallery_addition_rows.json", rows)
    save(destination, result)
    save(out / "done.json", {"completed": time.time(), "result_sha256": digest(destination), "scores_sha256": digest(score_path)})
    state(out, "gallery_addition_completed", raw=result["raw"], added=len(added))


def run(phase):
    *_, night, out, _ = context_paths()
    with (LOCAL / "gallery_addition.lock").open("a") as own, threadpool_limits(limits=1):
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if phase == "audit":
            audit()
            return
        if (out / "closed.json").exists():
            return
        if not (out / "audit.done.json").exists():
            raise RuntimeError("run CPU audit before queueing any GPU work")
        state(out, "waiting_for_registered_gpu_queue")
        # Query504 is already queued; avoid racing it for the shared GPU lock.
        with (LOCAL / "followthrough.lock").open("a") as queue:
            fcntl.flock(queue, fcntl.LOCK_EX)
            with (LOCAL / "controller.lock").open("a") as main:
                fcntl.flock(main, fcntl.LOCK_EX)
                embed()
                evaluate()
                for suffix in (".json", "_rows.json"):
                    src = out / "results" / ("gallery_addition" + suffix)
                    dst = night / "results" / src.name
                    if dst.exists() and digest(src) != digest(dst):
                        raise RuntimeError("published gallery addition differs from committed result")
                    if not dst.exists():
                        save(dst, json.loads(src.read_text()))
                save(out / "published.json", {"published": time.time()})
                report()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["audit", "run"])
    run(parser.parse_args().phase)
