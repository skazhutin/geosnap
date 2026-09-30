"""Second bounded reference-only smart tranche; preparation, network and identity audit."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import sqlite3
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd
import shapely
from threadpoolctl import threadpool_limits

from ml.evaluation.moscow_split import _PerceptualHashIndex
from ml.research import gallery_scale
from ml.research import night_acquisition as acquisition
from ml.research import night_gallery_addition as first_addition
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.night_v7 import LOCAL, paths

SETUP = {"maximum_references": 6000, "selection_round_limit": 3000, "baseline_count": 102944,
         "sources": ["mapillary", "kartaview"], "license": "CC BY-SA 4.0",
         "download": acquisition.SETUP, "reference_only_selection": True,
         "development_adaptive_stage": "first bounded acquisition gain motivated this next tranche",
         "query_coordinates_used_for_selection": False, "calibration_final_access": False,
         "production_changes": False}
LEDGER = WORKSPACE / "data/evaluation/moscow_real_v2/split_audit.json"


def stage():
    cfg, gc, store, previous, night = paths()
    return cfg, gc, store, previous, night, night / "live_expansion2"


def state(out, phase, completed=0, total=0, **extra):
    value = dict(phase=phase, completed=completed, total=total, pid=os.getpid(), updated=time.time(), **extra)
    save(out / "status.json", value)
    save(LOCAL / "live_expansion2_status.json", value)
    print(json.dumps(value), flush=True)


def provider(source):
    return "mapillary" if source == "msls" else str(source)


def identities(frame):
    return {(provider(r.source), str(r.source_image_id).removeprefix("msls:")) for r in frame.itertuples()}


def protected_sequences(gc):
    """Read identity columns only in use; never use a query coordinate to select references."""
    development = gallery_scale.fixed_development(gc)
    sequences = {f"{provider(r.source)}::{str(r.sequence_id).removeprefix('msls:')}" for r in development.itertuples()}
    # Existing opaque identity ledger only; no historical query population or outcomes.
    ledger = json.loads(LEDGER.read_text())["file_phash_ledger"]
    sequences.update(f"{provider(r['source'])}::{str(r['sequence_id']).removeprefix('msls:')}"
                     for r in ledger if r["disposition"] != "gallery")
    return sequences


def select_tranche(pool, baseline, limit):
    """Continue the same water-fill state across two bounded3000-row calls."""
    if not 1 <= limit <= SETUP["maximum_references"]:
        raise ValueError("second acquisition limit must be1..6000")
    remaining, current = pool.copy(), baseline.copy()
    selected = []
    while len(remaining) and sum(map(len, selected)) < limit:
        count = min(SETUP["selection_round_limit"], limit - sum(map(len, selected)))
        batch = acquisition.smart_select(remaining, current, count)
        if batch.empty:
            break
        selected.append(batch)
        current = pd.concat([current, batch], ignore_index=True)
        remaining = remaining[~remaining.id.isin(batch.id)].copy()
    return pd.concat(selected, ignore_index=True) if selected else pool.iloc[:0].copy()


def input_contract(cfg, gc, previous, night):
    first = night / "gallery_addition"
    if not (first / "published.json").exists() or not (first / "done.json").exists():
        raise RuntimeError("the first expanded gallery must finish before selecting a second tranche")
    first_addition.verify_audited(first, previous, night)
    aoi = WORKSPACE / gc["aoi"]
    if digest(aoi) != acquisition.AOI_SHA:
        raise RuntimeError("exact Moscow R102269 AOI changed")
    plan = json.loads(acquisition.PLAN.read_text())
    if digest(acquisition.POOL) != plan["allowed_future_gallery_pool_sha256"]:
        raise RuntimeError("frozen allowed reference acquisition pool changed")
    files = [acquisition.POOL, acquisition.PLAN, aoi, night / "input_contract.json",
             first / "gallery.parquet", first / "descriptor_pool.json", first / "audit.done.json",
             first / "done.json", first / "published.json", first / "baseline_identity_scope.json",
             night / "reference_acquisition/selected.parquet", WORKSPACE / gc["query_manifest"], LEDGER,
             WORKSPACE / "data/evaluation/moscow_research_v5/gallery_expansion/download_union.parquet",
             WORKSPACE / "data/processed/moscow/live_physical_manifest.parquet"]
    sources = [Path(__file__), Path(acquisition.__file__), Path(first_addition.__file__), Path(gallery_scale.__file__)]
    return {"setup": SETUP, "config": cfg, "files": {str(p): digest(p) for p in files},
            "source_sha256": {p.name: digest(p) for p in sources}}


def verify_prepared(out, cfg, gc, previous, night, limit):
    registered = json.loads((out / "live_expansion.started.json").read_text())
    if registered["limit"] != limit or registered["contract"] != input_contract(cfg, gc, previous, night):
        raise RuntimeError("second acquisition source, population or input contract changed")
    for name, expected in registered["files"].items():
        if ("/" in name or not (name in {"selected.parquet", "smoke.parquet"} or name.startswith("batch-"))
                or digest(out / name) != expected):
            raise RuntimeError("second acquisition committed selection manifest changed")
    if registered["selected_sha256"] != registered["files"]["selected.parquet"]:
        raise RuntimeError("second acquisition selected population fingerprint differs")
    return registered


def prepare(limit=6000):
    cfg, gc, store, previous, night, out = stage()
    if (out / "live_expansion.started.json").exists():
        return verify_prepared(out, cfg, gc, previous, night, limit)
    if not acquisition.may_start(cfg, False):
        raise RuntimeError("06:00 cutoff: second acquisition not started")
    contract = input_contract(cfg, gc, previous, night)
    first = night / "gallery_addition"
    base = pd.read_parquet(first / "gallery.parquet")
    if len(base) != SETUP["baseline_count"]:
        raise RuntimeError("expected unchanged102944-reference first expanded gallery")
    known, known_stable = set(), set()
    known_paths = [first / "gallery.parquet", night / "reference_acquisition/selected.parquet",
        WORKSPACE / "data/evaluation/moscow_research_v5/gallery_expansion/download_union.parquet",
        WORKSPACE / "data/processed/moscow/live_physical_manifest.parquet"]
    for path in known_paths:
        frame = pd.read_parquet(path, columns=["id", "source", "source_image_id"])
        known |= identities(frame)
        known_stable.update(frame.id)
    allowed = pd.read_parquet(acquisition.POOL)
    allowed = allowed[~allowed.id.isin(known_stable)].copy()
    pool, excluded = acquisition.eligible_pool(allowed, known, acquisition.load_aoi_boundary(WORKSPACE / gc["aoi"]),
                                               store, acquisition.existing_names(store))
    pool = acquisition.decorate(pool)
    forbidden = protected_sequences(gc)
    forbidden_mask = pool.sequence_key.isin(forbidden)
    excluded["protected_sequence"] = int(forbidden_mask.sum())
    pool = pool[~forbidden_mask].copy()
    provenance = ["source", "source_image_id", "sequence_id", "license", "attribution", "source_url"]
    valid = pool[provenance].notna().all(axis=1)
    for name in provenance:
        valid &= pool[name].astype(str).str.strip().ne("")
    excluded["missing_provenance"] = int((~valid).sum())
    pool = pool[valid].copy()
    selected = select_tranche(pool, acquisition.decorate(base), limit)
    if selected.empty:
        raise RuntimeError("no unused eligible modern references remain")
    selected["production_compatible"] = True
    selected["usage_scope"] = "production-compatible source; pending image-level leakage audit"
    selected["acquisition_rank"] = np.arange(1, len(selected) + 1)
    if selected.id.duplicated().any() or selected.image_path.duplicated().any() or acquisition.validate_manifest_schema(selected):
        raise RuntimeError("invalid second selected reference manifest")
    out.mkdir(exist_ok=True)
    manifests = {"selected.parquet": selected, "smoke.parquet": selected.iloc[:acquisition.SETUP["smoke_size"]]}
    remaining = selected.iloc[acquisition.SETUP["smoke_size"]:]
    for start in range(0, len(remaining), acquisition.SETUP["batch_size"]):
        manifests[f"batch-{start // acquisition.SETUP['batch_size']:04d}.parquet"] = remaining.iloc[start:start + acquisition.SETUP["batch_size"]]
    files = {}
    for name, frame in manifests.items():
        acquisition.write_manifest(frame, out / name)
        files[name] = digest(out / name)
    if not acquisition.may_start(cfg, False):
        raise RuntimeError("cutoff passed during second acquisition preparation; no network started")
    registration = {"started": time.time(), "contract": contract, "limit": limit,
        "selected_count": len(selected), "selected_sha256": files["selected.parquet"], "files": files,
        "source_counts": selected.source.value_counts().to_dict(), "eligible_source_counts": pool.source.value_counts().to_dict(),
        "excluded": excluded, "canonical_store": str(store), "baseline_count": len(base),
        "whole_protected_sequence_count": len(forbidden), "forbidden_sequences_sha256": hashlib.sha256(json.dumps(sorted(forbidden)).encode()).hexdigest(),
        "query_coordinates_used": False, "first_selected3000_excluded_even_if_failed_download": True,
        "limitation": "development-adaptive gallery expansion; immutable historical v2 overlap is not independent evaluation"}
    save(out / "live_expansion.started.json", registration)
    save(out / "prepare.done.json", {"selected_sha256": registration["selected_sha256"], "completed": time.time()})
    state(out, "live_expansion_prepared", len(selected), len(selected), by_source=registration["source_counts"])
    return registration


@contextmanager
def downloader_context(limit):
    """Reuse the frozen downloader without changing its source or3000selection cap."""
    old = acquisition.stage, acquisition.prepare, acquisition.status
    acquisition.stage = lambda: (*stage()[:4], stage()[-1])
    acquisition.prepare = lambda ignored_limit: prepare(limit)
    acquisition.status = state
    try:
        yield
    finally:
        acquisition.stage, acquisition.prepare, acquisition.status = old


def download(limit=6000):
    with downloader_context(limit):
        acquisition.download(limit)


def cached_query_guard(gc, out, registered):
    path, receipt = out / "query_identity_guard.json", out / "query_identity_guard.receipt.json"
    expected = {"query_sha256": gc["query_sha256"], "ledger_sha256": registered["contract"]["files"][str(LEDGER)],
                "helper_sha256": registered["contract"]["source_sha256"]["gallery_scale.py"]}
    if receipt.exists():
        saved = json.loads(receipt.read_text())
        if saved["inputs"] != expected or digest(path) != saved["sha256"]:
            raise RuntimeError("cached second-tranche identity guard changed")
        data = json.loads(path.read_text())
        index = _PerceptualHashIndex(4)
        for number, value in enumerate(data["phashes"]):
            index.add(number, int(value, 16))
        return set(data["exact"]), set(data["pixels"]), set(map(tuple, data["identities"])), set(data["forbidden"]), index
    exact, pixels, identities_, forbidden, index = gallery_scale.query_guard(gc)
    # The existing guard exposes the same hash in several bands; only its
    # distinct fingerprint values are needed to reproduce the boolean gate.
    hashes = {value for bucket in index._bands.values() for _, value in bucket}
    data = {"exact": sorted(exact), "pixels": sorted(pixels), "identities": sorted(identities_),
            "forbidden": sorted(forbidden), "phashes": [f"{value:016x}" for value in sorted(hashes)]}
    save(path, data)
    save(receipt, {"inputs": expected, "sha256": digest(path)})
    return exact, pixels, identities_, forbidden, index


def fingerprint_reference(path):
    value = first_addition.fingerprint_file(path)
    # Invalid images still need byte identity for safe resume of their exclusion.
    if "file_sha256" not in value:
        value["file_sha256"] = digest(path)
    return value


def verify_audited(out, previous, night, registered):
    receipt = json.loads((out / "audit.done.json").read_text())
    for name, field in (("addition.parquet", "addition_sha256"), ("gallery.parquet", "gallery_sha256")):
        if digest(out / name) != receipt[field]:
            raise RuntimeError("second audited gallery manifest changed")
    base_path = night / "gallery_addition/gallery.parquet"
    if digest(base_path) != registered["contract"]["files"][str(base_path)]:
        raise RuntimeError("first expanded gallery changed after second-tranche selection")
    base, gallery, added = [pd.read_parquet(path) for path in (base_path, out / "gallery.parquet", out / "addition.parquet")]
    pd.testing.assert_frame_equal(gallery.iloc[:len(base)].reset_index(drop=True), base.reset_index(drop=True), check_dtype=False)
    pd.testing.assert_frame_equal(gallery.iloc[len(base):].reset_index(drop=True), added.reset_index(drop=True), check_dtype=False)
    if len(base) != SETUP["baseline_count"] or gallery.id.duplicated().any():
        raise RuntimeError("second expansion changed its base prefix or repeated identities")
    return gallery, added


def audit(limit=6000):
    cfg, gc, store, previous, night, out = stage()
    registered = verify_prepared(out, cfg, gc, previous, night, limit)
    download_receipt = json.loads((out / "download.done.json").read_text())
    if download_receipt["selected_sha256"] != registered["selected_sha256"]:
        raise RuntimeError("second download differs from its selected population")
    if (out / "audit.done.json").exists():
        verify_audited(out, previous, night, registered)
        return
    base = pd.read_parquet(night / "gallery_addition/gallery.parquet")
    selected = gallery_scale.decorate(pd.read_parquet(out / "selected.parquet"), gc)
    first_addition.verify_canonical_paths(selected.image_path, store)
    boundary = acquisition.load_aoi_boundary(WORKSPACE / gc["aoi"])
    if (not selected.production_compatible.all() or not selected.license.eq(SETUP["license"]).all()
            or not all(gallery_scale.gallery_role(r.source, r.sequence_id) for r in selected.itertuples())
            or not shapely.covers(boundary.geometry, shapely.points(selected.lon, selected.lat)).all()):
        raise RuntimeError("second source/license/role/exactAOI gate failed")
    db = sqlite3.connect(out / "image_audit.sqlite")
    try:
        db.execute("CREATE TABLE IF NOT EXISTS images (id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL)")
        fingerprints = {identity: json.loads(value) for identity, value in db.execute("SELECT id,fingerprint FROM images")}
        pending = []
        for row in selected.itertuples():
            if row.id in fingerprints:
                if digest(row.image_path) != fingerprints[row.id]["file_sha256"]:
                    raise RuntimeError("fingerprinted second-tranche image changed")
            elif Path(row.image_path).is_file():
                pending.append(row)
        with ThreadPoolExecutor(max_workers=2) as workers:
            for start in range(0, len(pending), 32):
                batch = pending[start:start + 32]
                for row, fingerprint in zip(batch, workers.map(fingerprint_reference, [r.image_path for r in batch]), strict=True):
                    fingerprints[row.id] = fingerprint
                    db.execute("INSERT INTO images VALUES (?,?)", (row.id, json.dumps(fingerprint)))
                db.commit()
                state(out, "live_expansion_fingerprints", len(fingerprints), len(selected))
    finally:
        db.close()
    guard = cached_query_guard(gc, out, registered)
    # Preserve all legacy spellings and add normalized keys; a broader denylist
    # must never be weakened to match an older helper's spelling.
    guard = (*guard[:3], set(guard[3]) | protected_sequences(gc), guard[4])
    development = gallery_scale.fixed_development(gc)
    dev_sequences = {f"{provider(r.source)}::{str(r.sequence_id).removeprefix('msls:')}" for r in development.itertuples()}
    if (set(base.sequence_key) & dev_sequences or identities(base) & identities(development)
            or set(base.file_sha256) & set(development.file_sha256)):
        raise RuntimeError("first expanded baseline intersects current development identities")
    selected["is_pano"] = [bool(json.loads(r.metadata_json or "{}").get("is_pano", False))
        or str(json.loads(r.metadata_json or "{}").get("projection", "")).upper() == "SPHERE" for r in selected.itertuples()]
    retained, exclusions = first_addition.filter_fingerprints(selected, fingerprints, base, guard)
    if not retained:
        save(out / "closed.json", {"reason": "no usable new references after identity audit", "exclusions": exclusions})
        return
    added = pd.DataFrame(retained)
    # Align columns without changing any original102944 reference value.
    added = added.reindex(columns=base.columns)
    gallery = pd.concat([base, added], ignore_index=True)
    added.to_parquet(out / "addition.parquet", index=False)
    gallery.to_parquet(out / "gallery.parquet", index=False)
    save(out / "baseline_identity_scope.json", {"current_development_sequence_id_sha_overlap": 0,
         "historical_protected_reference_count": int(base.sequence_key.isin(guard[3]).sum()),
         "baseline_unchanged": True, "historical_populations_are_not_independent_evaluation": True,
         "all_protected_sequences_excluded_from_new_references": True,
         "new_matches_implicating_any_baseline_sequence_require_review": True})
    save(out / "audit.done.json", {"added": len(added), "by_source": dict(Counter(added.source)),
         "gallery_sha256": digest(out / "gallery.parquet"), "addition_sha256": digest(out / "addition.parquet"),
         "baseline_count": len(base), "gallery_count": len(gallery), "original102944_prefix_unchanged": True,
         "exclusions": exclusions, "baseline_identity_scope_sha256": digest(out / "baseline_identity_scope.json"),
         "guard_receipt_sha256": digest(out / "query_identity_guard.receipt.json"),
         "provenance_scope": "newMapillary/KartaView compatible sources; inheritedMSLS staysresearch-only",
         "future_encoding": "reuse first addition descriptor_pool; encode only addition.parquet once; wait collage.lock thencontroller.lock"})
    verify_audited(out, previous, night, registered)
    state(out, "live_expansion_audit_completed", len(added), len(selected), gallery_count=len(gallery))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "download", "audit"])
    parser.add_argument("--limit", type=int, default=6000)
    args = parser.parse_args()
    *_, out = stage()
    out.mkdir(exist_ok=True)
    with (LOCAL / "live_expansion2.lock").open("a") as lock, threadpool_limits(limits=1):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            {"prepare": prepare, "download": download, "audit": audit}[args.action](args.limit)
        except BaseException as exc:
            save(out / "failure.json", {"failed": time.time(), "phase": args.action, "pid": os.getpid(), "error": repr(exc)})
            state(out, "live_expansion_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    main()
