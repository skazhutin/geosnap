"""Third bounded reference tranche; fixed gated selection, network and identity audit only."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from threadpoolctl import threadpool_limits

from ml.research import gallery_scale
from ml.research import night_acquisition as acquisition
from ml.research import night_gallery_addition as addition
from ml.research import night_live_expansion as expansion
from ml.research import night_quarantine_audit as quarantine
from ml.research.gallery_scale_storage import WORKSPACE, digest, save
from ml.research.night_v7 import LOCAL, paths

SETUP = {"maximum_references": 6000, "baseline_count": 107749, "sources": ["mapillary", "kartaview"],
         "license": "CC BY-SA 4.0", "latest_network_start": "2026-09-10T04:45:00+03:00",
         "download": acquisition.SETUP, "reuse_unambiguous_canonical_files_first": True,
         "selection": "existing reference-only smart density/heading/sequence/month policy",
         "query_coordinates_for_selection": False, "calibration_final_access": False, "production_changes": False}


def stage():
    cfg, gc, store, previous, night = paths()
    return cfg, gc, store, previous, night, night / "reference_tranche3"


def state(out, phase, completed=0, total=0, **extra):
    value = dict(phase=phase, completed=completed, total=total, pid=os.getpid(), updated=time.time(), **extra)
    save(out / "status.json", value)
    save(LOCAL / "reference_tranche3_status.json", value)
    print(json.dumps(value), flush=True)


def gate(parent, require_time=False):
    path, plan_path = parent / "expansion3_gate.json", parent / "expansion3_gate.plan.json"
    value, plan = json.loads(path.read_text()), json.loads(plan_path.read_text())
    if (value["eligible"] is not True or value["data_gate_passed"] is not True
            or value["registered_plan_sha256"] != digest(plan_path) or value["rule"] != plan["rule"]
            or value["rule"]["latest_network_start"] != SETUP["latest_network_start"]
            or value["rule"]["maximum_new_references"] != SETUP["maximum_references"]
            or value["paired_data_gain"]["gain_pp"] < .5 or value["paired_data_gain"]["ci95_pp"][0] <= 0):
        raise RuntimeError("mandatory common-gallery data expansion gate did not pass")
    for name, field in (("first", "baseline_rows_sha256"), ("expanded2", "candidate_rows_sha256")):
        if digest(parent / "evaluation" / name / "context_rows.json") != value[field]:
            raise RuntimeError("paired expansion-gate input changed")
    if require_time and datetime.now().astimezone() >= datetime.fromisoformat(SETUP["latest_network_start"]):
        raise RuntimeError("04:45 latest network-start cutoff reached")
    return value


def inputs():
    cfg, gc, store, previous, night, out = stage()
    parent = night / "live_expansion2_quarantine"
    gate(parent)
    _, _, _, verified_parent, _, _, _, baseline = quarantine.verify_audited()
    if parent != verified_parent or len(baseline) != SETUP["baseline_count"]:
        raise RuntimeError("third tranche needs the complete107749 common-quarantine parent")
    if not (parent / "expanded.done.json").exists():
        raise RuntimeError("complete parent pipeline before third selection")
    old = night / "live_expansion2"
    known_paths = [parent / "gallery.parquet", night / "reference_acquisition/selected.parquet", old / "selected.parquet"]
    physical_paths = [WORKSPACE / "data/evaluation/moscow_research_v5/gallery_expansion/download_union.parquet",
                      WORKSPACE / "data/processed/moscow/live_physical_manifest.parquet"]
    plan = json.loads(acquisition.PLAN.read_text())
    if digest(acquisition.POOL) != plan["allowed_future_gallery_pool_sha256"]:
        raise RuntimeError("frozen allowed reference pool changed")
    if digest(WORKSPACE / gc["aoi"]) != acquisition.AOI_SHA or digest(WORKSPACE / gc["query_manifest"]) != gc["query_sha256"]:
        raise RuntimeError("fixed administrative AOI or development identity population changed")
    files = [*known_paths, *physical_paths, acquisition.POOL, acquisition.PLAN,
        parent / "audit.done.json", parent / "quarantine.receipt.json", parent / "descriptor_pool.json",
        parent / "encode.done.json", parent / "expanded.done.json", parent / "evaluation/expanded2/done.json",
        parent / "evaluation/expanded2/top1_scores.json", parent / "evaluation/expanded2/mean_scores.json",
        parent / "evaluation/expanded2/context_evidence.json", parent / "expansion3_gate.json", parent / "expansion3_gate.plan.json",
        old / "live_expansion.started.json", old / "query_identity_guard.json", old / "query_identity_guard.receipt.json",
        previous / "descriptor_contract.json", WORKSPACE / gc["aoi"], WORKSPACE / gc["query_manifest"]]
    contract = {"setup": SETUP, "files": {str(p): digest(p) for p in files},
        "sources": {p.name: digest(p) for p in (Path(__file__), Path(acquisition.__file__), Path(addition.__file__),
                     Path(expansion.__file__), Path(quarantine.__file__), Path(gallery_scale.__file__))},
        "query_sha256": gc["query_sha256"], "parent_count": len(baseline), "quarantine": sorted(quarantine.QUARANTINE)}
    return cfg, gc, store, previous, night, out, parent, baseline, known_paths, physical_paths, contract


def cached_guard(gc, night):
    old = night / "live_expansion2"
    if not (old / "query_identity_guard.receipt.json").exists():
        raise RuntimeError("existing cached identity guard required; do not decode queries")
    registered = json.loads((old / "live_expansion.started.json").read_text())
    guard = expansion.cached_query_guard(gc, old, registered)
    development = pd.read_parquet(WORKSPACE / gc["query_manifest"], columns=quarantine.IDENTITY_COLUMNS)
    forbidden = quarantine.normalized_forbidden(guard[3]) | set(quarantine.sequence_keys(development)) | quarantine.QUARANTINE
    return (*guard[:3], forbidden, guard[4]), development


def local_mapping(pool, physical_frames, store):
    """Resolve193 possible old references once; ambiguous mappings are excluded, never copied."""
    known = {}
    for frame in physical_frames:
        for row in frame.itertuples():
            key = (expansion.provider(row.source), str(row.source_image_id).removeprefix("msls:"))
            known.setdefault(key, []).append(row)
    names, parents = acquisition.existing_names(store), acquisition.provider_roots(store)
    local, network, excluded = [], [], {}
    for record in pool.to_dict("records"):
        key = (record["source"], str(record["source_image_id"]))
        matches = known.get(key, [])
        target = acquisition.canonical_path(store, record["source"], record["id"], record["source_image_id"], parents)
        if not matches and (record["source"], target.name) not in names:
            network.append(record)
            continue
        paths_ = set()
        for row in matches:
            # Benchmark packaging cannot confer production imagery rights.
            if row.source != record["source"] or getattr(row, "license", None) != SETUP["license"]:
                continue
            p = Path(str(getattr(row, "image_path", "")))
            if p.is_file() and not p.is_symlink():
                resolved = p.resolve()
                if resolved.is_relative_to(store):
                    paths_.add(str(resolved))
        if len(paths_) != 1:
            excluded["ambiguous_or_unmapped_existing_physical"] = excluded.get("ambiguous_or_unmapped_existing_physical", 0) + 1
            continue
        local.append(record | {"image_path": next(iter(paths_))})
    return (pd.DataFrame(local).reindex(columns=pool.columns), pd.DataFrame(network).reindex(columns=pool.columns), excluded)


def verify_prepared():
    data = inputs()
    out, contract = data[5], data[-1]
    registered = json.loads((out / "tranche.started.json").read_text())
    if registered["contract"] != contract:
        raise RuntimeError("third reference tranche source/input contract changed")
    for name, sha in registered["files"].items():
        if Path(name).name != name or digest(out / name) != sha:
            raise RuntimeError("third reference tranche selection manifest changed")
    if registered["selected_sha256"] != registered["files"]["selected.parquet"]:
        raise RuntimeError("third reference selected identity hash differs")
    return data, registered


def prepare(limit=6000):
    *_, out = stage()
    if (out / "tranche.started.json").exists():
        return verify_prepared()[1]
    data = inputs()
    _, gc, store, _, night, out, parent, baseline, known_paths, physical_paths, contract = data
    if limit != SETUP["maximum_references"]:
        raise RuntimeError("one fixed6000-reference tranche only")
    gate(parent, require_time=True)
    known_ids, known_stable = set(), set()
    for path in known_paths:
        frame = pd.read_parquet(path, columns=["id", "source", "source_image_id"])
        known_ids |= expansion.identities(frame)
        known_stable.update(frame.id)
    allowed = pd.read_parquet(acquisition.POOL)
    allowed = allowed[~allowed.id.isin(known_stable)].copy()
    pool, excluded = acquisition.eligible_pool(allowed, known_ids, acquisition.load_aoi_boundary(WORKSPACE / gc["aoi"]), store, set())
    pool = acquisition.decorate(pool)
    guard, _ = cached_guard(gc, night)
    excluded["prohibited_sequences"] = int(pool.sequence_key.isin(guard[3]).sum())
    pool = pool[~pool.sequence_key.isin(guard[3])].copy()
    fields = ["source", "source_image_id", "sequence_id", "license", "attribution", "source_url"]
    valid = pool[fields].notna().all(axis=1)
    for key in fields:
        valid &= pool[key].astype(str).str.strip().ne("")
    excluded["missing_provenance"] = int((~valid).sum())
    pool = pool[valid].copy()
    local, network, skipped = local_mapping(pool, [pd.read_parquet(p) for p in physical_paths], store)
    excluded.update(skipped)
    base = acquisition.decorate(baseline)
    local = expansion.select_tranche(local, base, min(limit, len(local))) if len(local) else local
    current = pd.concat([base, local], ignore_index=True)
    network = expansion.select_tranche(network, current, limit - len(local)) if len(local) < limit and len(network) else network.iloc[:0]
    selected = pd.concat([local, network], ignore_index=True)
    selected["production_compatible"] = True
    selected["usage_scope"] = "production-compatible source; pending third-tranche identity audit"
    local = selected.iloc[:len(local)].copy()
    network = selected.iloc[len(local):].copy()
    if selected.empty or selected.id.duplicated().any() or selected.image_path.duplicated().any():
        raise RuntimeError("invalid third selected reference identities/paths")
    gate(parent, require_time=True)
    out.mkdir(exist_ok=True)
    frames = {"selected.parquet": selected, "local_reuse.parquet": local, "network_selected.parquet": network,
              "smoke.parquet": network.iloc[:50]}
    remainder = network.iloc[50:]
    for start in range(0, len(remainder), acquisition.SETUP["batch_size"]):
        frames[f"batch-{start // acquisition.SETUP['batch_size']:04d}.parquet"] = remainder.iloc[start:start + acquisition.SETUP["batch_size"]]
    for name, frame in frames.items():
        quarantine.write_view(frame, out / name)
    gate(parent, require_time=True)
    files = {name: digest(out / name) for name in frames}
    registered = {"started": time.time(), "contract": contract, "selected_count": len(selected),
        "selected_sha256": files["selected.parquet"], "network_count": len(network), "local_reuse_count": len(local),
        "files": files, "by_source": selected.source.value_counts().to_dict(), "exclusions": excluded,
        "original_prior9000_selected_identities_excluded": True, "query_coordinates_used_for_selection": False,
        "gate_sha256": digest(parent / "expansion3_gate.json"), "canonical_store": str(store)}
    save(out / "tranche.started.json", registered)
    save(out / "prepare.done.json", {"completed": time.time(), "selected_sha256": registered["selected_sha256"]})
    state(out, "tranche3_prepared", len(selected), len(selected), network=len(network), local_reuse=len(local))
    return registered


@contextmanager
def download_context(data, registered):
    """Frozen downloader, network-only manifests, and a guard at first HTTP dispatch."""
    _, _, store, previous, _, out, parent, *_ = data
    originals = acquisition.stage, acquisition.prepare, acquisition.status, requests.Session.request
    request_lock = threading.Lock()
    first_path = out / "first_http_dispatch.json"
    checked = False

    def request(self, *args, **kwargs):
        nonlocal checked
        with request_lock:
            if not checked:
                if digest(parent / "expansion3_gate.json") != registered["gate_sha256"]:
                    raise RuntimeError("expansion gate changed before first HTTP dispatch")
                if first_path.exists():
                    marker = json.loads(first_path.read_text())
                    if marker["selected_sha256"] != registered["selected_sha256"] or marker["gate_sha256"] != registered["gate_sha256"]:
                        raise RuntimeError("third network resume identity changed")
                else:
                    gate(parent, require_time=True)
                    now = datetime.now().astimezone()
                    if now >= datetime.fromisoformat(SETUP["latest_network_start"]):
                        raise RuntimeError("04:45 cutoff before actual first HTTP dispatch")
                    save(first_path, {"first_http_dispatch_at": now.isoformat(), "unix_seconds": time.time(),
                         "selected_sha256": registered["selected_sha256"], "gate_sha256": registered["gate_sha256"],
                         "timestamp_scope": "immediately before first requests.Session.request dispatch; no URL recorded"})
                checked = True
        return originals[3](self, *args, **kwargs)

    acquisition.stage = lambda: ({}, {}, store, previous, out)
    acquisition.prepare = lambda ignored: registered | {"selected_count": registered["network_count"]}
    acquisition.status = state
    requests.Session.request = request
    try:
        yield
    finally:
        acquisition.stage, acquisition.prepare, acquisition.status, requests.Session.request = originals


def download():
    prepare()
    data, registered = verify_prepared()
    out, parent = data[5], data[6]
    if (out / "download.done.json").exists():
        save_download_scope(out, registered)
        return
    gate(parent, require_time=not (out / "first_http_dispatch.json").exists())
    if registered["network_count"]:
        with download_context(data, registered):
            acquisition.download(6000)
    else:
        save(out / "download.done.json", {"selected_sha256": registered["selected_sha256"], "selected_count": 0,
             "valid_files": 0, "reason": "all_selected_references_reuse_existing_canonical_files", "network_requests": 0})
    save_download_scope(out, registered)


def save_download_scope(out, registered):
    save(out / "download_scope.json", {"selected_total": registered["selected_count"], "local_reuse": registered["local_reuse_count"],
         "network_requested_population": registered["network_count"], "network_receipt_sha256": digest(out / "download.done.json"),
         "no_physical_copies": True, "local_files_still_require_full_identity_audit": True})


def audit():
    data, registered = verify_prepared()
    _, gc, store, _, night, out, _, baseline, _, _, contract = data
    downloaded = json.loads((out / "download.done.json").read_text())
    if downloaded["selected_sha256"] != registered["selected_sha256"]:
        raise RuntimeError("third downloaded population changed")
    marker = out / "audit.started.json"
    if marker.exists():
        if json.loads(marker.read_text())["contract"] != contract:
            raise RuntimeError("third reference audit source contract changed")
    else:
        save(marker, {"started": time.time(), "contract": contract})
    if (out / "audit.done.json").exists():
        verify_audited()
        return
    selected = gallery_scale.decorate(pd.read_parquet(out / "selected.parquet"), gc)
    quarantine.validate_selected(selected, gc, store)
    db = sqlite3.connect(out / "image_audit.sqlite")
    try:
        db.execute("CREATE TABLE IF NOT EXISTS images (id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL)")
        fingerprints = {identity: json.loads(value) for identity, value in db.execute("SELECT id,fingerprint FROM images")}
        if set(fingerprints) - set(selected.id):
            raise RuntimeError("third audit database includes unselected identities")
        pending = []
        for row in selected.itertuples():
            if row.id in fingerprints:
                if digest(Path(row.image_path)) != fingerprints[row.id]["file_sha256"]:
                    raise RuntimeError("fingerprinted third-reference photo changed")
            elif Path(row.image_path).is_file():
                pending.append(row)
        with ThreadPoolExecutor(max_workers=2) as workers:
            for start in range(0, len(pending), 32):
                batch = pending[start:start + 32]
                for row, fp in zip(batch, workers.map(expansion.fingerprint_reference, [r.image_path for r in batch]), strict=True):
                    fingerprints[row.id] = fp
                    db.execute("INSERT INTO images VALUES (?,?)", (row.id, json.dumps(fp)))
                db.commit()
                state(out, "tranche3_fingerprints", len(fingerprints), len(selected))
    finally:
        db.close()
    guard, development = cached_guard(gc, night)
    quarantine.development_checks(development, baseline)
    selected["is_pano"] = [bool(json.loads(r.metadata_json or "{}").get("is_pano", False))
        or str(json.loads(r.metadata_json or "{}").get("projection", "")).upper() == "SPHERE" for r in selected.itertuples()]
    # Preserve the exact gate, including failure on any new evidence implicating the parent.
    retained, exclusions = addition.filter_fingerprints(selected, fingerprints, baseline, guard)
    added = pd.DataFrame(retained).reindex(columns=baseline.columns)
    gallery = pd.concat([baseline, added], ignore_index=True)
    if gallery.id.duplicated().any() or set(added.sequence_key) & guard[3]:
        raise RuntimeError("third approved gallery repeats identities or prohibited sequences")
    dev_checks = quarantine.development_checks(development, gallery)
    quarantine.write_view(added, out / "addition.parquet")
    quarantine.write_view(gallery, out / "gallery.parquet")
    save(out / "baseline_identity_scope.json", {"current_development_identity_checks": dev_checks,
         "common_parent_prefix_unchanged": True, "new_parent_sequence_implication_is_fatal": True,
         "historical_protected_reference_count": int(baseline.sequence_key.isin(guard[3]).sum()),
         "limitation": "historical protected-sequence independence remains uncertified; inherited research-only MSLS remains marked"})
    save(out / "audit.done.json", {"completed": time.time(), "contract": contract, "added": len(added),
        "by_source": added.source.value_counts().to_dict(), "baseline_count": len(baseline), "gallery_count": len(gallery),
        "gallery_sha256": digest(out / "gallery.parquet"), "addition_sha256": digest(out / "addition.parquet"),
        "fingerprint_database_sha256": digest(out / "image_audit.sqlite"),
        "baseline_identity_scope_sha256": digest(out / "baseline_identity_scope.json"), "exclusions": exclusions,
        "local_reuse_selected": registered["local_reuse_count"], "no_reference_descriptors_computed": True,
        "parent107749_prefix_unchanged": True})
    verify_audited()
    state(out, "tranche3_audit_completed", len(added), len(selected), gallery_count=len(gallery))


def verify_audited():
    data, registered = verify_prepared()
    _, gc, _, previous, night, out, _, baseline, _, _, contract = data
    done = json.loads((out / "audit.done.json").read_text())
    if done["contract"] != contract:
        raise RuntimeError("third audited source/input contract changed")
    for name, key in (("gallery.parquet", "gallery_sha256"), ("addition.parquet", "addition_sha256"),
                       ("image_audit.sqlite", "fingerprint_database_sha256"), ("baseline_identity_scope.json", "baseline_identity_scope_sha256")):
        if digest(out / name) != done[key]:
            raise RuntimeError("third audited artifact changed")
    added, gallery = pd.read_parquet(out / "addition.parquet"), pd.read_parquet(out / "gallery.parquet")
    for actual, expected in ((gallery.iloc[:len(baseline)], baseline), (gallery.iloc[len(baseline):], added)):
        pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False)
    if (len(baseline) != 107749 or len(added) != done["added"] or len(gallery) != done["gallery_count"]
            or gallery.id.duplicated().any() or set(gallery.sequence_key) & quarantine.QUARANTINE):
        raise RuntimeError("third gallery population/prefix invariant failed")
    return gc, previous, night, out, baseline, added, gallery


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("prepare", "download", "audit"))
    args = parser.parse_args()
    *_, out = stage()
    out.mkdir(exist_ok=True)
    os.nice(max(0, 10 - os.getpriority(os.PRIO_PROCESS, 0)))
    with (LOCAL / "reference_tranche3.lock").open("a") as lock, threadpool_limits(limits=1):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            {"prepare": prepare, "download": download, "audit": audit}[args.action]()
        except BaseException as exc:
            save(out / "failure.json", {"failed": time.time(), "phase": args.action, "pid": os.getpid(), "error": repr(exc)})
            state(out, "tranche3_failed", error=repr(exc))
            raise


if __name__ == "__main__":
    main()
