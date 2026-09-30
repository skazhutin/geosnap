"""Bounded reference-only acquisition; no query manifests, inference or evaluation.

The immutable candidate pool was assigned to gallery-role sequences before ML.
New photographs go directly into the existing canonical provider directories.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import heapq
import json
import os
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import h3
import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from ml.ingestion.common import PermanentDownloadError, validate_download_url, validate_image_file
from ml.ingestion.download_images import SOURCE_DOWNLOAD_HOST_SUFFIXES
from ml.ingestion.download_images import run as download_images
from ml.ingestion.mapillary_citywide import load_aoi_boundary
from ml.ingestion.merge_sources import safe_filename
from ml.ingestion.schema import validate_manifest_schema, write_manifest
from ml.research.gallery_scale import gallery_role
from ml.research.gallery_scale_storage import WORKSPACE, digest, save, settings

LOCAL = WORKSPACE / "data/evaluation/moscow_night_v7"
POOL = WORKSPACE / "data/evaluation/moscow_research_v5/prospective/gallery_acquisition_pool.parquet"
PLAN = WORKSPACE / "data/evaluation/moscow_research_v5/gallery_expansion/plan.json"
AOI_SHA = "33b5dbf852cb94e5292e7848974fba78641dd76339cad142e73b7419b5db4a8a"
SETUP = {"maximum_references": 3000, "smoke_size": 50, "minimum_smoke_valid_fraction": .8,
         "workers": 4, "retries": 2, "timeout_sec": 20, "batch_size": 250,
         "maximum_download_seconds": 1800, "minimum_batch_valid_fraction": .5,
         "h3_resolution": 9, "heading_bin_degrees": 45, "seed": 20260909,
         "selection": "reference-only water-fill cells, heading, sequence and capture-month diversity",
         "sources": ["mapillary", "kartaview"], "license": "CC BY-SA 4.0",
         "calibration_final_access": False, "query_coordinates_used": False,
         "production_changes": False, "inference": False}


def stable(value):
    return hashlib.sha256(f"{SETUP['seed']}:{value}".encode()).hexdigest()


def stage():
    gc, root, store, previous = settings()
    cfg = json.loads((WORKSPACE / "configs/moscow_night_v7.json").read_text())
    night = root / cfg["run_id"]
    return cfg, gc, store, previous, night / "reference_acquisition"


def may_start(cfg, started, now=None):
    return started or (now or datetime.now().astimezone()) < datetime.fromisoformat(cfg["cutoff"])


def status(out, phase, completed=0, total=0, **extra):
    value = {"phase": phase, "completed": completed, "total": total, "pid": os.getpid(),
             "updated": time.time(), **extra}
    save(out / "status.json", value)
    save(LOCAL / "acquisition_status.json", value)
    print(json.dumps(value), flush=True)


def provider_roots(store):
    """Resolve two directories once per phase, not for every NTFS image path."""
    root = store.resolve()
    parents = {source: (root / "geosnap" / source).resolve() for source in SETUP["sources"]}
    if any(not parent.is_relative_to(root) for parent in parents.values()):
        raise RuntimeError("canonical provider directory escapes image store")
    return parents


def canonical_path(store, source, identity, source_image_id, parents=None):
    if source not in SETUP["sources"] or not identity or str(identity) != safe_filename(str(identity)):
        raise ValueError("invalid canonical reference identity")
    filename = f"{identity}_{safe_filename(str(source_image_id))}.jpg"
    return (parents if parents is not None else provider_roots(store))[source] / filename


def existing_names(store):
    """List only existing provider directories; never scan private query images."""
    names = set()
    for source in SETUP["sources"]:
        for directory in (store / "geosnap" / source, store / "geosnap/gallery_expansion" / source):
            if directory.is_dir():
                with os.scandir(directory) as entries:
                    names.update((source, entry.name) for entry in entries)
    return names


def eligible_pool(frame, known_ids, boundary, store, names):
    """Reject unknown roles/licenses/URLs; metadata only, no query inputs."""
    kept, reasons = [], Counter()
    known_ids = set(known_ids)
    parents, destinations = provider_roots(store), []
    for row in frame.sort_values(["source", "source_image_id", "id"]).itertuples():
        identity = (str(row.source), str(row.source_image_id))
        sequence = str(row.sequence_id)
        reason = None
        if row.source not in SETUP["sources"]:
            reason = "source"
        elif row.license != SETUP["license"]:
            reason = "license"
        elif sequence in {"", "nan", "None", "<NA>"} or not gallery_role(row.source, sequence):
            reason = "query_or_unknown_sequence_role"
        elif identity in known_ids:
            reason = "known_identity"
        elif not np.isfinite([row.lat, row.lon]).all() or not boundary.covers(float(row.lon), float(row.lat)):
            reason = "outside_exact_aoi"
        else:
            try:
                validate_download_url(str(row.download_url), allowed_host_suffixes=SOURCE_DOWNLOAD_HOST_SUFFIXES[row.source])
            except PermanentDownloadError:
                reason = "invalid_download_url"
        if reason:
            reasons[reason] += 1
            continue
        target = canonical_path(store, row.source, str(row.id), row.source_image_id, parents)
        if (row.source, target.name) in names:
            reasons["existing_physical_filename"] += 1
            continue
        kept.append(row.Index)
        destinations.append(str(target))
        known_ids.add(identity)  # Reject repeated source identities within the pool.
    result = frame.loc[kept].copy().reset_index(drop=True)
    result["image_path"] = destinations
    return result, dict(reasons)


def decorate(frame):
    result = frame.copy()
    result["cell"] = [h3.latlng_to_cell(float(a), float(b), SETUP["h3_resolution"]) for a, b in zip(result.lat, result.lon, strict=True)]
    result["heading_bin"] = [int(float(h) % 360 // 45) if pd.notna(h) and np.isfinite(h) else -1 for h in result.heading]
    result["sequence_key"] = [f"{'mapillary' if s == 'msls' else s}::{str(q).removeprefix('msls:')}"
                              for s, q in zip(result.source, result.sequence_id, strict=True)]
    dates = pd.to_datetime(result.captured_at, utc=True, errors="coerce", format="mixed")
    result["month"] = dates.dt.strftime("%Y-%m").fillna("unknown")
    result["selection_hash"] = [stable(i) for i in result.id.astype(str)]
    return result


def smart_select(pool, gallery, limit):
    if not 1 <= limit <= SETUP["maximum_references"]:
        raise ValueError("acquisition limit must be between 1 and 3000")
    pool = pool.reset_index(drop=True)
    records = pool.to_dict("index")
    density = Counter(gallery.cell)
    heading = Counter(zip(gallery.cell, gallery.heading_bin, strict=True))
    sequence = Counter(gallery.sequence_key)
    month = Counter(zip(gallery.cell, gallery.month, strict=True))
    buckets = {cell: group.index.tolist() for cell, group in pool.groupby("cell", sort=True)}
    heap = [(density[cell], stable(cell), cell) for cell in buckets]
    heapq.heapify(heap)
    selected = []
    while heap and len(selected) < limit:
        _, _, cell = heapq.heappop(heap)
        positions = buckets[cell]
        j = min(positions, key=lambda i: (
            records[i]["heading_bin"] < 0, heading[(cell, records[i]["heading_bin"])],
            sequence[records[i]["sequence_key"]], records[i]["month"] == "unknown",
            month[(cell, records[i]["month"])], records[i]["selection_hash"], str(records[i]["id"])))
        row = records[j]
        selected.append(j)
        positions.remove(j)
        density[cell] += 1
        heading[(cell, row["heading_bin"])] += 1
        sequence[row["sequence_key"]] += 1
        month[(cell, row["month"])] += 1
        if positions:
            heapq.heappush(heap, (density[cell], stable(cell), cell))
    return pool.iloc[selected].copy().reset_index(drop=True)


def verify_prepared(out, cfg, limit):
    receipt = json.loads((out / "acquisition.started.json").read_text())
    if receipt["setup"] != SETUP or receipt["config"] != cfg or receipt["limit"] != limit or receipt["source_sha256"] != digest(Path(__file__)):
        raise RuntimeError("acquisition preregistration changed")
    for filename, expected in receipt["files"].items():
        if filename not in {"selected.parquet", "smoke.parquet"} and not (
            filename.startswith("batch-") and filename.endswith(".parquet") and "/" not in filename):
            raise RuntimeError("unexpected acquisition manifest path")
        if digest(out / filename) != expected:
            raise RuntimeError("prepared acquisition manifest changed")
    if receipt["selected_sha256"] != receipt["files"]["selected.parquet"]:
        raise RuntimeError("selected acquisition manifest fingerprint differs")
    return receipt


def prepare(limit=3000):
    if not 1 <= limit <= SETUP["maximum_references"]:
        raise ValueError("acquisition limit must be between 1 and 3000")
    cfg, gc, store, previous, out = stage()
    marker = out / "acquisition.started.json"
    if marker.exists():
        return verify_prepared(out, cfg, limit)
    if not may_start(cfg, False):
        raise RuntimeError("06:00 cutoff: do not start a new acquisition experiment")
    out.mkdir(parents=True, exist_ok=True)
    LOCAL.mkdir(parents=True, exist_ok=True)
    status(out, "acquisition_prepare")
    plan = json.loads(PLAN.read_text())
    if digest(POOL) != plan["allowed_future_gallery_pool_sha256"]:
        raise RuntimeError("frozen reference-only acquisition pool changed")
    gpath = previous / "manifests/G2_smart.parquet"
    night_inputs = out.parent / "input_contract.json"
    if digest(gpath) != json.loads(night_inputs.read_text())["gallery_sha256"]:
        raise RuntimeError("fixed G2 selection baseline changed")
    aoi = WORKSPACE / gc["aoi"]
    if digest(aoi) != AOI_SHA:
        raise RuntimeError("exact Moscow R102269 polygon changed")
    boundary = load_aoi_boundary(aoi)
    known_paths = [gpath, WORKSPACE / "data/evaluation/moscow_research_v5/gallery_expansion/download_union.parquet",
                   WORKSPACE / "data/processed/moscow/live_physical_manifest.parquet"]
    known = set()
    for filename in known_paths:
        frame = pd.read_parquet(filename, columns=["source", "source_image_id"])
        known.update(map(tuple, frame.astype(str).to_numpy()))
    pool, excluded = eligible_pool(pd.read_parquet(POOL), known, boundary, store, existing_names(store))
    if pool.empty:
        raise RuntimeError("no new eligible references")
    selected = smart_select(decorate(pool), decorate(pd.read_parquet(gpath)), limit)
    selected["production_compatible"] = True
    selected["usage_scope"] = "production-compatible-source; pending image leakage/license audit"
    selected["acquisition_rank"] = np.arange(1, len(selected) + 1)
    if selected.image_path.duplicated().any() or validate_manifest_schema(selected):
        raise RuntimeError("invalid selected reference manifest")
    files = {}
    manifests = {"selected.parquet": selected, "smoke.parquet": selected.iloc[:SETUP["smoke_size"]]}
    remainder = selected.iloc[SETUP["smoke_size"]:]
    for start in range(0, len(remainder), SETUP["batch_size"]):
        manifests[f"batch-{start // SETUP['batch_size']:04d}.parquet"] = remainder.iloc[start:start + SETUP["batch_size"]]
    for name, frame in manifests.items():
        write_manifest(frame, out / name)
        files[name] = digest(out / name)
    if not may_start(cfg, False):
        raise RuntimeError("cutoff passed during preparation; no network was started")
    receipt = {"started": time.time(), "setup": SETUP, "config": cfg, "limit": limit,
         "selected_count": len(selected), "selected_sha256": files["selected.parquet"],
         "source_sha256": digest(Path(__file__)), "files": files,
         "inputs": {str(p): digest(p) for p in [POOL, PLAN, aoi, night_inputs, *known_paths]},
         "source_counts": selected.source.value_counts().to_dict(), "eligible_source_counts": pool.source.value_counts().to_dict(),
         "excluded": excluded, "canonical_store": str(store),
         "role_rule": "pre-existing20260905 deterministic provider sequence gallery-role; no query records read",
         "downstream_gate": "downloaded files are NOT an approved gallery; run leakage/provenance/license audit before descriptors/evaluation"}
    save(marker, receipt)
    save(out / "prepare.done.json", {"completed": time.time(), "selected_sha256": receipt["selected_sha256"]})
    status(out, "acquisition_prepared", len(selected), len(selected), by_source=receipt["source_counts"])
    return receipt


def preflight_destinations(frame, store):
    """Never let the shared downloader replace an existing invalid/user file."""
    parents = provider_roots(store)
    for row in frame.itertuples():
        expected = canonical_path(store, row.source, str(row.id), row.source_image_id, parents)
        path = Path(row.image_path)
        if path != expected or path.is_symlink():
            raise RuntimeError("download destination differs from canonical identity")
        if path.exists():
            validation = validate_image_file(path, min_valid_size_bytes=1000, min_width=64, min_height=64,
                                             max_file_size_bytes=25 * 1024**2)
            if not validation.valid:
                raise RuntimeError("existing invalid canonical file preserved; investigate before resuming")


def smoke_passes(stats):
    requested = int(stats["images_requested"])
    valid = int(stats["images_downloaded"]) + int(stats["already_existing_images_skipped"])
    return requested > 0 and valid / requested >= SETUP["minimum_smoke_valid_fraction"]


def download_batch(manifest, store, out):
    frame = pd.read_parquet(manifest)
    preflight_destinations(frame, store)
    return download_images(manifest, out / f"{manifest.stem}.errors.json", retries=SETUP["retries"],
                           min_valid_size_bytes=1000, workers=SETUP["workers"], timeout_sec=SETUP["timeout_sec"],
                           min_width=64, min_height=64, stats_path=out / f"{manifest.stem}.stats.json")


def download(limit=3000):
    _, _, store, _, out = stage()
    receipt = prepare(limit)
    if (out / "download.done.json").exists():
        return
    start_path = out / "download.started.json"
    if start_path.exists():
        started = json.loads(start_path.read_text())
        if started["selected_sha256"] != receipt["selected_sha256"]:
            raise RuntimeError("download time budget belongs to a different population")
    else:
        started = {"started": time.time(), "selected_sha256": receipt["selected_sha256"]}
        save(start_path, started)
    smoke_path = out / "smoke.done.json"
    if smoke_path.exists():
        smoke = json.loads(smoke_path.read_text())
    else:
        status(out, "acquisition_smoke", 0, receipt["selected_count"])
        statistics = download_batch(out / "smoke.parquet", store, out)
        smoke = {"completed": time.time(), "passed": smoke_passes(statistics), "stats": statistics,
                 "manifest_sha256": receipt["files"]["smoke.parquet"]}
        save(smoke_path, smoke)
    if smoke["manifest_sha256"] != receipt["files"]["smoke.parquet"]:
        raise RuntimeError("smoke population changed")
    if not smoke["passed"]:
        save(out / "download.closed.json", {"reason": "first50 valid fraction below80%; no broad download", "smoke": smoke})
        status(out, "acquisition_smoke_failed", smoke["stats"]["images_requested"], receipt["selected_count"])
        raise RuntimeError("network smoke failed; broader acquisition was not started")
    all_stats = [smoke["stats"]]
    stop_reason = None
    for name in sorted(n for n in receipt["files"] if n.startswith("batch-")):
        marker = out / f"{Path(name).stem}.done.json"
        if marker.exists():
            batch = json.loads(marker.read_text())
            if batch["manifest_sha256"] != receipt["files"][name]:
                raise RuntimeError("download batch population changed")
        else:
            if time.time() - started["started"] >= SETUP["maximum_download_seconds"]:
                stop_reason = "maximum_download_seconds_reached_between_batches"
                break
            batch = {"stats": download_batch(out / name, store, out), "manifest_sha256": receipt["files"][name],
                     "completed": time.time()}
            save(marker, batch)
        all_stats.append(batch["stats"])
        status(out, "acquisition_download", sum(s["images_requested"] for s in all_stats), receipt["selected_count"],
               valid=sum(s["images_downloaded"] + s["already_existing_images_skipped"] for s in all_stats),
               failed=sum(s["failed_downloads"] for s in all_stats))
        statistics = batch["stats"]
        valid = statistics["images_downloaded"] + statistics["already_existing_images_skipped"]
        if valid / max(1, statistics["images_requested"]) < SETUP["minimum_batch_valid_fraction"]:
            stop_reason = "committed_batch_valid_fraction_below50percent"
            break
    attempted = sum(s["images_requested"] for s in all_stats)
    result = {"completed": time.time(), "selected_sha256": receipt["selected_sha256"],
              "selected_count": receipt["selected_count"], "valid_files": sum(s["images_downloaded"] + s["already_existing_images_skipped"] for s in all_stats),
              "failed_downloads": sum(s["failed_downloads"] for s in all_stats), "batches": all_stats,
              "attempted": attempted, "skipped_remaining": receipt["selected_count"] - attempted,
              "reason": stop_reason, "elapsed_seconds": time.time() - started["started"],
              "time_limit": "best effort between batches; finish any in-flight batch",
              "status": "bounded_download_complete_pending_downstream_audit", "gallery_approved": False}
    save(out / "download.done.json", result)
    status(out, "acquisition_download_completed", attempted, receipt["selected_count"], valid=result["valid_files"],
           skipped_remaining=result["skipped_remaining"], reason=stop_reason)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "download"])
    parser.add_argument("--limit", type=int, default=3000)
    args = parser.parse_args()
    LOCAL.mkdir(parents=True, exist_ok=True)
    with (LOCAL / "acquisition.lock").open("a") as lock, threadpool_limits(limits=1):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        (prepare if args.action == "prepare" else download)(args.limit)


if __name__ == "__main__":
    main()
