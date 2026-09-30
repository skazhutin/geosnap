"""Fixed SAGE-L gallery experiment; development only, no confidence or model search.

Run phases separately or use --phase run. Expensive image audits and descriptors
are checkpointed on the external volume. No calibration/final manifest is read.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import heapq
import io
import json
import os
import sqlite3
import subprocess
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from zipfile import ZipFile

import h3
import imagehash
import numpy as np
import pandas as pd
import shapely
from PIL import Image, ImageOps
from threadpoolctl import threadpool_limits

from ml.evaluation.moscow_split import _PerceptualHashIndex
from ml.ingestion.mapillary_citywide import load_aoi_boundary
from ml.ingestion.msls_moscow import load_moscow_metadata
from ml.research.gallery_scale_storage import WORKSPACE, copy_verified, digest, save, settings
from ml.research.metrics import paired_group_bootstrap, raw_metrics, retrieval_metrics
from ml.research.prepare_queries import score as sequence_role_hash
from ml.research.vector_evaluation import distances

V5 = WORKSPACE / "data/evaluation/moscow_research_v5"
SLIM = ["id", "source", "source_image_id", "sequence_id", "image_path", "lat", "lon", "captured_at",
        "heading", "license", "attribution", "source_url", "metadata_json"]


def fixed_development(config):
    path = WORKSPACE / config["query_manifest"]
    if path != V5 / "prospective/development.parquet" or config["query_sha256"] != "6bcc0b4a618682e05e15305dc76b9f02af635e73573e06ea2567d6094ad3e7d9":
        raise RuntimeError("only the unchanged registered development split may be read")
    if digest(path) != config["query_sha256"]:
        raise RuntimeError("fixed development population changed")
    return pd.read_parquet(path)


def fixed_g0(config):
    path = WORKSPACE / config["baseline_gallery"]
    if digest(path) != config["baseline_gallery_sha256"]:
        raise RuntimeError("G0 identity changed")
    return pd.read_parquet(path)


def stable(value, seed=20260908):
    return hashlib.sha256(f"{seed}:{value}".encode()).hexdigest()


def resolved_images(paths):
    """Resolve a directory once, avoiding 31k repeated NTFS symlink probes."""
    parents = {}
    result = []
    for value in paths:
        path = Path(value)
        if path.parent not in parents:
            parents[path.parent] = path.parent.resolve()
        result.append(str(parents[path.parent] / path.name))
    return result


def gallery_role(source, sequence):
    # MSLS retains historical Mapillary sequence keys. Strip the packaging
    # prefix so its frames cannot evade the existing provider-sequence split.
    provider = "mapillary" if source == "msls" else source
    sequence = str(sequence).removeprefix("msls:")
    return int(sequence_role_hash("role", f"{provider}::{sequence}")[:8], 16) % 10 < 6


def decorate(frame, config):
    frame = frame.copy()
    frame["cell"] = [h3.latlng_to_cell(float(a), float(b), config["h3_resolution"])
                     for a, b in zip(frame.lat, frame.lon, strict=True)]
    frame["heading_bin"] = [int(float(x) % 360 // 45) if pd.notna(x) else -1 for x in frame.heading]
    frame["sequence_key"] = [f"{'mapillary' if s == 'msls' else s}::{str(q).removeprefix('msls:')}"
                             for s, q in zip(frame.source, frame.sequence_id, strict=True)]
    dates = pd.to_datetime(frame.captured_at, utc=True, errors="coerce", format="mixed")
    frame["season"] = [f"{d.year}-{(d.month % 12) // 3}" if pd.notna(d) else "unknown" for d in dates]
    frame["view_bucket"] = frame.get("view_direction", pd.Series("unknown", index=frame.index)).fillna("unknown").astype(str)
    frame["production_compatible"] = frame.source.isin(["mapillary", "kartaview"]) & frame.license.eq("CC BY-SA 4.0")
    frame["usage_scope"] = np.where(frame.production_compatible, "production-compatible", "research-only")
    frame["selection_hash"] = [stable(i, config["seed"]) for i in frame.id]
    return frame


def smart_order(pool, baseline):
    """One deterministic greedy policy: water-fill cells, then diversify views.

    All arguments are references. There is deliberately no query argument.
    Each cell's candidates are recomputed when its count changes, ensuring
    dynamic heading/sequence counts cannot leave stale priorities in the heap.
    """
    density = Counter(baseline.cell)
    sequences = Counter(baseline.sequence_key)
    headings = Counter(zip(baseline.cell, baseline.heading_bin, strict=True))
    seasons = Counter(zip(baseline.cell, baseline.season, strict=True))
    viewpoints = Counter(zip(baseline.cell, baseline.get("view_bucket", pd.Series("unknown", index=baseline.index)), strict=True))
    buckets = {c: list(g.index) for c, g in pool.groupby("cell", sort=True)}
    heap = [(density[c], stable(c), c) for c in buckets]
    heapq.heapify(heap)
    result = []
    records = pool.to_dict("index")
    while heap:
        _, _, cell = heapq.heappop(heap)
        indices = buckets[cell]
        chosen = min(indices, key=lambda i: (
            sequences[records[i]["sequence_key"]],
            records[i]["heading_bin"] < 0,
            headings[(cell, records[i]["heading_bin"])],
            viewpoints[(cell, records[i].get("view_bucket", "unknown"))],
            seasons[(cell, records[i]["season"])], records[i]["selection_hash"],
        ))
        row = records[chosen]
        result.append(chosen)
        indices.remove(chosen)
        density[cell] += 1
        sequences[row["sequence_key"]] += 1
        headings[(cell, row["heading_bin"])] += 1
        viewpoints[(cell, row.get("view_bucket", "unknown"))] += 1
        seasons[(cell, row["season"])] += 1
        if indices:
            heapq.heappush(heap, (density[cell], stable(cell), cell))
    return result


def archive_inventory(store, output):
    path = output / "archives.json"
    if path.exists():
        return json.loads(path.read_text())
    records = []
    # Actual observed store layout: package directories with top-level ZIPs.
    for directory in sorted(store.iterdir()):
        if not directory.is_dir() or directory.name == "geosnap":
            continue
        for archive in directory.glob("*.zip"):
            if archive.name.startswith("._"):
                continue
            with ZipFile(archive) as z:
                members = z.infolist()
                counts = Counter()
                for m in members:
                    if m.filename.endswith(".jpg"):
                        counts["/".join(m.filename.split("/")[:2])] += 1
                records.append({"path": str(archive), "bytes": archive.stat().st_size,
                                "jpg_members": sum(counts.values()), "cities": dict(counts),
                                "moscow_jpg": sum(1 for m in members if m.filename.startswith("train_val/moscow/")
                                                  and m.filename.endswith(".jpg")),
                                "metadata": "train_val/moscow/database/raw.csv" in z.namelist()})
    # The already-audited Moscow query volume is still in Downloads. It is
    # a known GeoSnap dataset asset, not an arbitrary user file.
    prior = json.loads((WORKSPACE / "data/raw/moscow/msls_extract.stats.json").read_text())
    if not any(r["moscow_jpg"] == 77496 for r in records):
        source = Path(prior["archives"]["msls_images_vol_1.zip"]["path"])
        if not source.is_file():
            raise RuntimeError("Moscow MSLS query volume is absent locally")
        records.append({"path": str(source), "bytes": source.stat().st_size,
                        "moscow_jpg": 77496, "known_from_verified_prior_extraction": True, "metadata": False})
    save(path, records)
    return records


def prepare():
    config, _, store, output = settings()
    if (output / "metadata_ready.json").exists():
        return
    boundary_path = WORKSPACE / config["aoi"]
    props = json.loads(boundary_path.read_text())["features"][0]["properties"]
    if props["osm_id"] != 102269 or props["osm_type"] != "relation":
        raise RuntimeError("exact Moscow R102269 required")
    boundary = load_aoi_boundary(boundary_path)
    archives = archive_inventory(store, output)
    metadata = next(Path(r["path"]) for r in archives if r["metadata"])
    prior = json.loads((WORKSPACE / "data/raw/moscow/msls_selection.stats.json").read_text())
    if digest(metadata) != prior["metadata_archive"]["sha256"]:
        raise RuntimeError("MSLS metadata differs from the previously audited official archive")
    msls, audit = load_moscow_metadata(metadata, strict_aoi=False)
    inside = shapely.covers(boundary.geometry, shapely.points(msls.lon, msls.lat))
    audit["inside_exact_R102269"] = int(inside.sum())
    audit["outside_exact_R102269_after_bbox"] = int((~inside).sum())
    msls = msls[inside].copy()
    rows = pd.DataFrame({
        "id": "msls:" + msls.key.astype(str), "source": "msls", "source_image_id": msls.key.astype(str),
        "sequence_id": "msls:" + msls.sequence_key.astype(str), "lat": msls.lat, "lon": msls.lon,
        "captured_at": msls.msls_capture_timestamp.astype(str), "heading": msls.ca,
        "is_pano": msls.pano, "view_direction": msls.view_direction,
        "license": "CC BY-NC-SA 4.0", "attribution": "Mapillary Street-Level Sequences (MSLS) dataset contributors",
        "source_url": "https://www.mapillary.com/dataset/places", "original_split": msls.msls_original_split,
        "archive_member": msls.msls_archive_member,
    })
    rows["image_path"] = [str(store / "geosnap/msls" / s / f"{key}.jpg")
                          for s, key in zip(rows.original_split, rows.source_image_id, strict=True)]
    by_split = {"query": next(r["path"] for r in archives if r["moscow_jpg"] == 77496),
                "database": next(r["path"] for r in archives if r["moscow_jpg"] == 171878)}
    rows["archive_path"] = rows.original_split.map(by_split)
    rows = decorate(rows, config)
    rows.to_parquet(output / "msls_moscow_metadata.parquet", index=False)
    save(output / "metadata_ready.json", {"aoi_sha256": boundary.sha256, "relation": 102269,
        "msls": audit, "panorama_count": int(rows.is_pano.sum()), "sequences": rows.sequence_key.nunique(),
        "metadata_sha256": digest(metadata), "license": "research-only; preserve bundled license verbatim",
        "warning": "bundled license calls itself NC-SA but links to by-nc legalcode; no redistribution authorized here",
        "model_training_overlap": "SAGE upstream trains on MSLS; this remains development evidence, not unseen-test proof",
        "calibration_or_final_opened": False})
    print("MSLS Moscow metadata", len(rows), "panoramas", int(rows.is_pano.sum()), flush=True)


def fingerprint_bytes(data):
    try:
        with Image.open(io.BytesIO(data)) as opened:
            image = ImageOps.exif_transpose(opened).convert("RGB")
            image.load()
        phash = str(imagehash.phash(image))
        return {"file_sha256": hashlib.sha256(data).hexdigest(), "perceptual_hash": phash,
                "pixel_sha256": hashlib.sha256(image.tobytes()).hexdigest(),
                "width": image.width, "height": image.height, "decode_ok": True}
    except Exception as exc:
        return {"decode_ok": False, "reason": type(exc).__name__}


def query_guard(config):
    queries = fixed_development(config)
    exact, pixels, identities, sequences = set(), set(), set(), set()
    index = _PerceptualHashIndex(4)
    counter = 0
    for row in queries.itertuples():
        identities.add((str(row.source), str(row.source_image_id)))
        sequences.add(f"{row.source}::{row.sequence_id}")
        if digest(Path(row.image_path)) != row.file_sha256:
            raise RuntimeError("development photo changed")
        exact.add(row.file_sha256)
        with Image.open(row.image_path) as im:
            original = ImageOps.exif_transpose(im).convert("RGB")
            original.load()
        pixels.add(hashlib.sha256(original.tobytes()).hexdigest())
        for rotation in (None, Image.Transpose.ROTATE_90, Image.Transpose.ROTATE_180, Image.Transpose.ROTATE_270):
            image = original if rotation is None else original.transpose(rotation)
            for candidate in (image, ImageOps.mirror(image)):
                index.add(counter, int(str(imagehash.phash(candidate)), 16))
                counter += 1
    # Reuse the existing opaque identity ledger; never read historic test rows
    # or their coordinates. All dispositions contribute fingerprint denylists.
    ledger = json.loads((WORKSPACE / "data/evaluation/moscow_real_v2/split_audit.json").read_text())["file_phash_ledger"]
    for row in ledger:
        if row["disposition"] == "gallery":
            continue
        identities.add((str(row["source"]), str(row["source_image_id"])))
        sequences.add(f"{row['source']}::{row['sequence_id']}")
        exact.add(row["file_sha256"])
        index.add(counter, int(row["perceptual_hash"], 16))
        counter += 1
    return exact, pixels, identities, sequences, index


def candidate_metadata(config, output):
    if (output / "candidate_metadata.parquet").exists():
        return pd.read_parquet(output / "candidate_metadata.parquet")
    frames = []
    for path in (WORKSPACE / "data/raw/moscow/live_manifest.parquet", V5 / "gallery_expansion/audited_union.parquet"):
        frames.append(pd.read_parquet(path, columns=SLIM))
    live = pd.concat(frames, ignore_index=True).drop_duplicates(["source", "source_image_id"])
    # File enumeration instead of a costly stat for each undispatched API row.
    names = {}
    for parent in (WORKSPACE / "data/raw/moscow/images/mapillary", WORKSPACE / "data/raw/moscow/images/kartaview",
                   V5 / "gallery_expansion/images"):
        for directory, _, files in os.walk(parent):
            canonical_directory = Path(directory).resolve()
            for name in files:
                if name.endswith(".jpg") and not name.startswith("._"):
                    names[name] = str(canonical_directory / name)
    live = live[live.source.isin(["mapillary", "kartaview"])].copy()
    live["image_path"] = [names.get(Path(p).name) for p in live.image_path]
    live = live[live.image_path.notna()].copy()
    def pano(row):
        m = json.loads(row.metadata_json or "{}")
        return bool(m.get("is_pano", False)) or str(m.get("projection", "")).upper() == "SPHERE"
    live["is_pano"] = [pano(r) for r in live.itertuples()]
    live = live.drop(columns="metadata_json")
    live["archive_path"], live["archive_member"] = None, None
    boundary = load_aoi_boundary(WORKSPACE / config["aoi"])
    live = live[shapely.covers(boundary.geometry, shapely.points(live.lon, live.lat))]
    live = decorate(live, config)
    msls = decorate(pd.read_parquet(output / "msls_moscow_metadata.parquet"), config)
    combined = pd.concat([live, msls], ignore_index=True)
    g0_ids = set(fixed_g0(config).id)
    combined = combined[~combined.id.isin(g0_ids)]
    allowed = [gallery_role(s, q) for s, q in zip(combined.source, combined.sequence_id, strict=True)]
    excluded = int(len(combined) - sum(allowed))
    combined = combined[allowed].sort_values("selection_hash").reset_index(drop=True)
    combined.to_parquet(output / "candidate_metadata.parquet", index=False)
    save(output / "candidate_metadata_audit.json", {"available_moscow_by_source": dict(Counter(pd.concat([live, msls]).source)),
        "new_candidates_before_image_audit": dict(Counter(combined.source)), "query_role_sequences_excluded_frames": excluded,
        "inputs": "reference metadata, exact administrative polygon and pre-existing whole-sequence role hash",
        "selection_uses_query_coordinates": False})
    return combined


def audit():
    config, _, _, output = settings()
    if (output / "candidate_pool.parquet").exists():
        return
    candidates = candidate_metadata(config, output)
    exact, pixels, identities, forbidden_sequences, query_index = query_guard(config)
    db = sqlite3.connect(output / "image_audit.sqlite")
    db.execute("CREATE TABLE IF NOT EXISTS images (id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL)")
    cached = {i: json.loads(v) for i, v in db.execute("SELECT id,fingerprint FROM images")}
    # Read ZIP members in archive order, amortizing NTFS seeks. Decode bounded
    # groups, never a list of all JPEG bytes, and commit each group for resume.
    with ThreadPoolExecutor(max_workers=6) as workers:
        for archive, group in candidates.groupby("archive_path", dropna=False, sort=False):
            z = ZipFile(archive) if pd.notna(archive) else None
            missing = [r for r in group.itertuples() if r.id not in cached]
            if z:
                missing.sort(key=lambda r: z.getinfo(r.archive_member).header_offset)
            for start in range(0, len(missing), 128):
                batch = missing[start:start + 128]
                data, read_errors = [], {}
                for row in batch:
                    try:
                        data.append(z.read(row.archive_member) if z else Path(row.image_path).read_bytes())
                    except Exception as exc:
                        data.append(b"")
                        read_errors[row.id] = type(exc).__name__
                for row, fp in zip(batch, workers.map(fingerprint_bytes, data), strict=True):
                    if row.id in read_errors:
                        fp = {"decode_ok": False, "reason": read_errors[row.id]}
                    cached[row.id] = fp
                    db.execute("INSERT INTO images VALUES (?,?)", (row.id, json.dumps(fp)))
                db.commit()
                if start % 5120 == 0:
                    save(output / "audit_progress.json", {"fingerprinted": len(cached),
                        "candidates": len(candidates), "updated": time.time()})
                    print("audited images", len(cached), "/", len(candidates), flush=True)
            if z:
                z.close()
    db.close()
    counts = Counter()
    leaked_sequences = set(forbidden_sequences)
    rows = []
    for row in candidates.to_dict("records"):
        fp = cached[row["id"]]
        if not fp["decode_ok"]:
            counts["undecodable"] += 1
            continue
        provider = "mapillary" if row["source"] == "msls" else row["source"]
        if ((provider, row["source_image_id"]) in identities or fp["file_sha256"] in exact
                or fp["pixel_sha256"] in pixels or query_index.matches(int(fp["perceptual_hash"], 16))):
            leaked_sequences.add(row["sequence_key"])
            counts["query_identity_or_phash_match"] += 1
            continue
        rows.append(row | fp)
    pool = pd.DataFrame(rows)
    counts["prohibited_sequence_frames"] = int(pool.sequence_key.isin(leaked_sequences).sum())
    pool = pool[~pool.sequence_key.isin(leaked_sequences)].copy()
    # No spatial-proximity duplicate suppression. Exact physical duplicates
    # share one identity across packages; different headings remain useful.
    base = fixed_g0(config)
    base_hashes = set(base.file_sha256)
    pool = pool[~pool.file_sha256.isin(base_hashes)]
    before = len(pool)
    pool = pool.sort_values(["production_compatible", "selection_hash"], ascending=[False, True])
    pool = pool.drop_duplicates("file_sha256").drop_duplicates("pixel_sha256")
    counts["exact_reference_duplicates"] = before - len(pool)
    reference_index = _PerceptualHashIndex(4)
    for i, value in enumerate(base.perceptual_hash):
        reference_index.add(i, int(value, 16))
    retained = []
    for row in pool.itertuples():
        value = int(row.perceptual_hash, 16)
        if reference_index.matches(value):
            counts["reference_phash_near_duplicate"] += 1
            continue
        retained.append(row.Index)
        reference_index.add(len(base) + len(retained), value)
    pool = pool.loc[retained]
    pool = pool.reset_index(drop=True)
    pool.to_parquet(output / "candidate_pool.parquet", index=False)
    save(output / "candidate_audit.json", {"pool_count": len(pool), "by_source": dict(Counter(pool.source)),
        "exclusions": dict(counts), "panoramas": int(pool.is_pano.sum()),
        "cross_source": "source IDs, normalized Mapillary/MSLS sequence keys, SHA, decoded pixels and development pHash (all rotations/mirrors)",
        "phash_threshold": 4, "whole_sequence_exclusion_after_query_duplicate_match": True,
        "calibration_final_access": "none; their entire deterministic query-role sequences are forbidden",
        "limitation": "unknown legacy-to-modern provider sequence aliases cannot be proven absent; no held-out claim is made"})
    print("usable candidate pool", len(pool), dict(Counter(pool.source)), flush=True)


def select(g2=False):
    config, _, store, output = settings()
    manifests = output / "manifests"
    manifests.mkdir(exist_ok=True)
    if (output / ("g2_selection.json" if g2 else "selection.json")).exists():
        return
    pool = pd.read_parquet(output / "candidate_pool.parquet")
    if pool[["source", "source_image_id", "sequence_id", "license", "attribution", "source_url"]].isna().any().any():
        raise RuntimeError("candidate provenance is incomplete")
    if not pool.license.isin(["CC BY-SA 4.0", "CC BY-NC-SA 4.0"]).all():
        raise RuntimeError("unapproved source license; existing gates stay enabled")
    base = fixed_g0(config)
    if not shapely.covers(load_aoi_boundary(WORKSPACE / config["aoi"]).geometry,
                          shapely.points(base.lon, base.lat)).all():
        raise RuntimeError("G0 contains references outside exact AOI; cannot silently alter baseline")
    base = decorate(base, config)
    base["is_pano"] = [bool(json.loads(v or "{}").get("is_pano", False)) for v in base.metadata_json]
    base["image_path"] = resolved_images(base.image_path)
    if not all(Path(p).is_relative_to(store) for p in base.image_path):
        raise RuntimeError("G0 physical images have not all migrated to the canonical store")
    base = base.drop(columns=[c for c in base.columns if c not in set(pool.columns) | {"id", "image_path"}])
    if g2:
        a = json.loads((output / "results/G1_smart.json").read_text())
        b = json.loads((output / "results/G1_random.json").read_text())
        if a["raw"]["accuracy_100m"] <= b["raw"]["accuracy_100m"]:
            save(output / "g2_selection.json", {"status": "skipped_smart_did_not_win_top1_100m",
                                               "rule": "strictly positive point gain; significance reported separately"})
            return
        order = json.loads((output / "smart_order.json").read_text())
        chosen = pool.set_index("id").loc[order[:max(0, config["targets"][1] - len(base))]].reset_index()
        pd.concat([base, chosen], ignore_index=True).to_parquet(manifests / "G2_smart.parquet", index=False)
        if not set(pd.read_parquet(manifests / "G1_smart.parquet", columns=["id"]).id).issubset(set(base.id) | set(chosen.id)):
            raise RuntimeError("G2 does not contain G1_smart")
        save(output / "g2_selection.json", {"status": "selected", "references": len(base) + len(chosen),
                                           "manifest_sha256": digest(manifests / "G2_smart.parquet")})
        return
    order = smart_order(pool, base)
    ordered_ids = pool.loc[order].id.tolist()
    save(output / "smart_order.json", ordered_ids)
    budget = min(len(pool), max(0, config["targets"][0] - len(base)))
    rng = np.random.default_rng(config["seed"])
    # A single draw from the identical audited pool. G0 order remains fixed.
    random = pool.iloc[rng.permutation(len(pool))[:budget]]
    smart = pool.loc[order[:budget]]
    galleries = {"G0": base, "G1_random": pd.concat([base, random], ignore_index=True),
                 "G1_smart": pd.concat([base, smart], ignore_index=True)}
    for name, frame in galleries.items():
        if frame.id.duplicated().any():
            raise RuntimeError("gallery contains duplicate identity")
        frame.to_parquet(manifests / f"{name}.parquet", index=False)
    save(output / "stage_contract.json", {"config_sha256": digest(WORKSPACE / "configs/moscow_gallery_scale.json"),
        "query_sha256": config["query_sha256"], "aoi_sha256": digest(WORKSPACE / config["aoi"]),
        "source_sha256_at_selection": digest(Path(__file__)), "candidate_pool_sha256": digest(output / "candidate_pool.parquet")})
    save(output / "selection.json", {"seed": config["seed"], "new_references_per_G1": budget,
        "pool_sha256": digest(output / "candidate_pool.parquet"), "smart_order_sha256": digest(output / "smart_order.json"),
        "manifests": {n: digest(manifests / f"{n}.parquet") for n in galleries},
        "query_coordinates_used": False, "selection": "H3-9 density water filling, sequence novelty, heading bins, seasons",
        "scope": "all galleries containing MSLS are research-only; production gates unchanged",
        "G2_rule": "run only if G1_smart raw top1 <=100m point accuracy exceeds G1_random"})


def selected_union(output):
    return pd.concat([pd.read_parquet(p) for p in sorted((output / "manifests").glob("G*.parquet"))],
                     ignore_index=True).drop_duplicates("id").sort_values("id").reset_index(drop=True)


def materialize():
    _, _, store, output = settings()
    frame = selected_union(output)
    all_receipts = {}
    cache_path = output / "materialized.json"
    if cache_path.exists():
        all_receipts = json.loads(cache_path.read_text())
    for archive, group in frame[frame.source.eq("msls")].groupby("archive_path", sort=False):
        with ZipFile(archive) as z:
            for i, row in enumerate(group.itertuples()):
                path = Path(row.image_path)
                if not path.is_relative_to(store) or path.name != row.source_image_id + ".jpg":
                    raise RuntimeError("unsafe canonical image path")
                if row.id in all_receipts:
                    if not path.is_file() or path.stat().st_size != all_receipts[row.id]["bytes"]:
                        raise RuntimeError("materialized image disappeared")
                    continue
                if path.is_file():
                    actual = digest(path)
                    if actual != row.file_sha256:
                        raise RuntimeError("existing physical MSLS image differs from ZIP; do not overwrite")
                else:
                    data = z.read(row.archive_member)
                    if hashlib.sha256(data).hexdigest() != row.file_sha256:
                        raise RuntimeError("audited archive member changed")
                    path.parent.mkdir(parents=True, exist_ok=True)
                    temporary = path.with_name(path.name + ".extracting")
                    temporary.write_bytes(data)
                    if digest(temporary) != row.file_sha256:
                        raise RuntimeError("extracted image checksum mismatch")
                    os.replace(temporary, path)
                all_receipts[row.id] = {"sha256": row.file_sha256, "bytes": path.stat().st_size}
                if i % 512 == 0:
                    save(cache_path, all_receipts)
            save(cache_path, all_receipts)
    # Every selected source is opened through its canonical path; no G*/images.
    smoke = frame.groupby("source", sort=True).head(4)
    for row in smoke.itertuples():
        with Image.open(row.image_path) as im:
            im.load()
        if digest(Path(row.image_path)) != row.file_sha256:
            raise RuntimeError("canonical reference smoke hash failed")
    save(output / "canonical_smoke.json", {"opened_ids": smoke.id.tolist(), "store": str(store),
        "all_selected_references": len(frame), "separate_per_gallery_image_copies": 0})


def compatible_cache(config, output):
    contract_path = output / "descriptor_contract.json"
    paths = [WORKSPACE / "data/embeddings/moscow_research_v5/sage-vitl",
             WORKSPACE / "data/embeddings/moscow_research_v5/gallery_union/sage-vitl"]
    cache, metas = {}, []
    for path in paths:
        meta = json.loads((path / "build_metadata.json").read_text())
        if meta["retriever"]["device"] != config["device"] or not meta["normalized"] or meta["dtype"] != "float32":
            raise RuntimeError("existing descriptor device/dtype/normalization mismatch")
        for name, expected in meta["artifact_sha256"].items():
            if digest(path / name) != expected:
                raise RuntimeError("existing descriptor fingerprint changed")
        if metas and metas[0]["retriever"] != meta["retriever"]:
            raise RuntimeError("existing SAGE-L caches have unequal fingerprints")
        ids = json.loads((path / "id_mapping.json").read_text())
        if isinstance(ids, dict):
            ids = ids.get("ids", ids.get("reference_ids"))
        if ids and isinstance(ids[0], dict):
            if [entry["row"] for entry in ids] != list(range(len(ids))):
                raise RuntimeError("existing descriptor row mapping is not contiguous")
            ids = [entry["reference_id"] for entry in ids]
        refs = [json.loads(line) for line in (path / "reference_metadata.jsonl").read_text().splitlines()]
        for i, (identity, ref) in enumerate(zip(ids, refs, strict=True)):
            cache[identity] = {"path": str((path / "descriptors.npy").resolve()), "row": i,
                               "image_sha256": ref["metadata"]["file_sha256"]}
        metas.append(meta)
    contract = {"retriever": metas[0]["retriever"], "batch_size": config["batch_size"],
                "torch_threads": config["torch_threads"], "gallery_metadata_sha256": [
                    digest(p / "build_metadata.json") for p in paths],
                "source": str((WORKSPACE / ".cache/torch/hub/chenshunpeng_SAGE_c7d6241c4885526d99d6c78c158024fc2a37097c").resolve()),
                "checkpoint_sha256": "31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc"}
    if contract_path.exists():
        saved = json.loads(contract_path.read_text())
        if saved != contract:
            raise RuntimeError("descriptor contract changed during this stage")
    else:
        save(contract_path, contract)
    return cache, contract


def create_model(config):
    import torch

    from ml.research.retrievers import research_retriever
    torch.set_num_threads(config["torch_threads"])
    os.environ["HF_HOME"] = str(WORKSPACE / ".cache/huggingface")
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["GEOSNAP_SAGE_SOURCE_DIR"] = str(WORKSPACE / ".cache/torch/hub/chenshunpeng_SAGE_c7d6241c4885526d99d6c78c158024fc2a37097c")
    checkpoint = WORKSPACE / ".cache/huggingface/hub/models--shunpeng--SAGE/blobs/31212d543304258b22cfce63aa9d433527a086832cfaec7fc9bca99fd53286dc"
    os.environ["GEOSNAP_SAGE_CHECKPOINT"] = str(checkpoint)
    model = research_retriever("sage-vitl", device=config["device"], allow_device_fallback=False,
                              batch_size=config["batch_size"], cache_dir=str(WORKSPACE / ".cache/torch/hub"))
    model.load()
    return model


def descriptor_chunks(output):
    """Retain an explicitly audited unreadable cache file without reopening it."""
    directory = output / "descriptors"
    recovery_path = output / "descriptor_recovery.json"
    excluded = set()
    if recovery_path.exists():
        recovery = json.loads(recovery_path.read_text())
        if recovery["contract_sha256"] != digest(output / "descriptor_contract.json"):
            raise RuntimeError("descriptor recovery belongs to a different encoder contract")
        excluded = set(recovery["excluded_receipts"])
        for name in excluded:
            try:
                valid = name == f"chunk-{int(name[6:-5]):06d}.json"
            except ValueError:
                valid = False
            if not valid:
                raise RuntimeError("invalid excluded descriptor receipt name")
    receipts = sorted(directory.glob("chunk-*.json"))
    # Reserve skipped and orphaned serials; never overwrite the unreadable
    # inode or an uncommitted vector file after a storage interruption.
    names = {p.name for p in receipts} | {p.name for p in directory.glob("chunk-*.npy")} | excluded
    serial = max((int(Path(name).stem.split("-")[1]) for name in names), default=-1) + 1
    return [p for p in receipts if p.name not in excluded], serial


def embed():
    config, _, _, output = settings()
    frame = selected_union(output)
    cache, contract = compatible_cache(config, output)
    contract_hash = digest(output / "descriptor_contract.json")
    directory = output / "descriptors"
    directory.mkdir(exist_ok=True)
    receipts, serial = descriptor_chunks(output)
    previously_committed = 0
    for receipt_path in receipts:
        receipt = json.loads(receipt_path.read_text())
        vector_path = receipt_path.with_suffix(".npy")
        if receipt["contract_sha256"] != contract_hash or digest(vector_path) != receipt["descriptors_sha256"]:
            raise RuntimeError("resumed chunk differs from its fixed fingerprint")
        for i, (identity, sha) in enumerate(zip(receipt["ids"], receipt["image_sha256"], strict=True)):
            if identity in cache:
                raise RuntimeError("image encoded more than once")
            cache[identity] = {"path": str(vector_path), "row": i, "image_sha256": sha}
        previously_committed += len(receipt["ids"])
    for row in frame.itertuples():
        if row.id in cache and cache[row.id]["image_sha256"] != row.file_sha256:
            raise RuntimeError("same image ID points to different physical bytes")
    missing = frame[~frame.id.isin(cache)]
    print("descriptors reused", len(frame) - len(missing), "new", len(missing), flush=True)
    if len(missing):
        model = create_model(config)
        metadata = model.metadata.to_dict()
        if metadata != contract["retriever"]:
            raise RuntimeError("loaded encoder differs from previously verified gallery fingerprint")
        for start in range(0, len(missing), 256):
            batch = missing.iloc[start:start + 256]
            for row in batch.itertuples():
                if digest(Path(row.image_path)) != row.file_sha256:
                    raise RuntimeError("photo changed before descriptor encoding")
            before = time.monotonic()
            vectors = model.embed_batch(batch.image_path.tolist())
            if vectors.dtype != np.float32 or not np.isfinite(vectors).all() or not np.allclose(
                    np.linalg.norm(vectors, axis=1), 1, atol=1e-5):
                raise RuntimeError("non-unit or invalid SAGE descriptors")
            path = directory / f"chunk-{serial:06d}.npy"
            temporary = path.with_suffix(".writing")
            with temporary.open("wb") as f:
                np.save(f, vectors, allow_pickle=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temporary, path)
            receipt = {"contract_sha256": contract_hash, "ids": batch.id.tolist(),
                       "image_sha256": batch.file_sha256.tolist(), "descriptors_sha256": digest(path),
                       "seconds": time.monotonic() - before, "shape": list(vectors.shape)}
            save(path.with_suffix(".json"), receipt)
            for i, row in enumerate(batch.itertuples()):
                cache[row.id] = {"path": str(path), "row": i, "image_sha256": row.file_sha256}
            serial += 1
            if start % 2048 == 0:
                save(output / "descriptor_progress.json", {"selected_references": len(frame),
                    "reused_at_phase_start": len(frame) - len(missing), "new_completed": start + len(batch),
                    "new_required": len(missing),
                    "committed_new_descriptor_rows": previously_committed + start + len(batch),
                    "total_new_descriptor_rows_required": previously_committed + len(missing),
                    "updated": time.time()})
                print("new descriptors", min(start + len(batch), len(missing)), "/", len(missing), flush=True)
        model.close()
    save(output / "descriptor_pool.json", {"contract_sha256": contract_hash, "entries": cache,
                                           "note": "one physical descriptor per image; galleries select rows"})


def query_vectors(config):
    queries = fixed_development(config)
    directory = V5 / "development/global_sage-vitl_new"
    saved = json.loads((directory / "query_contract.json").read_text())
    if saved["contract"]["manifest_sha256"] != config["query_sha256"] or digest(directory / "queries.npy") != saved["descriptors_sha256"]:
        raise RuntimeError("development descriptor fingerprint changed")
    if saved["contract"]["image_sha256"] != queries.file_sha256.tolist():
        raise RuntimeError("query descriptor ordering differs from fixed image hashes")
    if saved["descriptors_sha256"] != "aad595a7963c4103e3c48b8d8f0463d2edb6a7a01bfe5f98c3099c82a5deed73":
        raise RuntimeError("the previously verified MPS SAGE-L query cache is required")
    return queries, np.load(directory / "queries.npy", allow_pickle=False)


def exact_scores(frame, qvectors, entries):
    """One result matrix; at most one existing descriptor block in memory."""
    scores = np.empty((len(qvectors), len(frame)), dtype=np.float32)
    groups = defaultdict(list)
    for i, row in enumerate(frame.itertuples()):
        entry = entries[row.id]
        if entry["image_sha256"] != row.file_sha256:
            raise RuntimeError("descriptor identity mismatch during retrieval")
        groups[entry["path"]].append((i, entry["row"]))
    with threadpool_limits(limits=2):
        for path, selected in groups.items():
            block = np.load(path, allow_pickle=False)
            positions, rows = np.asarray(selected).T
            # Avoid making a full 100k x 8448 matrix or a second FAISS copy.
            for start in range(0, len(rows), 2048):
                subset = rows[start:start + 2048]
                scores[:, positions[start:start + 2048]] = qvectors @ block[subset].T
            del block
    return scores


def summarize(frame, queries, scores):
    latitude, longitude = np.radians(frame.lat.to_numpy(float)), np.radians(frame.lon.to_numpy(float))
    gallery_headings = frame.heading.to_numpy(float)
    errors, ranks, positives, nearest, heading_coverage, top100 = [], [], [], [], [], []
    for i, query in enumerate(queries.itertuples()):
        d = distances(query.lat, query.lon, latitude, longitude)
        order = np.argsort(-scores[i], kind="stable")
        pos = np.flatnonzero(d[order] <= 100)
        ranks.append(int(pos[0]) + 1 if len(pos) else None)
        positives.append(int((d <= 100).sum()))
        errors.append(float(d[order[0]]))
        nearest.append(float(d.min()))
        heading_delta = np.abs((gallery_headings - query.heading + 180) % 360 - 180)
        heading_coverage.append(bool(((d <= 100) & (heading_delta <= 45)).any()))
        top100.append(order[:100].tolist())
    density = frame.groupby("cell").size()
    bins = frame[frame.heading_bin.ge(0)].groupby("cell").heading_bin.nunique()
    raw = raw_metrics(errors)
    result = {
        "gallery": {"references": len(frame), "by_source": dict(Counter(frame.source)),
                    "unique_provider_sequences": frame.sequence_key.nunique(), "occupied_h3_r9": len(density),
                    "median_references_per_occupied_cell": float(density.median()),
                    "heading_available_fraction": float(frame.heading.notna().mean()),
                    "mean_heading_bins_per_occupied_cell": float(bins.reindex(density.index, fill_value=0).mean()),
                    "occupied_cell_heading_bins": len(frame[["cell", "heading_bin"]].drop_duplicates()),
                    "production_compatible": bool(frame.production_compatible.all())},
        "coverage": {**{f"within_{m}m": float(np.mean(np.array(nearest) <= m)) for m in (25, 50, 100)},
                     "within_100m_and_heading_45deg": float(np.mean(heading_coverage)),
                     "query_heading_available": int(queries.heading.notna().sum())},
        "raw": raw, "retrieval": retrieval_metrics(ranks, gallery_size=len(frame), positive_counts=positives),
        "diagnosis": {"no_coverage": sum(p == 0 for p in positives),
                      "retrieval_miss_top100": sum(p > 0 and (r is None or r > 100) for p, r in zip(positives, ranks, strict=True)),
                      "wrong_top1_positive_in_top100": sum(r is not None and 1 < r <= 100 for r in ranks),
                      "correct_top1": sum(r == 1 for r in ranks)},
    }
    rows = {"query_ids": queries.id.tolist(), "errors_m": errors, "positive_ranks": ranks,
            "positive_count_100m": positives, "nearest_reference_m": nearest, "top100_gallery_rows": top100}
    return result, rows


def evaluate(name):
    config, _, _, output = settings()
    destination = output / "results" / f"{name}.json"
    if destination.exists():
        return
    manifest = output / "manifests" / f"{name}.parquet"
    frame = pd.read_parquet(manifest)
    queries, qvectors = query_vectors(config)
    entries = json.loads((output / "descriptor_pool.json").read_text())["entries"]
    scores = exact_scores(frame, qvectors, entries)
    result, rows = summarize(frame, queries, scores)
    result.update({"name": name, "query_sha256": config["query_sha256"], "gallery_sha256": digest(manifest),
                   "descriptor_contract_sha256": digest(output / "descriptor_contract.json"),
                   "estimator": "top1_reference_coordinate", "confidence_filtering": False})
    save(output / "results" / f"{name}_rows.json", rows)
    save(destination, result)
    print(name, "references", len(frame), "raw100", result["raw"]["accuracy_100m"], flush=True)


def perspective(image, yaw_degrees, size=322):
    """90-degree pinhole projection, zero pitch/roll, bilinear longitude wrap.

    Yaw is relative to the panorama center. No heading ground truth is needed
    because the four views span a full turn and retain a single parent ID.
    """
    a = np.asarray(image.convert("RGB"), dtype=np.float32)
    height, width = a.shape[:2]
    xy = (np.arange(size, dtype=float) + 0.5) / size * 2 - 1
    x, y = np.meshgrid(xy, xy)
    lon = np.arctan2(x, np.ones_like(x)) + np.radians(yaw_degrees)
    lat = np.arctan2(-y, np.sqrt(1 + x * x))
    u = ((lon / (2 * np.pi) + 0.5) * width - 0.5) % width
    v = np.clip((0.5 - lat / np.pi) * height - 0.5, 0, height - 1)
    x0, y0 = np.floor(u).astype(int), np.floor(v).astype(int)
    x1, y1 = (x0 + 1) % width, np.minimum(y0 + 1, height - 1)
    wx, wy = (u - x0)[..., None], (v - y0)[..., None]
    pixels = ((1 - wy) * ((1 - wx) * a[y0, x0] + wx * a[y0, x1])
              + wy * ((1 - wx) * a[y1, x0] + wx * a[y1, x1]))
    return Image.fromarray(np.clip(np.rint(pixels), 0, 255).astype(np.uint8))


def panorama():
    config, _, _, output = settings()
    if (output / "panorama.json").exists():
        return
    pool_audit = json.loads((output / "candidate_audit.json").read_text())
    name = "G2_smart" if (output / "results/G2_smart.json").exists() else "G1_smart"
    frame = pd.read_parquet(output / "manifests" / f"{name}.parquet")
    eligible = frame[frame.is_pano.fillna(False)].sort_values("selection_hash")
    # Only actual 2:1 equirectangular images; a flag alone is insufficient.
    selected = []
    for row in eligible.itertuples():
        with Image.open(row.image_path) as im:
            if abs(im.width / im.height - 2) <= 0.05:
                selected.append(row.Index)
    if len(selected) < config["panorama_minimum"]:
        save(output / "panorama.json", {"status": "skipped_too_few_eligible_equirectangular_references",
            "available_pool_flagged_panoramas": pool_audit["panoramas"], "gallery": name,
            "gallery_flagged": len(eligible), "verified_equirectangular": len(selected),
            "predeclared_minimum": config["panorama_minimum"]})
        return
    selected = selected[:config["panorama_maximum"]]
    parents = frame.loc[selected]
    directory = output / "panorama_descriptors"
    directory.mkdir(exist_ok=True)
    model = None
    pieces = []
    for i, row in enumerate(parents.itertuples()):
        path = directory / f"{row.id}.npy"
        receipt_path = path.with_suffix(".json")
        if receipt_path.exists():
            receipt = json.loads(receipt_path.read_text())
            if receipt["image_sha256"] != row.file_sha256 or receipt["descriptors_sha256"] != digest(path):
                raise RuntimeError("panorama cache mismatch")
            values = np.load(path, allow_pickle=False)
        else:
            if model is None:
                model = create_model(config)
            with Image.open(row.image_path) as im:
                views = [perspective(im, yaw, config["panorama_output_size"]) for yaw in config["panorama_yaws"]]
            values = model.embed_batch(views)
            with path.open("wb") as f:
                np.save(f, values, allow_pickle=False)
            save(receipt_path, {"image_sha256": row.file_sha256, "descriptors_sha256": digest(path),
                                "contract_sha256": digest(output / "descriptor_contract.json"),
                                "yaws": config["panorama_yaws"], "hfov": 90, "pitch": 0,
                                "physical_parent_id": row.id})
        pieces.append(values)
        if i % 100 == 0:
            print("panorama parents", i, "/", len(parents), flush=True)
    if model:
        model.close()
    queries, qvectors = query_vectors(config)
    entries = json.loads((output / "descriptor_pool.json").read_text())["entries"]
    scores = exact_scores(frame, qvectors, entries)
    views = np.concatenate(pieces)
    with threadpool_limits(limits=2):
        replacement = (qvectors @ views.T).reshape(len(queries), len(parents), 4).max(axis=2)
    scores[:, selected] = replacement
    result, rows = summarize(frame, queries, scores)
    original_rows = json.loads((output / "results" / f"{name}_rows.json").read_text())
    paired = paired_group_bootstrap(original_rows["errors_m"], rows["errors_m"], queries.h3_coarse.tolist())
    save(output / "results/panorama_B_rows.json", rows)
    save(output / "panorama.json", {"status": "completed_development_only", "gallery": name,
        "parents": len(parents), "views": len(parents) * 4, "physical_image_copies_created": 0,
        "parent_scores": "maximum of four perspective similarities, one retrieval identity and coordinate per parent",
        "A": json.loads((output / "results" / f"{name}.json").read_text()), "B": result,
        "paired_gain": paired, "work_stops_after_this_single_AB": True})


def archive_cleanup():
    _, _, store, output = settings()
    receipt_path = output / "storage/download_archives.json"
    if receipt_path.exists():
        return
    archives = archive_inventory(store, output)
    prior = json.loads((WORKSPACE / "data/raw/moscow/msls_extract.stats.json").read_text())
    meta = json.loads((WORKSPACE / "data/raw/moscow/msls_selection.stats.json").read_text())["metadata_archive"]
    sources = [(Path(r["path"]), name) for name, r in prior["archives"].items()]
    sources.append((Path(meta["path"]), "msls_metadata.zip"))
    receipts = []
    for source, official_name in sources:
        if source.is_symlink():
            prior_receipt = output / "storage" / f"{official_name}.verified.json"
            if prior_receipt.exists():
                receipts.append(json.loads(prior_receipt.read_text()))
            continue
        if not source.is_file():
            raise RuntimeError("known GeoSnap archive disappeared during cleanup")
        candidates = [Path(r["path"]) for r in archives if Path(r["path"]).is_relative_to(store)
                      and r["bytes"] == source.stat().st_size]
        target = candidates[0] if candidates else store / "archives" / official_name
        if target.exists() and digest(target) != digest(source):
            raise RuntimeError("external archive differs; original Downloads file retained")
        verified = copy_verified(source, target)
        with ZipFile(target) as z:
            # Actually decode one Moscow image, or parse a metadata row.
            entries = [n for n in z.namelist() if n.startswith("train_val/moscow/") and n.endswith(".jpg")]
            if entries:
                with Image.open(io.BytesIO(z.read(entries[0]))) as im:
                    im.load()
            else:
                if "key" not in z.read("train_val/moscow/database/raw.csv").splitlines()[0].decode():
                    raise RuntimeError("archive metadata smoke failed")
        save(output / "storage" / f"{official_name}.verified.json", {"source": str(source),
             "canonical": str(target), **verified, "smoke": "passed before removal"})
        # A symlink preserves the previously recorded download path without
        # retaining another 11 GB archive on the internal disk.
        source.unlink()
        source.symlink_to(target)
        receipts.append({"source": str(source), "canonical": str(target), **verified})
        print("archive local duplicate removed", official_name, flush=True)
    save(receipt_path, {"verified_local_bytes_removed": sum(r["bytes"] for r in receipts), "archives": receipts})


def report():
    config, _, store, output = settings()
    queries = fixed_development(config)
    names = ["G0", "G1_random", "G1_smart", "G2_smart"]
    results = {name: json.loads((output / "results" / f"{name}.json").read_text())
               for name in names if (output / "results" / f"{name}.json").exists()}
    rows = {name: json.loads((output / "results" / f"{name}_rows.json").read_text()) for name in results}
    comparisons = {}
    for a, b in [("G0", "G1_smart"), ("G1_random", "G1_smart"), ("G1_smart", "G2_smart")]:
        if a in rows and b in rows:
            comparisons[f"{a}_to_{b}"] = paired_group_bootstrap(rows[a]["errors_m"], rows[b]["errors_m"],
                                                                queries.h3_coarse.tolist())
    protected = json.loads((V5 / "baseline_snapshot.json").read_text())["files"]
    changed = [p for p, sha in protected.items() if digest(WORKSPACE / p) != sha]
    if changed:
        raise RuntimeError(f"production baseline changed: {changed}")
    storage = json.loads((output / "storage/summary.json").read_text())
    archives = json.loads((output / "storage/download_archives.json").read_text())
    payload = {"status": "completed_development_gallery_experiment_stop_here", "canonical_store": str(store),
        "query_count": len(queries), "query_sha256": config["query_sha256"], "model": "SAGE ViT-L No-Encoder",
        "device": config["device"], "primary_estimator": "top1 coordinate, exact cosine, no abstention",
        "galleries": results, "paired_comparisons": comparisons,
        "panorama": json.loads((output / "panorama.json").read_text()),
        "storage": {"verified_local_bytes_removed": storage["internal_bytes_removed"] + archives["verified_local_bytes_removed"],
                    "note": "logical bytes in removed verified copies; APFS hardlinks and other activity can change actual free-space gain",
                    "directories": storage["directories"], "archives": archives["archives"]},
        "inventory": json.loads((output / "initial_inventory.json").read_text()),
        "usable_pool": json.loads((output / "candidate_audit.json").read_text()),
        "production_frozen_files_verified": len(protected), "calibration_and_final_opened_in_this_stage": False,
        "limits": ["MSLS-containing galleries are research-only", "existing development population is fixed, not a new final test",
                   "legacy Mapillary alias coverage cannot be certified complete; duplicate audit and full-sequence exclusions are retained"]}
    last_name = "G2_smart" if "G2_smart" in results else max(
        ["G1_smart", "G1_random"], key=lambda n: results[n]["raw"]["accuracy_100m"])
    failure_groups = {k: v for k, v in results[last_name]["diagnosis"].items() if k != "correct_top1"}
    largest = max(failure_groups, key=failure_groups.get)
    labels = {"no_coverage": "нет reference в пределах 100 м",
              "retrieval_miss_top100": "положительный reference не попадает в top-100",
              "wrong_top1_positive_in_top100": "положительный reference есть в top-100, но top-1 неверен"}
    next_steps = {"no_coverage": "Расширить production-compatible галерею по пустым и редким H3-ячейкам Москвы.",
                  "retrieval_miss_top100": "Провести один контролируемый тест улучшения retriever на этой фиксированной галерее и development-выборке.",
                  "wrong_top1_positive_in_top100": "Проверить один способ визуальной проверки top-100 кандидатов при фиксированных SAGE-L и галерее."}
    scale_gain = (results["G2_smart"]["raw"]["accuracy_100m"] - results["G1_smart"]["raw"]["accuracy_100m"]
                  if "G2_smart" in results else None)
    next_step = next_steps[largest]
    if scale_gain is not None and scale_gain >= 0.02:
        next_step = "Расширить production-compatible галерею той же политикой отбора: измеренный прирост от 60k к 100k ещё существенен."
    payload["conclusion"] = {"diagnostic_gallery": last_name, "largest_error_group": largest,
        "bottleneck": labels[largest], "G1_smart_minus_random_raw100_pp": 100 * (
            results["G1_smart"]["raw"]["accuracy_100m"] - results["G1_random"]["raw"]["accuracy_100m"]),
        "G2_minus_G1_raw100_pp": None if scale_gain is None else 100 * scale_gain,
        "highest_value_next_step": next_step, "next_step_was_executed": False,
        "note": "point estimates guide this recommendation; inspect paired intervals before a significance claim"}
    save(output / "report.json", payload)
    def pct(v):
        return f"{v * 100:.2f}"
    def rank(v):
        return "∞" if v is None else f"{v:,.1f}"
    header = ["Метрика"] + names
    lines = ["# GeoSnap: фиксированный SAGE-L, размер и состав галереи", "", f"Canonical store: `{store}`.", "",
             "Development: 1 184 неизменённых запроса. Exact cosine, координаты top-1, без abstention. "
             "MSLS — research-only; production не изменён. Calibration/final не открывались в этом этапе.", "",
             "| " + " | ".join(header) + " |", "| " + " | ".join(["---"] * len(header)) + " |"]
    metrics = [
        ("References", lambda r: str(r["gallery"]["references"])),
        ("Mapillary / KartaView / MSLS", lambda r: " / ".join(str(r["gallery"]["by_source"].get(s, 0)) for s in ["mapillary", "kartaview", "msls"])),
        ("Sequences / H3-9 cells", lambda r: f'{r["gallery"]["unique_provider_sequences"]} / {r["gallery"]["occupied_h3_r9"]}'),
        ("Median refs/cell / mean heading bins/cell", lambda r: f'{r["gallery"]["median_references_per_occupied_cell"]:.1f} / {r["gallery"]["mean_heading_bins_per_occupied_cell"]:.2f}'),
        ("Heading available, %", lambda r: pct(r["gallery"]["heading_available_fraction"])),
        ("Coverage ≤25 / 50 / 100 m, %", lambda r: " / ".join(pct(r["coverage"][f"within_{m}m"]) for m in (25, 50, 100))),
        ("Coverage ≤100 m + heading ≤45°, %", lambda r: pct(r["coverage"]["within_100m_and_heading_45deg"])),
        ("R@1 / 5 / 10, %", lambda r: " / ".join(pct(r["retrieval"]["recall_at"][str(k)]) for k in (1, 5, 10))),
        ("R@20 / 50 / 100, %", lambda r: " / ".join(pct(r["retrieval"]["recall_at"][str(k)]) for k in (20, 50, 100))),
        ("Positive rank median / p75 / p90, all queries", lambda r: " / ".join(rank(r["retrieval"]["positive_rank_quantiles_all_query"][q]) for q in ("0.5", "0.75", "0.9"))),
        ("Positive rank median / p75 / p90, covered only", lambda r: " / ".join(rank(r["retrieval"]["positive_rank_quantiles_covered_only"][q]) for q in ("0.5", "0.75", "0.9"))),
        ("RAW ≤25 / 50 / 100 m, %", lambda r: " / ".join(pct(r["raw"][f"accuracy_{m}m"]) for m in (25, 50, 100))),
        ("Median / p90 error, m", lambda r: f'{r["raw"]["median_error_m"]:.0f} / {r["raw"]["p90_error_m"]:.0f}'),
        (">500 m, %", lambda r: pct(r["raw"]["catastrophic_gt500m_rate"])),
        ("No coverage / retrieval miss / wrong top1", lambda r: " / ".join(str(r["diagnosis"][k]) for k in ("no_coverage", "retrieval_miss_top100", "wrong_top1_positive_in_top100"))),
    ]
    for label, fn in metrics:
        lines.append("| " + " | ".join([label] + [fn(results[n]) if n in results else "не запускался: smart не выиграл" for n in names]) + " |")
    lines += ["", "∞ включает запросы без nearby reference; они остаются в denominator. "
              "Wrong top1 означает положительный reference на позициях 2–100; retrieval miss — положительный reference ниже 100.",
              "", f"Удалено проверенных локальных копий: {payload['storage']['verified_local_bytes_removed'] / 1e9:.2f} GB (логические байты).",
              "", "Полные метрики, paired geographic bootstrap, hashes и ограничения: `report.json` рядом с `manifests/`, `descriptors/`, `results/`.",
              "", "После этого этапа новые модели, confidence, reranking, обучение и production switch не запускались."]
    audit = payload["usable_pool"]
    lines += ["", f"Исходный store: {payload['inventory']['formats']}. "
              "Точное AOI: 249 190 кадров MSLS в доступных архивах, 225 panorama flags; "
              "это число до image/leakage-аудита, а не размер usable pool.",
              "", f"Новые допустимые references после аудита: {audit['by_source']}; "
              f"G0: {results['G0']['gallery']['by_source']}. MSLS помечен research-only в каждой строке.",
              "", "Проверенные локальные копии удалены из следующих каталогов; старые пути сохранены symlink:", ""]
    for entry in storage["directories"]:
        lines.append(f"- `{entry['source']}` — {entry['verified_bytes'] / 1e9:.3f} GB; canonical `{entry['canonical']}`.")
    for entry in archives["archives"]:
        lines.append(f"- `{entry['source']}` — {entry['bytes'] / 1e9:.3f} GB; проверенная копия архива на внешнем томе.")
    pano = payload["panorama"]
    if pano["status"].startswith("skipped"):
        lines += ["", f"Panorama A/B пропущен: в контрольной галерее подтверждено {pano['verified_equirectangular']} "
                  f"equirectangular parents, минимум был заранее установлен в {pano['predeclared_minimum']}."]
    else:
        lines += ["", f"Panorama A/B: {pano['parents']} родителей; RAW100 "
                  f"{pct(pano['A']['raw']['accuracy_100m'])}% → {pct(pano['B']['raw']['accuracy_100m'])}%. "
                  "Четыре views объединены в одного physical parent; дополнительные JPG не созданы."]
    lines += ["", f"Основная группа ошибок ({last_name}): {labels[largest]} — {failure_groups[largest]}/{len(queries)}.",
              "", f"Следующий наиболее ценный шаг: {next_step} Он не запущен.",
              "", f"Manifests: `{output / 'manifests'}`; descriptors: `{output / 'descriptors'}` "
              f"и `{output / 'descriptor_pool.json'}`; отчёт: `{output / 'report.json'}`.",
              "", "Парные geographic-bootstrap интервалы (5000 повторов; значения уже в процентных пунктах):", "",
              "```json", json.dumps(comparisons, ensure_ascii=False, indent=2), "```"]
    text = "\n".join(lines) + "\n"
    (output / "report.md").write_text(text)
    (WORKSPACE / "docs/gallery_scale_v6_results.md").write_text(text)
    save(WORKSPACE / "data/evaluation/moscow_gallery_scale_v6/result_pointer.json", {"report": str(output / "report.json"),
        "report_sha256": digest(output / "report.json"), "status": payload["status"]})
    print("REPORT COMPLETE", str(output / "report.md"), flush=True)


def run():
    _, _, _, output = settings()
    log = output / "run.log"
    if (output / "report.json").exists() and (output / "report.md").exists() and (
            WORKSPACE / "data/evaluation/moscow_gallery_scale_v6/result_pointer.json").exists():
        print("This gallery stage is complete; no further experiment runs.", flush=True)
        return
    local_state = WORKSPACE / "data/evaluation/moscow_gallery_scale_v6"
    local_state.mkdir(exist_ok=True)
    lock = (local_state / "controller.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    subprocess.Popen(["/usr/bin/caffeinate", "-i", "-w", str(os.getpid())],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    steps = (["storage"] if not (output / "storage/summary.json").exists() else [])
    steps += ["prepare", "archive-cleanup", "audit", "select", "materialize", "embed",
             "G0", "G1_random", "G1_smart", "select-g2", "materialize", "embed", "G2_smart", "panorama", "report"]
    with log.open("a", buffering=1) as stream:
        for phase in steps:
            if phase == "G2_smart" and not (output / "manifests/G2_smart.parquet").exists():
                continue
            save(output / "status.json", {"status": "running", "phase": phase, "pid": os.getpid(), "updated": time.time()})
            stream.write(f"\nPHASE {phase}\n")
            command = ([sys.executable, "-m", "ml.research.gallery_scale_storage"] if phase == "storage" else
                       [sys.executable, "-m", "ml.research.gallery_scale", "--phase", phase])
            result = subprocess.run(command,
                                    cwd=WORKSPACE, stdout=stream, stderr=subprocess.STDOUT, check=False)
            if result.returncode:
                save(output / "status.json", {"status": "failed_resumable", "phase": phase, "returncode": result.returncode})
                raise RuntimeError(f"gallery phase {phase} failed; see {log}")
        save(output / "status.json", {"status": "completed_stop_waiting_for_user", "report": str(output / "report.md")})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    phases = {"prepare": prepare, "archive-cleanup": archive_cleanup, "audit": audit, "select": select,
              "select-g2": lambda: select(g2=True), "materialize": materialize, "embed": embed,
              **{n: (lambda name=n: evaluate(name)) for n in ["G0", "G1_random", "G1_smart", "G2_smart"]},
              "panorama": panorama, "report": report, "run": run}
    parser.add_argument("--phase", choices=phases, required=True)
    phase = parser.parse_args().phase
    phases[phase]()
