"""Metadata-only prospective query assignment, before any candidate inference.

The eventual builder must download, audit exact/near duplicates and seal the
private manifests. Assignment alone is NOT a completed or sealed benchmark.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

import h3
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from ml.ingestion.mapillary_citywide import load_aoi_boundary
from ml.ingestion.merge_sources import normalize_record
from ml.ingestion.schema import write_manifest
from ml.research.seal import sha256, write_once

ROOT = Path("data/evaluation/moscow_research_v5")
RADIUS = 6371008.8
TARGETS = {"development": 1200, "calibration": 800, "final": 1200}


def score(*values):
    return hashlib.sha256(("20260905\0" + "\0".join(str(v) for v in values)).encode()).hexdigest()


def distant_mask(frame, others, minimum):
    if frame.empty or others.empty:
        return np.ones(len(frame), dtype=bool)
    tree = BallTree(np.radians(others[["lat", "lon"]].to_numpy(float)), metric="haversine")
    distances, _ = tree.query(np.radians(frame[["lat", "lon"]].to_numpy(float)), k=1)
    return distances[:, 0] * RADIUS >= minimum


def assign(frame, historical_development, targets=TARGETS):
    """Whole-sequence roles, deterministic geo blocks and measured embargo."""
    if frame[["source", "sequence_id", "source_image_id"]].isna().any().any():
        raise ValueError("every record needs a provider, sequence and image identity")
    frame = frame.copy()
    frame["sequence_key"] = frame.source.astype(str) + "::" + frame.sequence_id.astype(str)
    frame["selection_hash"] = [
        score("frame", s, i) for s, i in zip(frame.sequence_key, frame.source_image_id, strict=True)
    ]
    frame = frame.sort_values("selection_hash").drop_duplicates("sequence_key")
    frame["gallery_role"] = [int(score("role", s)[:8], 16) % 10 < 6 for s in frame.sequence_key]
    gallery = frame[frame.gallery_role].copy()
    query = frame[~frame.gallery_role].copy()
    query["evaluation_geo_group_id"] = [
        h3.latlng_to_cell(float(a), float(b), 7) for a, b in zip(query.lat, query.lon, strict=True)
    ]
    query["evaluation_split"] = [
        ("development" if (v := int(score("block", g)[:8], 16) % 8) < 3 else "calibration" if v < 5 else "final")
        for g in query.evaluation_geo_group_id
    ]
    query["source_region_rank"] = query.groupby(["source", "evaluation_geo_group_id"]).cumcount()
    # Round-robin areas within each source, rather than concentrating on long captures.
    query = query.sort_values(["source_region_rank", "selection_hash"])
    selected = {}
    counts = {}
    previous = query.iloc[:0]
    for split in ("final", "calibration", "development"):
        current = query[query.evaluation_split == split].copy()
        counts[split] = {"assigned": len(current)}
        if split != "development":
            current = current[distant_mask(current, historical_development, 250)]
        counts[split]["after_historical_development_embargo"] = len(current)
        current = current[distant_mask(current, previous, 250)]
        counts[split]["after_cross_split_embargo"] = len(current)
        kept = []
        for index, row in current.iterrows():
            if kept:
                coordinates = current.loc[kept, ["lat", "lon"]].to_numpy(float)
                a, b = np.radians(coordinates[:, 0]), np.radians(coordinates[:, 1])
                c, d = np.radians([row.lat, row.lon])
                hav = np.sin((a - c) / 2) ** 2 + np.cos(a) * np.cos(c) * np.sin((b - d) / 2) ** 2
                if np.any(2 * RADIUS * np.arcsin(np.sqrt(np.clip(hav, 0, 1))) < 30):
                    continue
            kept.append(index)
            if len(kept) >= targets[split]:
                break
        selected[split] = current.loc[kept].copy()
        counts[split]["selected"] = len(kept)
        counts[split]["providers"] = dict(Counter(selected[split].source))
        previous = pd.concat([previous, selected[split]], ignore_index=True)
    return selected, gallery, counts


def run(discovery):
    private = ROOT / "prospective/private"
    if (ROOT / "prospective/assignment.json").exists():
        raise ValueError("prospective assignment already exists; never resample a test")
    history_paths = sorted(Path("data/evaluation").glob("moscow_real*/*.parquet"))
    historical = pd.concat([pd.read_parquet(p) for p in history_paths], ignore_index=True)
    exposed = set(zip(historical.source.astype(str), historical.sequence_id.astype(str), strict=True))
    boundary = load_aoi_boundary(Path("data/raw/moscow/moscow_admin_boundary.geojson"))
    rows = []
    for source, path in [("mapillary", discovery), ("kartaview", Path("data/raw/moscow/kartaview_sequences_raw.json"))]:
        for r in json.loads(path.read_text()):
            if (source, str(r.get("sequence_id"))) in exposed:
                continue
            if not boundary.covers(float(r["lon"]), float(r["lat"])):
                continue
            row = normalize_record(source, r, image_root=ROOT / "prospective/private/images")
            if row is not None:
                rows.append(row)
    frame = pd.DataFrame(rows).drop_duplicates(["source", "source_image_id"])
    old_dev = pd.read_parquet("data/evaluation/moscow_real_v4/development_queries.parquet")
    selected, gallery, counts = assign(frame, old_dev)
    private.mkdir(parents=True, exist_ok=True)
    manifests = {}
    for split, current in selected.items():
        path = private / f"{split}_assigned.parquet"
        write_manifest(current, path)
        path.chmod(0o600)
        manifests[split] = {"path": str(path), "sha256": sha256(path)}
    # Acquisition may use only gallery-role sequences; query coordinates are not inputs.
    # Keep all discovered records from these sequences for later independent density sampling.
    keys = set(gallery.sequence_key)
    gallery_pool = frame[(frame.source.astype(str) + "::" + frame.sequence_id.astype(str)).isin(keys)]
    write_manifest(gallery_pool, ROOT / "prospective/gallery_acquisition_pool.parquet")
    report = {
        "status": "metadata_assigned_not_yet_downloaded_audited_or_sealed",
        "protocol_sha256": sha256(Path("configs/moscow_research_v5_protocol.json")),
        "source_discovery_sha256": sha256(discovery),
        "historical_manifest_hashes": {str(p): sha256(p) for p in history_paths},
        "eligible_fresh_records": len(frame),
        "eligible_fresh_sequences": frame.groupby(["source", "sequence_id"]).ngroups,
        "gallery_role_sequences": len(gallery),
        "gallery_acquisition_pool_rows": len(gallery_pool),
        "splits": counts,
        "private_manifests": manifests,
        "rules": {
            "gallery_role_probability": 0.6,
            "query_role_probability": 0.4,
            "h3_resolution": 7,
            "embargo_m": 250,
            "spacing_m": 30,
            "one_query_per_provider_sequence": True,
            "no_positive_requirement": True,
            "no_model_inference_used": True,
            "historical_v4_development_embargo_for_calibration_and_final_m": 250,
        },
    }
    write_once(ROOT / "prospective/assignment.json", report)
    print(
        json.dumps(
            {
                k: report[k]
                for k in [
                    "status",
                    "eligible_fresh_records",
                    "eligible_fresh_sequences",
                    "gallery_role_sequences",
                    "splits",
                ]
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--discovery", type=Path, required=True)
    run(parser.parse_args().discovery)
