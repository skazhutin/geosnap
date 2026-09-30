"""Reference acquisition by global coverage and viewpoint novelty, without query labels."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import h3
import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from ml.ingestion.merge_sources import normalize_record
from ml.ingestion.schema import write_manifest
from ml.research.prepare_queries import RADIUS, ROOT, score
from ml.research.seal import sha256, write_once


def run(discovery: Path, budget: int):
    output = ROOT / "gallery_expansion"
    if (output / "plan.json").exists():
        raise ValueError("acquisition plan is already fixed")
    baseline = pd.read_parquet("data/evaluation/moscow_real_v4/gallery.parquet")
    future_gallery = pd.read_parquet(ROOT / "prospective/gallery_acquisition_pool.parquet")
    allowed = set(zip(baseline.source.astype(str), baseline.sequence_id.astype(str), strict=True)) | set(
        zip(future_gallery.source.astype(str), future_gallery.sequence_id.astype(str), strict=True)
    )
    baseline_ids = set(zip(baseline.source.astype(str), baseline.source_image_id.astype(str), strict=True))
    development = pd.read_parquet(
        "data/evaluation/moscow_real_v4/development_queries.parquet", columns=["source", "sequence_id"]
    )
    blocked = set(zip(development.source.astype(str), development.sequence_id.astype(str), strict=True))
    rows = []
    sources = [("mapillary", discovery), ("kartaview", Path("data/raw/moscow/kartaview_sequences_raw.json"))]
    for source, path in sources:
        for r in json.loads(path.read_text()):
            if (source, str(r.get("sequence_id"))) not in allowed or (source, str(r.get("sequence_id"))) in blocked:
                continue
            if (source, str(r["source_image_id"])) in baseline_ids:
                continue
            row = normalize_record(source, r, image_root=output / "images")
            if row is not None:
                rows.append(row)
    pool = pd.DataFrame(rows).drop_duplicates(["source", "source_image_id"])
    from ml.ingestion.mapillary_citywide import load_aoi_boundary

    boundary = load_aoi_boundary(Path("data/raw/moscow/moscow_admin_boundary.geojson"))
    pool = pool[[boundary.covers(float(r.lon), float(r.lat)) for r in pool.itertuples()]].copy()
    pool["cell"] = [h3.latlng_to_cell(float(r.lat), float(r.lon), 9) for r in pool.itertuples()]
    pool["heading_bin"] = [int(float(h) % 360 // 45) if pd.notna(h) else -1 for h in pool.heading]
    pool["selection_hash"] = [score("reference", r.source, r.source_image_id) for r in pool.itertuples()]
    tree = BallTree(np.radians(baseline[["lat", "lon"]].to_numpy(float)), metric="haversine")
    nearest, _ = tree.query(np.radians(pool[["lat", "lon"]].to_numpy(float)))
    pool["nearest_baseline_m"] = nearest[:, 0] * RADIUS
    existing = {
        (
            h3.latlng_to_cell(float(r.lat), float(r.lon), 9),
            int(float(r.heading) % 360 // 45) if pd.notna(r.heading) else -1,
        )
        for r in baseline.itertuples()
    }
    pool["new_view_bin"] = [(r.cell, r.heading_bin) not in existing for r in pool.itertuples()]
    # Round-robin cell/viewpoint groups, preferring entirely uncovered coordinates.
    diverse = pool.sort_values(["nearest_baseline_m", "new_view_bin", "selection_hash"], ascending=[False, False, True])
    diverse["within_bin_rank"] = diverse.groupby(["cell", "heading_bin"]).cumcount()
    diverse = diverse.sort_values(
        ["within_bin_rank", "new_view_bin", "nearest_baseline_m", "selection_hash"],
        ascending=[True, False, False, True],
    ).head(budget)
    random = pool.sort_values("selection_hash").head(len(diverse))
    union = pd.concat([diverse, random]).drop_duplicates("id")
    output.mkdir(parents=True, exist_ok=True)
    for name, frame in [("diversity_selected", diverse), ("random_matched", random), ("download_union", union)]:
        write_manifest(frame, output / f"{name}.parquet")
    report = {
        "status": "fixed_metadata_only_acquisition_plan",
        "budget_per_arm": len(diverse),
        "candidate_pool_rows": len(pool),
        "download_union_rows": len(union),
        "baseline_manifest_sha256": sha256(Path("data/evaluation/moscow_real_v4/gallery.parquet")),
        "source_discovery_sha256": sha256(discovery),
        "allowed_future_gallery_pool_sha256": sha256(ROOT / "prospective/gallery_acquisition_pool.parquet"),
        "query_coordinates_used": False,
        "query_sequences_used": False,
        "sampling": "matched budget global H3/viewpoint coverage against hash-random control",
        "files": {str(p.name): sha256(p) for p in output.glob("*.parquet")},
    }
    write_once(output / "plan.json", report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--discovery", type=Path, required=True)
    parser.add_argument("--budget", type=int, default=6000)
    args = parser.parse_args()
    run(args.discovery, args.budget)
