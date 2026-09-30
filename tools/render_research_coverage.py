"""Local, reference-only map; no query data or production changes."""
from __future__ import annotations

import json

import pandas as pd
import requests
import shapely

from ml.research.gallery_scale_storage import WORKSPACE, digest, save, settings


def main():
    _, _, _, previous = settings()
    manifest = previous / "manifests/G2_smart.parquet"
    frame = pd.read_parquet(manifest, columns=["lat", "lon", "source", "cell"])
    boundary_path = WORKSPACE / "data/raw/moscow/moscow_admin_boundary.geojson"
    raw = json.loads(boundary_path.read_text())
    geometry = shapely.from_geojson(json.dumps(raw))
    if not shapely.intersects_xy(geometry, frame.lon.to_numpy(), frame.lat.to_numpy()).all():
        raise RuntimeError("reference outside exact Moscow polygon")
    if len(frame) != 100000:
        raise RuntimeError("expected current G2 gallery")
    sources = ["mapillary", "kartaview", "msls"]
    destination = WORKSPACE / "data/evaluation/moscow_night_v7/map"
    destination.mkdir(parents=True, exist_ok=True)
    for filename in ["leaflet.js", "leaflet.css"]:
        p = destination / filename
        if not p.exists():
            response = requests.get(f"https://unpkg.com/leaflet@1.9.4/dist/{filename}", timeout=30)
            response.raise_for_status()
            p.write_bytes(response.content)
    payload = {"points": [[round(r.lat, 6), round(r.lon, 6), sources.index(r.source)] for r in frame.itertuples()],
               "counts": [int((frame.source == s).sum()) for s in sources], "cells": int(frame.cell.nunique()),
               "boundary": json.loads(shapely.to_geojson(geometry.simplify(0.00003, preserve_topology=True)))}
    template = (WORKSPACE / "tools/research_coverage.html").read_text()
    html = template.replace("__DATA__", json.dumps(payload, separators=(",", ":")))
    (destination / "coverage.html").write_text(html)
    save(destination / "receipt.json", {"manifest_sha256": digest(manifest), "exact_boundary_sha256": digest(boundary_path),
         "references": len(frame), "by_source": frame.source.value_counts().to_dict(), "h3_cells": frame.cell.nunique(),
         "all_references_inside_exact_boundary": True, "boundary_display_simplification_degrees": 0.00003,
         "query_data_used": False, "coverage_means": "geographic proximity to a reference, not localization accuracy"})
    print(destination / "coverage.html")


if __name__ == "__main__":
    main()
