"""Attach cached street-length metrics to the existing local comparison map."""
from __future__ import annotations

import json

import numpy as np

from tools.compare_research_coverage import COS_ORIGIN, RADIUS, ROOT, digest, save
from tools.fetch_street_network import output_root


def aggregated_grid(latlon, weight, distance, spacing=50):
    lat, lon = np.deg2rad(latlon).T
    xy = np.column_stack([RADIUS * COS_ORIGIN * lon, RADIUS * np.sin(lat) / COS_ORIGIN])
    bins = np.floor(xy / spacing).astype(np.int32)
    cells, inverse = np.unique(bins, axis=0, return_inverse=True)
    lengths = np.bincount(inverse, weights=weight)
    means = np.column_stack([np.bincount(inverse, weights=weight * distance[:, i]) / lengths for i in range(3)])
    x, y = ((cells + .5) * spacing).T
    centers = np.rad2deg(np.column_stack([np.arcsin(y * COS_ORIGIN / RADIUS), x / (RADIUS * COS_ORIGIN)]))
    return np.column_stack([np.round(centers, 6), np.round(means)]).tolist()


def main():
    out = output_root()
    metrics = json.loads((out / "summary.json").read_text())
    probes_path = out / "probes_10m.npz"
    checks = json.loads((out / "metrics_10m.json").read_text())
    if digest(probes_path) != checks["cache_sha256"]:
        raise RuntimeError("Street probe cache changed")
    probes = np.load(probes_path)
    kinds, weights, distances, latlon = (probes[k] for k in ["category", "weight_m", "distances_m", "latlon"])
    grids = {}
    for name, mask in [("streets", kinds == 0), ("extended", np.ones(len(kinds), dtype=bool))]:
        grids[name] = aggregated_grid(latlon[mask], weights[mask], distances[mask])
    street_data = {"groups": metrics["groups"], "grid": grids, "display_spacing_m": 50,
                   "spacing_m": 10, "snapshot": metrics["snapshot"]}
    save(out / "display_grids.json", street_data)
    dest = ROOT / "data/evaluation/moscow_night_v7/map/comparison.html"
    text = dest.read_text()
    payload = json.JSONDecoder().raw_decode(text.split("const data=", 1)[1])[0]
    if payload["summary"]["counts"] != metrics["counts"]:
        raise RuntimeError("Map and street evaluation galleries differ")
    payload["streets"] = street_data
    template = ROOT / "tools/research_coverage_compare.html"
    html = template.read_text().replace("__DATA__", json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    dest.write_text(html)
    save(dest.with_name("street_summary.json"), metrics)
    save(out / "render.receipt.json", {"html_sha256": digest(dest), "street_summary_sha256": digest(out / "summary.json"),
         "metrics_10m_sha256": digest(out / "metrics_10m.json"), "template_sha256": digest(template),
         "display_cells": {k: len(v) for k, v in grids.items()}, "display_aggregation": "Length-weighted mean nearest distance within each 50m geographic cell; metric table uses all <=10m intervals, not these display cells"})
    print(dest, flush=True)


if __name__ == "__main__":
    main()
