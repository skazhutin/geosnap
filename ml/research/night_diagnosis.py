"""Cached-result diagnosis only; never changes queries, references or scores."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
from threadpoolctl import threadpool_limits

from ml.research.gallery_scale_storage import save
from ml.research.metrics import raw_metrics
from ml.research.night_v7 import LOCAL, inputs
from ml.research.vector_evaluation import distances


def run():
    _, _, _, out, q, _, g, _ = inputs()
    target = out / "bottleneck_strata.json"
    if target.exists():
        return
    latitude, longitude = np.radians(g.lat.to_numpy(float)), np.radians(g.lon.to_numpy(float))
    heading = pd.to_numeric(g.heading, errors="coerce").to_numpy(float)
    months = pd.to_datetime(g.captured_at, utc=True, errors="coerce", format="mixed").dt.month.to_numpy(float)
    qmonths = pd.to_datetime(q.captured_at, utc=True, errors="coerce", format="mixed").dt.month.to_numpy(float)
    nearest, aligned, season_aligned = [], [], []
    with threadpool_limits(limits=1):
        for i, row in enumerate(q.itertuples()):
            d = distances(row.lat, row.lon, latitude, longitude)
            gap = np.abs((heading - float(row.heading) + 180) % 360 - 180)
            season = np.abs(months - qmonths[i])
            season = np.minimum(season, 12 - season)
            a = np.isfinite(gap) & (gap <= 45)
            s = a & np.isfinite(season) & (season <= 1)
            nearest.append(float(d.min()))
            aligned.append(float(d[a].min()) if a.any() else np.inf)
            season_aligned.append(float(d[s].min()) if s.any() else np.inf)
    near, aligned, season_aligned = np.array(nearest), np.array(aligned), np.array(season_aligned)
    masks = {"no_reference_100m": near > 100,
             "coverage100_without_heading45": (near <= 100) & (aligned > 100),
             "heading45_coverage100": aligned <= 100,
             "heading45_and_month_gap1_coverage100": season_aligned <= 100,
             "heading45_coverage25": aligned <= 25}
    methods = {}
    for name in ("baseline", "context"):
        rows = json.loads((out / "results" / f"{name}_rows.json").read_text())
        if rows["query_ids"] != q.id.tolist():
            raise RuntimeError("diagnosis query identity changed")
        errors = np.asarray(rows["errors_m"])
        ranks = np.array([np.inf if x is None else x for x in rows["positive_ranks_through100"]])
        methods[name] = {key: {"queries": int(mask.sum()), "fraction_all_queries": float(mask.mean()),
             "raw_within_stratum": raw_metrics(errors[mask]) if mask.any() else None,
             "retrieved_positive_top100": int((mask & (ranks <= 100)).sum()),
             "retrieval_misses": int((mask & (near <= 100) & (ranks > 100)).sum()),
             "wrong_top1_with_positive_top100": int((mask & (ranks > 1) & (ranks <= 100)).sum())}
             for key, mask in masks.items()}
    report = {"kind": "posthoc_diagnostic_counts_not_new_accuracy_claim", "query_count": len(q),
         "coverage": {"any_heading": {str(k): float(np.mean(near <= k)) for k in (25, 50, 100)},
             "heading_gap45": {str(k): float(np.mean(aligned <= k)) for k in (25, 50, 100)},
             "heading_gap45_month_gap1": {str(k): float(np.mean(season_aligned <= k)) for k in (25, 50, 100)}},
         "methods": methods, "limitations": ["heading/time metadata are proxies, not verified visual overlap",
             "season diagnostic ignores year and daylight; missing metadata cannot satisfy a constraint",
             "strata overlap; no query is excluded from the main accuracy denominator"],
         "inference_or_gallery_selection_changed": False, "calibration_final_access": False}
    save(target, report)
    save(LOCAL / "bottleneck_strata.json", report)
    print(json.dumps(report["coverage"]))


if __name__ == "__main__":
    run()
