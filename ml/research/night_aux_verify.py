"""Reconcile the separate synthetic multi-photo result; no final/test access."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from ml.research.gallery_scale_storage import digest, save, settings
from ml.research.metrics import raw_metrics
from ml.research.vector_evaluation import distances


def run():
    _, root, _, previous = settings()
    out = root / "night_v7_20260909/aux_multiview"
    report = json.loads((out / "report.json").read_text())
    receipt = json.loads((out / "prepared.json").read_text())
    if digest(out / "queries.npz") != receipt["query_vectors_sha256"]:
        raise RuntimeError("auxiliary query cache changed")
    with np.load(out / "queries.npz", allow_pickle=False) as data:
        parents, eligible = data["parents"], data["eligible"]
    g = pd.read_parquet(previous / "manifests/G2_smart.parquet", columns=["lat", "lon", "sequence_key"])
    assert not set(g.sequence_key.iloc[parents]) & set(g.sequence_key.iloc[eligible])
    rows = json.loads((out / "rows.json").read_text())
    assert len(rows) == len(parents) * 4 == report["orientation_anchored_cases"]
    assert {(r["parent"], r["yaw"]) for r in rows} == {(int(p), yaw) for p in parents for yaw in (0, 90, 180, 270)}
    allowed = set(eligible)
    lat, lon = np.radians(g.lat.to_numpy(float)), np.radians(g.lon.to_numpy(float))
    for name, summary in report["results"].items():
        errors, ranks = [], []
        for row in rows:
            truth = g.iloc[row["parent"]]
            result = row["methods"][name]
            indices = np.array(result["top100_gallery_rows"])
            assert len(indices) == 100 and len(set(indices)) == 100 and set(indices) <= allowed
            d = distances(truth.lat, truth.lon, lat[indices], lon[indices])
            np.testing.assert_allclose(d[0], result["error_m"], atol=1e-7)
            errors.append(float(d[0]))
            positive = np.flatnonzero(d <= 100)
            rank = int(positive[0]) + 1 if len(positive) else None
            assert rank == result["positive_rank_through100"]
            ranks.append(rank or np.inf)
        assert raw_metrics(errors) == summary["raw"]
        for k in (1, 10, 100):
            assert np.mean(np.asarray(ranks) <= k) == summary["R_at"][str(k)]
    result = {"verified": True, "physical_places": len(parents), "dependent_orientation_cases": len(rows),
              "methods": len(report["results"]), "report_sha256": digest(out / "report.json"),
              "rows_sha256": digest(out / "rows.json"), "real_user_accuracy_claim": False}
    save(out / "verification.json", result)
    print(json.dumps(result))


if __name__ == "__main__":
    run()
