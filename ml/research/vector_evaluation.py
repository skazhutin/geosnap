"""Shared complete-gallery scoring for registered development experiments."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from ml.research.metrics import paired_group_bootstrap, raw_metrics, retrieval_metrics
from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256


def development_frame(path: Path):
    historical = Path("data/evaluation/moscow_real_v4/development_queries.parquet")
    prospective = ROOT / "prospective/development.parquet"
    if path.resolve() == historical.resolve():
        if sha256(path) != "17d0f339d3a34d4826a2d7cf337861a566a48b23d76fb1353919d9733fbcf64d":
            raise ValueError("historical development manifest changed")
    elif path.resolve() == prospective.resolve():
        seal = json.loads((ROOT / "prospective/seal.json").read_text())
        if sha256(path) != seal["splits"]["development"]["sha256"]:
            raise ValueError("prospective development manifest changed")
    else:
        raise ValueError("registered development queries only")
    return pd.read_parquet(path)


def distances(lat, lon, gallery_lat, gallery_lon):
    a, b = np.radians([lat, lon])
    return (
        2
        * 6371008.8
        * np.arcsin(
            np.sqrt(
                np.clip(
                    np.sin((gallery_lat - a) / 2) ** 2
                    + np.cos(a) * np.cos(gallery_lat) * np.sin((gallery_lon - b) / 2) ** 2,
                    0,
                    1,
                )
            )
        )
    )


def evaluate_scores(scores, queries, ids, references):
    if scores.shape != (len(queries), len(ids)) or not np.isfinite(scores).all():
        raise ValueError("scores must cover every registered query and reference")
    lat = np.radians([r["metadata"]["lat"] for r in references])
    lon = np.radians([r["metadata"]["lon"] for r in references])
    result = {"errors": [], "ranks": [], "top100": [], "nearest_reference_m": [], "positive_count_100m": []}
    for i, row in enumerate(queries.itertuples()):
        d = distances(row.lat, row.lon, lat, lon)
        order = np.argsort(-scores[i], kind="stable")
        positive = np.flatnonzero(d[order] <= 100)
        result["errors"].append(float(d[order[0]]))
        result["ranks"].append(int(positive[0]) + 1 if len(positive) else None)
        result["nearest_reference_m"].append(float(d.min()))
        result["positive_count_100m"].append(int((d <= 100).sum()))
        result["top100"].append({"ids": [ids[j] for j in order[:100]], "scores": scores[i, order[:100]].tolist()})
    return result


def save_results(output, query_path, queries, records, gallery_sizes, provenance):
    output.mkdir(parents=True, exist_ok=True)
    baseline = records[next(iter(records))]["errors"]
    metrics = {
        name: {
            "raw_top1": raw_metrics(r["errors"]),
            "retrieval": retrieval_metrics(r["ranks"], gallery_size=gallery_sizes[name]),
            "paired_gain_vs_baseline_top1": paired_group_bootstrap(
                baseline, r["errors"], queries.h3_coarse.astype(str).tolist()
            ),
        }
        for name, r in records.items()
    }
    (output / "summary.json").write_text(
        json.dumps(
            {
                "kind": "development_only",
                "query_manifest_sha256": sha256(query_path),
                "provenance": provenance,
                "metrics": metrics,
            },
            indent=2,
            allow_nan=False,
        )
        + "\n"
    )
    (output / "retrieval.json").write_text(
        json.dumps({"query_ids": queries.id.tolist(), "methods": records}, allow_nan=False) + "\n"
    )
    print(json.dumps({n: m["raw_top1"] for n, m in metrics.items()}, indent=2), flush=True)
