"""Development-only geographic localization sweep on the stronger retrieval stream."""

from __future__ import annotations

import itertools
import json
from dataclasses import asdict

from ml.evaluation.product_recovery import _gallery_metadata_and_density
from ml.localization.geo import haversine_m
from ml.localization.models import Candidate
from ml.localization.product_policy import AggregationStrategy, ProductAggregationConfig, aggregate_geographic_modes
from ml.research.metrics import paired_group_bootstrap, raw_metrics
from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256, write_once
from ml.research.vector_evaluation import development_frame


def run():
    queries_path = ROOT / "prospective/development.parquet"
    queries = development_frame(queries_path)
    gallery_path = ROOT / "gallery_expansion/gallery_union.parquet"
    gallery = _gallery_metadata_and_density(gallery_path)
    stream_path = ROOT / "development/gallery_fusion_new/union_larger_budget/retrieval.json"
    stream = json.loads(stream_path.read_text())
    if queries.id.tolist() != stream["query_ids"]:
        raise ValueError("registered query order differs from the fixed retrieval stream")
    retrieved = stream["methods"]["fusion50"]
    candidates = []
    for matches in retrieved["top100"]:
        candidates.append(
            [
                Candidate(
                    reference_id=rid,
                    lat=float(gallery[rid]["lat"]),
                    lon=float(gallery[rid]["lon"]),
                    retrieval_score=float(value),
                    rank=rank,
                    source=gallery[rid]["source"],
                    metadata=gallery[rid],
                )
                for rank, (rid, value) in enumerate(zip(matches["ids"], matches["scores"], strict=True), 1)
            ]
        )
    target = ROOT / "development/geographic_sweep_union"
    target.mkdir(exist_ok=True)
    configuration = {
        "depths": [10, 30, 100],
        "radii_m": [50, 100],
        "temperatures": [0.02, 0.04, 0.08],
        "strategies": [s.value for s in AggregationStrategy if s != AggregationStrategy.LEGACY_WEIGHTED_MEDOID],
        "rank_decay": 0.5,
        "maximum_diameter": "1.5 * radius",
    }
    write_once(
        target / "contract.json",
        {
            "query_manifest_sha256": sha256(queries_path),
            "stream_sha256": sha256(stream_path),
            "gallery_sha256": sha256(gallery_path),
            "source_sha256": sha256(__import__("pathlib").Path(__file__)),
            "grid": configuration,
            "calibration_or_final_used": False,
        },
    )
    records = []
    for depth, radius, temperature, strategy in itertools.product(
        configuration["depths"], configuration["radii_m"], configuration["temperatures"], configuration["strategies"]
    ):
        config = ProductAggregationConfig(
            strategy=AggregationStrategy(strategy),
            cluster_radius_m=radius,
            max_cluster_diameter_m=1.5 * radius,
            score_temperature=temperature,
            rank_decay_exponent=0.5,
        )
        errors, predictions = [], []
        for query, matches in zip(queries.itertuples(), candidates, strict=True):
            prediction = aggregate_geographic_modes(matches[:depth], config=config)
            predictions.append([prediction.lat, prediction.lon])
            errors.append(haversine_m(query.lat, query.lon, prediction.lat, prediction.lon))
        index = len(records)
        summary = {"index": index, "depth": depth, "config": asdict(config), "raw": raw_metrics(errors)}
        records.append(summary)
        write_once(
            target / f"rows_{index}.json",
            {"query_ids": queries.id.tolist(), "errors": errors, "predictions": predictions},
        )
        print(index, depth, radius, temperature, strategy, summary["raw"]["accuracy_100m"], flush=True)
    records.sort(
        key=lambda r: (
            -r["raw"]["accuracy_100m"],
            -r["raw"]["accuracy_50m"],
            -r["raw"]["accuracy_25m"],
            r["raw"]["catastrophic_gt500m_rate"],
            r["raw"]["median_error_m"],
            r["raw"]["p90_error_m"],
            r["index"],
        )
    )
    for record in records[:5]:
        errors = json.loads((target / f"rows_{record['index']}.json").read_text())["errors"]
        record["paired_gain_vs_top1"] = paired_group_bootstrap(retrieved["errors"], errors, queries.h3_coarse.tolist())
    write_once(
        target / "summary.json",
        {
            "kind": "development_only_grid",
            "top1_raw": raw_metrics(retrieved["errors"]),
            "trials": records,
            "retrieval_unchanged": True,
            "confidence_not_fit_or_thresholded": True,
        },
    )


if __name__ == "__main__":
    run()
