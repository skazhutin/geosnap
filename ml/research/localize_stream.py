"""Evaluate complete raw coordinates and product confidence on development streams."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from threadpoolctl import threadpool_limits

from ml.evaluation.product_recovery import _gallery_metadata_and_density
from ml.localization.confidence_model import PART2_5_FEATURE_NAMES, ConfidenceModel, fit_confidence_model
from ml.localization.geo import haversine_m
from ml.localization.models import Candidate
from ml.localization.product_policy import AggregationStrategy, ProductAggregationConfig, aggregate_geographic_modes
from ml.research.metrics import geographic_intervals, product_metrics, raw_metrics, retrieval_metrics, select_threshold
from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256
from ml.research.vector_evaluation import development_frame


def run(
    stream_path: Path,
    gallery_path: Path,
    query_path: Path,
    output: Path,
    method: str | None = None,
    selection: str = "geographic",
):
    if selection not in {"geographic", "top1"}:
        raise ValueError("unknown coordinate selection")
    permitted = [
        Path("data/evaluation/moscow_real_v4/development_queries.parquet").resolve(),
        (ROOT / "prospective/development.parquet").resolve(),
    ]
    if query_path.resolve() not in permitted:
        raise ValueError("localizer experiment accepts registered development only")
    frame = development_frame(query_path).set_index("id")
    stream = json.loads(stream_path.read_text())
    if method is not None:
        stream["methods"] = {method: stream["methods"][method]}
    ids = stream["query_ids"]
    if set(ids) != set(frame.index) or len(ids) != len(frame):
        raise ValueError("query identities do not match retrieval stream")
    groups = [str(frame.loc[i].h3_coarse) for i in ids]
    gallery = _gallery_metadata_and_density(gallery_path)
    frozen = json.loads(Path("configs/moscow_real_v3_confidence_model.json").read_text())
    baseline_model = ConfidenceModel.from_dict(frozen.get("model", frozen))
    output.mkdir(parents=True, exist_ok=True)
    result = {
        "kind": "development_only_raw_and_product",
        "stream_sha256": sha256(stream_path),
        "query_manifest_sha256": sha256(query_path),
        "gallery_manifest_sha256": sha256(gallery_path),
        "coordinate_selection": selection,
        "confidence_features": "full geographic mode analysis; labels follow the actual selected coordinate",
        "methods": {},
    }
    for name, record in stream["methods"].items():
        errors = []
        features = []
        predictions = []
        for qid, matches in zip(ids, record["top100"], strict=True):
            candidates = []
            for rank, (rid, score) in enumerate(zip(matches["ids"][:30], matches["scores"][:30], strict=True), 1):
                r = gallery[rid]
                candidates.append(
                    Candidate(
                        reference_id=rid,
                        lat=float(r["lat"]),
                        lon=float(r["lon"]),
                        retrieval_score=float(score),
                        rank=rank,
                        source=str(r["source"]),
                        metadata=r,
                    )
                )
            localized = aggregate_geographic_modes(
                candidates, config=ProductAggregationConfig(strategy=AggregationStrategy.DENSITY_AWARE_MODE_VOTE)
            )
            features.append(dict(localized.features))
            lat, lon = (
                (localized.lat, localized.lon) if selection == "geographic" else (candidates[0].lat, candidates[0].lon)
            )
            predictions.append([lat, lon])
            errors.append(haversine_m(float(frame.loc[qid].lat), float(frame.loc[qid].lon), lat, lon))
        baseline_scores = [baseline_model.predict_proba(f) for f in features]
        metric = {
            "raw": raw_metrics(errors),
            "retrieval": retrieval_metrics(record["ranks"], gallery_size=len(gallery)),
            "unchanged_baseline_confidence_policy": product_metrics(
                errors, np.asarray(baseline_scores) >= 0.9349250249145314
            ),
        }
        metric["unchanged_baseline_policy_intervals"] = geographic_intervals(
            errors, np.asarray(baseline_scores) >= 0.9349250249145314, groups
        )
        if 0 < sum(e <= 100 for e in errors) < len(errors):
            with threadpool_limits(limits=2):
                trained, evidence = fit_confidence_model(
                    features,
                    [e <= 100 for e in errors],
                    groups=groups,
                    development_ids=ids,
                    feature_names=PART2_5_FEATURE_NAMES,
                    random_seed=20260905,
                )
            model_path = output / f"{name}_confidence.json"
            model_path.write_text(json.dumps(trained.to_dict(), indent=2) + "\n")
            (output / f"{name}_cv.json").write_text(json.dumps(evidence, indent=2) + "\n")
            oof = evidence["cross_validation"]["oof_probabilities"]
            if oof is not None:
                metric["exploratory_oof_product"] = select_threshold(errors, oof)
        result["methods"][name] = metric
        (output / f"{name}_rows.json").write_text(
            json.dumps(
                {
                    "query_ids": ids,
                    "groups": groups,
                    "features": features,
                    "errors": errors,
                    "predictions": predictions,
                    "baseline_confidence_scores": baseline_scores,
                },
                allow_nan=False,
            )
            + "\n"
        )
        print(name, json.dumps(metric["raw"]), flush=True)
    (output / "summary.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream", type=Path, required=True)
    parser.add_argument("--gallery", type=Path, default=Path("data/evaluation/moscow_real_v4/gallery.parquet"))
    parser.add_argument(
        "--queries", type=Path, default=Path("data/evaluation/moscow_real_v4/development_queries.parquet")
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--method")
    parser.add_argument("--selection", choices=["geographic", "top1"], default="geographic")
    args = parser.parse_args()
    run(args.stream, args.gallery, args.queries, args.output, args.method, args.selection)
