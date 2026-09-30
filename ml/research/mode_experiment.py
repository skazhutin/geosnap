"""Development-only coordinate selection experiments on the production retrieval stream.

Models rank geographic hypotheses; they cannot abstain or access ground truth
as features. The historical v4 development identities are hash-pinned here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GroupKFold
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits

from ml.evaluation.product_recovery import _candidate, _gallery_metadata_and_density
from ml.localization.confidence_model import ConfidenceModel
from ml.localization.geo import haversine_m
from ml.localization.product_policy import AggregationStrategy, ProductAggregationConfig, aggregate_geographic_modes
from ml.research.metrics import (
    paired_group_bootstrap,
    product_metrics,
    raw_metrics,
    retrieval_metrics,
    select_threshold,
)

BUNDLE = Path("data/evaluation/moscow_real_v4")
DEVELOPMENT_HASH = "17d0f339d3a34d4826a2d7cf337861a566a48b23d76fb1353919d9733fbcf64d"
GALLERY_HASH = "ff7cbc7e7147226e5aed41c4ccb1048d4bc17fec965c7fc5e0cb94e5bb1c35da"


def mode_features(localized, candidates):
    """Only inference-available retrieval and relative geography, never labels."""
    top = candidates[0]
    scores = np.array([c.retrieval_score for c in candidates])
    result = []
    for m in localized.modes:
        members = m.candidates
        ranks = np.array([c.rank for c in members])
        similarities = np.array([c.retrieval_score for c in members])
        result.append(
            [
                top.retrieval_score,
                float(scores[0] - scores[1]),
                float(scores.std()),
                float(similarities.max()),
                float(similarities.mean()),
                float(similarities.std()),
                float(similarities.max() - scores[0]),
                1 / float(ranks.min()),
                math.log1p(float(ranks.min())),
                math.log1p(float(ranks.mean())),
                math.log1p(len(members)),
                math.log1p(m.independent_sequences),
                m.provider_count,
                math.log1p(m.local_gallery_density_100m),
                m.raw_score,
                m.mass_fraction,
                m.raw_score / max(localized.modes[0].raw_score, 1e-12),
                m.p90_spread_m / 100,
                m.diameter_m / 100,
                m.viewpoint_bucket_count,
                float(any(c.reference_id == top.reference_id for c in members)),
                math.log1p(haversine_m(top.lat, top.lon, m.lat, m.lon)),
                math.log1p(len(localized.modes)),
            ]
        )
    return np.asarray(result, dtype=np.float64)


def run(output: Path):
    for name, expected in [("development_queries.parquet", DEVELOPMENT_HASH), ("gallery.parquet", GALLERY_HASH)]:
        if hashlib.sha256((BUNDLE / name).read_bytes()).hexdigest() != expected:
            raise ValueError("historical development/gallery identity mismatch")
    source_path = BUNDLE / "reports/sage_development_top50_source.json"
    source = json.loads(source_path.read_text())
    queries = pd.read_parquet(BUNDLE / "development_queries.parquet").set_index("id")
    rows = source["per_query"]
    if {r["query_id"] for r in rows} != set(queries.index):
        raise ValueError("source report is not exactly the registered development split")
    gallery = _gallery_metadata_and_density(BUNDLE / "gallery.parquet")
    frozen = json.loads(Path("configs/moscow_real_v3_confidence_model.json").read_text())
    # The confidence artifact wraps the fitted model.
    confidence = ConfidenceModel.from_dict(frozen.get("model", frozen))
    groups = np.array([str(queries.loc[r["query_id"]]["h3_coarse"]) for r in rows])
    ranks = [r["retrieval_diagnostics"]["by_positive_distance_m"]["100"]["positive_rank"] for r in rows]
    baseline, baseline_scores, top1, features, mode_errors, owner, offsets = [], [], [], [], [], [], []
    coverage_rows = []
    raw_candidates = {s.value: [] for s in AggregationStrategy}
    for i, row in enumerate(rows):
        truth = (float(queries.loc[row["query_id"]]["lat"]), float(queries.loc[row["query_id"]]["lon"]))
        candidates = [_candidate(m, gallery) for m in row["matches"]]
        quality = float(row["query_quality"].get("confidence_signal", 1))
        base = aggregate_geographic_modes(
            candidates[:30],
            config=ProductAggregationConfig(strategy=AggregationStrategy.DENSITY_AWARE_MODE_VOTE),
            query_quality=quality,
        )
        baseline.append(haversine_m(*truth, base.lat, base.lon))
        baseline_scores.append(confidence.predict_proba(base.features))
        top1.append(haversine_m(*truth, candidates[0].lat, candidates[0].lon))
        for strategy in AggregationStrategy:
            localized = aggregate_geographic_modes(
                candidates, config=ProductAggregationConfig(strategy=strategy), query_quality=quality
            )
            raw_candidates[strategy.value].append(haversine_m(*truth, localized.lat, localized.lon))
        localized = aggregate_geographic_modes(
            candidates,
            config=ProductAggregationConfig(strategy=AggregationStrategy.DENSITY_AWARE_MODE_VOTE),
            query_quality=quality,
        )
        offsets.append(len(mode_errors))
        features.extend(mode_features(localized, candidates))
        # Labels computed only AFTER feature extraction.
        mode_errors.extend(haversine_m(*truth, m.lat, m.lon) for m in localized.modes)
        owner.extend([i] * len(localized.modes))
        coverage_rows.append(
            {
                "provider": row["source"],
                "density": row["local_gallery_density_bucket"],
                "heading": row["positive_heading_gap_bucket"],
                "time": row["positive_temporal_gap_bucket"],
                "positive_provider": row["positive_provider_pair"],
                "retrieved_50": ranks[i] is not None and ranks[i] <= 50,
                "correct": baseline[-1] <= 100,
            }
        )
    offsets.append(len(mode_errors))
    x, error, owner = np.asarray(features), np.asarray(mode_errors), np.asarray(owner)
    labels = error <= 100
    folds = list(GroupKFold(n_splits=5).split(np.zeros(len(rows)), groups=groups))
    summary = {
        "protocol_sha256": hashlib.sha256(Path("configs/moscow_research_v5_protocol.json").read_bytes()).hexdigest(),
        "kind": "historical_v4_development_exploration_not_final_evidence",
        "source_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
        "retrieval": retrieval_metrics(ranks, gallery_size=len(gallery)),
        "baseline": raw_metrics(baseline),
        "baseline_product_frozen_threshold": product_metrics(
            baseline, np.asarray(baseline_scores) >= 0.9349250249145314
        ),
        "baseline_exploratory_precision_coverage": select_threshold(baseline, baseline_scores),
        "top1": raw_metrics(top1),
        "candidates": {},
        "mode_oracle_100m": float(
            np.mean([np.min(error[offsets[i] : offsets[i + 1]]) <= 100 for i in range(len(rows))])
        ),
        "validation": "5-fold H3 resolution6 grouped OOF; hyperparameter comparisons exploratory; new calibration untouched",
    }
    predictions = {"baseline": baseline, "top1": top1}
    for name, errors in raw_candidates.items():
        summary["candidates"][name] = {
            "raw": raw_metrics(errors),
            "paired_gain": paired_group_bootstrap(baseline, errors, groups),
        }
        predictions[name] = errors
    for family in ["logistic", "hgb_leaf7", "hgb_leaf15", "hgb_leaf31"]:
        oof = np.zeros(len(error))
        fold_scores = []
        for train_q, test_q in folds:
            train, test = np.isin(owner, train_q), np.isin(owner, test_q)
            model = (
                make_pipeline(StandardScaler(), LogisticRegression(C=0.1, max_iter=600))
                if family == "logistic"
                else HistGradientBoostingClassifier(
                    max_leaf_nodes=int(family.split("leaf")[1]),
                    max_iter=100,
                    learning_rate=0.06,
                    l2_regularization=10,
                    min_samples_leaf=40,
                    early_stopping=False,
                    random_state=20260905,
                )
            )
            # Equal total weight per query, regardless of number of hypotheses.
            weights = 1 / np.bincount(owner)[owner[train]]
            with threadpool_limits(limits=2):
                if family == "logistic":
                    model.fit(x[train], labels[train], logisticregression__sample_weight=weights)
                else:
                    model.fit(x[train], labels[train], sample_weight=weights)
                oof[test] = model.predict_proba(x[test])[:, 1]
            selected = [offsets[i] + int(np.argmax(oof[offsets[i] : offsets[i + 1]])) for i in test_q]
            fold_scores.append(
                {
                    "query_count": len(test_q),
                    "raw100": float(np.mean(error[selected] <= 100)),
                    "baseline100": float(np.mean(np.asarray(baseline)[test_q] <= 100)),
                }
            )
        choices = [offsets[i] + int(np.argmax(oof[offsets[i] : offsets[i + 1]])) for i in range(len(rows))]
        predicted = error[choices]
        summary["candidates"][family] = {
            "raw": raw_metrics(predicted),
            "paired_gain": paired_group_bootstrap(baseline, predicted, groups),
            "folds": fold_scores,
            "exploratory_oof_product_curve": select_threshold(predicted, oof[choices]),
        }
        predictions[family] = predicted.tolist()
        print(family, json.dumps(summary["candidates"][family]["raw"]), flush=True)
    coverage = pd.DataFrame(coverage_rows)
    summary["strata"] = {
        field: {
            str(k): {"n": len(g), "raw100": float(g.correct.mean()), "recall50": float(g.retrieved_50.mean())}
            for k, g in coverage.groupby(field)
        }
        for field in ["provider", "density", "heading", "time", "positive_provider"]
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
    (output / "predictions.json").write_text(
        json.dumps(
            {"query_ids": [r["query_id"] for r in rows], "groups": groups.tolist(), "errors": predictions},
            allow_nan=False,
        )
        + "\n"
    )
    print(json.dumps({k: summary[k] for k in ["baseline", "retrieval", "mode_oracle_100m"]}, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path, default=Path("data/evaluation/moscow_research_v5/development/mode_experiment")
    )
    run(parser.parse_args().output)
