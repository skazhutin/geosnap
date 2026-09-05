"""Development-only v3 product-policy evaluation and calibration selection."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from ml.ingestion.schema import read_manifest
from ml.localization.confidence_model import fit_confidence_model
from ml.localization.geo import EARTH_RADIUS_M, haversine_m
from ml.localization.models import Candidate
from ml.localization.product_policy import (
    AggregationStrategy,
    ProductAggregationConfig,
    aggregate_geographic_modes,
)

K_VALUES = (5, 10, 15, 20, 30, 40, 50)
AGGREGATION_VALUES = tuple(AggregationStrategy)
BOOTSTRAP_SEED = 20260902


class ProductRecoveryError(RuntimeError):
    """A sealed evaluation or product-policy contract was violated."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _percentile(values: Sequence[float], q: float) -> float | None:
    return None if not values else float(np.percentile(np.asarray(values), q * 100.0))


def _wilson(successes: int, total: int, z: float = 1.959963984540054) -> list[float] | None:
    if total <= 0:
        return None
    proportion = successes / total
    denominator = 1.0 + z * z / total
    center = (proportion + z * z / (2.0 * total)) / denominator
    half = (
        z
        * math.sqrt(proportion * (1.0 - proportion) / total + z * z / (4.0 * total * total))
        / denominator
    )
    return [max(0.0, center - half), min(1.0, center + half)]


def _bootstrap_answer_rate(
    accepted: Sequence[bool], *, resamples: int = 10000, seed: int = BOOTSTRAP_SEED
) -> list[float]:
    values = np.asarray(accepted, dtype=np.float64)
    rng = np.random.default_rng(seed)
    estimates = rng.binomial(len(values), float(values.mean()), size=resamples) / len(values)
    return [float(value) for value in np.percentile(estimates, [2.5, 97.5])]


def _gallery_metadata_and_density(gallery_manifest: Path) -> dict[str, dict[str, Any]]:
    try:
        from sklearn.neighbors import BallTree
    except ImportError as exc:  # pragma: no cover - offline dependency
        raise ProductRecoveryError("scikit-learn is required for gallery density") from exc
    frame = read_manifest(gallery_manifest, allow_empty=False)
    radians = np.radians(frame[["lat", "lon"]].to_numpy(dtype=np.float64))
    tree = BallTree(radians, metric="haversine")
    densities = {
        radius: tree.query_radius(radians, r=radius / EARTH_RADIUS_M, count_only=True)
        for radius in (25.0, 50.0, 100.0)
    }
    result: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(frame.to_dict("records")):
        for radius, values in densities.items():
            row[f"local_gallery_density_{int(radius)}m"] = int(values[index])
        result[str(row["id"])] = row
    return result


def _query_metadata(query_manifest: Path) -> dict[str, dict[str, Any]]:
    frame = read_manifest(query_manifest, allow_empty=False)
    return {str(row["id"]): row for row in frame.to_dict("records")}


def _candidate(match: Mapping[str, Any], gallery: Mapping[str, Mapping[str, Any]]) -> Candidate:
    reference_id = str(match["reference_id"])
    metadata = dict(gallery[reference_id])
    return Candidate(
        reference_id=reference_id,
        lat=float(match["lat"]),
        lon=float(match["lon"]),
        retrieval_score=float(match["score"]),
        rank=int(match["rank"]),
        city_id=str(metadata.get("city_id") or "moscow"),
        source=str(metadata.get("source") or match.get("source") or "unknown"),
        metadata=metadata,
    )


def _validate_source_report(
    report: Mapping[str, Any], *, query_manifest: Path, gallery_manifest: Path
) -> None:
    inputs = report.get("dataset", {}).get("fingerprint_inputs", {})
    expected_query = _sha256_file(query_manifest)
    expected_gallery = _sha256_file(gallery_manifest)
    if inputs.get("query_manifest_sha256") != expected_query:
        raise ProductRecoveryError("benchmark report does not match the declared query manifest")
    if inputs.get("gallery_manifest_sha256") != expected_gallery:
        raise ProductRecoveryError("benchmark report does not match the declared gallery manifest")
    if int(report.get("retrieval_configuration", {}).get("top_k", 0)) != 50:
        raise ProductRecoveryError("product recovery requires one source retrieval run at top_k=50")


def replay_report(
    report_path: Path,
    *,
    gallery_manifest: Path,
    query_manifest: Path,
    k: int,
    aggregation: AggregationStrategy,
    gallery_context: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], Mapping[str, Any]]:
    """Replay a top-50 report without opening images or a final-test manifest."""

    if k not in K_VALUES:
        raise ValueError(f"K must be one of {K_VALUES}")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    _validate_source_report(report, query_manifest=query_manifest, gallery_manifest=gallery_manifest)
    gallery = dict(gallery_context or _gallery_metadata_and_density(gallery_manifest))
    query = _query_metadata(query_manifest)
    report_rows = report.get("per_query", [])
    if {str(row["query_id"]) for row in report_rows} != set(query):
        raise ProductRecoveryError("benchmark report and query manifest have different query IDs")
    config = ProductAggregationConfig(strategy=aggregation)
    rows: list[dict[str, Any]] = []
    for source_row in report_rows:
        query_id = str(source_row["query_id"])
        query_row = query[query_id]
        matches = source_row.get("matches", [])[:k]
        if len(matches) != k:
            raise ProductRecoveryError(f"query {query_id!r} does not contain exactly K matches")
        candidates = [_candidate(match, gallery) for match in matches]
        quality = float(source_row.get("query_quality", {}).get("confidence_signal", 1.0))
        localized = aggregate_geographic_modes(
            candidates,
            config=config,
            query_quality=quality,
            true_coordinate=(float(query_row["lat"]), float(query_row["lon"])),
        )
        error = haversine_m(
            float(query_row["lat"]),
            float(query_row["lon"]),
            localized.lat,
            localized.lon,
        )
        rows.append(
            {
                "query_id": query_id,
                "source": str(query_row.get("source") or "unknown").lower(),
                "region": str(query_row.get("evaluation_area_h3") or "unknown"),
                "h3_coarse": str(query_row.get("h3_coarse") or "unknown"),
                "resolution_bucket": (
                    "lt1600"
                    if max(int(query_row.get("width") or 0), int(query_row.get("height") or 0)) < 1600
                    else (
                        "1600_2499"
                        if max(int(query_row.get("width") or 0), int(query_row.get("height") or 0)) < 2500
                        else "ge2500"
                    )
                ),
                "local_gallery_density_bucket": str(
                    source_row.get("local_gallery_density_bucket") or "unknown"
                ),
                "positive_provider_pair": str(
                    source_row.get("positive_provider_pair") or "unknown"
                ),
                "positive_heading_gap_bucket": str(
                    source_row.get("positive_heading_gap_bucket") or "unknown"
                ),
                "positive_temporal_gap_bucket": str(
                    source_row.get("positive_temporal_gap_bucket") or "unknown"
                ),
                "evaluation_geo_group_id": str(
                    query_row.get("evaluation_geo_group_id") or query_id
                ),
                "true_lat": float(query_row["lat"]),
                "true_lon": float(query_row["lon"]),
                "predicted_lat": localized.lat,
                "predicted_lon": localized.lon,
                "error_m": error,
                "correct_25m": error <= 25.0,
                "correct_50m": error <= 50.0,
                "correct_100m": error <= 100.0,
                "catastrophic_500m": error > 500.0,
                "features": dict(localized.features),
                "heuristic_confidence": float(
                    localized.diagnostics["legacy_handwritten_confidence"]
                ),
                "diagnostics": dict(localized.diagnostics),
                "matches": matches,
                "retrieval_diagnostics": source_row["retrieval_diagnostics"],
            }
        )
    return rows, report


def retrieval_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for distance in (50, 100):
        ranks: list[int] = []
        for row in rows:
            diagnostics = row["retrieval_diagnostics"]["by_positive_distance_m"][str(distance)]
            if diagnostics.get("positive_rank") is not None:
                ranks.append(int(diagnostics["positive_rank"]))
        output[str(distance)] = {
            "recall_at": {
                str(k): sum(rank <= k for rank in ranks) / len(rows)
                for k in (1, 5, 10, 20, 50)
            },
            "positive_rank": {
                "positive_query_count": len(ranks),
                "median": _percentile(ranks, 0.5),
                "p75": _percentile(ranks, 0.75),
                "p90": _percentile(ranks, 0.9),
            },
        }
    return output


def raw_localization_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    errors = [float(row["error_m"]) for row in rows]
    return {
        "query_count": len(rows),
        "accuracy_within_m": {
            str(distance): sum(error <= distance for error in errors) / len(errors)
            for distance in (25, 50, 100)
        },
        "error_gt_100m_count": sum(error > 100.0 for error in errors),
        "error_gt_500m_count": sum(error > 500.0 for error in errors),
        "median_error_m": _percentile(errors, 0.5),
        "p90_error_m": _percentile(errors, 0.9),
    }


def mode_structure_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    diagnostics = [row["diagnostics"] for row in rows]
    return {
        "mean_geographic_cluster_count": float(
            np.mean([value["geographic_cluster_count"] for value in diagnostics])
        ),
        "mean_winning_cluster_mass": float(
            np.mean([value["winning_cluster_mass"] for value in diagnostics])
        ),
        "correct_cluster_retrieved_but_lost_count": sum(
            bool(value["correct_cluster_retrieved_but_lost"]) for value in diagnostics
        ),
        "singleton_false_cluster_count": sum(
            bool(value["singleton_false_cluster"]) for value in diagnostics
        ),
        "multimodal_case_count": sum(bool(value["multimodal_case"]) for value in diagnostics),
        "correct_cluster_rank": {
            str(rank): sum(value["correct_cluster_rank"] == rank for value in diagnostics)
            for rank in (1, 2, 3, 4, 5)
        },
    }


def paired_bootstrap_accuracy(
    baseline: Sequence[bool],
    candidate: Sequence[bool],
    *,
    resamples: int = 10000,
    seed: int = BOOTSTRAP_SEED,
) -> dict[str, Any]:
    if len(baseline) != len(candidate) or not baseline:
        raise ValueError("paired accuracy inputs must have the same non-zero length")
    differences = np.asarray(candidate, dtype=np.int8) - np.asarray(baseline, dtype=np.int8)
    category_counts = np.asarray(
        [np.sum(differences == -1), np.sum(differences == 0), np.sum(differences == 1)],
        dtype=np.int64,
    )
    probabilities = category_counts / len(differences)
    rng = np.random.default_rng(seed)
    bootstrap_counts = rng.multinomial(len(differences), probabilities, size=resamples)
    estimates = (bootstrap_counts[:, 2] - bootstrap_counts[:, 0]) / len(differences)
    return {
        "point_difference": float(differences.mean()),
        "bootstrap_95": [float(value) for value in np.percentile(estimates, [2.5, 97.5])],
        "resamples": resamples,
    }


def _slice_precision(
    rows: Sequence[Mapping[str, Any]], accepted: Sequence[bool], key: str
) -> dict[str, Any]:
    indexes: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        if accepted[index]:
            indexes[str(row[key])].append(index)
    result: dict[str, Any] = {}
    for name, selected in sorted(indexes.items()):
        successes = sum(bool(rows[index]["correct_100m"]) for index in selected)
        result[name] = {
            "successes": successes,
            "answered": len(selected),
            "precision": successes / len(selected),
            "wilson_95": _wilson(successes, len(selected)),
        }
    return result


def product_metrics(
    rows: Sequence[Mapping[str, Any]],
    scores: Sequence[float],
    threshold: float,
    *,
    include_bootstrap: bool = True,
) -> dict[str, Any]:
    accepted = [float(score) >= threshold for score in scores]
    selected = [row for row, keep in zip(rows, accepted, strict=True) if keep]
    errors = [float(row["error_m"]) for row in selected]
    answered = len(selected)
    successes = sum(error <= 100.0 for error in errors)
    return {
        "threshold": threshold,
        "query_count": len(rows),
        "answered_count": answered,
        "answer_rate": answered / len(rows),
        "answer_rate_bootstrap_95": (
            _bootstrap_answer_rate(accepted) if include_bootstrap else None
        ),
        "conditional_accuracy_within_m": {
            str(distance): (
                None if not errors else sum(error <= distance for error in errors) / len(errors)
            )
            for distance in (25, 50, 100)
        },
        "conditional_accuracy_lte_100m_wilson_95": _wilson(successes, answered),
        "false_confident_gt_100m_count": sum(error > 100.0 for error in errors),
        "false_confident_gt_500m_count": sum(error > 500.0 for error in errors),
        "false_confident_gt_500m_rate": (
            None if not errors else sum(error > 500.0 for error in errors) / len(errors)
        ),
        "accepted_error_m": {
            "median": _percentile(errors, 0.5),
            "p90": _percentile(errors, 0.9),
            "p95": _percentile(errors, 0.95),
        },
        "provider_slices": _slice_precision(rows, accepted, "source"),
        "regional_slices": _slice_precision(rows, accepted, "region"),
    }


def select_operating_point(
    rows: Sequence[Mapping[str, Any]],
    scores: Sequence[float],
    *,
    precision_floor: float,
    minimum_answered: int = 40,
) -> dict[str, Any]:
    score_values = np.asarray(scores, dtype=np.float64)
    correct = np.asarray([bool(row["correct_100m"]) for row in rows], dtype=bool)
    catastrophic = np.asarray([bool(row["catastrophic_500m"]) for row in rows], dtype=bool)
    group_indexes: dict[str, list[np.ndarray]] = {}
    for key in ("source", "region"):
        indexes: dict[str, list[int]] = defaultdict(list)
        for index, row in enumerate(rows):
            indexes[str(row[key])].append(index)
        group_indexes[key] = [
            np.asarray(values, dtype=np.int64)
            for values in indexes.values()
            if len(values) >= 20
        ]
    candidates: list[dict[str, Any]] = []
    for threshold in sorted({0.0, 1.0, *(float(score) for score in scores)}):
        accepted = score_values >= threshold
        answered = int(accepted.sum())
        successes = int(np.logical_and(accepted, correct).sum())
        conditional = None if not answered else successes / answered
        catastrophic_count = int(np.logical_and(accepted, catastrophic).sum())
        catastrophic_rate = None if not answered else catastrophic_count / answered

        def strata_pass(
            indexes: Sequence[np.ndarray],
            floor: float,
            accepted_mask: np.ndarray = accepted,
        ) -> bool:
            for selected in indexes:
                stratum_accepted = accepted_mask[selected]
                stratum_answered = int(stratum_accepted.sum())
                if stratum_answered >= 20:
                    stratum_correct = int(
                        np.logical_and(stratum_accepted, correct[selected]).sum()
                    )
                    if stratum_correct / stratum_answered < floor:
                        return False
            return True

        provider_ok = strata_pass(group_indexes["source"], 0.80)
        region_ok = strata_pass(group_indexes["region"], 0.75)
        feasible = bool(
            answered >= minimum_answered
            and conditional is not None
            and conditional >= precision_floor
            and catastrophic_rate is not None
            and catastrophic_rate <= 0.01
            and provider_ok
            and region_ok
        )
        candidates.append(
            {
                "threshold": threshold,
                "answered_count": answered,
                "answer_rate": answered / len(rows),
                "conditional_accuracy_lte_100m": conditional,
                "false_confident_gt_500m_count": catastrophic_count,
                "false_confident_gt_500m_rate": catastrophic_rate,
                "feasible": feasible,
                "provider_guard_passed": provider_ok,
                "regional_guard_passed": region_ok,
            }
        )
    feasible_rows = [row for row in candidates if row["feasible"]]
    selected = (
        None
        if not feasible_rows
        else max(
            feasible_rows,
            key=lambda row: (
                row["answer_rate"],
                row["conditional_accuracy_lte_100m"],
                row["threshold"],
            ),
        )
    )
    compact_curve = [
        row
        for position, row in enumerate(candidates)
        if position in {0, len(candidates) - 1}
        or row["answered_count"] % 10 == 0
        or row["feasible"]
    ]
    selected_metrics = None
    if selected is not None:
        selected_metrics = product_metrics(rows, scores, float(selected["threshold"])) | {
            "feasible": True,
            "provider_guard_passed": selected["provider_guard_passed"],
            "regional_guard_passed": selected["regional_guard_passed"],
        }
    return {
        "objective": f"maximize_answer_rate_at_empirical_precision_gte_{precision_floor:.2f}",
        "minimum_answered": minimum_answered,
        "feasible": selected is not None,
        "selected": selected_metrics,
        "curve": compact_curve,
    }


def hard_gate_audit(
    rows: Sequence[Mapping[str, Any]],
    scores: Sequence[float],
    confidence_threshold: float,
) -> dict[str, Any]:
    gates: dict[str, list[bool]] = {
        "out_of_coverage_similarity": [
            float(row["features"]["top1_similarity"]) < 0.15 for row in rows
        ],
        "minimum_cluster_mass": [
            float(row["diagnostics"]["winning_cluster_mass"]) < 0.45 for row in rows
        ],
        "minimum_cluster_mass_margin": [
            float(row["features"]["geographic_mode_margin"]) < 0.10 for row in rows
        ],
        "minimum_cluster_candidates": [
            float(row["features"]["winning_candidate_count"]) < 2.0 for row in rows
        ],
        "confidence_threshold": [float(score) < confidence_threshold for score in scores],
    }
    baseline_precision = sum(bool(row["correct_100m"]) for row in rows) / len(rows)
    result: dict[str, Any] = {}
    for name, rejected in gates.items():
        sole = [
            value and not any(other[index] for key, other in gates.items() if key != name)
            for index, value in enumerate(rejected)
        ]
        accepted = [not value for value in rejected]
        accepted_count = sum(accepted)
        accepted_correct = sum(
            bool(row["correct_100m"]) and keep
            for row, keep in zip(rows, accepted, strict=True)
        )
        result[name] = {
            "rejected_count": sum(rejected),
            "sole_rejection_count": sum(sole),
            "correct_localizations_rejected": sum(
                bool(row["correct_100m"]) and reject
                for row, reject in zip(rows, rejected, strict=True)
            ),
            "incorrect_localizations_rejected": sum(
                not bool(row["correct_100m"]) and reject
                for row, reject in zip(rows, rejected, strict=True)
            ),
            "answer_rate_loss": sum(rejected) / len(rows),
            "precision_after_gate": (
                None if not accepted_count else accepted_correct / accepted_count
            ),
            "precision_gain": (
                None
                if not accepted_count
                else accepted_correct / accepted_count - baseline_precision
            ),
        }
    return result


def _fit_and_evaluate_confidence(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    features = [row["features"] for row in rows]
    labels = [bool(row["correct_100m"]) for row in rows]
    groups = [str(row["evaluation_geo_group_id"]) for row in rows]
    ids = [str(row["query_id"]) for row in rows]
    models: dict[str, Any] = {}
    for isotonic in (False, True):
        model, report = fit_confidence_model(
            features,
            labels,
            groups=groups,
            development_ids=ids,
            isotonic=isotonic,
        )
        scores = report["cross_validation"]["oof_probabilities"]
        models[model.method] = {
            "cross_validation": report["cross_validation"],
            "artifact": model.to_dict(),
            "primary": select_operating_point(rows, scores, precision_floor=0.90),
            "secondary": select_operating_point(rows, scores, precision_floor=0.85),
        }
    heuristic_scores = [float(row["heuristic_confidence"]) for row in rows]
    models["legacy_handwritten"] = {
        "primary": select_operating_point(rows, heuristic_scores, precision_floor=0.90),
        "secondary": select_operating_point(rows, heuristic_scores, precision_floor=0.85),
    }
    return models


def run_development_grid(
    *,
    report_paths: Mapping[str, Path],
    gallery_manifest: Path,
    development_manifest: Path,
    output_path: Path,
) -> dict[str, Any]:
    if "test" in development_manifest.name.lower():
        raise ProductRecoveryError("development grid refuses a test manifest")
    gallery_context = _gallery_metadata_and_density(gallery_manifest)
    model_results: dict[str, Any] = {}
    correctness_by_configuration: dict[str, list[bool]] = {}
    for model_name, report_path in report_paths.items():
        configurations: dict[str, Any] = {}
        retrieval: dict[str, Any] | None = None
        source_report: Mapping[str, Any] | None = None
        for k in K_VALUES:
            for aggregation in AGGREGATION_VALUES:
                rows, source_report = replay_report(
                    report_path,
                    gallery_manifest=gallery_manifest,
                    query_manifest=development_manifest,
                    k=k,
                    aggregation=aggregation,
                    gallery_context=gallery_context,
                )
                retrieval = retrieval or retrieval_metrics(rows)
                key = f"k{k}_{aggregation.value}"
                correctness_by_configuration[f"{model_name}::{key}"] = [
                    bool(row["correct_100m"]) for row in rows
                ]
                configurations[key] = {
                    "k": k,
                    "aggregation": aggregation.value,
                    "raw_localization": raw_localization_metrics(rows),
                    "mode_structure": mode_structure_metrics(rows),
                    "confidence": _fit_and_evaluate_confidence(rows),
                }
        assert source_report is not None and retrieval is not None
        model_results[model_name] = {
            "source_report": str(report_path),
            "source_report_sha256": _sha256_file(report_path),
            "model_metadata": source_report["model"],
            "retrieval": retrieval,
            "latency": source_report["primary"]["latency"],
            "configurations": configurations,
        }
    baseline_correctness = correctness_by_configuration.get(
        "megaloc::k50_legacy_weighted_medoid"
    )
    if baseline_correctness is None:
        raise ProductRecoveryError("development grid requires the unchanged MegaLoc baseline")
    for model_name, model in model_results.items():
        for config_name, configuration in model["configurations"].items():
            configuration["paired_bootstrap_vs_unchanged_megaloc"] = paired_bootstrap_accuracy(
                baseline_correctness,
                correctness_by_configuration[f"{model_name}::{config_name}"],
            )
    payload = {
        "schema_version": 1,
        "kind": "moscow_real_v3_development_product_grid",
        "selection_data": "development_only",
        "gallery_manifest": str(gallery_manifest),
        "gallery_sha256": _sha256_file(gallery_manifest),
        "development_manifest": str(development_manifest),
        "development_sha256": _sha256_file(development_manifest),
        "k_values": list(K_VALUES),
        "aggregation_values": [value.value for value in AGGREGATION_VALUES],
        "models": model_results,
    }
    _atomic_json(output_path, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gallery-manifest", type=Path, required=True)
    parser.add_argument("--development-manifest", type=Path, required=True)
    parser.add_argument("--report", action="append", default=[], metavar="MODEL=JSON")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reports: dict[str, Path] = {}
    for item in args.report:
        name, separator, path = item.partition("=")
        if not separator or not name or not path:
            raise SystemExit(f"invalid --report {item!r}; expected MODEL=JSON")
        reports[name] = Path(path)
    if not reports:
        raise SystemExit("at least one --report is required")
    payload = run_development_grid(
        report_paths=reports,
        gallery_manifest=args.gallery_manifest,
        development_manifest=args.development_manifest,
        output_path=args.output,
    )
    print(
        json.dumps(
            {
                "models": list(payload["models"]),
                "configuration_count_per_model": len(K_VALUES) * len(AGGREGATION_VALUES),
                "output": str(args.output),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
