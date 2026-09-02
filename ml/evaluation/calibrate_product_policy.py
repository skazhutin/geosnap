"""Fit v3 confidence on development and select thresholds on calibration only."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from ml.localization.confidence_model import fit_confidence_model
from ml.localization.product_policy import AggregationStrategy

from .product_recovery import (
    _gallery_metadata_and_density,
    _sha256_file,
    hard_gate_audit,
    mode_structure_metrics,
    product_metrics,
    raw_localization_metrics,
    replay_report,
    retrieval_metrics,
    select_operating_point,
)


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def calibrate_product_policy(
    *,
    development_report: Path,
    calibration_report: Path,
    gallery_manifest: Path,
    development_manifest: Path,
    calibration_manifest: Path,
    retriever: str,
    k: int,
    aggregation: AggregationStrategy,
    confidence_method: str,
    output_report: Path,
    output_model: Path,
) -> dict[str, Any]:
    if "test" in development_manifest.name.lower() or "test" in calibration_manifest.name.lower():
        raise ValueError("product calibration refuses test manifests")
    if confidence_method not in {"logistic", "logistic_isotonic"}:
        raise ValueError("confidence_method must be logistic or logistic_isotonic")
    gallery_context = _gallery_metadata_and_density(gallery_manifest)
    development_rows, _ = replay_report(
        development_report,
        gallery_manifest=gallery_manifest,
        query_manifest=development_manifest,
        k=k,
        aggregation=aggregation,
        gallery_context=gallery_context,
    )
    calibration_rows, _ = replay_report(
        calibration_report,
        gallery_manifest=gallery_manifest,
        query_manifest=calibration_manifest,
        k=k,
        aggregation=aggregation,
        gallery_context=gallery_context,
    )
    development_ids = [str(row["query_id"]) for row in development_rows]
    calibration_ids = [str(row["query_id"]) for row in calibration_rows]
    model, cross_validation = fit_confidence_model(
        [row["features"] for row in development_rows],
        [bool(row["correct_100m"]) for row in development_rows],
        groups=[str(row["evaluation_geo_group_id"]) for row in development_rows],
        development_ids=development_ids,
        calibration_ids=calibration_ids,
        isotonic=confidence_method == "logistic_isotonic",
    )
    calibration_scores = [model.predict_proba(row["features"]) for row in calibration_rows]
    primary = select_operating_point(
        calibration_rows,
        calibration_scores,
        precision_floor=0.90,
    )
    secondary = select_operating_point(
        calibration_rows,
        calibration_scores,
        precision_floor=0.85,
    )
    if primary["feasible"]:
        selected_label = "primary_90_percent"
        selected = primary["selected"]
    elif secondary["feasible"]:
        selected_label = "secondary_85_percent_explicit_fallback"
        selected = secondary["selected"]
    else:
        selected_label = "no_defensible_operating_point"
        selected = None
    gate_threshold = 1.0 if selected is None else float(selected["threshold"])
    model_payload = model.to_dict() | {
        "training_manifest": str(development_manifest),
        "training_manifest_sha256": _sha256_file(development_manifest),
        "calibration_manifest_for_threshold_only": str(calibration_manifest),
        "calibration_manifest_sha256": _sha256_file(calibration_manifest),
    }
    _atomic_json(output_model, model_payload)
    payload = {
        "schema_version": 1,
        "kind": "moscow_real_v3_product_calibration",
        "fit_split": "development_only",
        "threshold_split": "calibration_only",
        "pipeline": {
            "retriever": retriever,
            "top_k": k,
            "aggregation": aggregation.value,
            "confidence_method": confidence_method,
        },
        "inputs": {
            "development_report": str(development_report),
            "development_report_sha256": _sha256_file(development_report),
            "calibration_report": str(calibration_report),
            "calibration_report_sha256": _sha256_file(calibration_report),
            "gallery_manifest_sha256": _sha256_file(gallery_manifest),
            "development_manifest_sha256": _sha256_file(development_manifest),
            "calibration_manifest_sha256": _sha256_file(calibration_manifest),
        },
        "development": {
            "retrieval": retrieval_metrics(development_rows),
            "raw_localization": raw_localization_metrics(development_rows),
            "mode_structure": mode_structure_metrics(development_rows),
            "confidence_cross_validation": cross_validation,
        },
        "calibration": {
            "retrieval": retrieval_metrics(calibration_rows),
            "raw_localization": raw_localization_metrics(calibration_rows),
            "mode_structure": mode_structure_metrics(calibration_rows),
            "score_count": len(calibration_scores),
            "primary_operating_point": primary,
            "secondary_operating_point": secondary,
            "selected_operating_point": selected_label,
            "selected_metrics": selected,
            "hard_gate_audit": hard_gate_audit(
                calibration_rows,
                calibration_scores,
                gate_threshold,
            ),
            "all_rows_at_selected_threshold": (
                None
                if selected is None
                else product_metrics(calibration_rows, calibration_scores, gate_threshold)
            ),
        },
        "confidence_model": {
            "path": str(output_model),
            "sha256": _sha256_file(output_model),
        },
    }
    _atomic_json(output_report, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--development-report", type=Path, required=True)
    parser.add_argument("--calibration-report", type=Path, required=True)
    parser.add_argument("--gallery-manifest", type=Path, required=True)
    parser.add_argument("--development-manifest", type=Path, required=True)
    parser.add_argument("--calibration-manifest", type=Path, required=True)
    parser.add_argument("--retriever", required=True)
    parser.add_argument("--top-k", type=int, required=True)
    parser.add_argument(
        "--aggregation",
        choices=tuple(value.value for value in AggregationStrategy),
        required=True,
    )
    parser.add_argument(
        "--confidence-method",
        choices=("logistic", "logistic_isotonic"),
        required=True,
    )
    parser.add_argument("--output-report", type=Path, required=True)
    parser.add_argument("--output-model", type=Path, required=True)
    args = parser.parse_args()
    payload = calibrate_product_policy(
        development_report=args.development_report,
        calibration_report=args.calibration_report,
        gallery_manifest=args.gallery_manifest,
        development_manifest=args.development_manifest,
        calibration_manifest=args.calibration_manifest,
        retriever=args.retriever,
        k=args.top_k,
        aggregation=AggregationStrategy(args.aggregation),
        confidence_method=args.confidence_method,
        output_report=args.output_report,
        output_model=args.output_model,
    )
    print(
        json.dumps(
            {
                "selected_operating_point": payload["calibration"][
                    "selected_operating_point"
                ],
                "selected_metrics": payload["calibration"]["selected_metrics"],
                "output_report": str(args.output_report),
                "output_model": str(args.output_model),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
