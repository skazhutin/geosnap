"""Claim the sealed v4 test and evaluate both frozen policies in one transaction."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np

from ml.localization.confidence_model import ConfidenceModel, MultinomialRiskModel
from ml.localization.product_policy import AggregationStrategy
from ml.retrieval import create_retriever
from ml.runtime_config import FrozenRuntimeConfig, sha256_file

from .final_policy import policy_metrics
from .moscow_benchmark import run_moscow_benchmark
from .product_recovery import (
    mode_structure_metrics,
    raw_localization_metrics,
    replay_report,
    retrieval_metrics,
)
from .test_seal import begin_test_opening, complete_test_opening


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _validate_historical_baseline(
    frozen: FrozenRuntimeConfig,
) -> tuple[dict[str, Any], ConfidenceModel]:
    baseline = dict(frozen.payload["historical_baseline"])
    config_path = Path(str(baseline["config"]))
    artifact_path = Path(str(baseline["confidence_artifact"]))
    if sha256_file(config_path) != baseline["config_sha256"]:
        raise RuntimeError("historical Part 2.5 configuration hash mismatch")
    if sha256_file(artifact_path) != baseline["confidence_artifact_sha256"]:
        raise RuntimeError("historical Part 2.5 confidence artifact hash mismatch")
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if (
        config["retriever"]["name"] != "sage-vitb"
        or int(config["retrieval"]["top_k"]) != 30
        or config["localization"]["aggregation"] != "density_aware_mode_vote"
    ):
        raise RuntimeError("historical baseline is not the exact Part 2.5 policy")
    model = ConfidenceModel.from_dict(
        json.loads(artifact_path.read_text(encoding="utf-8"))
    )
    return config, model


def run_final_v4(
    *,
    bundle: Path,
    frozen_config_path: Path,
    frozen_hash_path: Path,
    state_dir: Path,
    output_dir: Path,
    device: str,
    model_cache: Path | None,
) -> dict[str, Any]:
    opening = begin_test_opening(
        bundle=bundle,
        frozen_config=frozen_config_path,
        frozen_hash=frozen_hash_path,
        state_dir=state_dir,
    )
    if opening.generation != "v4":
        raise RuntimeError("final v4 evaluator requires a v4 test seal")
    frozen = FrozenRuntimeConfig.load(frozen_config_path, verify_index=True)
    baseline_config, baseline_model = _validate_historical_baseline(frozen)
    gallery_manifest = bundle / "gallery.parquet"
    retriever = create_retriever(
        frozen.retriever,
        device=device,
        batch_size=8,
        cache_dir=model_cache,
    )
    try:
        source, source_path, _ = run_moscow_benchmark(
            gallery_manifest_path=gallery_manifest,
            query_manifest_path=opening.test_manifest,
            retriever=retriever,
            output_dir=output_dir,
            expected_model=frozen.retriever,
            report_stem="v4_final_sage_top50_source",
            top_k=50,
            confidence_threshold=0.0,
            gallery_embedding_dir=Path(str(frozen.payload["embedding"]["directory"])),
        )
    finally:
        retriever.close()

    baseline_localization = baseline_config["localization"]
    baseline_rows, _ = replay_report(
        source_path,
        gallery_manifest=gallery_manifest,
        query_manifest=opening.test_manifest,
        k=int(baseline_config["retrieval"]["top_k"]),
        aggregation=AggregationStrategy(str(baseline_localization["aggregation"])),
    )
    baseline_scores = [
        baseline_model.predict_proba(row["features"]) for row in baseline_rows
    ]
    baseline_product = policy_metrics(
        baseline_rows,
        baseline_scores,
        float(baseline_localization["confidence_threshold"]),
        bootstrap=True,
    )

    candidate_localization = frozen.payload["localization"]
    candidate_rows, _ = replay_report(
        source_path,
        gallery_manifest=gallery_manifest,
        query_manifest=opening.test_manifest,
        k=frozen.top_k,
        aggregation=AggregationStrategy(str(candidate_localization["aggregation"])),
    )
    artifact_path = frozen.confidence_model_path
    if artifact_path is None:
        raise RuntimeError("v4 candidate confidence artifact is missing")
    candidate_model = MultinomialRiskModel.from_dict(
        json.loads(artifact_path.read_text(encoding="utf-8"))
    )
    candidate_probabilities = [
        candidate_model.predict_proba(row["features"]) for row in candidate_rows
    ]
    correctness_scores = [value[0] for value in candidate_probabilities]
    catastrophic_risk_scores = [value[2] for value in candidate_probabilities]
    candidate_product = policy_metrics(
        candidate_rows,
        correctness_scores,
        frozen.confidence_threshold,
        risk_scores=catastrophic_risk_scores,
        risk_threshold=float(candidate_localization["catastrophic_risk_threshold"]),
        bootstrap=True,
    )

    baseline_report = {
        "configuration": "exact_part_2_5_sage_policy_on_v4",
        "configuration_sha256": frozen.payload["historical_baseline"]["config_sha256"],
        "retrieval": retrieval_metrics(baseline_rows),
        "raw_localization": raw_localization_metrics(baseline_rows),
        "mode_structure": mode_structure_metrics(baseline_rows),
        "product": baseline_product,
        "coverage_status_counts": {
            "ok": baseline_product["answered_count"],
            "approximate": 0,
            "low_confidence": len(baseline_rows) - baseline_product["answered_count"],
            "out_of_coverage": 0,
        },
        "runtime": {"model_and_index": source["runtime"], "latency": source["primary"]["latency"]},
    }
    candidate_report = {
        "configuration": str(frozen.payload["configuration_id"]),
        "configuration_sha256": frozen.sha256,
        "retrieval": retrieval_metrics(candidate_rows),
        "raw_localization": raw_localization_metrics(candidate_rows),
        "mode_structure": mode_structure_metrics(candidate_rows),
        "product": candidate_product,
        "coverage_status_counts": {
            "ok": candidate_product["answered_count"],
            "approximate": 0,
            "low_confidence": len(candidate_rows) - candidate_product["answered_count"],
            "out_of_coverage": 0,
        },
        "confidence_score_distributions": {
            "correctness": {
                "median": float(np.median(correctness_scores)),
                "p90": float(np.percentile(correctness_scores, 90)),
            },
            "catastrophic_risk": {
                "median": float(np.median(catastrophic_risk_scores)),
                "p90": float(np.percentile(catastrophic_risk_scores, 90)),
            },
        },
        "runtime": {"model_and_index": source["runtime"], "latency": source["primary"]["latency"]},
    }
    baseline_path = output_dir / "v4_final_part2_5_baseline.json"
    candidate_path = output_dir / "v4_final_candidate.json"
    _atomic_json(baseline_path, baseline_report)
    _atomic_json(candidate_path, candidate_report)
    receipt = complete_test_opening(
        opening,
        {"baseline": baseline_path, "candidate": candidate_path},
    )
    payload = {
        "schema_version": 1,
        "kind": "moscow_real_v4_one_shot_final_test",
        "post_test_tuning": "forbidden",
        "frozen_config": str(frozen_config_path),
        "frozen_config_sha256": frozen.sha256,
        "test_manifest_sha256": opening.test_sha256,
        "source_report": str(source_path),
        "source_report_sha256": sha256_file(source_path),
        "baseline": baseline_report,
        "candidate": candidate_report,
        "receipt": str(receipt),
    }
    _atomic_json(output_dir / "v4_final_comparison.json", payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--frozen-config", type=Path, required=True)
    parser.add_argument("--frozen-hash", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--device", default="auto")
    parser.add_argument("--model-cache", type=Path)
    args = parser.parse_args()
    result = run_final_v4(
        bundle=args.bundle,
        frozen_config_path=args.frozen_config,
        frozen_hash_path=args.frozen_hash,
        state_dir=args.state_dir,
        output_dir=args.output_dir,
        device=args.device,
        model_cache=args.model_cache,
    )
    print(
        json.dumps(
            {
                "baseline": result["baseline"]["product"],
                "candidate": result["candidate"]["product"],
                "receipt": result["receipt"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
