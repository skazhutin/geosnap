"""Open the sealed v3 test once and evaluate baseline plus frozen candidate."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ml.localization.confidence_model import ConfidenceModel
from ml.localization.product_policy import AggregationStrategy
from ml.retrieval import create_retriever
from ml.runtime_config import FrozenRuntimeConfig, sha256_file

from .moscow_benchmark import run_moscow_benchmark
from .product_recovery import (
    mode_structure_metrics,
    product_metrics,
    raw_localization_metrics,
    replay_report,
    retrieval_metrics,
)
from .test_seal import begin_test_opening, complete_test_opening


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _source_benchmark(
    *,
    model: str,
    gallery_manifest: Path,
    test_manifest: Path,
    embedding_dir: Path,
    output_dir: Path,
    stem: str,
    device: str,
) -> tuple[dict[str, Any], Path]:
    retriever = create_retriever(model, device=device, batch_size=8)
    try:
        payload, json_path, _ = run_moscow_benchmark(
            gallery_manifest_path=gallery_manifest,
            query_manifest_path=test_manifest,
            retriever=retriever,
            output_dir=output_dir,
            expected_model=model,
            report_stem=stem,
            top_k=50,
            confidence_threshold=0.0,
            gallery_embedding_dir=embedding_dir,
        )
    finally:
        retriever.close()
    return payload, json_path


def _safety_adjusted_scores(
    rows: Sequence[Mapping[str, Any]],
    scores: Sequence[float],
    safety: Mapping[str, Any],
) -> tuple[list[float], dict[str, int]]:
    adjusted: list[float] = []
    counts = {"out_of_coverage": 0, "hard_gate_low_confidence": 0}
    for row, score in zip(rows, scores, strict=True):
        out_of_coverage = (
            safety.get("out_of_coverage_similarity") is not None
            and float(row["features"]["top1_similarity"])
            < float(safety["out_of_coverage_similarity"])
        )
        hard_gate = bool(
            (
                safety.get("minimum_cluster_mass") is not None
                and float(row["diagnostics"]["winning_cluster_mass"])
                < float(safety["minimum_cluster_mass"])
            )
            or (
                safety.get("minimum_cluster_mass_margin") is not None
                and float(row["features"]["geographic_mode_margin"])
                < float(safety["minimum_cluster_mass_margin"])
            )
            or (
                safety.get("minimum_cluster_candidates") is not None
                and float(row["features"]["winning_candidate_count"])
                < int(safety["minimum_cluster_candidates"])
            )
        )
        if out_of_coverage:
            counts["out_of_coverage"] += 1
        elif hard_gate:
            counts["hard_gate_low_confidence"] += 1
        adjusted.append(-1.0 if out_of_coverage or hard_gate else float(score))
    return adjusted, counts


def run_final_v3(
    *,
    bundle: Path,
    frozen_config_path: Path,
    frozen_hash_path: Path,
    state_dir: Path,
    output_dir: Path,
    baseline_embedding_dir: Path,
    device: str,
) -> dict[str, Any]:
    opening = begin_test_opening(
        bundle=bundle,
        frozen_config=frozen_config_path,
        frozen_hash=frozen_hash_path,
        state_dir=state_dir,
    )
    frozen = FrozenRuntimeConfig.load(frozen_config_path, verify_index=True)
    gallery_manifest = bundle / "gallery.parquet"
    baseline_source, baseline_source_path = _source_benchmark(
        model="megaloc",
        gallery_manifest=gallery_manifest,
        test_manifest=opening.test_manifest,
        embedding_dir=baseline_embedding_dir,
        output_dir=output_dir,
        stem="v3_final_baseline_megaloc_top50_source",
        device=device,
    )
    if frozen.retriever == "megaloc":
        candidate_source = baseline_source
        candidate_source_path = baseline_source_path
    else:
        candidate_embedding_dir = Path(str(frozen.payload["embedding"]["directory"]))
        candidate_source, candidate_source_path = _source_benchmark(
            model=frozen.retriever,
            gallery_manifest=gallery_manifest,
            test_manifest=opening.test_manifest,
            embedding_dir=candidate_embedding_dir,
            output_dir=output_dir,
            stem="v3_final_candidate_top50_source",
            device=device,
        )

    baseline_rows, _ = replay_report(
        baseline_source_path,
        gallery_manifest=gallery_manifest,
        query_manifest=opening.test_manifest,
        k=50,
        aggregation=AggregationStrategy.LEGACY_WEIGHTED_MEDOID,
    )
    baseline_safety = {
        "out_of_coverage_similarity": 0.15,
        "minimum_cluster_mass": 0.45,
        "minimum_cluster_mass_margin": 0.10,
        "minimum_cluster_candidates": 2,
    }
    baseline_scores, baseline_status = _safety_adjusted_scores(
        baseline_rows,
        [float(row["heuristic_confidence"]) for row in baseline_rows],
        baseline_safety,
    )
    baseline_product = product_metrics(baseline_rows, baseline_scores, 1.0)

    localization_config = frozen.payload["localization"]
    candidate_rows, _ = replay_report(
        candidate_source_path,
        gallery_manifest=gallery_manifest,
        query_manifest=opening.test_manifest,
        k=frozen.top_k,
        aggregation=AggregationStrategy(str(localization_config["aggregation"])),
    )
    confidence_path = frozen.confidence_model_path
    if confidence_path is None:
        raise RuntimeError("v3 frozen candidate must contain a confidence artifact")
    confidence_model = ConfidenceModel.from_dict(
        json.loads(confidence_path.read_text(encoding="utf-8"))
    )
    candidate_scores, candidate_status = _safety_adjusted_scores(
        candidate_rows,
        [confidence_model.predict_proba(row["features"]) for row in candidate_rows],
        localization_config.get("safety_gates", {}),
    )
    candidate_product = product_metrics(
        candidate_rows,
        candidate_scores,
        frozen.confidence_threshold,
    )
    candidate_answered = int(candidate_product["answered_count"])
    candidate_coverage = {
        "ok": candidate_answered,
        "approximate": 0,
        "low_confidence": len(candidate_rows)
        - candidate_answered
        - candidate_status["out_of_coverage"],
        "out_of_coverage": candidate_status["out_of_coverage"],
    }
    baseline_report = {
        "configuration": "unchanged_v2_megaloc_k50_fail_closed",
        "retrieval": retrieval_metrics(baseline_rows),
        "raw_localization": raw_localization_metrics(baseline_rows),
        "product": baseline_product,
        "status_gate_counts": baseline_status,
        "runtime": baseline_source["runtime"],
    }
    candidate_report = {
        "configuration": str(frozen.payload["configuration_id"]),
        "retrieval": retrieval_metrics(candidate_rows),
        "raw_localization": raw_localization_metrics(candidate_rows),
        "mode_structure": mode_structure_metrics(candidate_rows),
        "product": candidate_product,
        "coverage_status_counts": candidate_coverage,
        "runtime": candidate_source["runtime"],
    }
    baseline_report_path = output_dir / "v3_final_baseline.json"
    candidate_report_path = output_dir / "v3_final_candidate.json"
    _atomic_json(baseline_report_path, baseline_report)
    _atomic_json(candidate_report_path, candidate_report)
    receipt = complete_test_opening(
        opening,
        {"baseline": baseline_report_path, "candidate": candidate_report_path},
    )
    payload = {
        "schema_version": 1,
        "kind": "moscow_real_v3_one_shot_final_test",
        "post_test_tuning": "forbidden",
        "frozen_config": str(frozen_config_path),
        "frozen_config_sha256": sha256_file(frozen_config_path),
        "test_manifest_sha256": opening.test_sha256,
        "baseline": baseline_report,
        "candidate": candidate_report,
        "source_reports": {
            "baseline": str(baseline_source_path),
            "candidate": str(candidate_source_path),
        },
        "receipt": str(receipt),
    }
    _atomic_json(output_dir / "v3_final_comparison.json", payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--frozen-config", type=Path, required=True)
    parser.add_argument("--frozen-hash", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--baseline-embedding-dir", type=Path, required=True)
    parser.add_argument("--device", default="auto")
    args = parser.parse_args()
    result = run_final_v3(
        bundle=args.bundle,
        frozen_config_path=args.frozen_config,
        frozen_hash_path=args.frozen_hash,
        state_dir=args.state_dir,
        output_dir=args.output_dir,
        baseline_embedding_dir=args.baseline_embedding_dir,
        device=args.device,
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
