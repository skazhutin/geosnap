"""Paired retrieval-regression audit with denominators and uncertainty."""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from .product_recovery import _sha256_file, _wilson


def _hit(row: Mapping[str, Any], *, k: int, distance_m: int) -> bool:
    rank = row["retrieval_diagnostics"]["by_positive_distance_m"][str(distance_m)].get(
        "positive_rank"
    )
    return rank is not None and int(rank) <= k


def _paired_interval(
    baseline: Sequence[bool],
    candidate: Sequence[bool],
    *,
    seed: int,
    resamples: int = 10000,
) -> list[float]:
    differences = np.asarray(candidate, dtype=np.float64) - np.asarray(
        baseline, dtype=np.float64
    )
    rng = np.random.default_rng(seed)
    estimates = np.empty(resamples, dtype=np.float64)
    for start in range(0, resamples, 1000):
        count = min(1000, resamples - start)
        indexes = rng.integers(0, len(differences), size=(count, len(differences)))
        estimates[start : start + count] = differences[indexes].mean(axis=1)
    return [float(value) for value in np.percentile(estimates, [2.5, 97.5])]


def paired_stratum_audit(
    baseline_report: Mapping[str, Any],
    candidate_report: Mapping[str, Any],
    *,
    k: int = 20,
    distance_m: int = 100,
    dimensions: Sequence[str] = (
        "source",
        "h3_coarse",
        "positive_provider_pair",
        "positive_heading_gap_bucket",
        "positive_temporal_gap_bucket",
        "local_gallery_density_bucket",
    ),
    seed: int = 20260902,
) -> dict[str, Any]:
    baseline_by_id = {
        str(row["query_id"]): row for row in baseline_report.get("per_query", [])
    }
    candidate_by_id = {
        str(row["query_id"]): row for row in candidate_report.get("per_query", [])
    }
    if not baseline_by_id or set(baseline_by_id) != set(candidate_by_id):
        raise ValueError("paired reports must contain the same non-empty query IDs")
    output: dict[str, Any] = {}
    hard_vetoes: list[dict[str, Any]] = []
    for dimension_position, dimension in enumerate(dimensions):
        query_ids: dict[str, list[str]] = defaultdict(list)
        for query_id, row in baseline_by_id.items():
            query_ids[str(row.get(dimension) or "unknown")].append(query_id)
        strata: dict[str, Any] = {}
        for stratum_position, (label, ids) in enumerate(sorted(query_ids.items())):
            baseline_values = [
                _hit(baseline_by_id[query_id], k=k, distance_m=distance_m)
                for query_id in ids
            ]
            candidate_values = [
                _hit(candidate_by_id[query_id], k=k, distance_m=distance_m)
                for query_id in ids
            ]
            baseline_successes = sum(baseline_values)
            candidate_successes = sum(candidate_values)
            difference = (candidate_successes - baseline_successes) / len(ids)
            interval = _paired_interval(
                baseline_values,
                candidate_values,
                seed=seed + dimension_position * 1000 + stratum_position,
            )
            major = len(ids) >= 30
            credible_regression = major and interval[1] < 0.0
            hard_veto = major and interval[1] < -0.05
            value = {
                "n": len(ids),
                "major_stratum_n_gte_30": major,
                "baseline": {
                    "successes": baseline_successes,
                    "total": len(ids),
                    "rate": baseline_successes / len(ids),
                    "wilson_95": _wilson(baseline_successes, len(ids)),
                },
                "candidate": {
                    "successes": candidate_successes,
                    "total": len(ids),
                    "rate": candidate_successes / len(ids),
                    "wilson_95": _wilson(candidate_successes, len(ids)),
                },
                "paired_difference": difference,
                "paired_bootstrap_95": interval,
                "classification": (
                    "credible_major_regression"
                    if credible_regression
                    else ("uncertain_small_stratum" if not major else "no_credible_regression")
                ),
                "hard_veto": hard_veto,
            }
            strata[label] = value
            if hard_veto:
                hard_vetoes.append({"dimension": dimension, "label": label} | value)
        output[dimension] = strata
    return {
        "metric": f"retrieval_recall_at_{k}_within_{distance_m}m",
        "query_count": len(baseline_by_id),
        "major_stratum_definition": "n_gte_30",
        "hard_veto_definition": "major stratum paired-bootstrap upper 95% endpoint below -5pp",
        "hard_vetoes": hard_vetoes,
        "dimensions": output,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--k", type=int, default=20)
    args = parser.parse_args()
    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    candidate = json.loads(args.candidate.read_text(encoding="utf-8"))
    payload = paired_stratum_audit(baseline, candidate, k=args.k) | {
        "baseline_report": str(args.baseline),
        "baseline_report_sha256": _sha256_file(args.baseline),
        "candidate_report": str(args.candidate),
        "candidate_report_sha256": _sha256_file(args.candidate),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(args.output.name + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, args.output)
    print(json.dumps({"hard_vetoes": payload["hard_vetoes"], "output": str(args.output)}, indent=2))


if __name__ == "__main__":
    main()
