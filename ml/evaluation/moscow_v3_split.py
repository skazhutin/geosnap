"""Promote a two-way Moscow query bundle into sealed v3 dev/calibration/test splits."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import tempfile
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ml.ingestion.common import write_json
from ml.ingestion.schema import read_manifest
from ml.localization.geo import haversine_m

from .moscow_split import (
    _pairwise_audit,
    _provider_sequence_values,
    _sha256_file,
    _stable_score,
    _text,
    _write_split_outputs,
)

SCHEMA_VERSION = 1
ALGORITHM_VERSION = "v3-dev-cal-test-geographic-components-v1"


class MoscowV3SplitError(RuntimeError):
    """The base bundle cannot be safely promoted into a sealed v3 bundle."""


def _resolution_bucket(row: Any) -> str:
    try:
        long_side = max(int(row.width), int(row.height))
    except (AttributeError, TypeError, ValueError):
        return "unknown"
    if long_side < 1600:
        return "lt1600"
    if long_side < 2500:
        return "1600_2499"
    return "ge2500"


def _feature_counts(frame: pd.DataFrame) -> Counter[str]:
    result: Counter[str] = Counter()
    for row in frame.itertuples():
        result[f"provider:{_text(row.source).lower() or 'unknown'}"] += 1
        result[f"area:{_text(row.evaluation_area_h3) or 'unknown'}"] += 1
        result[f"resolution:{_resolution_bucket(row)}"] += 1
    return result


def assign_policy_splits(frame: pd.DataFrame, *, seed: int) -> dict[str, str]:
    """Assign whole embargo-connected geo groups to development or calibration."""

    required = {"evaluation_geo_group_id", "source", "evaluation_area_h3", "width", "height"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise MoscowV3SplitError(f"base calibration is missing columns: {missing}")
    if frame.empty:
        raise MoscowV3SplitError("base calibration cannot be empty")
    if frame["evaluation_geo_group_id"].isna().any():
        raise MoscowV3SplitError("every policy query needs an evaluation_geo_group_id")

    groups = {
        str(group_id): current.reset_index(drop=True)
        for group_id, current in frame.groupby("evaluation_geo_group_id", sort=True, dropna=False)
    }
    if len(groups) < 2:
        raise MoscowV3SplitError("at least two geographic components are required")
    ordered = sorted(
        groups,
        key=lambda group_id: (
            -len(groups[group_id]),
            _stable_score(seed, "v3-policy-component", group_id),
        ),
    )
    split_names = ("development", "calibration")
    row_counts = {name: 0 for name in split_names}
    sequence_counts = {name: 0 for name in split_names}
    feature_counts = {name: Counter() for name in split_names}
    assignment: dict[str, str] = {}
    for position, group_id in enumerate(ordered):
        current = groups[group_id]
        current_features = _feature_counts(current)
        current_sequences = len(_provider_sequence_values(current))
        if position < len(split_names):
            selected = split_names[position]
        else:

            def score(
                candidate: str,
                *,
                current_features_bound: Counter[str] = current_features,
                current_rows: int = len(current),
                current_group_id: str = group_id,
            ) -> tuple[float, int, int, str]:
                other = split_names[1] if candidate == split_names[0] else split_names[0]
                feature_keys = set(feature_counts[candidate]) | set(feature_counts[other]) | set(
                    current_features_bound
                )
                imbalance = sum(
                    abs(
                        feature_counts[candidate][key]
                        + current_features_bound[key]
                        - feature_counts[other][key]
                    )
                    / max(
                        feature_counts[candidate][key]
                        + current_features_bound[key]
                        + feature_counts[other][key],
                        1,
                    )
                    for key in feature_keys
                ) / max(len(feature_keys), 1)
                candidate_rows = row_counts[candidate] + current_rows
                row_imbalance = abs(candidate_rows - row_counts[other]) / max(
                    candidate_rows + row_counts[other], 1
                )
                return (
                    row_imbalance + imbalance,
                    row_counts[candidate],
                    sequence_counts[candidate],
                    _stable_score(seed, "v3-policy-assignment", current_group_id, candidate),
                )

            selected = min(split_names, key=score)
        assignment[group_id] = selected
        row_counts[selected] += len(current)
        sequence_counts[selected] += current_sequences
        feature_counts[selected].update(current_features)
    return assignment


def _counts(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": len(frame),
        "providers": dict(sorted(Counter(_text(value).lower() for value in frame["source"]).items())),
        "provider_sequences": len(_provider_sequence_values(frame)),
        "evaluation_areas": int(frame["evaluation_area_h3"].nunique()),
        "geographic_components": int(frame["evaluation_geo_group_id"].nunique()),
        "resolution_buckets": dict(sorted(Counter(_resolution_bucket(row) for row in frame.itertuples()).items())),
    }


def _cross_geographic_audit(left: pd.DataFrame, right: pd.DataFrame, embargo_m: float) -> dict[str, Any]:
    minimum: float | None = None
    violations = 0
    for left_row in left.itertuples():
        for right_row in right.itertuples():
            distance = haversine_m(
                float(left_row.lat),
                float(left_row.lon),
                float(right_row.lat),
                float(right_row.lon),
            )
            minimum = distance if minimum is None else min(minimum, distance)
            if distance < embargo_m:
                violations += 1
    return {"minimum_distance_m": minimum, "embargo_violation_pairs": violations}


def _verify_existing_bundle(output_dir: Path) -> dict[str, Any] | None:
    manifest_path = output_dir / "bundle_manifest.json"
    audit_path = output_dir / "split_audit.json"
    if not manifest_path.is_file() or not audit_path.is_file():
        return None
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for name, expected in manifest.get("files", {}).items():
        path = output_dir / name
        if not path.is_file() or _sha256_file(path) != expected:
            return None
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    if audit.get("bundle", {}).get("fingerprint") != manifest.get("bundle_fingerprint"):
        return None
    return audit


def run(base_bundle: Path, output_dir: Path, *, seed: int = 20260902) -> dict[str, Any]:
    """Create a deterministic, immutable v3 bundle from a leakage-audited base split."""

    existing = _verify_existing_bundle(output_dir) if output_dir.is_dir() else None
    if existing is not None:
        return existing
    if output_dir.exists():
        raise MoscowV3SplitError(f"refusing to replace existing v3 bundle: {output_dir}")

    base_audit_path = base_bundle / "split_audit.json"
    base_manifest_path = base_bundle / "bundle_manifest.json"
    if not base_audit_path.is_file() or not base_manifest_path.is_file():
        raise MoscowV3SplitError("base bundle is missing its integrity manifests")
    base_audit = json.loads(base_audit_path.read_text(encoding="utf-8"))
    base_manifest = json.loads(base_manifest_path.read_text(encoding="utf-8"))
    for name, expected in base_manifest.get("files", {}).items():
        path = base_bundle / name
        if not path.is_file() or _sha256_file(path) != expected:
            raise MoscowV3SplitError(f"base bundle integrity mismatch: {name}")

    gallery = read_manifest(base_bundle / "gallery.parquet", allow_empty=True)
    policy_pool = read_manifest(base_bundle / "calibration_queries.parquet", allow_empty=True)
    test = read_manifest(base_bundle / "test_queries.parquet", allow_empty=True)
    assignment = assign_policy_splits(policy_pool, seed=seed)
    labels = policy_pool["evaluation_geo_group_id"].astype(str).map(assignment)
    development = policy_pool.loc[labels == "development"].copy().reset_index(drop=True)
    calibration = policy_pool.loc[labels == "calibration"].copy().reset_index(drop=True)
    development["evaluation_split"] = "development"
    calibration["evaluation_split"] = "calibration"
    test = test.copy()
    test["evaluation_split"] = "test"

    frames: Mapping[str, pd.DataFrame] = {
        "gallery": gallery,
        "development": development,
        "calibration": calibration,
        "test": test,
    }
    split_names = tuple(frames)
    pairwise: dict[str, Any] = {}
    embargo_m = float(base_audit["parameters"]["calibration_test_embargo_m"])
    geographic: dict[str, Any] = {}
    for left_position, left_name in enumerate(split_names):
        for right_name in split_names[left_position + 1 :]:
            key = f"{left_name}__{right_name}"
            pairwise[key] = _pairwise_audit(
                frames[left_name],
                frames[right_name],
                int(base_audit["parameters"]["phash_distance_threshold"]),
            )
            if left_name != "gallery" and right_name != "gallery":
                geographic[key] = _cross_geographic_audit(
                    frames[left_name], frames[right_name], embargo_m
                )
    if any(any(value != 0 for value in values.values()) for values in pairwise.values()):
        raise MoscowV3SplitError(f"non-zero v3 cross-split leakage: {pairwise}")
    if any(values["embargo_violation_pairs"] for values in geographic.values()):
        raise MoscowV3SplitError(f"v3 geographic embargo failed: {geographic}")

    assignment_fingerprint = hashlib.sha256(
        json.dumps(assignment, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    bundle_fingerprint = hashlib.sha256(
        f"{base_manifest['bundle_fingerprint']}\0{seed}\0{assignment_fingerprint}".encode()
    ).hexdigest()
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.{os.getpid()}.", dir=output_dir.parent))
    try:
        paths = {
            "gallery": staging / "gallery.parquet",
            "development": staging / "development_queries.parquet",
            "calibration": staging / "calibration_queries.parquet",
            "test": staging / "test_queries.parquet",
        }
        _write_split_outputs([(frames[name], paths[name]) for name in split_names])
        hashes = {name: _sha256_file(path) for name, path in paths.items()}
        test_seal = {
            "schema_version": 1,
            "status": "sealed_before_policy_tuning",
            "maximum_runs": 1,
            "test_manifest": "test_queries.parquet",
            "test_manifest_sha256": hashes["test"],
            "allowed_after": "configs/moscow_real_v3_frozen.json is written and hash-verified",
        }
        write_json(staging / "test_seal.json", test_seal)
        audit = {
            "schema_version": SCHEMA_VERSION,
            "algorithm_version": ALGORITHM_VERSION,
            "created_at": datetime.now(UTC).isoformat(),
            "seed": seed,
            "base_bundle": {
                "path": str(base_bundle),
                "fingerprint": base_manifest["bundle_fingerprint"],
                "audit_sha256": _sha256_file(base_audit_path),
                "manifest_sha256": _sha256_file(base_manifest_path),
                "historical_query_exclusions": base_audit.get("eligibility", {}).get(
                    "historical_query_exclusion_manifests", []
                ),
            },
            "parameters": {
                "geographic_embargo_m": embargo_m,
                "phash_distance_threshold": int(
                    base_audit["parameters"]["phash_distance_threshold"]
                ),
            },
            "sample_counts": {name: _counts(frame) for name, frame in frames.items()},
            "pairwise_leakage_audit": pairwise,
            "query_geographic_audit": geographic,
            "policy_assignment": {
                "development_components": sum(value == "development" for value in assignment.values()),
                "calibration_components": sum(value == "calibration" for value in assignment.values()),
                "fingerprint": assignment_fingerprint,
            },
            "hashes": {
                "gallery_parquet_sha256": hashes["gallery"],
                "development_queries_parquet_sha256": hashes["development"],
                "calibration_queries_parquet_sha256": hashes["calibration"],
                "sealed_test_queries_parquet_sha256": hashes["test"],
            },
            "bundle": {"fingerprint": bundle_fingerprint},
        }
        write_json(staging / "split_audit.json", audit)
        bundle_files = {
            path.name: _sha256_file(path)
            for path in (*paths.values(), staging / "test_seal.json", staging / "split_audit.json")
        }
        write_json(
            staging / "bundle_manifest.json",
            {
                "schema_version": SCHEMA_VERSION,
                "bundle_fingerprint": bundle_fingerprint,
                "files": bundle_files,
            },
        )
        if _verify_existing_bundle(staging) is None:
            raise MoscowV3SplitError("staged v3 bundle failed integrity verification")
        os.replace(staging, output_dir)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return json.loads((output_dir / "split_audit.json").read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-bundle", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20260902)
    args = parser.parse_args()
    audit = run(args.base_bundle, args.output_dir, seed=args.seed)
    print(json.dumps(audit["sample_counts"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
