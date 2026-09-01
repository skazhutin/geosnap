"""Create a new immutable dataset input by unioning prior and tranche manifests."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from ml.ingestion.common import write_json
from ml.ingestion.schema import PIPELINE_COLUMNS, coerce_manifest_schema, read_manifest, write_manifest


def _quality(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return -1.0
    return result if math.isfinite(result) else -1.0


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(
    input_manifests: Sequence[Path],
    output_manifest: Path,
    report_path: Path,
) -> dict[str, Any]:
    if len(input_manifests) < 2:
        raise ValueError("versioned manifest union requires at least two inputs")
    resolved = [path.resolve() for path in input_manifests]
    if len(set(resolved)) != len(resolved) or output_manifest.resolve() in set(resolved):
        raise ValueError("versioned manifest paths must be distinct")
    frames: list[pd.DataFrame] = []
    input_rows: list[dict[str, Any]] = []
    for priority, path in enumerate(input_manifests):
        frame = read_manifest(path, allow_empty=False).copy()
        frame["_versioned_input_priority"] = priority
        frames.append(frame)
        input_rows.append({"path": str(path), "rows": len(frame), "sha256": _sha256(path)})
    columns = set().union(*(frame.columns for frame in frames)) - {"_versioned_input_priority"}
    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined["_quality_order"] = combined["quality_score"].map(_quality)
    ordered = combined.sort_values(
        ["_quality_order", "_versioned_input_priority", "source", "source_image_id", "id"],
        ascending=[False, False, True, True, True],
        kind="mergesort",
    )
    duplicate_mask = ordered.duplicated(["source", "source_image_id"], keep="first")
    removed = ordered.loc[duplicate_mask]
    kept = ordered.loc[~duplicate_mask].sort_values(
        ["source", "source_image_id", "id"], kind="mergesort"
    )
    kept = kept.drop(columns=["_versioned_input_priority", "_quality_order"])
    for column in columns - set(kept.columns):
        kept[column] = pd.NA
    kept = coerce_manifest_schema(kept)
    extras = sorted(columns - set(PIPELINE_COLUMNS))
    kept = kept[[*PIPELINE_COLUMNS, *extras]]
    write_manifest(kept, output_manifest, allow_empty=False)
    report: dict[str, Any] = {
        "schema_version": 1,
        "inputs": input_rows,
        "input_rows": int(sum(len(frame) for frame in frames)),
        "output_rows": int(len(kept)),
        "duplicate_source_identities_removed": int(duplicate_mask.sum()),
        "removed_by_source": dict(sorted(Counter(str(value) for value in removed["source"]).items())),
        "selection": "highest quality, then later input priority, then stable identity",
        "output_manifest": str(output_manifest),
        "output_sha256": _sha256(output_manifest),
    }
    write_json(report_path, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Union immutable prior and targeted-tranche manifests")
    parser.add_argument("--input-manifest", action="append", type=Path, required=True)
    parser.add_argument("--output-manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = run(args.input_manifest, args.output_manifest, args.report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
