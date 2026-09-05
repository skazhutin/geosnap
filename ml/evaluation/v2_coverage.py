"""Generate the versioned v2 fixed-grid coverage/diversity evidence and map."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from ml.evaluation.product_quality_diagnosis import _coverage_grid, _write_coverage_map
from ml.ingestion.common import write_json


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gallery", type=Path, required=True)
    parser.add_argument("--calibration", type=Path, required=True)
    parser.add_argument("--test", type=Path, required=True)
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-png", type=Path, required=True)
    args = parser.parse_args()
    grid = _coverage_grid(
        pd.read_parquet(args.gallery),
        pd.read_parquet(args.calibration),
        pd.read_parquet(args.test),
        pd.read_parquet(args.source_manifest),
    )
    write_json(args.output_json, grid)
    _write_coverage_map(grid, args.output_png)
    print(json.dumps({"category_counts": grid["category_counts"], "flag_counts": grid["flag_counts"]}))


if __name__ == "__main__":
    main()
