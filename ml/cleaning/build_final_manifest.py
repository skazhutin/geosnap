"""Stage 3.5: build final manifest for embeddings/indexing."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

FINAL_COLUMNS = [
    "id",
    "source",
    "lat",
    "lon",
    "image_path",
    "h3_coarse",
    "h3_fine",
    "quality_score",
    "captured_at",
    "heading",
    "metadata_json",
]


def run(input_manifest: Path, output_manifest: Path) -> None:
    df = pd.read_parquet(input_manifest).copy()

    if "quality_score" not in df.columns:
        df["quality_score"] = 0.5
    if "heading" not in df.columns:
        df["heading"] = None
    if "metadata_json" not in df.columns:
        df["metadata_json"] = [{} for _ in range(len(df))]

    missing = [c for c in FINAL_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for final manifest: {missing}")

    final_df = df[FINAL_COLUMNS].copy()
    output_manifest.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(output_manifest, index=False)

    print({"rows": len(final_df), "output": str(output_manifest)})


def main() -> None:
    parser = argparse.ArgumentParser(description="Build final clean manifest")
    parser.add_argument("--manifest", default="data/processed/manifest_step4.parquet")
    parser.add_argument("--output", default="data/processed/manifest_clean.parquet")
    args = parser.parse_args()

    run(input_manifest=Path(args.manifest), output_manifest=Path(args.output))


if __name__ == "__main__":
    main()
