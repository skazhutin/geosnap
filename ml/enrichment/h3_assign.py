"""Stage 3.4: attach H3 cells to each record."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

try:
    import h3
except ImportError as exc:  # pragma: no cover
    raise SystemExit("h3 package is required: pip install h3") from exc


def to_h3(lat: float, lon: float, resolution: int) -> str:
    # Supports both h3-py v3 and v4 APIs.
    if hasattr(h3, "geo_to_h3"):
        return str(h3.geo_to_h3(lat, lon, resolution))
    return str(h3.latlng_to_cell(lat, lon, resolution))


def run(input_manifest: Path, output_manifest: Path, coarse_resolution: int, fine_resolution: int) -> None:
    df = pd.read_parquet(input_manifest)

    df["h3_coarse"] = df.apply(lambda r: to_h3(float(r["lat"]), float(r["lon"]), coarse_resolution), axis=1)
    df["h3_fine"] = df.apply(lambda r: to_h3(float(r["lat"]), float(r["lon"]), fine_resolution), axis=1)

    output_manifest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_manifest, index=False)

    print(
        {
            "rows": len(df),
            "coarse_resolution": coarse_resolution,
            "fine_resolution": fine_resolution,
            "unique_h3_coarse": int(df["h3_coarse"].nunique()),
            "unique_h3_fine": int(df["h3_fine"].nunique()),
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Assign H3 cells to manifest")
    parser.add_argument("--manifest", default="data/processed/manifest_step3.parquet")
    parser.add_argument("--output", default="data/processed/manifest_step4.parquet")
    parser.add_argument("--coarse-resolution", type=int, default=6)
    parser.add_argument("--fine-resolution", type=int, default=9)
    args = parser.parse_args()

    run(
        input_manifest=Path(args.manifest),
        output_manifest=Path(args.output),
        coarse_resolution=args.coarse_resolution,
        fine_resolution=args.fine_resolution,
    )


if __name__ == "__main__":
    main()
