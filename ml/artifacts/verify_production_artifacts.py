"""Verify installed production artifacts without downloading anything."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .manifest import validate_production_artifacts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("configs/production_artifacts.json"))
    parser.add_argument("--artifact-dir", type=Path, default=None)
    parser.add_argument("--include-optional", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            validate_production_artifacts(
                args.manifest,
                artifact_root=args.artifact_dir,
                required_only=not args.include_optional,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
