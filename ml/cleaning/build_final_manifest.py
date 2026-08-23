"""Finalize a cleaned manifest without dropping canonical provenance fields."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ml.ingestion.common import write_json
from ml.ingestion.schema import CANONICAL_COLUMNS, read_manifest, write_manifest

FINAL_COLUMNS = CANONICAL_COLUMNS


def run(
    input_manifest: Path,
    output_manifest: Path,
    *,
    report_path: Path | None = None,
) -> dict[str, Any]:
    df = read_manifest(input_manifest, allow_empty=True)
    # Canonical fields lead the file; all useful enrichment/diagnostic columns
    # follow in their existing order.  Provenance is never projected away.
    ordered = [*CANONICAL_COLUMNS, *(column for column in df.columns if column not in CANONICAL_COLUMNS)]
    final_df = df[ordered].copy()
    write_manifest(final_df, output_manifest, allow_empty=True, strict_reference=True)
    summary: dict[str, Any] = {
        "rows": len(final_df),
        "empty_manifest": len(final_df) == 0,
        "columns": ordered,
        "output": str(output_manifest),
    }
    target = report_path or output_manifest.with_suffix(".report.json")
    write_json(target, summary)
    target.with_suffix(".md").write_text(
        "# Final manifest\n\n"
        f"- Rows: {len(final_df)}\n"
        f"- Empty: {len(final_df) == 0}\n"
        f"- Required canonical fields retained: {', '.join(CANONICAL_COLUMNS)}\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Build final schema-validated clean manifest")
    parser.add_argument("--manifest", default="data/processed/manifest_step4.parquet")
    parser.add_argument("--output", default="data/processed/manifest_clean.parquet")
    parser.add_argument("--report")
    args = parser.parse_args()
    run(
        input_manifest=Path(args.manifest),
        output_manifest=Path(args.output),
        report_path=Path(args.report) if args.report else None,
    )


if __name__ == "__main__":
    main()
