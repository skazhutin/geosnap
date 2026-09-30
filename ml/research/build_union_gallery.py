"""Freeze a larger deployment gallery with independent union duplicate removal."""

from __future__ import annotations

import json

import pandas as pd

from ml.evaluation.moscow_split import _PerceptualHashIndex
from ml.ingestion.schema import write_manifest
from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256, write_once


def run():
    root = ROOT / "gallery_expansion"
    if (root / "union.json").exists():
        raise ValueError("union already fixed")
    baseline = pd.read_parquet("data/evaluation/moscow_real_v4/gallery.parquet")
    audited = pd.read_parquet(root / "audited_union.parquet")
    index = _PerceptualHashIndex(4)
    exact = set(baseline.file_sha256)
    for i, row in enumerate(baseline.itertuples()):
        index.add(i, int(str(row.perceptual_hash), 16))
    kept = []
    for row in audited.to_dict("records"):
        phash = int(str(row["perceptual_hash"]), 16)
        if row["file_sha256"] in exact or index.matches(phash):
            continue
        exact.add(row["file_sha256"])
        index.add(len(baseline) + len(kept), phash)
        kept.append(row)
    full = pd.concat([baseline, pd.DataFrame(kept)], ignore_index=True)
    path = root / "gallery_union.parquet"
    write_manifest(full, path)
    result = {
        "status": "larger_union_fixed_without_query_coordinates_or_outcomes",
        "baseline_count": len(baseline),
        "added_count": len(kept),
        "total_count": len(full),
        "audit_sha256": sha256(root / "audited_union.parquet"),
        "manifest_sha256": sha256(path),
        "sampling": "frozen union ordering, independent pHash4 dedup versus baseline and within union",
    }
    write_once(root / "union.json", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    run()
