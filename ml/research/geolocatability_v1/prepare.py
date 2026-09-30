"""Freeze image-only annotation inputs before any VLM inference."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
from PIL import Image

from .common import (
    OUT,
    QUERY,
    SCHEMA_VERSION,
    TECHNICAL,
    json_once,
    now,
    revision,
    sha256,
    verify_production,
    write_once,
)


def run() -> None:
    guard = verify_production()
    source = pd.read_parquet(QUERY, columns=["id", "image_path", "file_sha256"])
    if len(source) != 1184 or source.id.nunique() != 1184 or source.id.isna().any():
        raise RuntimeError("The frozen development population is not 1,184 unique identities")
    rows = []
    for item in source.itertuples(index=False):
        path = OUT.parents[2] / item.image_path
        if not path.is_file() or sha256(path) != item.file_sha256:
            raise RuntimeError(f"Missing or altered query image: {item.id}")
        with Image.open(path) as im:
            width, height = im.size
            im.verify()
        rows.append({"query_id": str(item.id), "image_path": str(path.resolve()),
                     "image_sha256": item.file_sha256, "width": width, "height": height})
    # The VLM runner sees only these five columns. It cannot import outcome data.
    manifest = OUT / "annotation_input_manifest.jsonl"
    write_once(manifest, "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows).encode())
    with np.load(TECHNICAL, allow_pickle=False) as z:
        if z["query_ids"].tolist() != source.id.tolist() or z["features"].shape[0] != 1184:
            raise RuntimeError("Existing technical features do not align with query order")
        feature_names = z["feature_names"].tolist()
    json_once(OUT / "input_receipt.json", {
        "created_at": now(), "code_revision": revision(), "schema_version": SCHEMA_VERSION,
        "source_query_manifest": str(QUERY), "source_query_manifest_sha256": sha256(QUERY),
        "annotation_manifest_sha256": sha256(manifest), "query_count": len(rows),
        "unique_query_count": len({r["query_id"] for r in rows}),
        "image_hashes_verified": True, "technical_feature_cache": str(TECHNICAL),
        "technical_feature_cache_sha256": sha256(TECHNICAL), "technical_feature_names": feature_names,
        "annotation_manifest_columns": list(rows[0]), "production_guard": guard,
        "outcome_columns_present": False,
    })
    print(json.dumps({"queries": len(rows), "manifest_sha256": sha256(manifest)}))


if __name__ == "__main__":
    run()
