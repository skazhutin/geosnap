"""Build a versioned exact SAGE-L index from verified cached descriptors.

No query images, query coordinates, or localization outcomes enter this build.
The old frozen production index and configuration are left untouched.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

from ml.indexing.faiss_index import FaissExactIndexBuilder
from ml.retrieval.sage import SageVitLRetriever

ROOT = Path(__file__).resolve().parents[1]
FILTERED = ROOT / "data/evaluation/gallery_quality_filter_v3_20260929/gallery_filtered.parquet"
POOL = ROOT / "data/evaluation/geographic_v8_20260928/staged/descriptor_pool.json"
PATHS = ROOT / "data/evaluation/geographic_v8_20260928/staged/paths.json"
EXPECTED_GALLERY_SHA256 = "fa24d16e387ea939536fe2760fe1615327797ad665602421d54e61b473ace39c"


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def build(output: Path) -> None:
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"refusing to overwrite an existing candidate: {output}")
    if digest(FILTERED) != EXPECTED_GALLERY_SHA256:
        raise RuntimeError("cleaned gallery manifest changed")
    gallery = pd.read_parquet(FILTERED)
    if len(gallery) != 111_032 or gallery.id.duplicated().any():
        raise RuntimeError("cleaned gallery IDs/count changed")
    pool = json.loads(POOL.read_text())["entries"]
    paths = json.loads(PATHS.read_text())
    selected = []
    used_sources = set()
    for row in gallery.itertuples(index=False):
        entry = pool.get(row.id)
        if entry is None or entry["image_sha256"] != row.file_sha256:
            raise RuntimeError(f"descriptor/image identity mismatch: {row.id}")
        source = entry["path"]
        if source not in paths:
            raise RuntimeError(f"missing staged descriptor source: {source}")
        selected.append((Path(paths[source]["path"]), int(entry["row"])))
        used_sources.add(source)
    for source in sorted(used_sources):
        item = paths[source]
        if digest(Path(item["path"])) != item["sha256"]:
            raise RuntimeError(f"staged descriptor shard changed: {source}")
    model = SageVitLRetriever()
    builder = FaissExactIndexBuilder(
        descriptor_dim=8448,
        expected_size=len(gallery),
        retriever_metadata=model.metadata.to_dict(),
        index_id="moscow_clean_v5_sage_l_111032",
        city_id="moscow",
        extra_metadata={
            "source_gallery_sha256": EXPECTED_GALLERY_SHA256,
            "source_pool_sha256": digest(POOL),
            "source_paths_sha256": digest(PATHS),
            "gallery_scope": "MSLS research-only images included at user request",
        },
    )
    for start in range(0, len(gallery), 512):
        stop = min(start + 512, len(gallery))
        batch = gallery.iloc[start:stop]
        vectors = np.empty((len(batch), 8448), dtype=np.float32)
        by_file = defaultdict(list)
        for offset, (filename, source_row) in enumerate(selected[start:stop]):
            by_file[filename].append((offset, source_row))
        for filename, assignments in by_file.items():
            shard = np.load(filename, mmap_mode="r", allow_pickle=False)
            positions, rows = np.asarray(assignments, dtype=np.int64).T
            vectors[positions] = shard[rows]
            del shard
        if not np.isfinite(vectors).all():
            raise RuntimeError(f"non-finite descriptor batch at {start}")
        metadata = [
            {
                "id": str(row.id), "source": str(row.source),
                "lat": float(row.lat), "lon": float(row.lon),
                "sequence_id": str(row.sequence_id),
                "heading": None if pd.isna(row.heading) else float(row.heading),
                "attribution": str(row.attribution), "license": str(row.license),
                "source_url": str(row.source_url),
            }
            for row in batch.itertuples(index=False)
        ]
        builder.add_batch(vectors, batch.id.tolist(), reference_metadata=metadata)
        if start % 8192 == 0:
            print(f"indexed {stop}/{len(gallery)}", flush=True)
    index = builder.finish()
    index.save(output)
    receipt = {
        "gallery_count": len(gallery),
        "gallery_sha256": EXPECTED_GALLERY_SHA256,
        "pool_sha256": digest(POOL), "paths_sha256": digest(PATHS),
        "index_id": index.build_metadata["index_id"],
        "index_artifact_sha256": index.build_metadata["artifact_sha256"],
    }
    (output / "build_receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    build(parser.parse_args().output)
