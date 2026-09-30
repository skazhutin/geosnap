"""Resumable verified local mirror of fixed inputs to avoid FUSE mmap faults."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pandas as pd

from ml.research import gallery_scale
from ml.research.gallery_scale_storage import digest, save
from ml.research.geographic_v8.common import GALLERY, LOCAL, NIGHT, QUERY, initialize

STAGE = LOCAL / "staged"


def copy_verified(source: Path, destination: Path):
    destination.parent.mkdir(parents=True, exist_ok=True)
    receipt = destination.with_name(destination.name + ".sha256.json")
    if receipt.exists():
        recorded = json.loads(receipt.read_text())
        if (recorded["source"] != str(source) or recorded["source_size"] != source.stat().st_size
                or digest(destination) != recorded["sha256"]):
            raise RuntimeError(f"Staged input changed: {destination}")
        return recorded["sha256"]
    h = hashlib.sha256()
    temp = destination.with_name(destination.name + ".writing")
    with source.open("rb") as src, temp.open("wb") as dst:
        while block := src.read(4 * 1024 * 1024):
            h.update(block)
            dst.write(block)
        dst.flush()
        os.fsync(dst.fileno())
    if temp.stat().st_size != source.stat().st_size or digest(temp) != h.hexdigest():
        raise RuntimeError(f"Staged copy failed byte/hash verification: {source}")
    os.replace(temp, destination)
    record = {"source": str(source), "source_size": source.stat().st_size, "sha256": h.hexdigest()}
    save(receipt, record)
    return record["sha256"]


def run():
    initialize()
    STAGE.mkdir(exist_ok=True)
    inputs = [(GALLERY, STAGE / "gallery.parquet"), (QUERY, STAGE / "development.parquet")]
    source = GALLERY.parent
    inputs += [(source / name, STAGE / name) for name in
               ("mean_scores.npy", "context_evidence.npz", "mean_rows.json", "context_rows.json", "descriptor_pool.json")]
    old_query = gallery_scale.V5 / "development/global_sage-vitl_new/queries.npy"
    inputs.append((old_query, STAGE / "query322.npy"))
    inputs += [(p, STAGE / "query504" / p.name) for p in sorted((NIGHT / "query504").glob("chunk-*.npz"))]
    receipt = {}
    for i, (src, dst) in enumerate(inputs):
        receipt[str(src)] = {"path": str(dst), "sha256": copy_verified(src, dst)}
        print(f"Staged fixed metadata/query {i+1}/{len(inputs)}: {src.name}", flush=True)
    gallery = pd.read_parquet(STAGE / "gallery.parquet", columns=["id"])
    entries = json.loads((STAGE / "descriptor_pool.json").read_text())["entries"]
    if len(gallery) != 112163:
        raise RuntimeError("Wrong gallery in local mirror")
    paths = sorted(set(entries[k]["path"] for k in gallery.id))
    mapping = {}
    for i, value in enumerate(paths):
        src = Path(value)
        # Keep fixed manifest/entry identities; remap only the physical file path.
        dst = STAGE / "descriptors" / f"{i:04d}.npy"
        sha = copy_verified(src, dst)
        mapping[value] = {"path": str(dst), "sha256": sha}
        if i % 25 == 0 or i + 1 == len(paths):
            print(f"Staged descriptor shards {i+1}/{len(paths)}", flush=True)
    save(STAGE / "paths.json", mapping)
    save(STAGE / "complete.json", {"fixed_inputs": receipt, "shards": len(mapping),
         "mapping_sha256": digest(STAGE / "paths.json"), "source_gallery_sha256": digest(GALLERY),
         "source_query_sha256": digest(QUERY), "no_model_inference": True})
    print("Verified local mirror complete", flush=True)


if __name__ == "__main__":
    run()
