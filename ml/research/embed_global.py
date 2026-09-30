"""Resumable experimental gallery embedding, with explicit query exclusion."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import torch

from ml.research.artifact_storage import mirror_artifacts, native_destination
from ml.research.prepare_queries import ROOT
from ml.research.retrievers import research_retriever
from ml.retrieval.embedding_job import EmbeddingJob, ReferenceImage


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--manifest", type=Path, default=Path("data/evaluation/moscow_real_v4/gallery.parquet"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--reuse-from", type=Path)
    parser.add_argument("--batch-size", type=int, default=4)
    args = parser.parse_args()
    permitted = [
        Path("data/evaluation/moscow_real_v4/gallery.parquet"),
        ROOT / "gallery_expansion/gallery_random.parquet",
        ROOT / "gallery_expansion/gallery_diversity.parquet",
        ROOT / "gallery_expansion/audited_union.parquet",
    ]
    if args.manifest.resolve() not in [p.resolve() for p in permitted]:
        raise ValueError("only registered gallery manifests may be embedded")
    args.output = native_destination(args.output)
    os.environ.setdefault("HF_HOME", str(Path(".cache/huggingface").resolve()))
    torch.set_num_threads(2)
    model = research_retriever(
        args.model, device="mps", allow_device_fallback=False, batch_size=args.batch_size, cache_dir=".cache/torch/hub"
    )

    def progress(item):
        if item.processed_inputs % 480 == 0 or item.processed_inputs == item.total_inputs:
            print(f"embedded {item.processed_inputs}/{item.total_inputs}; failures={item.failures}", flush=True)
        reserve = 3 * 1024**3 + item.total_inputs * model.descriptor_dim * 4
        if shutil.disk_usage(args.output).free < reserve:
            raise RuntimeError(
                "insufficient disk headroom; checkpoint retained for resumption after space is available"
            )

    args.output.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(args.output).free < 4 * 1024**3:
        raise RuntimeError("need at least 4 GiB free before starting gallery inference")
    frame = pd.read_parquet(args.manifest)
    if frame[["id", "image_path", "lat", "lon", "file_sha256"]].isna().any().any() or frame.id.duplicated().any():
        raise ValueError("audited references require unique identities, images, coordinates and hashes")
    # Research sampling is decode-only. Production's final-manifest quality-score
    # requirement must not silently filter this independent acquisition arm.
    records = [ReferenceImage.from_mapping(row, image_root=Path(".")) for row in frame.to_dict("records")]
    artifacts = EmbeddingJob(
        model, args.output, batch_size=48, reuse_from=args.reuse_from, progress_callback=progress
    ).run(records)
    model.close()
    if torch.backends.mps.is_available():
        torch.mps.empty_cache()
    print(json.dumps(asdict(artifacts), default=str, indent=2))
    mirror_artifacts(args.output)


if __name__ == "__main__":
    main()
