"""Build audited EDTformer descriptors without changing the production registry."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from pathlib import Path

import torch

from ml.research.edtformer import EDTformerRetriever
from ml.retrieval.embedding_job import EmbeddingJob, _read_manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.manifest.name != "gallery.parquet" or args.manifest.parent.name != "moscow_real_v4":
        raise ValueError("this exploratory job is restricted to the exact production gallery")
    torch.set_num_threads(2)
    logging.basicConfig(level=logging.INFO)
    model = EDTformerRetriever(device="mps", allow_device_fallback=False, batch_size=12)
    artifacts = EmbeddingJob(model, args.output, batch_size=48).run(_read_manifest(args.manifest, Path(".")))
    print(json.dumps(asdict(artifacts), default=str, indent=2))


if __name__ == "__main__":
    main()
