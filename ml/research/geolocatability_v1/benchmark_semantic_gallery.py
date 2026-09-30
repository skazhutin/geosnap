"""Mounted-disk throughput of pinned semantic image encoders on the fixed gallery."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from huggingface_hub import snapshot_download
from PIL import Image

from .common import json_once, sha256
from .extract_semantic_probe import MODELS, OUT, load_encoder

ROOT = Path(__file__).resolve().parents[3]
GALLERY = ROOT / "data/evaluation/geographic_v8_20260928/staged/gallery.parquet"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, choices=MODELS)
    parser.add_argument("--count", type=int, default=256)
    parser.add_argument("--batch-size", type=int, default=16)
    args = parser.parse_args()
    if args.count < 4 * args.batch_size or args.count % args.batch_size:
        raise ValueError("count must be at least four complete batches")
    gallery = pd.read_parquet(GALLERY, columns=["image_path"])
    assert len(gallery) == 112163
    indices = np.linspace(0, len(gallery) - 1, args.count, dtype=int)
    selected = gallery.image_path.iloc[indices].tolist()
    if not all(Path(path).is_file() for path in selected):
        raise RuntimeError("Mounted gallery images unavailable")
    spec = MODELS[args.model]
    snapshot = Path(snapshot_download(repo_id=spec["repo"], revision=spec["revision"],
                                      local_files_only=True,
                                      allow_patterns=["*.json", "*.safetensors", "*.txt",
                                                      "*.model", "*.jinja", "README.md"]))
    if sha256(snapshot / spec["weight_file"]) != spec["weight_sha256"]:
        raise RuntimeError("Checkpoint hash mismatch")
    encoder = load_encoder(args.model, snapshot)
    batch_times = []
    with torch.inference_mode():
        for start in range(0, len(selected), args.batch_size):
            t0 = time.perf_counter()
            images = []
            for path in selected[start:start + args.batch_size]:
                with Image.open(path) as image:
                    images.append(image.convert("RGB"))
            vectors = encoder(images)
            torch.mps.synchronize()
            if hasattr(vectors, "pooler_output"):
                vectors = vectors.pooler_output
            assert vectors.shape[0] == len(images)
            batch_times.append(time.perf_counter() - t0)
            if start % (args.batch_size * 8) == 0:
                print(json.dumps({"model": args.model, "done": start + len(images),
                                  "batch_s": round(batch_times[-1], 3)}), flush=True)
    steady_time = sum(batch_times[1:])
    steady_n = args.count - args.batch_size
    speed = steady_n / steady_time
    result = {"model": args.model, **spec,
              "gallery_manifest_sha256": sha256(GALLERY),
              "sample_rule": f"{args.count} evenly spaced rows of fixed 112163 gallery",
              "count": args.count, "batch_size": args.batch_size,
              "device": "mps", "torch_version": torch.__version__,
              "batch_seconds": [round(value, 6) for value in batch_times],
              "steady_images_per_second": round(speed, 4),
              "steady_seconds_per_image": round(1 / speed, 6),
              "projected_112163_hours": round(112163 / speed / 3600, 3),
              "first_half_steady_batch_s": round(float(np.mean(batch_times[1:1 + (len(batch_times) - 1)//2])), 3),
              "second_half_steady_batch_s": round(float(np.mean(batch_times[1 + (len(batch_times) - 1)//2:])), 3),
              "outcome_data_accessed": False,
              "limitation": "Short throughput probe is not a full six-hour endurance measurement"}
    destination = OUT / f"{args.model}_gallery_speed_{args.count}_b{args.batch_size}.json"
    json_once(destination, result)
    print(json.dumps({k: value for k, value in result.items() if k != "batch_seconds"},
                     ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
