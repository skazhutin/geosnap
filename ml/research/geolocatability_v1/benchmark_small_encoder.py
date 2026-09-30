"""Outcome-blind throughput probe for a small frozen image encoder."""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from PIL import Image
from transformers import AutoImageProcessor, AutoModel

ROOT = Path(__file__).resolve().parents[3]
INPUT = ROOT / "data/evaluation/geographic_v8_20260928/staged/gallery.parquet"
OUT = ROOT / "data/evaluation/geolocatability_speedpilot_20260929"
REPO = "facebook/dinov2-small"
REVISION = "ed25f3a31f01632728cabb09d1542f84ab7b0056"
WEIGHT_SHA256 = "ae1e99fcefd534ed978cdeb8326f08030c96e28b7a81ffcbc98a857c84d14be1"
COUNT = int(os.environ.get("GEOSNAP_BENCH_COUNT", "256"))
BATCH_SIZE = 16


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    paths = pd.read_parquet(INPUT, columns=["image_path"]).image_path
    assert len(paths) == 112163
    selected = paths.iloc[np.linspace(0, len(paths) - 1, COUNT, dtype=int)].tolist()
    snapshot = OUT / "model_dinov2_small"
    assert all((snapshot / name).is_file() for name in
               ("config.json", "preprocessor_config.json", "model.safetensors"))
    weight_sha256 = hashlib.sha256((snapshot / "model.safetensors").read_bytes()).hexdigest()
    assert weight_sha256 == WEIGHT_SHA256, "Downloaded model does not match pinned checkpoint"
    processor = AutoImageProcessor.from_pretrained(snapshot)
    model = AutoModel.from_pretrained(snapshot, use_safetensors=True).to("mps").eval()
    times = []
    with torch.inference_mode():
        for start in range(0, len(selected), BATCH_SIZE):
            t0 = time.perf_counter()
            images = []
            for path in selected[start:start + BATCH_SIZE]:
                with Image.open(path) as image:
                    images.append(image.convert("RGB"))
            inputs = processor(images=images, return_tensors="pt")
            inputs = {name: value.to("mps") for name, value in inputs.items()}
            features = model(**inputs).last_hidden_state[:, 0]
            torch.mps.synchronize()
            assert features.shape[0] == len(images)
            times.append(time.perf_counter() - t0)
            print(json.dumps({"processed": min(start + BATCH_SIZE, len(selected)),
                              "batch_s": round(times[-1], 3)}), flush=True)
    steady_s = sum(times[1:])
    steady_images = COUNT - BATCH_SIZE
    steady_batches = times[1:]
    midpoint = len(steady_batches) // 2
    result = {
        "purpose": "outcome-blind frozen-encoder throughput only; no quality model was fitted",
        "model": REPO, "revision": REVISION,
        "weight_sha256": weight_sha256,
        "input_manifest_sha256": hashlib.sha256(INPUT.read_bytes()).hexdigest(),
        "sample": f"{COUNT} equispaced reference images in fixed 112163-row gallery, read from mounted disk",
        "sample_count": COUNT, "batch_size": BATCH_SIZE, "device": "mps",
        "torch_version": torch.__version__,
        "all_elapsed_s": round(sum(times), 3),
        "steady_elapsed_s_excluding_first_batch": round(steady_s, 3),
        "steady_images_per_s": round(steady_images / steady_s, 3),
        "estimated_112163_s_at_steady_rate": round(112163 / (steady_images / steady_s)),
        "batch_times_s": [round(value, 3) for value in times],
        "first_half_steady_batch_s": round(float(np.mean(steady_batches[:midpoint])), 3),
        "second_half_steady_batch_s": round(float(np.mean(steady_batches[midpoint:])), 3),
    }
    target = OUT / f"dinov2_small_m1pro_{COUNT}_b16.json"
    target.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
