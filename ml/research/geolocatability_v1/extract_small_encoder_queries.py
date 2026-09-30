"""Cache frozen DINOv2-S features for the outcome-blind 1,184 query images."""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import numpy as np
import torch
from PIL import Image
from transformers import AutoImageProcessor, AutoModel

ROOT = Path(__file__).resolve().parents[3]
INPUT = ROOT / "data/evaluation/geolocatability_v1_20260928/vlm_input_manifest_1024.jsonl"
OUT = ROOT / "data/evaluation/geolocatability_speedpilot_20260929"
MODEL = OUT / "model_dinov2_small"
WEIGHT_SHA256 = "ae1e99fcefd534ed978cdeb8326f08030c96e28b7a81ffcbc98a857c84d14be1"


def main() -> None:
    assert hashlib.sha256((MODEL / "model.safetensors").read_bytes()).hexdigest() == WEIGHT_SHA256
    rows = [json.loads(line) for line in INPUT.read_text().splitlines()]
    ids = [row["query_id"] for row in rows]
    assert len(ids) == len(set(ids)) == 1184
    processor = AutoImageProcessor.from_pretrained(MODEL)
    model = AutoModel.from_pretrained(MODEL, use_safetensors=True).to("mps").eval()
    vectors = []
    start = time.monotonic()
    with torch.inference_mode():
        for offset in range(0, len(rows), 16):
            images = []
            for row in rows[offset:offset + 16]:
                with Image.open(row["vlm_image_path"]) as image:
                    images.append(image.convert("RGB"))
            pixels = processor(images=images, return_tensors="pt")["pixel_values"].to("mps")
            representation = model(pixel_values=pixels).last_hidden_state[:, 0]
            vectors.append(representation.float().cpu().numpy())
            if (offset // 16 + 1) % 10 == 0:
                print(json.dumps({"done": min(offset + 16, len(rows)),
                                  "elapsed_s": round(time.monotonic() - start, 1)}), flush=True)
    features = np.concatenate(vectors, axis=0)
    assert features.shape == (1184, 384) and np.isfinite(features).all()
    target = OUT / "query_features_dinov2_small.npz"
    temporary = OUT / "query_features_dinov2_small.tmp.npz"
    np.savez_compressed(temporary, query_id=np.asarray(ids), feature=features)
    temporary.replace(target)
    receipt = {"model": "facebook/dinov2-small", "revision": "ed25f3a31f01632728cabb09d1542f84ab7b0056",
               "weight_sha256": WEIGHT_SHA256, "input_manifest_sha256": hashlib.sha256(INPUT.read_bytes()).hexdigest(),
               "features_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
               "n": 1184, "dimensions": 384, "elapsed_s": round(time.monotonic() - start, 3),
               "outcome_data_accessed": False}
    (OUT / "query_features_receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2), flush=True)


if __name__ == "__main__":
    main()
