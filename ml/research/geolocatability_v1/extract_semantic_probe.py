"""Outcome-blind cached query features for two frozen semantic image encoders."""
from __future__ import annotations

import argparse
import io
import json
import time
from pathlib import Path

import numpy as np
import torch
from huggingface_hub import snapshot_download
from PIL import Image

from .common import json_once, revision, sha256, write_once

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/geolocatability_fast_models_v1_20260929"
INPUT = ROOT / "data/evaluation/geolocatability_v1_20260928/vlm_input_manifest_1024.jsonl"
CHUNK_SIZE = 32
MODELS = {
    "siglip2_base": {
        "repo": "google/siglip2-base-patch16-224",
        "revision": "75de2d55ec2d0b4efc50b3e9ad70dba96a7b2fa2",
        "weight_file": "model.safetensors",
        "weight_sha256": "612923381c76ec5a9bed335d1c48827e3f2e506ac31b044b63b2031fadee6a0b",
        "license": "apache-2.0",
    },
    "mobileclip2_s0": {
        "repo": "timm/MobileCLIP2-S0-OpenCLIP",
        "revision": "095906d28bf54d7584dc411e8ffe448f34289e05",
        "weight_file": "open_clip_model.safetensors",
        "weight_sha256": "ab91a1a0c4330d6b1913e24d5035dfdea15423316aaec649610c6b1c6ddd0e95",
        "license": "apple-amlr; research-only, not production-suitable",
    },
}


def load_encoder(name: str, snapshot: Path):
    if name == "siglip2_base":
        from transformers import AutoModel, AutoProcessor

        processor = AutoProcessor.from_pretrained(snapshot, local_files_only=True)
        model = AutoModel.from_pretrained(snapshot, local_files_only=True,
                                          use_safetensors=True).to("mps").eval()

        def encode(images):
            inputs = processor(images=images, return_tensors="pt")
            inputs = {key: value.to("mps") for key, value in inputs.items()}
            output = model.get_image_features(**inputs)
            return output.pooler_output if hasattr(output, "pooler_output") else output

        return encode
    if name == "mobileclip2_s0":
        import open_clip

        model, _, processor = open_clip.create_model_and_transforms(
            "MobileCLIP2-S0", pretrained=str(snapshot / "open_clip_model.safetensors"))
        model = model.to("mps").eval()

        def encode(images):
            pixels = torch.stack([processor(image) for image in images]).to("mps")
            return model.encode_image(pixels)

        return encode
    raise ValueError(name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, choices=MODELS)
    parser.add_argument("--limit", type=int, default=1184,
                        help="Multiple of 32; use 128 for a preliminary speed probe")
    parser.add_argument("--batch-size", type=int, default=16)
    args = parser.parse_args()
    rows = [json.loads(line) for line in INPUT.read_text().splitlines()]
    assert len(rows) == len({row["query_id"] for row in rows}) == 1184
    if args.limit <= 0 or args.limit > len(rows) or args.limit % CHUNK_SIZE:
        raise ValueError("limit must be a positive multiple of 32, at most 1184")
    if CHUNK_SIZE % args.batch_size:
        raise ValueError("batch size must divide 32")
    spec = MODELS[args.model]
    snapshot = Path(snapshot_download(repo_id=spec["repo"], revision=spec["revision"],
                                      local_files_only=True,
                                      allow_patterns=["*.json", "*.safetensors", "*.txt",
                                                      "*.model", "*.jinja", "README.md"]))
    if sha256(snapshot / spec["weight_file"]) != spec["weight_sha256"]:
        raise RuntimeError("Checkpoint hash mismatch")
    output = OUT / "semantic_features" / args.model
    output.mkdir(parents=True, exist_ok=True)
    input_sha = sha256(INPUT)
    existing = [start for start in range(0, args.limit, CHUNK_SIZE)
                if not (output / f"{start:04d}-{start + CHUNK_SIZE - 1:04d}.npz").exists()]
    encoder = load_encoder(args.model, snapshot) if existing else None
    with torch.inference_mode():
        for start in range(0, args.limit, CHUNK_SIZE):
            end = start + CHUNK_SIZE
            feature_path = output / f"{start:04d}-{end - 1:04d}.npz"
            receipt_path = output / f"{start:04d}-{end - 1:04d}.json"
            if feature_path.exists():
                if not receipt_path.exists():
                    raise RuntimeError(f"Feature chunk exists without timing receipt: {feature_path}")
                receipt = json.loads(receipt_path.read_text())
                if receipt["feature_sha256"] != sha256(feature_path):
                    raise RuntimeError(f"Feature chunk changed: {feature_path}")
                continue
            features = []
            batch_seconds = []
            for offset in range(start, end, args.batch_size):
                selected = rows[offset:offset + args.batch_size]
                t0 = time.perf_counter()
                images = []
                for row in selected:
                    with Image.open(row["vlm_image_path"]) as image:
                        images.append(image.convert("RGB"))
                vectors = encoder(images)
                torch.mps.synchronize()
                values = vectors.float().cpu().numpy()
                assert values.shape[0] == len(images) and np.isfinite(values).all()
                features.append(values)
                batch_seconds.append(time.perf_counter() - t0)
            array = np.concatenate(features, axis=0)
            assert array.shape[0] == CHUNK_SIZE
            buffer = io.BytesIO()
            np.savez_compressed(buffer, query_id=np.asarray([r["query_id"] for r in rows[start:end]]),
                                feature=array)
            write_once(feature_path, buffer.getvalue())
            json_once(receipt_path, {"model": args.model, **spec, "code_revision": revision(),
                                     "source_script_sha256": sha256(Path(__file__)),
                                     "input_manifest_sha256": input_sha,
                                     "feature_sha256": sha256(feature_path),
                                     "start": start, "end_exclusive": end,
                                     "feature_dimensions": array.shape[1],
                                     "batch_size": args.batch_size,
                                     "batch_seconds": [round(value, 6) for value in batch_seconds],
                                     "outcome_data_accessed": False})
            print(json.dumps({"model": args.model, "done": end,
                              "images_per_second": round(CHUNK_SIZE / sum(batch_seconds), 3)}), flush=True)
    if args.limit == len(rows):
        chunks = []
        for start in range(0, len(rows), CHUNK_SIZE):
            data = np.load(output / f"{start:04d}-{start + CHUNK_SIZE - 1:04d}.npz")
            expected = [r["query_id"] for r in rows[start:start + CHUNK_SIZE]]
            assert data["query_id"].tolist() == expected
            chunks.append(data["feature"])
        values = np.concatenate(chunks, axis=0)
        destination = OUT / f"query_features_{args.model}.npz"
        if not destination.exists():
            buffer = io.BytesIO()
            np.savez_compressed(buffer, query_id=np.asarray([r["query_id"] for r in rows]),
                                feature=values)
            write_once(destination, buffer.getvalue())
        print(json.dumps({"complete": True, "shape": values.shape,
                          "feature_sha256": sha256(destination)}), flush=True)


if __name__ == "__main__":
    main()
