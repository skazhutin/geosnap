"""Resumable, outcome-blind SigLIP2 annotation of the fixed 112,163-image gallery."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from huggingface_hub import snapshot_download
from PIL import Image
from transformers import AutoModel, AutoProcessor

from ml.research.run_dashboard.publish import publish

from .common import revision, sha256, verify_production
from .extract_semantic_probe import MODELS
from .score_semantic_prompts import POSITIVE_COUNT, PROMPTS, PROMPT_VERSION

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/geolocatability_siglip2_gallery_v1_20260929"
HEAD = OUT / "quality_head.npz"
RECEIPT = OUT / "quality_head_receipt.json"
GALLERY = ROOT / "data/evaluation/geographic_v8_20260928/staged/gallery.parquet"
EXPECTED_GALLERY_SHA256 = "8d92a0bdf3492c4a73ce4032ab0966b3380794ab2860d2d68c8cfd5d7b9d094a"
CHUNKS = OUT / "chunks"
RUN_ID = "siglip2-gallery-v1"


def encode_output(value: object) -> torch.Tensor:
    return value.pooler_output if hasattr(value, "pooler_output") else value


def image_from_file(row: dict) -> tuple[Image.Image | None, str | None]:
    try:
        data = Path(row["image_path"]).read_bytes()
        if hashlib.sha256(data).hexdigest() != row["file_sha256"]:
            return None, "source_sha256_mismatch"
        with Image.open(io.BytesIO(data)) as opened:
            return opened.convert("RGB"), None
    except (OSError, ValueError) as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:160]}"


def load_frozen_head() -> dict[str, np.ndarray]:
    metadata = json.loads(RECEIPT.read_text())
    if sha256(HEAD) != metadata["head_sha256"]:
        raise RuntimeError("Frozen quality head changed")
    with np.load(HEAD) as values:
        head = {name: values[name].copy() for name in values.files}
    if head["ridge_coef"].shape != (768,):
        raise RuntimeError("Unexpected quality-head dimensions")
    return head


def load_model_and_text() -> tuple[object, object, np.ndarray]:
    spec = MODELS["siglip2_base"]
    snapshot = Path(snapshot_download(repo_id=spec["repo"], revision=spec["revision"],
                                      local_files_only=True,
                                      allow_patterns=["*.json", "*.safetensors", "*.txt",
                                                      "*.model", "*.jinja", "README.md"]))
    if sha256(snapshot / spec["weight_file"]) != spec["weight_sha256"]:
        raise RuntimeError("SigLIP2 checkpoint hash changed")
    processor = AutoProcessor.from_pretrained(snapshot, local_files_only=True)
    model = AutoModel.from_pretrained(snapshot, local_files_only=True,
                                      use_safetensors=True).to("mps").eval()
    tokens = processor(text=PROMPTS, padding="max_length", return_tensors="pt")
    tokens = {name: value.to("mps") for name, value in tokens.items()}
    with torch.inference_mode():
        text = encode_output(model.get_text_features(**tokens)).float().cpu().numpy()
    assert text.shape == (len(PROMPTS), 768) and np.isfinite(text).all()
    text /= np.linalg.norm(text, axis=1, keepdims=True).clip(1e-10)
    return model, processor, text


def score_features(vectors: np.ndarray, head: dict[str, np.ndarray],
                   text: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    assert vectors.ndim == 2 and vectors.shape[1] == 768 and np.isfinite(vectors).all()
    clean = np.where(np.isfinite(vectors), vectors, head["imputer_median"])
    scaled = (clean - head["scaler_mean"]) / head["scaler_scale"]
    semantic = np.clip(scaled @ head["ridge_coef"] + head["ridge_intercept"], 0, 1)
    normalized = vectors / np.linalg.norm(vectors, axis=1, keepdims=True).clip(1e-10)
    similarities = normalized @ text.T
    contrast = similarities[:, :POSITIVE_COUNT].mean(axis=1) - \
        similarities[:, POSITIVE_COUNT:].mean(axis=1)
    assert np.isfinite(semantic).all() and np.isfinite(contrast).all()
    return semantic, contrast


def encode_chunk(rows: list[dict], *, model: object, processor: object,
                 text: np.ndarray, head: dict[str, np.ndarray],
                 batch_size: int, executor: ThreadPoolExecutor,
                 sentinels: list[Path]) -> tuple[np.ndarray, list[dict], float]:
    started = time.monotonic()
    features = np.full((len(rows), 768), np.nan, dtype=np.float32)
    output = [{"id": row["id"], "source": row["source"],
               "source_file_sha256": row["file_sha256"], "status": "pending",
               "geolocatability_teacher_estimate": None,
               "semantic_prompt_contrast": None, "error": None} for row in rows]
    for offset in range(0, len(rows), batch_size):
        batch = rows[offset:offset + batch_size]
        loaded = list(executor.map(image_from_file, batch))
        valid_images, valid_positions = [], []
        for index, (image, error) in enumerate(loaded):
            position = offset + index
            if error is not None:
                output[position]["status"] = "failed"
                output[position]["error"] = error
            else:
                valid_images.append(image)
                valid_positions.append(position)
        if any(item["error"] and "No such file" in item["error"] for item in output[offset:offset + len(batch)]):
            if not all(path.is_file() for path in sentinels):
                raise RuntimeError("Gallery drive became unavailable; aborting before checkpoint")
        if not valid_images:
            continue
        try:
            inputs = processor(images=valid_images, return_tensors="pt")
            inputs = {name: value.to("mps") for name, value in inputs.items()}
            with torch.inference_mode():
                encoded = encode_output(model.get_image_features(**inputs))
                torch.mps.synchronize()
            values = encoded.float().cpu().numpy()
            if values.shape != (len(valid_images), 768) or not np.isfinite(values).all():
                raise RuntimeError("Non-finite or wrong-size image representation")
            semantic, contrast = score_features(values, head, text)
            for local, position in enumerate(valid_positions):
                features[position] = values[local]
                output[position]["status"] = "ok"
                output[position]["geolocatability_teacher_estimate"] = round(float(semantic[local]), 6)
                output[position]["semantic_prompt_contrast"] = round(float(contrast[local]), 7)
        finally:
            for image in valid_images:
                image.close()
    assert all(item["status"] in {"ok", "failed"} for item in output)
    return features, output, time.monotonic() - started


def verify_chunk(path: Path, expected_rows: list[dict], gallery_sha: str,
                 head_sha: str) -> dict:
    receipt = json.loads((path / "receipt.json").read_text())
    assert receipt["gallery_manifest_sha256"] == gallery_sha
    assert receipt["quality_head_sha256"] == head_sha
    assert receipt["model_revision"] == MODELS["siglip2_base"]["revision"]
    assert sha256(path / "features.npz") == receipt["features_sha256"]
    assert sha256(path / "scores.jsonl") == receipt["scores_sha256"]
    with np.load(path / "features.npz") as features:
        assert features["id"].tolist() == [row["id"] for row in expected_rows]
        assert features["feature"].shape == (len(expected_rows), 768)
    return receipt


def commit_chunk(start: int, gallery_sha: str, head_sha: str, rows: list[dict],
                 features: np.ndarray, scores: list[dict], elapsed: float,
                 batch_size: int, workers: int) -> dict:
    CHUNKS.mkdir(parents=True, exist_ok=True)
    target = CHUNKS / f"{start:06d}-{start + len(rows) - 1:06d}"
    if target.exists():
        raise FileExistsError(target)
    temporary = CHUNKS / f".pending-{start:06d}-{os.getpid()}-{uuid.uuid4().hex}"
    temporary.mkdir()
    with (temporary / "features.npz").open("xb") as output:
        np.savez_compressed(output, id=np.asarray([row["id"] for row in rows]),
                            feature=features)
        output.flush()
        os.fsync(output.fileno())
    with (temporary / "scores.jsonl").open("x") as output:
        for item in scores:
            output.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
        output.flush()
        os.fsync(output.fileno())
    receipt = {"start": start, "end_exclusive": start + len(rows),
               "rows": len(rows), "valid": sum(item["status"] == "ok" for item in scores),
               "failed": sum(item["status"] == "failed" for item in scores),
               "elapsed_s": round(elapsed, 4), "batch_size": batch_size,
               "decode_workers": workers, "model_revision": MODELS["siglip2_base"]["revision"],
               "model_weight_sha256": MODELS["siglip2_base"]["weight_sha256"],
               "gallery_manifest_sha256": gallery_sha, "quality_head_sha256": head_sha,
               "semantic_prompt_version": PROMPT_VERSION,
               "features_sha256": sha256(temporary / "features.npz"),
               "scores_sha256": sha256(temporary / "scores.jsonl"),
               "source_image_sha256_checked_for_decoded_rows": True,
               "localization_outcomes_used": False}
    with (temporary / "receipt.json").open("x") as output:
        json.dump(receipt, output, ensure_ascii=False, sort_keys=True, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    os.rename(temporary, target)
    verify_chunk(target, rows, gallery_sha, head_sha)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk-size", type=int, default=256)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--decode-workers", type=int, default=4)
    parser.add_argument("--max-new-chunks", type=int, default=0,
                        help="0 means finish the whole gallery; 2 is a resumable pilot")
    args = parser.parse_args()
    if args.chunk_size <= 0 or args.chunk_size % args.batch_size:
        raise ValueError("chunk-size must be positive and divide by batch-size")
    if args.decode_workers < 1:
        raise ValueError("decode-workers must be positive")
    production = verify_production()
    gallery_sha = sha256(GALLERY)
    if gallery_sha != EXPECTED_GALLERY_SHA256:
        raise RuntimeError("Fixed gallery manifest changed")
    frame = pd.read_parquet(GALLERY, columns=["id", "source", "image_path", "file_sha256"])
    assert len(frame) == frame.id.nunique() == 112163
    rows = frame.to_dict("records")
    sentinels = [Path(rows[position]["image_path"]) for position in (0, len(rows)//2, len(rows)-1)]
    if not all(path.is_file() for path in sentinels):
        raise RuntimeError("Gallery drive is not available")
    head_metadata = json.loads(RECEIPT.read_text())
    head = load_frozen_head()
    head_sha = head_metadata["head_sha256"]
    completed, durations, failed = 0, [], 0
    for start in range(0, len(rows), args.chunk_size):
        subset = rows[start:start + args.chunk_size]
        target = CHUNKS / f"{start:06d}-{start + len(subset) - 1:06d}"
        if not target.exists():
            break
        receipt = verify_chunk(target, subset, gallery_sha, head_sha)
        completed += receipt["rows"]
        failed += receipt["failed"]
        durations.append(receipt["elapsed_s"])
    else:
        print(json.dumps({"complete": True, "completed": completed, "failed": failed}), flush=True)
        return
    model, processor, text = load_model_and_text()
    new_chunks = 0
    with ThreadPoolExecutor(max_workers=args.decode_workers) as executor:
        for start in range(completed, len(rows), args.chunk_size):
            subset = rows[start:start + args.chunk_size]
            target = CHUNKS / f"{start:06d}-{start + len(subset) - 1:06d}"
            if target.exists():
                receipt = verify_chunk(target, subset, gallery_sha, head_sha)
            else:
                if not all(path.is_file() for path in sentinels):
                    raise RuntimeError("Gallery drive is no longer available")
                features, scores, elapsed = encode_chunk(
                    subset, model=model, processor=processor, text=text, head=head,
                    batch_size=args.batch_size, executor=executor, sentinels=sentinels)
                receipt = commit_chunk(start, gallery_sha, head_sha, subset,
                                       features, scores, elapsed, args.batch_size,
                                       args.decode_workers)
                new_chunks += 1
            completed += receipt["rows"]
            failed += receipt["failed"]
            durations.append(receipt["elapsed_s"])
            recent = durations[-min(8, len(durations)):]
            rate = args.chunk_size / (sum(recent) / len(recent))
            state = "complete" if completed == len(rows) else "running"
            publish(RUN_ID, title="SigLIP2 — 112 163 снимка", phase="Семантическая оценка галереи",
                    completed=completed, total=len(rows), state=state, unit="снимков",
                    rate_per_second=round(rate, 2),
                    eta_seconds=round((len(rows)-completed)/rate),
                    note=f"Ошибок: {failed}; только исследование, галерея не меняется")
            print(json.dumps({"completed": completed, "total": len(rows),
                              "failed": failed, "chunk_seconds": receipt["elapsed_s"],
                              "rate_recent": round(rate, 2),
                              "eta_hours": round((len(rows)-completed)/rate/3600, 2)}), flush=True)
            if args.max_new_chunks and new_chunks >= args.max_new_chunks:
                break
    verify_production()
    print(json.dumps({"process_complete": completed == len(rows),
                      "completed": completed, "failed": failed,
                      "new_chunks": new_chunks,
                      "production_guarded_files": production["guarded_files"]}), flush=True)


if __name__ == "__main__":
    main()
