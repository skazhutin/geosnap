"""Fixed text-prompt scores from semantic image/text models, without outcome labels."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import torch
from huggingface_hub import snapshot_download

from .common import json_once, sha256, write_once
from .extract_semantic_probe import MODELS, OUT

PROMPTS = [
    "A street-level photograph of a wide outdoor scene with buildings, roads, and stable visible landmarks.",
    "A distinctive urban intersection with recognizable architecture, road geometry, signs, or transit infrastructure.",
    "A close-up photograph of a blank wall or nearby object with no visible surroundings.",
    "A severely blurred or obstructed photograph with no recognizable stable scene.",
    "A photograph showing mostly ground or sky instead of the surrounding street.",
    "A generic vegetation-only or featureless road scene with few stable landmarks.",
]
POSITIVE_COUNT = 2
PROMPT_VERSION = "semantic-geolocatability-contrast-v1"


def text_features(name: str, snapshot: Path) -> np.ndarray:
    if name == "siglip2_base":
        from transformers import AutoModel, AutoProcessor

        processor = AutoProcessor.from_pretrained(snapshot, local_files_only=True)
        model = AutoModel.from_pretrained(snapshot, local_files_only=True,
                                          use_safetensors=True).to("mps").eval()
        inputs = processor(text=PROMPTS, padding="max_length", return_tensors="pt")
        inputs = {key: value.to("mps") for key, value in inputs.items()}
        with torch.inference_mode():
            result = model.get_text_features(**inputs)
            if hasattr(result, "pooler_output"):
                result = result.pooler_output
            return result.float().cpu().numpy()
    if name == "mobileclip2_s0":
        import open_clip

        model, _, _ = open_clip.create_model_and_transforms(
            "MobileCLIP2-S0", pretrained=str(snapshot / "open_clip_model.safetensors"))
        model = model.to("mps").eval()
        tokenizer = open_clip.get_tokenizer("MobileCLIP2-S0")
        with torch.inference_mode():
            return model.encode_text(tokenizer(PROMPTS).to("mps")).float().cpu().numpy()
    raise ValueError(name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, choices=MODELS)
    args = parser.parse_args()
    spec = MODELS[args.model]
    feature_path = OUT / f"query_features_{args.model}.npz"
    if not feature_path.exists():
        raise FileNotFoundError(feature_path)
    feature = np.load(feature_path)
    images = feature["feature"].astype(np.float32)
    ids = feature["query_id"].tolist()
    assert len(ids) == len(set(ids)) == len(images) == 1184
    snapshot = Path(snapshot_download(repo_id=spec["repo"], revision=spec["revision"],
                                      local_files_only=True,
                                      allow_patterns=["*.json", "*.safetensors", "*.txt",
                                                      "*.model", "*.jinja", "README.md"]))
    if sha256(snapshot / spec["weight_file"]) != spec["weight_sha256"]:
        raise RuntimeError("Checkpoint hash mismatch")
    texts = text_features(args.model, snapshot)
    assert texts.shape == (len(PROMPTS), images.shape[1])
    assert np.isfinite(texts).all() and np.isfinite(images).all()
    images /= np.linalg.norm(images, axis=1, keepdims=True).clip(1e-10)
    texts /= np.linalg.norm(texts, axis=1, keepdims=True).clip(1e-10)
    similarity = images @ texts.T
    primary = similarity[:, :POSITIVE_COUNT].mean(axis=1) - similarity[:, POSITIVE_COUNT:].mean(axis=1)
    destination = OUT / f"semantic_prompt_scores_{args.model}.jsonl"
    rows = [{"query_id": query_id,
             "similarities": [round(float(score), 7) for score in similarity[index]],
             "positive_minus_negative_mean": round(float(primary[index]), 7)}
            for index, query_id in enumerate(ids)]
    write_once(destination, "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows).encode())
    prompt_hash = hashlib.sha256(json.dumps(PROMPTS, sort_keys=True).encode()).hexdigest()
    json_once(OUT / f"semantic_prompt_scores_{args.model}_receipt.json", {
        "model": args.model, **spec, "prompt_version": PROMPT_VERSION,
        "prompts": PROMPTS, "positive_count": POSITIVE_COUNT,
        "prompt_sha256": prompt_hash, "feature_sha256": sha256(feature_path),
        "scores_sha256": sha256(destination), "n": len(ids), "outcome_data_accessed": False,
        "primary_score": "mean cosine similarity to first two positive prompts minus mean to four negative prompts; no fitted weights",
    })
    print(json.dumps({"model": args.model, "n": len(ids), "scores_sha256": sha256(destination)}), flush=True)


if __name__ == "__main__":
    main()
