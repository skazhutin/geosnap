"""Resumable, outcome-blind prompt-VLM speed probe on frozen query images."""
from __future__ import annotations

import argparse
import json
import re
import statistics
import time
from pathlib import Path

from huggingface_hub import snapshot_download
from mlx_vlm import generate, load
from mlx_vlm.prompt_utils import apply_chat_template

from .common import json_once, now, sha256, write_once

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/geolocatability_fast_models_v1_20260929"
MANIFEST = OUT / "speed32_image_input.jsonl"
PROMPT = (
    "Judge visible pixels only. Geolocation utility: 0=no usable street context, "
    "1=very weak, 2=generic, 3=good stable landmarks, 4=distinctive stable landmarks. "
    "A sharp blank wall scores 0; a slightly blurred intersection can score 3. "
    "Do not guess the location. Reply with exactly one digit 0, 1, 2, 3, or 4."
)
PROMPT_VERSION = "street-geolocatability-ordinal-short-v1"
MODELS = {
    "smolvlm2_256m": {
        "repo": "mlx-community/SmolVLM2-256M-Video-Instruct-mlx",
        "revision": "79901655c1a3d7ed6646c91325e3fde47e40e611",
        "weight_sha256": "e37d0714a5e2db99b9e4c8747028fc6ddf16f87f4a34ac175c82757d3d8e6636",
        "license": "apache-2.0",
    },
    "smolvlm2_500m": {
        "repo": "mlx-community/SmolVLM2-500M-Video-Instruct-mlx",
        "revision": "fa57db46815177fbdfd65cc85a2b3416a8332268",
        "weight_sha256": "a9839c8f79ecc93e54a00dc73cc0e68ba477debcd065d50c1c289fbb1075f981",
        "license": "apache-2.0",
    },
    "lfm25_vl_450m_4bit": {
        "repo": "LiquidAI/LFM2.5-VL-450M-MLX-4bit",
        "revision": "f19926f17a25164d4cbcdc16d9eaf4714b807cfb",
        "weight_sha256": "074084021890e1974f8fff43be6edb9b768d2a52895039fee83d6b8e2385c261",
        "license": "lfm1.0",
    },
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, choices=MODELS)
    args = parser.parse_args()
    spec = MODELS[args.model]
    receipt = json.loads((OUT / "input_receipt.json").read_text())
    if sha256(MANIFEST) != receipt["speed_manifest_sha256"]:
        raise RuntimeError("Frozen speed input changed")
    rows = [json.loads(line) for line in MANIFEST.read_text().splitlines()]
    assert len(rows) == len({r["query_id"] for r in rows}) == 32
    raw_dir = OUT / "prompt_vlm_raw" / args.model
    raw_dir.mkdir(parents=True, exist_ok=True)
    completed = {r["query_id"] for r in rows if (raw_dir / f"{r['query_id']}.json").exists()}
    if completed != {r["query_id"] for r in rows}:
        snapshot = Path(snapshot_download(repo_id=spec["repo"], revision=spec["revision"],
                                          local_files_only=True))
        if sha256(snapshot / "model.safetensors") != spec["weight_sha256"]:
            raise RuntimeError("Checkpoint hash mismatch")
        model, processor = load(str(snapshot))
        formatted = apply_chat_template(processor, model.config, PROMPT, num_images=1)
        for index, row in enumerate(rows):
            destination = raw_dir / f"{row['query_id']}.json"
            if destination.exists():
                continue
            if sha256(Path(row["vlm_image_path"])) != row["vlm_image_sha256"]:
                raise RuntimeError(f"Input image changed: {row['query_id']}")
            start = time.monotonic()
            try:
                answer = generate(model, processor, formatted, image=row["vlm_image_path"],
                                  max_tokens=8, temperature=0.0, verbose=False).text
                exact = re.fullmatch(r"\s*[0-4]\s*", answer)
                digits = re.findall(r"(?<!\d)[0-4](?!\d)", answer)
                result = {"status": "ok", "raw_answer": answer,
                          "format_compliant": bool(exact),
                          "parsed_score": int(digits[0]) if len(digits) == 1 else None}
            except Exception as exc:
                result = {"status": "error", "error_type": type(exc).__name__,
                          "error_message": str(exc)[:500]}
            result.update({"query_id": row["query_id"], "image_sha256": row["vlm_image_sha256"],
                           "model_repo": spec["repo"], "model_revision": spec["revision"],
                           "model_weight_sha256": spec["weight_sha256"],
                           "prompt_version": PROMPT_VERSION, "prompt": PROMPT,
                           "elapsed_s": round(time.monotonic() - start, 6), "completed_at": now()})
            json_once(destination, result)
            if index % 4 == 0 or index == len(rows) - 1:
                print(json.dumps({"model": args.model, "done": index + 1,
                                  "last_elapsed_s": result["elapsed_s"],
                                  "last_status": result["status"]}), flush=True)
    raw = [json.loads((raw_dir / f"{r['query_id']}.json").read_text()) for r in rows]
    for row, result in zip(rows, raw, strict=True):
        if (result["query_id"] != row["query_id"] or result["image_sha256"] != row["vlm_image_sha256"]
                or result["model_revision"] != spec["revision"] or result["prompt_version"] != PROMPT_VERSION):
            raise RuntimeError("Raw output does not match frozen inputs")
    elapsed = [r["elapsed_s"] for r in raw if r["status"] == "ok"]
    steady = elapsed[2:]  # first two images warm the model/device
    summary = {"model": args.model, **spec, "prompt_version": PROMPT_VERSION,
               "input_manifest_sha256": receipt["speed_manifest_sha256"],
               "n": len(raw), "ok": len(elapsed),
               "valid_one_digit": sum(r.get("format_compliant", False) for r in raw),
               "parsed_n": sum(r.get("parsed_score") is not None for r in raw),
               "mean_steady_s_per_image": round(statistics.mean(steady), 6) if steady else None,
               "median_steady_s_per_image": round(statistics.median(steady), 6) if steady else None,
               "images_per_second": round(1 / statistics.mean(steady), 4) if steady else None,
               "raw_files": [str(raw_dir / f"{r['query_id']}.json") for r in rows],
               "raw_sha256": {r["query_id"]: sha256(raw_dir / f"{r['query_id']}.json") for r in rows},
               "outcome_data_accessed": False}
    summary_path = OUT / f"prompt_vlm_{args.model}_speed32_summary.json"
    if not summary_path.exists():
        json_once(summary_path, summary)
    print(json.dumps({k: v for k, v in summary.items() if k not in {"raw_files", "raw_sha256"}},
                     ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
