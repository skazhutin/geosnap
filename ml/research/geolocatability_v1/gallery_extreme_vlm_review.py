"""Resumable, outcome-blind second opinion on extreme gallery-quality candidates.

Raw VLM judgments are research evidence, never a command to delete photographs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from PIL import Image, ImageOps

from .annotate import MODEL, MODEL_REPO, MODEL_REVISION

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/gallery_extreme_quality_v1_20260929"
GALLERY = ROOT / "data/evaluation/geographic_v8_20260928/staged/gallery.parquet"
REASONS = {"severe_blur", "nearly_black", "nearly_white", "obstructed", "no_street_view", "other", "none"}
PROMPTS = (
    "Inspect only visible pixels. Is this photograph so technically damaged or obstructed that it has almost no usable street detail? "
    "A normal but visually repetitive street is NOT defective. Return only JSON with booleans severe_technical_defect, "
    "street_view_absent, and a reason from: severe_blur, nearly_black, nearly_white, obstructed, no_street_view, other, none.",
    "For a street-reference gallery, decide if the visible photo is an extreme reject: almost black/white, heavily blurred, "
    "dominated by an obstruction, or not showing an outdoor street at all. Keep ordinary streets, vegetation and low-distinctiveness "
    "scenes when they are visibly intact. Evaluate pixels only, not filename or location. Return JSON with booleans "
    "severe_technical_defect, street_view_absent, and one reason: severe_blur, nearly_black, nearly_white, "
    "obstructed, no_street_view, other, none.",
)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def atomic_new(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n").encode()
    temporary = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    with temporary.open("xb") as target:
        target.write(payload)
        target.flush()
        os.fsync(target.fileno())
    if path.exists():
        temporary.unlink()
        raise FileExistsError(path)
    os.replace(temporary, path)


def parse(text: str) -> dict:
    result = json.loads(text.strip())
    if set(result) != {"severe_technical_defect", "street_view_absent", "reason"}:
        raise ValueError("Wrong keys")
    if not isinstance(result["severe_technical_defect"], bool) or not isinstance(result["street_view_absent"], bool):
        raise ValueError("Wrong boolean value")
    if result["reason"] not in REASONS:
        raise ValueError("Wrong reason")
    return result


def run(candidate_file: Path, passes: int, limit: int | None, max_seconds: float | None) -> None:
    from mlx_vlm import generate, load
    from mlx_vlm.prompt_utils import apply_chat_template

    candidates = [json.loads(line) for line in candidate_file.read_text().splitlines()]
    if len({item["id"] for item in candidates}) != len(candidates):
        raise RuntimeError("Duplicate candidate IDs")
    frame = pd.read_parquet(GALLERY, columns=["id", "image_path", "file_sha256"])
    lookup = frame.set_index("id")
    if not set(item["id"] for item in candidates).issubset(lookup.index):
        raise RuntimeError("Candidate absent from fixed gallery")
    output = OUT / "vlm_review" / candidate_file.stem
    output.mkdir(parents=True, exist_ok=True)
    receipt = {"candidate_sha256": sha256(candidate_file), "gallery_sha256": sha256(GALLERY),
               "code_sha256": sha256(Path(__file__)), "model_repo": MODEL_REPO,
               "model_revision": MODEL_REVISION, "model_weight_sha256": sha256(MODEL / "model.safetensors"),
               "prompt_sha256": [hashlib.sha256(p.encode()).hexdigest() for p in PROMPTS],
               "temperature": 0.0, "max_tokens": 160, "pixel_only": True,
               "no_localization_outcomes": True, "candidate_only_no_gallery_change": True}
    contract = output / "contract.json"
    if contract.exists():
        if json.loads(contract.read_text()) != receipt:
            raise RuntimeError("VLM review contract changed")
    else:
        atomic_new(contract, receipt)

    model, processor = load(str(MODEL))
    def timed_out(signum, frame):
        raise TimeoutError("VLM pass exceeded 120 seconds")

    previous_handler = signal.signal(signal.SIGALRM, timed_out)
    start = time.monotonic()
    reviewed = 0
    for item in candidates:
        if limit is not None and reviewed >= limit:
            break
        if max_seconds is not None and time.monotonic() - start >= max_seconds:
            break
        ident = item["id"]
        raw_folder = output / "raw" / ident
        raw_folder.mkdir(parents=True, exist_ok=True)
        source = Path(lookup.loc[ident, "image_path"])
        if not source.is_file():
            unavailable = raw_folder / "source_unavailable.json"
            if not unavailable.exists():
                atomic_new(unavailable, {"id": ident, "at": datetime.now(timezone.utc).isoformat()})
            reviewed += 1
            continue
        prepared = output / "prepared" / f"{ident}.jpg"
        if not prepared.exists():
            with Image.open(source) as image:
                image = ImageOps.exif_transpose(image).convert("RGB")
                image.thumbnail((1024, 1024), Image.Resampling.LANCZOS)
                prepared.parent.mkdir(parents=True, exist_ok=True)
                image.save(prepared, format="JPEG", quality=90)
        for number in range(1, passes + 1):
            destination = raw_folder / f"pass{number}.json"
            if destination.exists():
                continue
            instruction = PROMPTS[number - 1] + " Return only a JSON object with exactly these three keys."
            formatted = apply_chat_template(processor, model.config, instruction, num_images=1,
                                            enable_thinking=False)
            attempts = []
            for _ in range(2):
                t0 = time.monotonic()
                try:
                    signal.setitimer(signal.ITIMER_REAL, 120)
                    try:
                        generated = generate(model, processor, formatted, image=[str(prepared)],
                                             max_tokens=160, temperature=0.0, verbose=False)
                    finally:
                        signal.setitimer(signal.ITIMER_REAL, 0)
                    raw = generated.text
                    parsed = parse(raw)
                    error = None
                except Exception as exc:
                    raw, parsed, error = "", None, f"{type(exc).__name__}: {exc}"
                attempts.append({"raw_text": raw, "parsed": parsed, "error": error,
                                 "elapsed_s": round(time.monotonic() - t0, 3)})
                if parsed is not None:
                    break
            atomic_new(destination, {"id": ident, "pass": number,
                                     "prepared_sha256": sha256(prepared), "attempts": attempts,
                                     "valid": attempts[-1]["parsed"] is not None,
                                     "at": datetime.now(timezone.utc).isoformat()})
        reviewed += 1
        if reviewed % 10 == 0 or reviewed == 1:
            print(json.dumps({"reviewed_this_run": reviewed, "candidate_total": len(candidates),
                              "elapsed_s": round(time.monotonic() - start, 1)}), flush=True)
    print(json.dumps({"status": "run_ended", "reviewed_this_run": reviewed,
                      "candidate_total": len(candidates), "elapsed_s": round(time.monotonic() - start, 1)}), flush=True)
    signal.signal(signal.SIGALRM, previous_handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-file", type=Path, required=True)
    parser.add_argument("--passes", type=int, choices=(1, 2), default=1)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--max-seconds", type=float)
    args = parser.parse_args()
    run(args.candidate_file, args.passes, args.limit, args.max_seconds)
