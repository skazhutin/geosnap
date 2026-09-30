"""Blind, resumable Qwen second opinion on 20 random gallery images per SigLIP2 band."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image, ImageOps

from .annotate import MODEL, MODEL_REPO, MODEL_REVISION
from .scan_siglip2_gallery import EXPECTED_GALLERY_SHA256, GALLERY
from .common import sha256, verify_production

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "data/evaluation/geolocatability_siglip2_gallery_v1_20260929"
OUT = ROOT / "data/evaluation/geolocatability_siglip2_gallery_qwen100_v1_20260929"
MANIFEST = OUT / "blind_input_manifest.jsonl"
INPUT_RECEIPT = OUT / "input_receipt.json"
RAW = OUT / "raw"
SEED = 20260929
N_PER_BAND = 20
PROMPTS = (
    "Assess this single street image using visible pixels only. First judge stable location clues, then capture defects. ",
    "Inspect capture quality first, then independently judge whether persistent scene details distinguish this place from other streets. ",
)
BASE = (
    "Do not identify the city or coordinates. Do not use filename, metadata, model predictions, or previous judgments. "
    "Technical quality and geolocation usefulness are different: a sharp blank wall has low geolocatability; "
    "a somewhat blurry distinctive junction can still be useful. "
    "Geolocatability scale: 0=no distinguishing evidence; 0.25=very weak; 0.5=ambiguous but potentially usable; "
    "0.75=good stable structure; 1=highly distinctive stable clues. "
    "Return only strict JSON with exactly these keys: technical_quality and geolocatability (numbers 0..1), "
    "stable_landmarks_visible and severe_capture_defect (booleans), and short_reason (at most 20 words describing visible content). "
    "A generic but intact road is not a severe capture defect."
)


def write_once(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = ((value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)) + "\n").encode()
    temporary = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    with temporary.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    if path.exists():
        temporary.unlink()
        raise FileExistsError(path)
    os.replace(temporary, path)


def prepare() -> None:
    verify_production()
    frozen = json.loads((SOURCE / "annotation_freeze.json").read_text())
    if sha256(GALLERY) != EXPECTED_GALLERY_SHA256 or frozen["gallery_manifest_sha256"] != EXPECTED_GALLERY_SHA256:
        raise RuntimeError("Fixed gallery changed")
    scores = SOURCE / "gallery_scores.jsonl"
    if sha256(scores) != frozen["gallery_scores_jsonl_sha256"]:
        raise RuntimeError("Frozen scores changed")
    if MANIFEST.exists():
        receipt = json.loads(INPUT_RECEIPT.read_text())
        if sha256(MANIFEST) != receipt["blind_input_manifest_sha256"] or receipt["scores_sha256"] != sha256(scores):
            raise RuntimeError("Existing blind selection changed")
        print(json.dumps({"status": "already_prepared", "count": 100}), flush=True)
        return
    by_band = {i: [] for i in range(5)}
    for line in scores.read_text().splitlines():
        row = json.loads(line)
        if row["status"] == "ok":
            band = min(4, int(row["geolocatability_teacher_estimate"] * 5))
            by_band[band].append(row["id"])
    rng = random.Random(SEED)
    chosen = [ident for band in range(5) for ident in rng.sample(by_band[band], N_PER_BAND)]
    rng.shuffle(chosen)
    gallery = pd.read_parquet(GALLERY, columns=["id", "image_path", "file_sha256"]).set_index("id")
    rows = [{"id": ident, "image_path": str(gallery.loc[ident, "image_path"]),
             "file_sha256": str(gallery.loc[ident, "file_sha256"])} for ident in chosen]
    assert len(rows) == len({row["id"] for row in rows}) == 100
    write_once(MANIFEST, "\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows))
    write_once(INPUT_RECEIPT, {"seed": SEED, "count_per_band": N_PER_BAND, "count": len(rows),
                               "scores_sha256": sha256(scores), "gallery_sha256": sha256(GALLERY),
                               "blind_input_manifest_sha256": sha256(MANIFEST),
                               "manifest_excludes_siglip_score_and_geosnap_outcome": True})
    print(json.dumps({"status": "prepared", "count": len(rows), "per_band": N_PER_BAND}), flush=True)


def parse(response: str) -> dict:
    value = json.loads(response.strip())
    if set(value) != {"technical_quality", "geolocatability", "stable_landmarks_visible",
                      "severe_capture_defect", "short_reason"}:
        raise ValueError("Wrong output keys")
    for key in ("technical_quality", "geolocatability"):
        if isinstance(value[key], bool) or not isinstance(value[key], (int, float)) or not 0 <= value[key] <= 1:
            raise ValueError(f"Invalid {key}")
        value[key] = float(value[key])
    for key in ("stable_landmarks_visible", "severe_capture_defect"):
        if not isinstance(value[key], bool):
            raise ValueError(f"Invalid {key}")
    if not isinstance(value["short_reason"], str) or not value["short_reason"].strip():
        raise ValueError("Invalid short_reason")
    value["short_reason"] = " ".join(value["short_reason"].split()[:20])
    return value


def run() -> None:
    from mlx_vlm import generate, load
    from mlx_vlm.prompt_utils import apply_chat_template

    receipt = json.loads(INPUT_RECEIPT.read_text())
    if sha256(MANIFEST) != receipt["blind_input_manifest_sha256"] or sha256(GALLERY) != receipt["gallery_sha256"]:
        raise RuntimeError("Blind selection or gallery changed")
    rows = [json.loads(line) for line in MANIFEST.read_text().splitlines()]
    assert len(rows) == len({x["id"] for x in rows}) == 100
    if not any(Path(row["image_path"]).is_file() for row in rows[:5]):
        raise RuntimeError("Gallery photo volume is unavailable; connect it before Qwen review")
    contract = {"model_repo": MODEL_REPO, "model_revision": MODEL_REVISION,
                "model_weight_sha256": sha256(MODEL / "model.safetensors"),
                "blind_input_manifest_sha256": sha256(MANIFEST), "temperature": 0,
                "max_tokens": 200, "passes": 2,
                "prompt_sha256": [hashlib.sha256((p + BASE).encode()).hexdigest() for p in PROMPTS],
                "score_and_outcome_blind": True}
    contract_path = OUT / "qwen_contract.json"
    if contract_path.exists():
        if json.loads(contract_path.read_text()) != contract:
            raise RuntimeError("Qwen contract changed")
    else:
        write_once(contract_path, contract)
    model, processor = load(str(MODEL))
    def timeout_handler(signum, frame):
        raise TimeoutError("Qwen pass exceeded 120 seconds")
    previous_handler = signal.signal(signal.SIGALRM, timeout_handler)
    started = time.monotonic()
    try:
        for index, row in enumerate(rows, 1):
            folder = RAW / row["id"]
            if all((folder / f"pass{n}.json").exists() for n in (1, 2)):
                continue
            image_path = Path(row["image_path"])
            if not image_path.is_file() or sha256(image_path) != row["file_sha256"]:
                if not (folder / "source_failure.json").exists():
                    write_once(folder / "source_failure.json", {"id": row["id"], "reason": "missing_or_sha_mismatch"})
                continue
            prepared = OUT / "prepared" / f"{row['id']}.jpg"
            if not prepared.exists():
                with Image.open(image_path) as im:
                    im = ImageOps.exif_transpose(im).convert("RGB")
                    im.thumbnail((1024, 1024), Image.Resampling.LANCZOS)
                    prepared.parent.mkdir(parents=True, exist_ok=True)
                    im.save(prepared, format="JPEG", quality=90)
            for number in (1, 2):
                destination = folder / f"pass{number}.json"
                if destination.exists():
                    continue
                instruction = PROMPTS[number - 1] + BASE
                formatted = apply_chat_template(processor, model.config, instruction,
                                                num_images=1, enable_thinking=False)
                attempts = []
                for _ in range(2):
                    t0 = time.monotonic()
                    try:
                        signal.setitimer(signal.ITIMER_REAL, 120)
                        try:
                            result = generate(model, processor, formatted, image=[str(prepared)],
                                              max_tokens=200, temperature=0, verbose=False)
                        finally:
                            signal.setitimer(signal.ITIMER_REAL, 0)
                        answer = result.text
                        parsed = parse(answer)
                        error = None
                    except Exception as exc:
                        answer, parsed, error = "", None, f"{type(exc).__name__}: {exc}"
                    attempts.append({"raw_text": answer, "parsed": parsed, "error": error,
                                     "elapsed_s": round(time.monotonic() - t0, 3)})
                    if parsed is not None:
                        break
                write_once(destination, {"id": row["id"], "pass": number,
                                         "source_file_sha256": row["file_sha256"],
                                         "prepared_sha256": sha256(prepared), "attempts": attempts,
                                         "valid": attempts[-1]["parsed"] is not None,
                                         "at": datetime.now(timezone.utc).isoformat()})
            if index % 10 == 0 or index == 1:
                print(json.dumps({"done": index, "total": 100,
                                  "elapsed_s": round(time.monotonic() - started, 1)}), flush=True)
    finally:
        signal.signal(signal.SIGALRM, previous_handler)


def analyze() -> None:
    receipt = json.loads(INPUT_RECEIPT.read_text())
    if sha256(MANIFEST) != receipt["blind_input_manifest_sha256"]:
        raise RuntimeError("Blind manifest changed")
    rows = [json.loads(line) for line in MANIFEST.read_text().splitlines()]
    outcomes = []
    for row in rows:
        passes = []
        for number in (1, 2):
            path = RAW / row["id"] / f"pass{number}.json"
            if not path.exists():
                raise RuntimeError(f"Qwen pass missing: {row['id']} / {number}")
            raw = json.loads(path.read_text())
            if raw["id"] != row["id"] or raw["source_file_sha256"] != row["file_sha256"]:
                raise RuntimeError("Qwen raw identity or source mismatch")
            passes.append(raw)
        valid = [p["attempts"][-1]["parsed"] for p in passes if p["valid"]]
        outcomes.append({"id": row["id"], "qwen_valid_passes": len(valid),
                         "qwen_geolocatability": float(np.median([v["geolocatability"] for v in valid])) if valid else None,
                         "qwen_technical_quality": float(np.median([v["technical_quality"] for v in valid])) if valid else None,
                         "qwen_score_disagreement": abs(valid[0]["geolocatability"] - valid[1]["geolocatability"]) if len(valid) == 2 else None,
                         "qwen_severe_defect_votes": sum(v["severe_capture_defect"] for v in valid),
                         "qwen_reasons": [v["short_reason"] for v in valid]})
    # Freeze Qwen-only judgments before opening the SigLIP2 scores.
    frozen_path = OUT / "qwen_blind_annotations.jsonl"
    if not frozen_path.exists():
        write_once(frozen_path, "\n".join(json.dumps(x, ensure_ascii=False, sort_keys=True) for x in outcomes))
    if not (OUT / "qwen_blind_freeze.json").exists():
        write_once(OUT / "qwen_blind_freeze.json", {"manifest_sha256": sha256(MANIFEST),
                                                    "annotations_sha256": sha256(frozen_path),
                                                    "raw_complete": True, "score_and_outcome_blind": True})
    frozen = json.loads((OUT / "qwen_blind_freeze.json").read_text())
    if sha256(frozen_path) != frozen["annotations_sha256"]:
        raise RuntimeError("Frozen Qwen annotations changed")
    score_freeze = json.loads((SOURCE / "annotation_freeze.json").read_text())
    scores_path = SOURCE / "gallery_scores.jsonl"
    if sha256(scores_path) != score_freeze["gallery_scores_jsonl_sha256"]:
        raise RuntimeError("SigLIP2 scores changed")
    wanted = {x["id"] for x in rows}
    lookup = {}
    for line in scores_path.read_text().splitlines():
        item = json.loads(line)
        if item["id"] in wanted:
            lookup[item["id"]] = item["geolocatability_teacher_estimate"]
    assert len(lookup) == 100
    joined = []
    for item in outcomes:
        score = lookup[item["id"]]
        joined.append({**item, "siglip2_score": score, "siglip2_band": min(4, int(score * 5)),
                       "absolute_difference": abs(score - item["qwen_geolocatability"]) if item["qwen_geolocatability"] is not None else None})
    assert all(sum(x["siglip2_band"] == band for x in joined) == 20 for band in range(5))
    result_path = OUT / "comparison.jsonl"
    if not result_path.exists():
        write_once(result_path, "\n".join(json.dumps(x, ensure_ascii=False, sort_keys=True) for x in joined))
    index_path = OUT / "review_index.md"
    if not index_path.exists():
        images = {item["id"]: item["image_path"] for item in rows}
        sections = ["# Stratified SigLIP2 / Qwen photo review", "",
                    "Twenty images were selected at random within each frozen SigLIP2 band. "
                    "Qwen saw only pixels. Scores are automatic pseudo-labels, not human judgments.", ""]
        for band in range(5):
            sections.extend([f"## SigLIP2 band {band}: {band / 5:.1f}–{(band + 1) / 5:.1f}", "",
                             "| Photo | SigLIP2 | Qwen | Qwen visible-content reason |",
                             "|---|---:|---:|---|"])
            for item in sorted((x for x in joined if x["siglip2_band"] == band),
                               key=lambda x: x["siglip2_score"]):
                reason = (item["qwen_reasons"][0] if item["qwen_reasons"] else "No valid answer").replace("|", "/")
                photo = images[item["id"]]
                qwen = f"{item['qwen_geolocatability']:.3f}" if item["qwen_geolocatability"] is not None else "failed"
                sections.append(f"| [Photo {item['id']}](<{photo}>) | {item['siglip2_score']:.3f} | {qwen} | {reason} |")
            sections.append("")
        write_once(index_path, "\n".join(sections))
    valid = [x for x in joined if x["qwen_geolocatability"] is not None]
    by_band = []
    for band in range(5):
        sample = [x for x in valid if x["siglip2_band"] == band]
        by_band.append({"band": band, "n": len(sample),
                        "siglip2_median": float(np.median([x["siglip2_score"] for x in sample])) if sample else None,
                        "qwen_median": float(np.median([x["qwen_geolocatability"] for x in sample])) if sample else None,
                        "mean_absolute_difference": float(np.mean([x["absolute_difference"] for x in sample])) if sample else None,
                        "severe_defect_any_votes": sum(x["qwen_severe_defect_votes"] > 0 for x in sample)})
    report = {"selected": 100, "qwen_at_least_one_valid": len(valid),
              "qwen_two_valid": sum(x["qwen_valid_passes"] == 2 for x in joined),
              "mean_absolute_difference": float(np.mean([x["absolute_difference"] for x in valid])) if valid else None,
              "spearman": float(pd.Series([x["siglip2_score"] for x in valid]).corr(
                  pd.Series([x["qwen_geolocatability"] for x in valid]), method="spearman")) if len(valid) > 2 else None,
              "qwen_pass_disagreement_median": float(np.median([x["qwen_score_disagreement"] for x in joined if x["qwen_score_disagreement"] is not None])) if any(x["qwen_score_disagreement"] is not None for x in joined) else None,
              "by_band": by_band, "qwen_blind_freeze_sha256": sha256(OUT / "qwen_blind_freeze.json"),
              "comparison_sha256": sha256(result_path), "production": verify_production()}
    if not (OUT / "comparison_summary.json").exists():
        write_once(OUT / "comparison_summary.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("phase", choices=("prepare", "run", "analyze"))
    phase = cli.parse_args().phase
    {"prepare": prepare, "run": run, "analyze": analyze}[phase]()
