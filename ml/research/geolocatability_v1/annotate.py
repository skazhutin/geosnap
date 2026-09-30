"""Three independent, pixel-only local VLM annotations; resumable per image/pass."""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import math
import signal
import time
from pathlib import Path

from .common import OUT, PROMPT_VERSION, SCHEMA_VERSION, json_once, now, revision, sha256

MODEL_REPO = "mlx-community/Qwen3.5-4B-MLX-4bit"
MODEL_REVISION = "32f3e8ecf65426fc3306969496342d504bfa13f3"
MODEL = OUT / "model_qwen35"
GENERATION_TIMEOUT_S = 180
NUMERIC = (
    "motion_blur", "out_of_focus", "too_dark", "overexposed", "obstructed_view",
    "dirty_or_glare_through_glass", "orientation_problem", "close_surface", "mostly_ground",
    "mostly_sky", "inside_vehicle_or_indoor", "vegetation_dominated",
    "generic_repetitive_scene", "stable_landmarks_visible", "distinctive_buildings",
    "street_geometry_visible", "signage_or_text_visible", "street_furniture_visible",
    "sufficient_scene_context", "technical_quality", "geolocatability",
)
BOOLEAN = ("usable_single_photo", "retake_recommended")
REASONS = (
    "MOTION_BLUR", "OUT_OF_FOCUS", "TOO_DARK", "OVEREXPOSED", "OBSTRUCTED",
    "DIRTY_GLASS_OR_GLARE", "TOO_CLOSE", "GROUND_HEAVY", "SKY_HEAVY",
    "VEGETATION_ONLY", "NO_LANDMARKS", "INSUFFICIENT_CONTEXT", "ORIENTATION", "OTHER",
)
TECH_KEYS = NUMERIC[:7]
SCENE_KEYS = NUMERIC[7:19]
GLOBAL_KEYS = NUMERIC[19:]
COMPACT_KEYS = {"t", "s", "g", "u", "r", "a", "note"}

SYSTEM = (
    "You are annotating an image for a visual geolocation research dataset. "
    "Do NOT guess where the image was taken. Do NOT identify the city unless necessary to describe visible content. "
    "Do NOT use filenames, EXIF, metadata, GPS, previous predictions or dataset outcomes. "
    "Evaluate only the visible pixels. Judge whether the scene provides stable visual evidence "
    "that could distinguish this physical location from other street scenes. Technical image quality "
    "and geolocation usefulness are separate concepts. A sharp but featureless wall can be technically "
    "excellent but useless for geolocation. A slightly blurry image can still be useful if it contains "
    "distinctive stable structures. Return JSON only."
)
SCALE = (
    "Geolocatability 0.0: almost no distinguishing evidence (blank wall, extreme blur, ground, obstruction); "
    "0.25: very weak; 0.50: potentially usable but ambiguous; 0.75: good stable visual structure; "
    "1.0: highly distinctive stable location cues. Do not estimate whether any particular geolocation model succeeds. "
    "Each numeric field is a float from 0.0 to 1.0. For defect fields, 1.0 means strongly present; "
    "for positive visual cues, 1.0 means strongly present. Technical quality 1.0 means excellent capture. "
    "An inside-vehicle view can remain useful if the street outside is visible. Recommend a retake only "
    "when another direction or better capture would likely reveal substantially more location evidence."
)
PROMPTS = (
    "Inspect the image for capture defects first, then stable scene evidence. "
    "Assess a single photograph for visual location matching.",
    "First judge distinctive, persistent scene structure; then inspect blur, exposure, glass, obstruction and orientation. "
    "Separately rate photo clarity and usefulness for recognizing the physical place.",
    "Imagine comparing this view with many other streets. Which visible cues are repeatable and distinguishing? "
    "Now independently score capture problems and whether a better photograph is warranted.",
)


def prompt(pass_number: int) -> str:
    return (
        SYSTEM + "\n" + PROMPTS[pass_number - 1] + "\n" + SCALE + "\n"
        "Return one STRICT JSON object. Include EVERY key below, even if its score is 0.0. "
        "Each of these is a float in [0,1]: " + ", ".join(NUMERIC) + ". "
        "Also include usable_single_photo and retake_recommended as JSON booleans; "
        "retake_reason must be null when retake_recommended=false, otherwise one exact quoted string from: "
        + ", ".join(REASONS) + ". Include short_reason as a concise visible-content string. "
        "Double-quote every JSON key and string. Do not add or omit keys. No markdown."
    )


def parse_compact(text: str) -> dict:
    value = json.loads(text.strip())
    if not isinstance(value, dict) or set(value) != COMPACT_KEYS:
        raise ValueError("Compact JSON keys mismatch")
    for name, keys in (("t", TECH_KEYS), ("s", SCENE_KEYS), ("g", GLOBAL_KEYS)):
        if not isinstance(value[name], list) or len(value[name]) != len(keys):
            raise ValueError(f"Compact {name} must contain {len(keys)} scores")
    if not isinstance(value["u"], bool) or not isinstance(value["r"], bool):
        raise ValueError("Compact booleans invalid")
    if isinstance(value["a"], bool) or not isinstance(value["a"], int) or not 0 <= value["a"] <= len(REASONS):
        raise ValueError("Compact retake action invalid")
    if value["r"] != (value["a"] != 0):
        raise ValueError("Compact retake/action mismatch")
    expanded = {key: item for name, keys in (("t", TECH_KEYS), ("s", SCENE_KEYS), ("g", GLOBAL_KEYS))
                for key, item in zip(keys, value[name], strict=True)}
    expanded.update({"usable_single_photo": value["u"], "retake_recommended": value["r"],
                     "retake_reason": REASONS[value["a"] - 1] if value["a"] else None,
                     "short_reason": value["note"]})
    return validate(json.dumps(expanded))


def validate(text: str) -> dict:
    value = json.loads(text.strip())
    expected = set(NUMERIC) | set(BOOLEAN) | {"retake_reason", "short_reason"}
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("JSON keys mismatch")
    for key in NUMERIC:
        v = value[key]
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or not 0 <= v <= 1:
            raise ValueError(f"Invalid numeric {key}: {v!r}")
        value[key] = float(v)
    for key in BOOLEAN:
        if not isinstance(value[key], bool):
            raise ValueError(f"Invalid boolean {key}")
    if value["retake_recommended"]:
        if value["retake_reason"] not in REASONS:
            # Preserve the raw response. OTHER is the schema's explicit catch-all
            # for a model-provided reason outside the controlled vocabulary.
            if not isinstance(value["retake_reason"], str) or not value["retake_reason"].strip():
                raise ValueError("Invalid retake reason")
            value["retake_reason"] = "OTHER"
    elif value["retake_reason"] is not None:
        raise ValueError("Retake reason must be null if no retake")
    if not isinstance(value["short_reason"], str) or not 0 < len(value["short_reason"]) <= 1000:
        raise ValueError("Invalid short reason")
    # Keep the model's original text in raw_text; the canonical explanation is
    # bounded without changing any visual score or categorical decision.
    value["short_reason"] = " ".join(value["short_reason"].split()[:18])
    return value


def run(limit: int | None = None) -> None:
    from mlx_vlm import generate, load
    from mlx_vlm.prompt_utils import apply_chat_template

    manifest = OUT / "annotation_input_manifest.jsonl"
    receipt = json.loads((OUT / "input_receipt.json").read_text())
    if sha256(manifest) != receipt["annotation_manifest_sha256"]:
        raise RuntimeError("Frozen annotation manifest changed")
    rows = [json.loads(line) for line in manifest.read_text().splitlines()]
    if len(rows) != 1184:
        raise RuntimeError("Wrong annotation population")
    vlm_manifest = OUT / "vlm_input_manifest_1024.jsonl"
    vlm_receipt = json.loads((OUT / "vlm_input_receipt_1024.json").read_text())
    if sha256(vlm_manifest) != vlm_receipt["vlm_manifest_sha256"] or vlm_receipt["source_manifest_sha256"] != sha256(manifest):
        raise RuntimeError("VLM input preprocessing manifest changed")
    vlm_rows = [json.loads(line) for line in vlm_manifest.read_text().splitlines()]
    if [x["query_id"] for x in vlm_rows] != [x["query_id"] for x in rows]:
        raise RuntimeError("VLM input identities/order differ")
    if not (MODEL / "model.safetensors").is_file():
        raise RuntimeError("Pinned VLM checkpoint missing")
    model_hash = sha256(MODEL / "model.safetensors")
    settings = {"model_repo": MODEL_REPO, "model_revision": MODEL_REVISION,
                "model_weight_sha256": model_hash, "mlx_vlm_version": importlib.metadata.version("mlx-vlm"),
                "prompt_version": PROMPT_VERSION, "schema_version": SCHEMA_VERSION,
                "temperature": 0.0, "max_tokens": 512, "max_attempts_per_pass": 3,
                "code_revision": revision(), "manifest_sha256": sha256(manifest),
                "vlm_input_manifest_sha256": sha256(vlm_manifest),
                "vlm_input_preprocessing": vlm_receipt["preprocessing"],
                "started_at": now(), "image_pixels_only": True}
    settings_path = OUT / "inference_settings_qwen35_full1024.json"
    if settings_path.exists():
        previous = json.loads(settings_path.read_text())
        for key in settings.keys() - {"started_at"}:
            if previous[key] != settings[key]:
                raise RuntimeError(f"Inference settings changed on resume: {key}")
    else:
        json_once(settings_path, settings)
    timeout_policy = OUT / "inference_timeout_policy.json"
    if not timeout_policy.exists():
        json_once(timeout_policy, {"generation_timeout_s": GENERATION_TIMEOUT_S,
                                   "first_applied_after_completed_probe_images": 8,
                                   "model_and_prompts_unchanged": True,
                                   "failure_handling": "TimeoutError recorded as bounded attempt failure"})
    elif json.loads(timeout_policy.read_text())["generation_timeout_s"] != GENERATION_TIMEOUT_S:
        raise RuntimeError("Generation timeout policy changed")

    def timed_out(signum, frame):
        raise TimeoutError(f"Local VLM generation exceeded {GENERATION_TIMEOUT_S} seconds")

    previous_handler = signal.signal(signal.SIGALRM, timed_out)

    model, processor = load(str(MODEL))
    processed = 0
    started = time.monotonic()
    for index, (row, vlm_row) in enumerate(zip(rows, vlm_rows, strict=True)):
        ident = row["query_id"]
        folder = OUT / "annotations/raw_qwen35_full1024" / ident
        if all((folder / f"pass{n}.json").exists() for n in (1, 2, 3)):
            continue
        if (folder / "failure.json").exists() or (folder / "recovery_failure.json").exists():
            continue
        image = Path(vlm_row["vlm_image_path"])
        if not image.is_file() or sha256(image) != vlm_row["vlm_image_sha256"]:
            json_once(folder / "failure.json", {"type": "unreadable_image", "at": now(),
                                                "detail": "Image absent or SHA-256 mismatch"})
            continue
        for number in (1, 2, 3):
            destination = folder / f"pass{number}.json"
            if destination.exists():
                validate(json.loads(destination.read_text())["raw_text"])
                continue
            # A previous library version or stricter parser may have rejected
            # structurally sound JSON. Recover from its immutable raw output.
            for previous_path in sorted(folder.glob(f"pass{number}_attempt*.json")):
                previous = json.loads(previous_path.read_text())
                try:
                    recovered = validate(previous["raw_text"])
                except (ValueError, TypeError, json.JSONDecodeError):
                    continue
                if not destination.exists():
                    json_once(destination, {**previous, "parsed": recovered,
                                            "valid": True, "error": None,
                                            "recovered_from": previous_path.name,
                                            "recovered_at": now()})
                break
            if destination.exists():
                continue
            instruction = prompt(number)
            last_error = None
            start_attempt = 1
            for attempt in range(start_attempt, start_attempt + 3):
                attempt_file = folder / f"pass{number}_attempt{attempt}.json"
                if attempt_file.exists():
                    old = json.loads(attempt_file.read_text())
                    if old.get("valid"):
                        if not destination.exists():
                            json_once(destination, old)
                        last_error = None
                        break
                    last_error = old.get("error")
                    continue
                t0 = time.monotonic()
                raw = ""
                try:
                    attempt_instruction = instruction + (
                        "\nStrictly use null or ONE exact uppercase token from the listed retake reasons."
                        if attempt > start_attempt else ""
                    )
                    formatted = apply_chat_template(processor, model.config, attempt_instruction,
                                                    num_images=1, enable_thinking=False)
                    signal.setitimer(signal.ITIMER_REAL, GENERATION_TIMEOUT_S)
                    try:
                        result = generate(model, processor, formatted, image=[str(image)],
                                          max_tokens=512, temperature=0.0, verbose=False)
                    finally:
                        signal.setitimer(signal.ITIMER_REAL, 0)
                    raw = result.text
                    parsed = validate(raw)
                    error = None
                except Exception as exc:
                    parsed = None
                    error = f"{type(exc).__name__}: {exc}"
                record = {"query_id": ident, "pass": number, "attempt": attempt,
                          "prompt_sha256": __import__("hashlib").sha256(attempt_instruction.encode()).hexdigest(),
                          "prompt_version": PROMPT_VERSION, "model_revision": MODEL_REVISION,
                          "completed_at": now(), "elapsed_s": round(time.monotonic() - t0, 3),
                          "raw_text": raw, "valid": parsed is not None, "parsed": parsed,
                          "error": error}
                json_once(attempt_file, record)
                if parsed is not None:
                    json_once(destination, record)
                    last_error = None
                    break
                last_error = error
            if last_error is not None:
                fail_path = folder / ("recovery_failure.json" if (folder / "failure.json").exists() else "failure.json")
                if not fail_path.exists():
                    json_once(fail_path, {"type": "vlm_or_json_failure", "at": now(),
                                                "pass": number, "last_error": last_error})
                break
        processed += 1
        if processed % 10 == 0 or processed == 1:
            print(json.dumps({"new_images": processed, "through_index": index + 1,
                              "elapsed_s": round(time.monotonic() - started, 1)}), flush=True)
        if limit is not None and processed >= limit:
            break
    signal.signal(signal.SIGALRM, previous_handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int)
    run(parser.parse_args().limit)
