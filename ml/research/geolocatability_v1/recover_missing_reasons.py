"""Blind, append-only repair of passes missing only the descriptive short_reason."""
from __future__ import annotations

import json
import signal
import time
from pathlib import Path

from .annotate import BOOLEAN, MODEL, MODEL_REVISION, NUMERIC, SYSTEM, validate
from .common import OUT, json_once, now, sha256

PROMPT_VERSION = "missing-short-reason-targeted-v1"
MAX_TOKENS = 128
TIMEOUT_SECONDS = 180


def eligible(path: Path) -> dict | None:
    try:
        record = json.loads(path.read_text())
        value = json.loads(record["raw_text"])
        required = set(NUMERIC) | set(BOOLEAN) | {"retake_reason", "short_reason"}
        if set(value) != required - {"short_reason"}:
            return None
        validate(json.dumps({**value, "short_reason": "Temporary validation text"}))
        return value
    except (ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None


def caption_prompt(number: int) -> str:
    focus = (
        "Describe the most relevant visible scene evidence in one short sentence.",
        "Describe visible persistent structures and any limitation to visual place recognition.",
        "Summarize what is visibly useful or missing for distinguishing this street view.",
    )[number - 1]
    return (SYSTEM + "\n" + focus + "\nReturn exactly one JSON object with one key: "
            "short_reason. Its value must be a concise string of at most 18 words. "
            "Do not guess a city, coordinates, model success or image outcome. No other keys or markdown.")


def parse_caption(text: str) -> str:
    value = json.loads(text.strip())
    if not isinstance(value, dict) or set(value) != {"short_reason"} or \
       not isinstance(value["short_reason"], str) or not value["short_reason"].strip():
        raise ValueError("Targeted short_reason JSON invalid")
    return " ".join(value["short_reason"].split()[:18])


def run() -> None:
    from mlx_vlm import generate, load
    from mlx_vlm.prompt_utils import apply_chat_template

    source = OUT / "vlm_input_manifest_1024.jsonl"
    rows = [json.loads(line) for line in source.read_text().splitlines()]
    if len(rows) != 1184:
        raise RuntimeError("Wrong blind query population")
    settings_path = OUT / "missing_reason_recovery_settings.json"
    if not settings_path.exists():
        json_once(settings_path, {"created_at": now(), "prompt_version": PROMPT_VERSION,
            "prompt_sha256": {str(i): __import__("hashlib").sha256(caption_prompt(i).encode()).hexdigest()
                              for i in (1, 2, 3)},
            "model_revision": MODEL_REVISION, "model_weight_sha256": sha256(MODEL / "model.safetensors"),
            "vlm_input_manifest_sha256": sha256(source), "temperature": 0.0,
            "max_tokens": MAX_TOKENS, "max_attempts_per_missing_pass": 3,
            "generation_timeout_s": TIMEOUT_SECONDS, "outcomes_accessed": False})
    settings = json.loads(settings_path.read_text())
    if settings["vlm_input_manifest_sha256"] != sha256(source) or settings["model_weight_sha256"] != sha256(MODEL / "model.safetensors"):
        raise RuntimeError("Recovery input/model changed")
    work = []
    for row in rows:
        folder = OUT / "annotations/raw_qwen35_full1024" / row["query_id"]
        if not (folder / "failure.json").is_file():
            continue
        for number in (1, 2, 3):
            if (folder / f"pass{number}.json").is_file():
                continue
            candidates = [(path, eligible(path)) for path in sorted(folder.glob(f"pass{number}_attempt*.json"))]
            candidates = [(path, value) for path, value in candidates if value is not None]
            if candidates:
                work.append((row, number, candidates[0][0], candidates[0][1]))
    if not work:
        print(json.dumps({"eligible_missing_passes": 0, "at": now()}), flush=True)
        return
    model, processor = load(str(MODEL))

    def timeout_handler(signum, frame):
        raise TimeoutError("Targeted caption generation timed out")

    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    recovered = 0
    for row, number, source_path, original in work:
        folder = source_path.parent
        output = folder / f"pass{number}.json"
        if output.exists():
            continue
        instruction = caption_prompt(number)
        formatted = apply_chat_template(processor, model.config, instruction,
                                        num_images=1, enable_thinking=False)
        caption = None
        caption_source = None
        for attempt in (1, 2, 3):
            attempt_path = folder / f"short_reason_pass{number}_attempt{attempt}.json"
            if attempt_path.exists():
                record = json.loads(attempt_path.read_text())
                if record["valid"]:
                    caption = parse_caption(record["raw_text"])
                    caption_source = attempt_path
                    break
                continue
            t0 = time.monotonic()
            raw = ""
            try:
                signal.setitimer(signal.ITIMER_REAL, TIMEOUT_SECONDS)
                try:
                    answer = generate(model, processor, formatted, image=[row["vlm_image_path"]],
                                      max_tokens=MAX_TOKENS, temperature=0.0, verbose=False)
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
                raw = answer.text
                caption = parse_caption(raw)
                error = None
            except Exception as exc:
                caption = None
                error = f"{type(exc).__name__}: {exc}"
            json_once(attempt_path, {"query_id": row["query_id"], "pass": number,
                        "attempt": attempt, "raw_text": raw, "valid": caption is not None,
                        "error": error, "elapsed_s": round(time.monotonic()-t0, 3),
                        "completed_at": now(), "prompt_version": PROMPT_VERSION})
            if caption is not None:
                caption_source = attempt_path
                break
        if caption is None:
            if not (folder / "recovery_failure.json").exists():
                json_once(folder / "recovery_failure.json", {"type": "targeted_caption_failure",
                    "at": now(), "pass": number, "source_attempt": source_path.name})
            continue
        assembled = {**original, "short_reason": caption}
        parsed = validate(json.dumps(assembled, ensure_ascii=False))
        json_once(output, {"query_id": row["query_id"], "pass": number,
                           "attempt": "targeted_reason_recovery", "raw_text": json.dumps(assembled, ensure_ascii=False),
                           "parsed": parsed, "valid": True, "error": None,
                           "recovered_at": now(), "source_numeric_attempt": source_path.name,
                           "source_caption_attempt": caption_source.name,
                           "recovery_prompt_version": PROMPT_VERSION})
        recovered += 1
        if recovered % 10 == 0:
            print(json.dumps({"recovered_passes": recovered, "eligible": len(work)}), flush=True)
    signal.signal(signal.SIGALRM, old_handler)
    print(json.dumps({"recovered_passes": recovered, "eligible": len(work), "at": now()}), flush=True)


if __name__ == "__main__":
    run()
