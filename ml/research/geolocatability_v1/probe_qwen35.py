"""Outcome-free schema-adherence probe on the first eight manifest images."""
from __future__ import annotations

import json
import time

from .annotate import NUMERIC, REASONS, SCALE, SYSTEM, validate
from .common import OUT, json_once, now, sha256

MODEL = OUT / "model_qwen35"
PROMPT = (
    SYSTEM + "\n" + SCALE + "\n"
    "Return one STRICT JSON object. Include EVERY key below, even if its score is 0.0. "
    "Each of these is a float in [0,1]: " + ", ".join(NUMERIC) + ". "
    "Also include usable_single_photo and retake_recommended as JSON booleans; "
    "retake_reason must be null when retake_recommended=false, otherwise one exact quoted string from: "
    + ", ".join(REASONS) + ". Include short_reason as a concise visible-content string. "
    "Double-quote every JSON key and string. Do not add or omit keys. No markdown."
)


def run() -> None:
    from mlx_vlm import generate, load
    from mlx_vlm.prompt_utils import apply_chat_template

    manifest = [json.loads(line) for line in (OUT / "vlm_input_manifest_1024.jsonl").read_text().splitlines()]
    model, processor = load(str(MODEL))
    formatted = apply_chat_template(processor, model.config, PROMPT, num_images=1,
                                    enable_thinking=False)
    for i, row in enumerate(manifest[:8]):
        target = OUT / "probes/qwen35_full" / f"{i:02d}_{row['query_id']}.json"
        if target.exists():
            continue
        t0 = time.monotonic()
        raw = ""
        try:
            response = generate(model, processor, formatted, image=[row["vlm_image_path"]],
                                max_tokens=512, temperature=0.0, verbose=False)
            raw = response.text
            parsed = validate(raw)
            error = None
        except Exception as exc:
            parsed = None
            error = f"{type(exc).__name__}: {exc}"
        json_once(target, {"query_id": row["query_id"], "raw_text": raw, "parsed": parsed,
                           "error": error, "valid": parsed is not None,
                           "elapsed_s": round(time.monotonic()-t0, 3),
                           "model_weight_sha256": sha256(MODEL / "model.safetensors"),
                           "prompt_sha256": __import__("hashlib").sha256(PROMPT.encode()).hexdigest(),
                           "completed_at": now()})
        print(json.dumps({"index": i, "valid": parsed is not None,
                          "error": error, "elapsed_s": round(time.monotonic()-t0, 1)}), flush=True)


if __name__ == "__main__":
    run()
