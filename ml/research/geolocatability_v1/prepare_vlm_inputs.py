"""Cache display-oriented 1024-pixel JPEGs for repeatable, bounded VLM inference."""
from __future__ import annotations

import io
import json
from pathlib import Path

from PIL import Image, ImageOps

from .common import OUT, json_once, now, sha256, write_once

MAX_EDGE = 1024


def run() -> None:
    source_manifest = OUT / "annotation_input_manifest.jsonl"
    rows = [json.loads(line) for line in source_manifest.read_text().splitlines()]
    if len(rows) != 1184:
        raise RuntimeError("Wrong frozen input population")
    result = []
    for index, row in enumerate(rows):
        original = Path(row["image_path"])
        if sha256(original) != row["image_sha256"]:
            raise RuntimeError(f"Original image changed: {row['query_id']}")
        with Image.open(original) as source:
            display = ImageOps.exif_transpose(source).convert("RGB")
            display.thumbnail((MAX_EDGE, MAX_EDGE), Image.Resampling.LANCZOS)
            width, height = display.size
            buffer = io.BytesIO()
            display.save(buffer, format="JPEG", quality=94, subsampling=0, optimize=True)
        target = OUT / "vlm_inputs_1024" / f"{row['query_id']}.jpg"
        content = buffer.getvalue()
        if target.exists():
            if sha256(target) != __import__("hashlib").sha256(content).hexdigest():
                raise RuntimeError(f"Cached VLM input differs: {target}")
        else:
            write_once(target, content)
        result.append({"query_id": row["query_id"], "original_image_sha256": row["image_sha256"],
                       "vlm_image_path": str(target.resolve()), "vlm_image_sha256": sha256(target),
                       "vlm_width": width, "vlm_height": height})
        if (index + 1) % 200 == 0:
            print(f"Prepared {index + 1}/1184 VLM inputs", flush=True)
    target_manifest = OUT / "vlm_input_manifest_1024.jsonl"
    write_once(target_manifest, "".join(json.dumps(r, sort_keys=True) + "\n" for r in result).encode())
    json_once(OUT / "vlm_input_receipt_1024.json", {
        "prepared_at": now(), "source_manifest_sha256": sha256(source_manifest),
        "vlm_manifest_sha256": sha256(target_manifest), "query_count": len(result),
        "preprocessing": {"exif_orientation": "display", "long_edge_max": MAX_EDGE,
                          "resize": "Pillow LANCZOS", "color": "RGB",
                          "encoded": "JPEG quality 94, 4:4:4, no EXIF"},
        "outcome_data_accessed": False,
    })
    print(json.dumps({"count": len(result), "manifest_sha256": sha256(target_manifest)}))


if __name__ == "__main__":
    run()
