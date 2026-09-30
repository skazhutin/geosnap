"""Small, post-freeze interpretation sheets; never edits teacher annotations."""
from __future__ import annotations

import io
import json

import pandas as pd
from PIL import Image, ImageDraw, ImageFont, ImageOps

from .common import OUT, sha256, write_once


def render(name: str, frame: pd.DataFrame, image_paths: dict[str, str]) -> None:
    selected = frame.head(16)
    cell_w, cell_h = 290, 245
    canvas = Image.new("RGB", (4 * cell_w, 4 * cell_h), (246, 247, 248))
    draw = ImageDraw.Draw(canvas)
    font = ImageFont.load_default()
    for slot, row in enumerate(selected.itertuples()):
        x, y = (slot % 4) * cell_w, (slot // 4) * cell_h
        try:
            with Image.open(image_paths[row.query_id]) as original:
                thumb = ImageOps.contain(original.convert("RGB"), (cell_w - 10, cell_h - 36))
            canvas.paste(thumb, (x + (cell_w - thumb.width)//2, y + 4))
            label = f"{row.query_id[:8]}  geo={row.geolocatability_median:.2f} tech={row.technical_quality_median:.2f}"
            draw.text((x + 5, y + cell_h - 28), label, fill=(15, 20, 25), font=font)
        except Exception as exc:
            draw.text((x + 5, y + 15), f"Unreadable {row.query_id[:8]}: {str(exc)[:26]}",
                      fill=(170, 0, 0), font=font)
    if len(selected) == 0:
        draw.text((20, 20), "No eligible images under the prespecified selection rule", fill=(15, 20, 25), font=font)
    buffer = io.BytesIO()
    canvas.save(buffer, format="PNG")
    write_once(OUT / "contact_sheets" / f"{name}.png", buffer.getvalue())


def run() -> None:
    frozen = json.loads((OUT / "annotation_freeze.json").read_text())
    if sha256(OUT / "geolocatability_annotations.jsonl") != frozen["canonical_jsonl_sha256"]:
        raise RuntimeError("Blind annotation changed")
    frame = pd.read_parquet(OUT / "analysis_outcome_join.parquet")
    valid = frame[frame.annotation_status == "valid"].copy()
    manifest = [json.loads(line) for line in (OUT / "annotation_input_manifest.jsonl").read_text().splitlines()]
    paths = {r["query_id"]: r["image_path"] for r in manifest}
    cases = {
        "lowest_geolocatability": valid.sort_values(["geolocatability_median", "query_id"]),
        "highest_geolocatability": valid.sort_values(["geolocatability_median", "query_id"], ascending=[False, True]),
        "largest_vlm_disagreement": valid.sort_values(["geolocatability_range", "query_id"], ascending=[False, True]),
        "high_technical_low_geolocatability": valid[(valid.technical_quality_median >= .75) &
                                                      (valid.geolocatability_median <= .4)].sort_values(["geolocatability_median", "query_id"]),
        "low_technical_high_geolocatability": valid[(valid.technical_quality_median <= .4) &
                                                     (valid.geolocatability_median >= .6)].sort_values(["geolocatability_median", "query_id"], ascending=[False, True]),
        "high_geolocatability_retrieval_failure": valid[(valid.failure_bucket == "retrieval_failure") &
                                                        (valid.geolocatability_median >= .6)].sort_values(["geolocatability_median", "query_id"], ascending=[False, True]),
        "low_geolocatability_baseline_correct": valid[(valid.failure_bucket == "correct") &
                                                      (valid.geolocatability_median <= .4)].sort_values(["geolocatability_median", "query_id"]),
    }
    for name, case in cases.items():
        render(name, case, paths)
    print(json.dumps({name: min(16, len(case)) for name, case in cases.items()}))


if __name__ == "__main__":
    run()
