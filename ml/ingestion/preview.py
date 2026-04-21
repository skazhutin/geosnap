"""Sanity preview for random manifest samples."""

from __future__ import annotations

import argparse
import math
import random
from pathlib import Path

import pandas as pd
from PIL import Image, ImageDraw


def run(manifest_path: Path, count: int, output_image: Path) -> None:
    df = pd.read_parquet(manifest_path)
    if len(df) == 0:
        raise RuntimeError("Manifest is empty")

    sampled = df.sample(n=min(count, len(df)), random_state=random.randint(1, 10_000))

    thumb_size = (320, 180)
    columns = 4
    rows = math.ceil(len(sampled) / columns)
    canvas = Image.new("RGB", (columns * thumb_size[0], rows * thumb_size[1]), color=(30, 30, 30))

    print("Preview samples:")
    for idx, (_, row) in enumerate(sampled.iterrows()):
        path = Path(str(row["image_path"]))
        lat = float(row["lat"])
        lon = float(row["lon"])
        print(f"- {path} | ({lat:.6f}, {lon:.6f})")

        if not path.exists():
            continue
        try:
            with Image.open(path) as image:
                image = image.convert("RGB")
                image.thumbnail(thumb_size)
                thumb = Image.new("RGB", thumb_size, color=(0, 0, 0))
                px = (thumb_size[0] - image.size[0]) // 2
                py = (thumb_size[1] - image.size[1]) // 2
                thumb.paste(image, (px, py))
                draw = ImageDraw.Draw(thumb)
                draw.rectangle((0, thumb_size[1] - 22, thumb_size[0], thumb_size[1]), fill=(0, 0, 0))
                draw.text((5, thumb_size[1] - 18), f"{lat:.4f}, {lon:.4f}", fill=(255, 255, 255))

                row_no = idx // columns
                col_no = idx % columns
                canvas.paste(thumb, (col_no * thumb_size[0], row_no * thumb_size[1]))
        except Exception:  # noqa: BLE001
            continue

    output_image.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(output_image)
    print(f"Saved preview collage: {output_image}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate random dataset preview collage")
    parser.add_argument("--manifest", default="data/raw/manifest.parquet")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--output-image", default="data/raw/preview.jpg")
    args = parser.parse_args()

    run(Path(args.manifest), count=args.count, output_image=Path(args.output_image))


if __name__ == "__main__":
    main()
