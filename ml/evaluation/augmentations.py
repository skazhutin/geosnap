"""Deterministic robustness variants derived from a real held-out query image."""

from __future__ import annotations

import io
import random
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter

from ml.retrieval.base import ImageInput
from ml.retrieval.image_io import load_rgb_image


@dataclass(frozen=True, slots=True)
class AugmentedQuery:
    query_id: str
    variant: str
    lat: float
    lon: float
    image: Image.Image
    output_path: Path | None
    parameters: Mapping[str, Any]


def _motion_blur(image: Image.Image, size: int = 5) -> Image.Image:
    if size not in {3, 5}:
        raise ValueError("Pillow motion blur kernel size must be 3 or 5")
    kernel = [0.0] * (size * size)
    middle = size // 2
    for x in range(size):
        kernel[middle * size + x] = 1.0 / size
    return image.filter(ImageFilter.Kernel((size, size), kernel, scale=1.0))


def _temperature(image: Image.Image, warmth: float) -> Image.Image:
    array = np.asarray(image, dtype=np.float32)
    array[..., 0] *= 1.0 + warmth
    array[..., 2] *= 1.0 - warmth
    return Image.fromarray(np.clip(array, 0, 255).astype(np.uint8), mode="RGB")


def _perspective(image: Image.Image, fraction: float) -> Image.Image:
    width, height = image.size
    dx, dy = width * fraction, height * fraction
    # QUAD maps output corners to this source quadrilateral.  It gives a modest
    # deterministic viewpoint perturbation without inventing scene content.
    quad = (dx, dy, 0.0, height - dy, width - dx, height, width, 0.0)
    return image.transform(
        image.size,
        Image.Transform.QUAD,
        quad,
        resample=Image.Resampling.BICUBIC,
    )


def _safe_stem(value: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
    return stem or "query"


def generate_robustness_variants(
    source_image: ImageInput,
    *,
    query_id: str,
    lat: float,
    lon: float,
    output_dir: str | Path | None = None,
    seed: int = 0,
) -> list[AugmentedQuery]:
    """Create named variants while preserving the source ground-truth coordinate.

    The caller is responsible for supplying a genuinely real, held-out,
    geotagged source image.  The helper records derivation only; generated/AI
    imagery must not be passed off as geographic evaluation ground truth.
    """

    if not query_id.strip():
        raise ValueError("query_id is required")
    base = load_rgb_image(source_image)
    width, height = base.size
    if min(width, height) < 16:
        raise ValueError("source image is too small for robustness augmentation")
    rng = random.Random(seed)
    crop_fraction = 0.82
    crop_width, crop_height = int(width * crop_fraction), int(height * crop_fraction)
    left = rng.randint(0, width - crop_width)
    top = rng.randint(0, height - crop_height)

    jpeg_buffer = io.BytesIO()
    base.save(jpeg_buffer, format="JPEG", quality=45, optimize=False)
    jpeg_buffer.seek(0)
    with Image.open(jpeg_buffer) as jpeg_opened:
        jpeg_variant = jpeg_opened.convert("RGB").copy()

    occluded = base.copy()
    draw = ImageDraw.Draw(occluded)
    box_width, box_height = int(width * 0.22), int(height * 0.20)
    box_left = int(width * 0.62)
    box_top = int(height * 0.62)
    draw.rectangle(
        (box_left, box_top, box_left + box_width, box_top + box_height),
        fill=(110, 110, 110),
    )

    variant_values: list[tuple[str, Image.Image, dict[str, Any]]] = [
        (
            "resize_half",
            base.resize((max(8, width // 2), max(8, height // 2)), Image.Resampling.LANCZOS),
            {"scale": 0.5},
        ),
        (
            "random_crop",
            base.crop((left, top, left + crop_width, top + crop_height)).resize(
                (width, height), Image.Resampling.BICUBIC
            ),
            {"crop_fraction": crop_fraction, "left": left, "top": top, "seed": seed},
        ),
        ("jpeg_q45", jpeg_variant, {"quality": 45}),
        ("gaussian_blur", base.filter(ImageFilter.GaussianBlur(radius=1.2)), {"radius": 1.2}),
        ("motion_blur", _motion_blur(base, 5), {"kernel_size": 5}),
        ("brightness_low", ImageEnhance.Brightness(base).enhance(0.65), {"factor": 0.65}),
        ("warm_color", _temperature(base, 0.12), {"warmth": 0.12}),
        ("perspective", _perspective(base, 0.04), {"corner_fraction": 0.04}),
        (
            "partial_occlusion",
            occluded,
            {"box": [box_left, box_top, box_left + box_width, box_top + box_height]},
        ),
    ]

    destination = Path(output_dir) if output_dir is not None else None
    if destination is not None:
        destination.mkdir(parents=True, exist_ok=True)
    results: list[AugmentedQuery] = []
    safe_query_id = _safe_stem(query_id)
    for variant, image, parameters in variant_values:
        output_path = None
        if destination is not None:
            output_path = destination / f"{safe_query_id}__{variant}.jpg"
            image.save(output_path, format="JPEG", quality=92)
        results.append(
            AugmentedQuery(
                query_id=query_id,
                variant=variant,
                lat=float(lat),
                lon=float(lon),
                image=image,
                output_path=output_path,
                parameters=parameters,
            )
        )
    return results
