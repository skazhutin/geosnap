"""Small deterministic query-view aggregation policies for global VPR descriptors."""

from __future__ import annotations

from typing import Literal

import numpy as np
from PIL import Image

from .base import BaseRetriever, ImageInput, l2_normalize
from .image_io import load_rgb_image

type QueryAggregation = Literal["single", "five_crop_85"]
SUPPORTED_QUERY_AGGREGATIONS = ("single", "five_crop_85")


def _five_crop(image: Image.Image, fraction: float = 0.85) -> list[Image.Image]:
    width, height = image.size
    crop_width = max(1, round(width * fraction))
    crop_height = max(1, round(height * fraction))
    left = width - crop_width
    top = height - crop_height
    boxes = (
        (0, 0, crop_width, crop_height),
        (left, 0, width, crop_height),
        (0, top, crop_width, height),
        (left, top, width, height),
        (left // 2, top // 2, left // 2 + crop_width, top // 2 + crop_height),
    )
    return [image.crop(box) for box in boxes]


def embed_aggregated_query(
    retriever: BaseRetriever,
    image: ImageInput,
    *,
    policy: QueryAggregation | str = "single",
) -> np.ndarray:
    normalized_policy = str(policy).strip().lower()
    if normalized_policy not in SUPPORTED_QUERY_AGGREGATIONS:
        raise ValueError(f"unsupported query aggregation {policy!r}")
    if normalized_policy == "single":
        return retriever.embed_query(image)
    prepared = image.convert("RGB") if isinstance(image, Image.Image) else load_rgb_image(image)
    descriptors = retriever.embed_batch(_five_crop(prepared))
    descriptors = retriever.validate_descriptors(descriptors, expected_rows=5, normalize=True)
    return l2_normalize(np.mean(descriptors, axis=0, keepdims=True))[0]
