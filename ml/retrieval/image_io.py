"""Safe image coercion shared by retrievers and offline jobs."""

from __future__ import annotations

from pathlib import Path

import numpy as np
from PIL import Image, ImageOps

from .base import ImageInput, RetrieverError


def load_rgb_image(value: ImageInput) -> Image.Image:
    """Decode an input, apply EXIF orientation, and return a detached RGB image."""

    try:
        if isinstance(value, (str, Path)):
            with Image.open(value) as opened:
                opened.load()
                return ImageOps.exif_transpose(opened).convert("RGB").copy()
        if isinstance(value, Image.Image):
            value.load()
            return ImageOps.exif_transpose(value).convert("RGB").copy()
        array = np.asarray(value)
        if array.ndim not in (2, 3):
            raise ValueError(f"expected HxW or HxWxC array, got {array.shape}")
        if array.dtype != np.uint8:
            if np.issubdtype(array.dtype, np.floating) and array.size and array.max() <= 1.0:
                array = array * 255.0
            array = np.clip(array, 0, 255).astype(np.uint8)
        return Image.fromarray(array).convert("RGB")
    except RetrieverError:
        raise
    except Exception as exc:  # Pillow exposes several decoder-specific exceptions
        raise RetrieverError(f"cannot decode image input: {exc}") from exc
