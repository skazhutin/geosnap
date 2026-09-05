"""Shared production query-image quality diagnostics.

The API and offline Moscow evaluation must use the same bounded signals when
computing localization confidence.  This module deliberately contains no web
or model dependencies so both paths can call one implementation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from PIL import Image, ImageFilter, ImageStat


@dataclass(frozen=True, slots=True)
class QueryImageQuality:
    sharpness: float
    brightness: float
    exposure: float

    @property
    def confidence_signal(self) -> float:
        """Match the production service's interpretable confidence input."""

        return (self.sharpness + self.exposure) / 2.0


def measure_query_image_quality(
    image: Image.Image,
    *,
    diagnostic_max_edge: int = 512,
) -> QueryImageQuality:
    """Measure the exact bounded diagnostics used by the online API."""

    if diagnostic_max_edge < 1:
        raise ValueError("diagnostic_max_edge must be positive")
    rgb = image.convert("RGB")
    if rgb.width > diagnostic_max_edge or rgb.height > diagnostic_max_edge:
        scale = min(diagnostic_max_edge / rgb.width, diagnostic_max_edge / rgb.height)
        diagnostic = rgb.resize(
            (max(1, round(rgb.width * scale)), max(1, round(rgb.height * scale))),
            Image.Resampling.BILINEAR,
        )
    else:
        diagnostic = rgb
    gray = diagnostic.convert("L")
    brightness = ImageStat.Stat(gray).mean[0] / 255.0
    histogram = gray.histogram()
    exposure = sum(histogram[8:248]) / max(1, gray.width * gray.height)
    edges = gray.filter(ImageFilter.FIND_EDGES)
    if edges.width > 2 and edges.height > 2:
        edges = edges.crop((1, 1, edges.width - 1, edges.height - 1))
    edge_variance = ImageStat.Stat(edges).var[0]
    sharpness = 1.0 - math.exp(-edge_variance / 1000.0)
    return QueryImageQuality(
        sharpness=max(0.0, min(1.0, sharpness)),
        brightness=max(0.0, min(1.0, brightness)),
        exposure=max(0.0, min(1.0, exposure)),
    )
