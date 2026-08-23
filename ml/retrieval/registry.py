"""Production retriever construction."""

from __future__ import annotations

from typing import Any

from .base import BaseRetriever
from .dinov2_salad import DinoV2SaladRetriever
from .megaloc import MegaLocRetriever


def create_retriever(name: str, **kwargs: Any) -> BaseRetriever:
    """Create a production retriever by stable configuration name."""

    normalized = name.lower().strip().replace("_", "-")
    if normalized in {"megaloc", "mega-loc"}:
        return MegaLocRetriever(**kwargs)
    if normalized in {"dinov2-salad", "dino-salad", "salad"}:
        return DinoV2SaladRetriever(**kwargs)
    raise ValueError(
        f"unknown production retriever {name!r}; expected 'megaloc' or 'dinov2-salad'"
    )
