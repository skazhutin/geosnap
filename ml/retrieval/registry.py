"""Production retriever construction."""

from __future__ import annotations

from typing import Any

from .base import BaseRetriever
from .dinov2_salad import DinoV2SaladRetriever
from .megaloc import MegaLocRetriever
from .sage import SageVitBRetriever
from .selavprplusplus import SelaVPRPlusPlusBaseRetriever, SelaVPRPlusPlusRerankRetriever


def create_retriever(name: str, **kwargs: Any) -> BaseRetriever:
    """Create a production retriever by stable configuration name."""

    normalized = name.lower().strip().replace("_", "-")
    if normalized in {"megaloc", "mega-loc"}:
        return MegaLocRetriever(**kwargs)
    if normalized in {"dinov2-salad", "dino-salad", "salad"}:
        return DinoV2SaladRetriever(**kwargs)
    if normalized in {"sage-vitb", "sage", "sage-vit-b"}:
        return SageVitBRetriever(**kwargs)
    if normalized in {"selavprplusplus-base", "selavpr++-base", "selavprpp-base"}:
        return SelaVPRPlusPlusBaseRetriever(**kwargs)
    if normalized in {
        "selavprplusplus-base-rerank",
        "selavpr++-base-rerank",
        "selavprpp-base-rerank",
    }:
        return SelaVPRPlusPlusRerankRetriever(**kwargs)
    raise ValueError(
        f"unknown production retriever {name!r}; expected 'megaloc', 'dinov2-salad', "
        "'sage-vitb', 'selavprplusplus-base', or 'selavprplusplus-base-rerank'"
    )
