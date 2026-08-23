"""Visual place-recognition retrievers.

Production code should construct one of the official checkpoint-backed adapters
exported here.  The deterministic fixture retriever intentionally lives in
``ml.retrieval.testing`` and is not exported from this package.
"""

from .base import (
    BaseRetriever,
    DescriptorError,
    ModelDependencyError,
    ModelLoadError,
    RetrieverError,
    RetrieverMetadata,
)
from .dinov2_salad import DinoV2SaladRetriever
from .megaloc import MegaLocRetriever
from .registry import create_retriever

__all__ = [
    "BaseRetriever",
    "DescriptorError",
    "DinoV2SaladRetriever",
    "MegaLocRetriever",
    "ModelDependencyError",
    "ModelLoadError",
    "RetrieverError",
    "RetrieverMetadata",
    "create_retriever",
]
