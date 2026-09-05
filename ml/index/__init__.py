"""Compatibility imports for the former ``ml.index`` package name."""

from ml.indexing import (  # noqa: F401
    FaissExactIndex,
    FaissIndexError,
    FaissIndexWorker,
    FaissUnavailableError,
    RetrievalResult,
    build_index_from_embedding_artifacts,
)

__all__ = [
    "FaissExactIndex",
    "FaissIndexError",
    "FaissIndexWorker",
    "FaissUnavailableError",
    "RetrievalResult",
    "build_index_from_embedding_artifacts",
]
