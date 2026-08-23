"""Retriever contracts shared by offline jobs and the online service."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

type ImageInput = str | Path | Image.Image | np.ndarray


class RetrieverError(RuntimeError):
    """Base error raised by a visual retriever."""


class ModelDependencyError(RetrieverError):
    """A required inference dependency is not installed."""


class ModelLoadError(RetrieverError):
    """Official model code or pretrained weights could not be loaded."""


class DescriptorError(RetrieverError):
    """A model returned an invalid descriptor matrix."""


@dataclass(frozen=True, slots=True)
class RetrieverMetadata:
    """Reproducibility metadata persisted beside embeddings and indexes."""

    model_name: str
    descriptor_dim: int
    device: str
    preprocessing: str
    checkpoint: str
    repository: str | None = None
    revision: str | None = None
    normalized: bool = True
    extra: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def l2_normalize(descriptors: np.ndarray, *, epsilon: float = 1e-12) -> np.ndarray:
    """Return a finite, contiguous float32 matrix with unit-length rows."""

    matrix = np.asarray(descriptors, dtype=np.float32)
    if matrix.ndim == 1:
        matrix = matrix.reshape(1, -1)
    if matrix.ndim != 2:
        raise DescriptorError(f"descriptors must be a 2-D matrix, got shape {matrix.shape}")
    if not np.isfinite(matrix).all():
        raise DescriptorError("descriptors contain NaN or infinite values")
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    if np.any(norms <= epsilon):
        rows = np.flatnonzero(norms[:, 0] <= epsilon).tolist()
        raise DescriptorError(f"zero-norm descriptors at rows {rows[:10]}")
    return np.ascontiguousarray(matrix / norms, dtype=np.float32)


class BaseRetriever(ABC):
    """Minimal model-agnostic global descriptor interface.

    Implementations must load weights once, batch inference, return explicit
    float32 descriptors, and never substitute random/untrained weights when
    loading fails.
    """

    model_name: str
    descriptor_dim: int

    def __init__(self) -> None:
        self._loaded = False
        self._device = "unloaded"

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    @property
    def device(self) -> str:
        return self._device

    @property
    def preprocessing(self) -> str:
        return self.metadata.preprocessing

    @property
    @abstractmethod
    def metadata(self) -> RetrieverMetadata:
        """Return exact model, preprocessing, checkpoint, and device metadata."""

    @abstractmethod
    def load(self) -> BaseRetriever:
        """Load official pretrained weights once and return ``self``."""

    @abstractmethod
    def embed_batch(self, images: Sequence[ImageInput]) -> np.ndarray:
        """Embed images into an ``(N, descriptor_dim)`` normalized matrix."""

    def close(self) -> None:
        """Release implementation resources and reset readiness."""

        self._loaded = False
        self._device = "unloaded"

    def embed_query(self, image: ImageInput) -> np.ndarray:
        """Embed one query and return a one-dimensional descriptor."""

        matrix = self.embed_batch([image])
        # ``embed_batch`` already owns normalization. Re-normalizing here can
        # introduce batch-size-dependent last-bit drift in deterministic jobs.
        matrix = self.validate_descriptors(matrix, expected_rows=1, normalize=False)
        return matrix[0]

    def require_loaded(self) -> None:
        if not self.is_loaded:
            raise ModelLoadError(
                f"{self.model_name} is not loaded; call load() during service startup"
            )

    def validate_descriptors(
        self,
        descriptors: np.ndarray,
        *,
        expected_rows: int | None = None,
        normalize: bool = True,
    ) -> np.ndarray:
        matrix = np.asarray(descriptors, dtype=np.float32)
        if matrix.ndim != 2:
            raise DescriptorError(f"expected 2-D descriptors, got shape {matrix.shape}")
        if expected_rows is not None and matrix.shape[0] != expected_rows:
            raise DescriptorError(
                f"expected {expected_rows} descriptors, got {matrix.shape[0]}"
            )
        if matrix.shape[1] != self.descriptor_dim:
            raise DescriptorError(
                f"{self.model_name} descriptor dimension mismatch: "
                f"expected {self.descriptor_dim}, got {matrix.shape[1]}"
            )
        if normalize:
            return l2_normalize(matrix)
        if not np.isfinite(matrix).all():
            raise DescriptorError("descriptors contain NaN or infinite values")
        return np.ascontiguousarray(matrix, dtype=np.float32)
