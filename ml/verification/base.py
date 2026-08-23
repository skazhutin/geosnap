"""Backend-independent geometric-verifier contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence

from .models import GeometricEvidence, ImageInput


class BaseGeometricVerifier(ABC):
    """Verify a bounded set of references against one query image.

    Implementations should extract query features once per call. They must not
    silently fall back to another backend or manufacture evidence when loading
    an optional dependency fails.
    """

    backend_name: str

    def __init__(self, *, max_pairs: int = 20) -> None:
        if max_pairs < 1:
            raise ValueError("max_pairs must be >= 1")
        self.max_pairs = max_pairs

    def _validate_pair_count(self, references: Sequence[ImageInput]) -> None:
        if len(references) > self.max_pairs:
            raise ValueError(
                f"geometric verification is bounded to {self.max_pairs} pairs per query; "
                f"received {len(references)}"
            )

    @abstractmethod
    def verify(
        self,
        query_image: ImageInput,
        reference_images: Sequence[ImageInput],
    ) -> tuple[GeometricEvidence, ...]:
        """Return one evidence record per reference, preserving input order."""

    def verify_pair(
        self,
        query_image: ImageInput,
        reference_image: ImageInput,
    ) -> GeometricEvidence:
        return self.verify(query_image, [reference_image])[0]
