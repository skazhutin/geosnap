"""Deterministic TEST-ONLY descriptor extractor.

This module exists so unit/integration fixtures can exercise the complete image
to index path without downloading multi-hundred-megabyte checkpoints.  It is not
exported by :mod:`ml.retrieval`, cannot be constructed accidentally without an
explicit guard, and must never be selected as the production retriever.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
from PIL import Image

from .base import BaseRetriever, ImageInput, RetrieverMetadata, l2_normalize
from .image_io import load_rgb_image


class DeterministicFixtureRetriever(BaseRetriever):
    """Small deterministic color-grid descriptor for tests and fixtures only."""

    model_name = "TEST-ONLY-deterministic-color-grid-v1"
    descriptor_dim = 8 * 8 * 3

    def __init__(self, *, allow_test_only: bool = False) -> None:
        if not allow_test_only:
            raise RuntimeError(
                "DeterministicFixtureRetriever is TEST ONLY; pass allow_test_only=True "
                "from an explicit test/fixture"
            )
        super().__init__()

    @property
    def metadata(self) -> RetrieverMetadata:
        return RetrieverMetadata(
            model_name=self.model_name,
            descriptor_dim=self.descriptor_dim,
            device=self.device,
            preprocessing="TEST-ONLY-rgb-bilinear-8x8-centered-v1",
            checkpoint="none",
            normalized=True,
            extra={"test_only": True, "production_eligible": False},
        )

    def load(self) -> DeterministicFixtureRetriever:
        self._device = "cpu"
        self._loaded = True
        return self

    def embed_batch(self, images: Sequence[ImageInput]) -> np.ndarray:
        self.require_loaded()
        if not images:
            return np.empty((0, self.descriptor_dim), dtype=np.float32)
        rows: list[np.ndarray] = []
        for image in images:
            rgb = load_rgb_image(image)
            resized = rgb.resize((8, 8), resample=Image.Resampling.BILINEAR)
            row = np.asarray(resized, dtype=np.float32).reshape(-1) / 255.0 - 0.5
            # Normalize each input independently so output is bit-stable across
            # different fixture batch sizes (BLAS reduction order may differ).
            rows.append(l2_normalize(row)[0])
        return self.validate_descriptors(
            np.stack(rows), expected_rows=len(images), normalize=False
        )
