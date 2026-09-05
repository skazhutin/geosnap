from __future__ import annotations

import numpy as np
import pytest
from PIL import Image

from ml.retrieval.query_aggregation import embed_aggregated_query
from ml.retrieval.testing import DeterministicFixtureRetriever


def test_five_crop_query_aggregation_is_deterministic_and_normalized() -> None:
    pixels = np.random.default_rng(12).integers(0, 256, size=(80, 120, 3), dtype=np.uint8)
    image = Image.fromarray(pixels, mode="RGB")
    retriever = DeterministicFixtureRetriever(allow_test_only=True).load()

    first = embed_aggregated_query(retriever, image, policy="five_crop_85")
    second = embed_aggregated_query(retriever, image, policy="five_crop_85")

    np.testing.assert_array_equal(first, second)
    assert first.shape == (retriever.descriptor_dim,)
    assert np.linalg.norm(first) == pytest.approx(1.0)


def test_unknown_query_aggregation_fails_closed() -> None:
    retriever = DeterministicFixtureRetriever(allow_test_only=True).load()
    with pytest.raises(ValueError, match="unsupported"):
        embed_aggregated_query(retriever, Image.new("RGB", (20, 20)), policy="random")
