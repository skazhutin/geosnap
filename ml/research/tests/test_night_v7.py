from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd

from ml.research.night_v7 import align_blend, allowed_to_start, diverse_prefix


def test_blend_preserves_baseline_at_zero_and_score_scale():
    old = np.array([[0.7, 0.6, 0.5]])
    new = np.array([[1.0, 5.0, 2.0]])
    np.testing.assert_array_equal(align_blend(old, new, 0), old)
    got = align_blend(old, new, 1)
    np.testing.assert_allclose(got.mean(1), old.mean(1))
    np.testing.assert_allclose(got.std(1), old.std(1))
    assert got.argmax() == 1


def test_diversity_uses_reference_sequences_and_places():
    g = pd.DataFrame({"id": list("abcd"), "lat": [55.75, 55.751, 55.75, 55.76],
                      "lon": [37.6] * 4, "sequence_key": ["a", "a", "b", "c"]})
    selected = diverse_prefix(np.array([[0, 1, 2, 3]]), g, 2, 25)
    np.testing.assert_array_equal(selected, [[0, 3]])


def test_cutoff_blocks_new_work_but_allows_finishing(tmp_path):
    cfg = {"cutoff": (datetime.now(UTC) - timedelta(hours=1)).isoformat()}
    assert not allowed_to_start(cfg, "context", tmp_path)
    (tmp_path / "context.started.json").write_text("{}")
    assert allowed_to_start(cfg, "context", tmp_path)
