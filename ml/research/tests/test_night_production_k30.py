import numpy as np
import pytest

from ml.research.night_production_k30 import compare_orders


def test_K30_parity_distinguishes_identical_order_ties_and_changed_boundary_members():
    full = np.tile(np.arange(30), (3, 1))
    actual = full.copy()
    actual[1, -2:] = actual[1, -2:][::-1]
    actual[2, -1] = 30
    score = np.zeros((3, 30), np.float32)
    original_score = score.copy()
    score[0, 5] = np.float32(1e-7)
    result = compare_orders(actual, full, score, original_score)
    assert result["identical_order_rows"] == 1
    assert result["different_order_rows"] == 2
    assert result["different_top1_rows"] == 0
    assert result["same_members_different_order_rows"] == 1
    assert result["max_score_difference_same_order"] == float(np.float32(1e-7))
    with pytest.raises(RuntimeError, match="aligned"):
        compare_orders(actual[:, :20], full, score, original_score)
    score[0, 0] = np.nan
    with pytest.raises(RuntimeError, match="finite"):
        compare_orders(actual, full, score, original_score)
