import numpy as np
import pytest

from ml.research.night_quarantine_report import context_full_ranks, diagnosis


def test_diagnosis_preserves_uncovered_queries_and_distinguishes_selection_from_retrieval():
    before = {"positive_ranks": [None, 150, 2, 1], "errors_m": [1000, 1000, 700, 10]}
    after = {"positive_ranks_through100": [1, 2, 1, 2], "errors_m": [20, 300, 30, 1000]}
    result = diagnosis(before, after, np.array([False, True, True, True]), np.ones(4, bool))
    assert result["transition_matrix_before_to_after"] == [[0, 0, 0, 1], [0, 0, 1, 0], [0, 0, 0, 1], [0, 0, 1, 0]]
    assert result["raw_gained"]["100"] == 2 and result["raw_lost"]["100"] == 1
    assert result["new_coverage_count"] == result["newly_covered_correct"] == 1
    assert result["catastrophic_recovered"] == 3 and result["catastrophic_introduced"] == 1


def test_context_full_ranks_retain_exact_tail_and_uncovered_denominator():
    prefix = np.tile(np.arange(100), (4, 1))
    changed = prefix.copy()
    changed[:, :30] = changed[:, :30][:, ::-1]
    before = {"query_ids": list("abcd"), "top100_gallery_rows": prefix.tolist(), "positive_ranks": [1, 50, 450, None]}
    after = {"query_ids": list("abcd"), "top100_gallery_rows": changed.tolist(), "positive_ranks_through100": [30, 50, None, None]}
    assert context_full_ranks(before, after) == [30, 50, 450, None]
    after["top100_gallery_rows"][2][99] = 300
    with pytest.raises(RuntimeError, match="first30"):
        context_full_ranks(before, after)
