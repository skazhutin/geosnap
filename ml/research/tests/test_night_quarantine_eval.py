import numpy as np
import pandas as pd
import pytest

from ml.research.night_quarantine_eval import compose_scores, reuse_mask, subset_mapping


def test_common_quarantine_preserves_exact_old_scores_and_new_columns():
    rng = np.random.default_rng(923)
    old = rng.normal(size=(9, 18)).astype(np.float32)
    new = rng.normal(size=(9, 3)).astype(np.float32)
    positions = np.array([0, 1, 5, 9, 11, 17])
    actual = compose_scores(old, positions, new)
    assert np.array_equal(actual[:, :len(positions)], old[:, positions])
    assert np.array_equal(actual[:, len(positions):], new)
    assert np.array_equal(compose_scores(old, positions), old[:, positions])
    with pytest.raises(RuntimeError):
        compose_scores(old, positions, new[:-1])


def test_quarantine_mapping_rejects_reorder_or_changed_reference_coordinates():
    old = pd.DataFrame(dict(id=list("abcdef"), file_sha256=list("abcdef"), lat=np.arange(6.), lon=37.))
    selected = old.iloc[[0, 2, 3, 5]].reset_index(drop=True)
    assert subset_mapping(old, selected).tolist() == [0, 2, 3, 5]
    with pytest.raises(RuntimeError):
        subset_mapping(old, selected.iloc[::-1])
    selected.loc[2, "lat"] += .1
    with pytest.raises(AssertionError):
        subset_mapping(old, selected)


def test_context_reuse_follows_physical_ids_not_remapped_row_numbers():
    original = pd.DataFrame(dict(id=list("abcd")))
    clean = pd.DataFrame(dict(id=list("bcd")))
    original_choices = np.array([[1, 2], [2, 3], [0, 1]])
    clean_choices = np.array([[0, 1], [2, 1], [0, 1]])
    assert reuse_mask(clean, clean_choices, original, original_choices).tolist() == [True, False, False]
