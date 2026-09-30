import numpy as np

from ml.research.night_fol import match


def test_single_mutual_match_is_counted():
    result = match(np.array([[1., 0.]], np.float32), np.array([[1., 0.]], np.float32), device="cpu")
    assert result == [1., 1., 1.]


def test_matching_ignores_padding_and_repeated_features():
    a = np.array([[1., 0.], [0., 1.]], np.float32)
    b = np.array([[1., 0.], [1., 0.], [0., 1.], [0., 0.]], np.float32)
    assert match(a, b, device="cpu")[0] == 2


def test_empty_or_below_threshold_matches_are_zero():
    assert match(np.empty((0, 2), np.float32), np.ones((1, 2), np.float32), device="cpu") == [0., 0., 0.]
    a, b = np.array([[1., 0.]], np.float32), np.array([[0., 1.]], np.float32)
    assert match(a, b, device="cpu") == [0., 0., 0.]
