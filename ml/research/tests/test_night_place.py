import numpy as np
import pandas as pd

from ml.research.night_place import make_groups, rank, score_batch


def test_groups_require_pairwise_location_heading_and_independent_sequences():
    g = pd.DataFrame({"id": list("abcdefg"), "lat": [55.75] * 7,
        "lon": [37.6, 37.6002, 37.6004, 37.6, 37.6, 37.6, 37.6],
        "heading": [350, 10, 30, 350, np.nan, 350, 100],
        "sequence_key": ["mapillary::a", "mapillary::b", "mapillary::c", "mapillary::a",
                         "mapillary::e", "mapillary::None", "mapillary::g"]})
    groups = make_groups(g)
    assert groups[0] == [0, 1]  # 2 is near 1 but too far from 0; 3 repeats sequence.
    assert [4] in groups and [5] in groups and [6] in groups
    assert sorted(j for group in groups for j in group) == list(range(7))
    permutation = [4, 2, 6, 0, 1, 5, 3]
    shuffled = g.iloc[permutation].reset_index(drop=True)
    identities = [[g.id.iloc[j] for j in group] for group in groups]
    assert [[shuffled.id.iloc[j] for j in group] for group in make_groups(shuffled)] == identities


def test_cached_prototype_cosine_equals_direct_and_preserves_singletons():
    rng = np.random.default_rng(4)
    refs = rng.normal(size=(7, 12))
    refs /= np.linalg.norm(refs, axis=1, keepdims=True)
    q = rng.normal(size=(3, 12))
    q /= np.linalg.norm(q, axis=1, keepdims=True)
    groups = [[0, 3], [1], [2, 4, 5], [6]]
    sizes = np.array([len(g) for g in groups])
    flat = np.concatenate(groups)
    offsets = np.r_[0, sizes.cumsum()]
    owner = np.empty(7, dtype=int)
    owner[flat] = np.repeat(np.arange(4), sizes)
    sums = np.array([refs[g].sum(axis=0) for g in groups])
    norms = np.linalg.norm(sums, axis=1)
    norms[sizes == 1] = 1
    original = q @ refs.T
    actual = score_batch(original, flat, offsets, owner, norms, sizes)
    expected = (q @ (sums / np.linalg.norm(sums, axis=1, keepdims=True)).T)[:, owner]
    np.testing.assert_allclose(actual, expected, atol=1e-14)
    np.testing.assert_array_equal(actual[:, [1, 6]], original[:, [1, 6]])


def test_prototype_member_ties_choose_original_cosine_then_manifest_order():
    np.testing.assert_array_equal(rank(np.array([.8, .8, .6, .8]), np.array([.7, .9, .9, .9])), [1, 3, 0, 2])
