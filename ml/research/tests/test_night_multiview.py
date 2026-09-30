import numpy as np

from ml.research.night_multiview import predict_bundle, regional_scores


def test_two_view_shortlist_never_uses_other_images():
    positions = np.array([[0, 0, 0], [100, 0, 0], [200, 0, 0]], dtype=float)
    scores = np.array([[1, 0, .6], [0, 1, .6], [0, 0, 1], [0, 0, 1]], dtype=float)
    first = predict_bundle(scores, positions, radius=1, depth=1)[0]["place_two90"]
    changed = scores.copy()
    changed[2:] = [[1, 0, 0], [0, 1, 0]]
    second = predict_bundle(changed, positions, radius=1, depth=1)[0]["place_two90"]
    np.testing.assert_array_equal(first, second)
    assert 2 not in first


def test_different_reference_views_can_support_the_same_place_without_count_votes():
    positions = np.array([[0, 0, 0], [5, 0, 0], [500, 0, 0], [1000, 0, 0]], dtype=float)
    scores = np.array([[.8, .1, .9, .1], [.1, .8, .1, .9]])
    candidates, evidence = regional_scores(scores, positions, radius=10)
    np.testing.assert_allclose(evidence[:, :2], [[.8, .8], [.8, .8]])
    assert candidates[np.argmax(evidence.mean(axis=0))] in [0, 1]
    # Duplicating a gallery photograph does not add independent support.
    expanded_positions = np.concatenate([positions, positions[:1]])
    expanded_scores = np.concatenate([scores, scores[:, :1]], axis=1)
    _, expanded = regional_scores(expanded_scores, expanded_positions, radius=10)
    np.testing.assert_allclose(expanded[:, :4], evidence)


def test_mean_descriptor_and_mean_cosine_have_identical_ranking():
    rng = np.random.default_rng(8)
    q, refs = rng.normal(size=(4, 12)), rng.normal(size=(20, 12))
    q /= np.linalg.norm(q, axis=1, keepdims=True)
    refs /= np.linalg.norm(refs, axis=1, keepdims=True)
    pooled = q.mean(axis=0)
    pooled /= np.linalg.norm(pooled)
    np.testing.assert_array_equal(np.argsort(-(q @ refs.T).mean(axis=0)), np.argsort(-(pooled @ refs.T)))
