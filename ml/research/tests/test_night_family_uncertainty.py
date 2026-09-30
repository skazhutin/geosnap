import numpy as np

from ml.research.night_family_uncertainty import bootstrap_differences


def test_cluster_bootstrap_preserves_unequal_group_weights_and_pairing():
    # Always-correct-vs-baseline differences stay +100pp despite unequal sizes.
    differences = np.array([[1, 1, 1, 1], [-1, -1, -1, -1], [0, 0, 0, 0], [1, -1, -1, -1]])
    samples, count = bootstrap_differences(differences, np.array(["a", "b", "b", "b"]),
                                           resamples=2000, seed=9)
    assert count == 2
    np.testing.assert_array_equal(samples[:, :3], np.broadcast_to([100, -100, 0], (2000, 3)))
    assert set(samples[:, 3]) == {-100, -50, 100}
    replay, _ = bootstrap_differences(differences, np.array(["a", "b", "b", "b"]), resamples=2000, seed=9)
    np.testing.assert_array_equal(samples, replay)
