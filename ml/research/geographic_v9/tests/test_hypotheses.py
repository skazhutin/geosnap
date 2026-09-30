import numpy as np

from ml.research.geographic_v9.hypotheses import cluster_candidates


def test_nearby_chain_does_not_merge_distant_hypotheses():
    # A 60 m bridge must not merge locations 120 m apart into one hypothesis.
    lat = 55.75
    delta = 60 / 6371008.8 * 180 / np.pi
    coords = np.array([[lat, 37.6], [lat+delta, 37.6], [lat+2*delta, 37.6]])
    groups = cluster_candidates(np.array([0, 1, 2]), np.array([3., 2., 1.]), coords)
    assert len(groups) == 2
    assert sorted(len(group) for group in groups) == [1, 2]
