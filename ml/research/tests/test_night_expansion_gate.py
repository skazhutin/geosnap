from ml.research.night_expansion_gate import passes


def test_expansion_needs_both_useful_gain_and_positive_cluster_interval():
    assert not passes({"gain_pp": .49, "ci95_pp": [.2, .8]})
    assert not passes({"gain_pp": 1.2, "ci95_pp": [-.01, 2.]})
    assert not passes({"gain_pp": 1.2, "ci95_pp": [0., 2.]})
    assert passes({"gain_pp": .5, "ci95_pp": [.01, 1.]})
