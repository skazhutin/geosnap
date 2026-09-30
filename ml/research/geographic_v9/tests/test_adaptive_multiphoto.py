import numpy as np
import pytest

from ml.research.geographic_v9.adaptive_multiphoto import combine


def test_quality_weighting_prevents_bad_view_from_outvoting_good_view():
    # Photo 1 confidently matches a wrong place; photo 2 provides the good view.
    gps = np.array([[55.75, 37.60], [55.76, 37.62]])
    scores = np.array([[.10, .95], [.90, .10]], np.float32)
    result = combine(scores, gps, [.1, .9], shortlist_depth=2, output_depth=2)
    methods = result["methods"]
    assert methods["equal_score_mean"]["gallery_rows"][0] == 1
    assert methods["quality_weighted_score_mean"]["gallery_rows"][0] == 0
    assert methods["quality_weighted_geographic_consensus"]["gallery_rows"][0] == 0


def test_all_rejected_photos_cannot_force_a_location():
    with pytest.raises(ValueError, match="request another view"):
        combine([[.2, .8], [.7, .1]], [[55.75, 37.60], [55.76, 37.62]], [0., 0.])
