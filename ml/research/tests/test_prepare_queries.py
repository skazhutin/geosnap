import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

from ml.research.prepare_queries import RADIUS, assign


def pool():
    rows = []
    for y in range(20):
        for x in range(20):
            for image in range(2):
                rows.append(
                    {
                        "source": "mapillary",
                        "sequence_id": f"{x}-{y}",
                        "source_image_id": f"{x}-{y}-{image}",
                        "lat": 55.5 + y * 0.012 + image * 0.00001,
                        "lon": 37.3 + x * 0.015,
                    }
                )
    return pd.DataFrame(rows)


def test_assignment_is_sequence_disjoint_deterministic_and_embargoed():
    frame = pool()
    old = pd.DataFrame([{"lat": 55.56, "lon": 37.4}])
    targets = {"development": 40, "calibration": 30, "final": 40}
    splits, gallery, counts = assign(frame, old, targets)
    shuffled, _, _ = assign(frame.sample(frac=1, random_state=7), old, targets)
    assert all(counts[s]["selected"] > 0 for s in splits)
    for split, current in splits.items():
        assert current.source_image_id.tolist() == shuffled[split].source_image_id.tolist()
        assert len(current.sequence_id) == current.sequence_id.nunique()
        assert not set(current.sequence_key) & set(gallery.sequence_key)
        tree = BallTree(np.radians(current[["lat", "lon"]]), metric="haversine")
        for other, other_frame in splits.items():
            if other != split:
                distance, _ = tree.query(np.radians(other_frame[["lat", "lon"]]))
                assert distance.min() * RADIUS >= 250
        if split != "development":
            distance, _ = tree.query(np.radians(old[["lat", "lon"]]))
            assert distance.min() * RADIUS >= 250
