import numpy as np
import pandas as pd
import torch

from ml.research.reference_adaptation import ResidualMetric, training_pairs
from ml.research.vector_evaluation import distances


def test_initial_adapter_preserves_retrieval_and_receives_gradients():
    torch.manual_seed(7)
    x = torch.nn.functional.normalize(torch.randn(8, 16), dim=-1)
    model = ResidualMetric(16, rank=4)
    y, _ = model(x)
    torch.testing.assert_close(y @ y.T, x @ x.T)
    ((y[0] - y[1]) ** 2).sum().backward()
    assert model.up.weight.grad.abs().sum() > 0


def test_reference_mining_respects_capture_and_geographic_labels():
    rng = np.random.default_rng(7)
    matrix = rng.normal(size=(256, 16)).astype(np.float32)
    matrix /= np.linalg.norm(matrix, axis=1, keepdims=True)
    frame = pd.DataFrame(
        {
            "lat": [55.7 + (i // 8) * 0.005 + (i % 8) * 0.00001 for i in range(256)],
            "lon": 37.6,
            "source": "reference",
            "sequence_id": [str(i % 2) for i in range(256)],
            "heading": 0,
            "h3_coarse": [str(i // 8) for i in range(256)],
        }
    )
    pairs = training_pairs(frame, matrix, [str(i) for i in range(256)])
    coords = np.radians(frame[["lat", "lon"]].to_numpy())
    for i in pairs["anchors"]:
        d = distances(frame.iloc[i].lat, frame.iloc[i].lon, coords[:, 0], coords[:, 1])
        positive, negative = pairs["positive"][str(i)], pairs["negative"][str(i)]
        assert np.all(d[positive] <= 25)
        assert all(frame.iloc[j].sequence_id != frame.iloc[i].sequence_id for j in positive)
        assert len(set(negative)) == 8 and np.all(d[negative] >= 200)
