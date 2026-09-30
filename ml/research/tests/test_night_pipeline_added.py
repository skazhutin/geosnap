import numpy as np
import pandas as pd
import pytest

from ml.research import night_pipeline_added as pipeline


def inputs(tmp_path):
    source, out, night = [tmp_path / name for name in ("source", "out", "night")]
    source.mkdir()
    out.mkdir()
    (night / "results").mkdir(parents=True)
    pipeline.save(night / "results/query322_504_mean_rows.json", {"frozen": True})
    rng = np.random.default_rng(19)
    scores322 = rng.normal(size=(3, 125)).astype(np.float32)
    scores504 = rng.normal(size=(3, 125)).astype(np.float32)
    np.save(source / "scores.npy", scores322)
    mean = .5 * (scores322 + scores504)
    prefix = np.argsort(-mean[:, :120], axis=1, kind="stable")[:, :100]
    data = (None, None, night, None, None, np.empty((3, 8448)), None,
            pd.DataFrame(index=range(120)), prefix, None, None, None)
    return source, out, data, scores504, mean


def test_added_mean_preserves_old_retrieval_and_resumes_without_full_recompute(tmp_path, monkeypatch):
    source, out, data, scores504, expected = inputs(tmp_path)
    monkeypatch.setattr(pipeline, "state", lambda *a, **kw: None)
    monkeypatch.setattr(pipeline, "exact_scores", lambda *a: scores504.copy())
    got = pipeline.mean_scores(data, source, pd.DataFrame(index=range(125)), {"entries": {}}, out, {})
    np.testing.assert_array_equal(got, np.argsort(-expected, axis=1, kind="stable")[:, :100])
    np.testing.assert_array_equal(np.load(out / "mean_scores.npy"), expected)
    monkeypatch.setattr(pipeline, "exact_scores", lambda *a: pytest.fail("cached scores recomputed"))
    np.testing.assert_array_equal(pipeline.mean_scores(data, source, None, {}, out, {}), got)
    matrix = np.load(out / "mean_scores.npy")
    matrix[0, 0] += 1
    np.save(out / "mean_scores.npy", matrix)
    with pytest.raises(RuntimeError, match="cache changed"):
        pipeline.mean_scores(data, source, None, {}, out, {})


def test_added_scores_fail_if_original_gallery_no_longer_replays(tmp_path, monkeypatch):
    source, out, data, scores504, _ = inputs(tmp_path)
    monkeypatch.setattr(pipeline, "state", lambda *a, **kw: None)
    scores504[:, 0] = 1000
    monkeypatch.setattr(pipeline, "exact_scores", lambda *a: scores504)
    with pytest.raises(RuntimeError, match="old-gallery top100 parity"):
        pipeline.mean_scores(data, source, pd.DataFrame(index=range(125)), {"entries": {}}, out, {})
    assert not (out / "mean_scores.done.json").exists()
