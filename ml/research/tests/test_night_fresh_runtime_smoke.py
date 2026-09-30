import numpy as np
import pandas as pd
import pytest

from ml.research import night_fresh_runtime_smoke as smoke


def unit(seed, count):
    x = np.random.default_rng(seed).normal(size=(count, 8448)).astype(np.float32)
    return x / np.linalg.norm(x, axis=1, keepdims=True)


def test_fresh_vector_check_detects_cache_or_batch_dependence():
    actual = unit(7, 2)
    report = smoke.vector_parity(actual, actual.copy(), actual.copy(), actual.copy())
    assert report["verified"] and report["cached_max_abs"] == 0
    changed = actual.copy()
    changed[0, 0] += .01
    changed /= np.linalg.norm(changed, axis=1, keepdims=True)
    assert not smoke.vector_parity(actual, changed, actual, actual)["verified"]
    assert not smoke.vector_parity(actual, actual, changed, actual)["verified"]


def test_streamed_score_reference_row_mapping_and_identity_guard(tmp_path, monkeypatch):
    references, queries = unit(13, 8), unit(11, 4)
    path = tmp_path / "references.npy"
    np.save(path, references, allow_pickle=False)
    gallery = pd.DataFrame(dict(id=["a", "b", "c", "d"], file_sha256=["a", "b", "c", "d"]))
    source_rows = [7, 1, 5, 0]
    pool = {row.id: dict(path=str(path), row=j, image_sha256=row.file_sha256)
            for row, j in zip(gallery.itertuples(), source_rows, strict=True)}
    monkeypatch.setitem(smoke.SETUP, "reference_block_rows", 2)
    scores, shards = smoke.streamed_scores(gallery, queries, pool)
    np.testing.assert_allclose(scores, queries @ references[source_rows].T, atol=1e-7, rtol=1e-5)
    assert len(shards) == 1 and scores.shape == (4, 4)
    pool["b"]["image_sha256"] = "changed"
    with pytest.raises(RuntimeError, match="identity changed"):
        smoke.streamed_scores(gallery, queries, pool)


def test_parity_reports_tie_order_difference_without_hiding_correct_top1():
    scores = np.linspace(1, 0, 110, dtype=np.float32)[None, :]
    expected = scores.copy()
    expected[0, 10:12] = .8
    fresh = expected.copy()
    fresh[0, 11] += 1e-7
    prefix, report = smoke.ranking_parity(fresh, expected, [str(i) for i in range(110)])
    assert prefix.shape == (1, 100)
    assert report["same_top1"] == [True] and report["same_top100_set"] == [True]
    assert report["scores_within_tolerance"] and report["same_top100_order"] == [False]
