import numpy as np
import pytest
import torch

from ml.research import night_scale_context as trial


def basis(index, count=1):
    result = np.zeros((count, 8448), np.float32)
    result[:, index] = 1
    return result


def test_normalized_mean_attention_uses_original_unscaled_cosines_for_blend():
    q322, q504 = basis(0), basis(1)
    mean = trial.mean_queries(q322, q504)
    np.testing.assert_allclose(np.linalg.norm(mean, axis=1), 1, atol=1e-7)
    refs = np.concatenate([basis(0, 15), basis(1, 15)])
    calls = []

    def identity_encoder(features):
        calls.append(features.shape)
        return features

    contextual, original = trial.one_query(identity_encoder, mean[0], q322[0], q504[0], refs)
    assert calls == [torch.Size([31, 11, 768])]
    np.testing.assert_allclose(contextual, 1 / np.sqrt(2), atol=2e-7)
    np.testing.assert_array_equal(original, np.full(30, .5, np.float32))
    with pytest.raises(RuntimeError, match="opposing"):
        trial.mean_queries(q322, -q322)


def test_reranking_preserves_full_top100_membership_tail70_and_stable_ties():
    prefix = np.arange(100)[None, :]
    old = np.zeros((1, 30), np.float32)
    changed = trial.rank_prefix(prefix, np.arange(30, dtype=np.float32)[None, :], old)
    assert changed[0, 0] == 29
    np.testing.assert_array_equal(changed[:, 30:], prefix[:, 30:])
    np.testing.assert_array_equal(np.sort(changed), prefix)
    np.testing.assert_array_equal(trial.rank_prefix(prefix, old, old), prefix)


def test_committed_query_blocks_resume_without_attention_and_reject_corruption(tmp_path, monkeypatch):
    monkeypatch.setitem(trial.SETUP, "checkpoint_queries", 2)
    monkeypatch.setattr(trial, "state", lambda *args, **kwargs: None)
    chosen = np.array([[4, 8], [4, 9], [8, 9]])
    remap = np.array([[0, 1], [0, 2], [1, 2]])
    q322, q504, refs = basis(0, 3), basis(1, 3), basis(0, 3)
    means = trial.mean_queries(q322, q504)
    calls = []

    def predict(encoder, mean, left, right, references):
        calls.append(len(references))
        return np.array([.2, .3], np.float32), np.array([.4, .5], np.float32)

    monkeypatch.setattr(trial, "one_query", predict)
    args = (None, ["q0", "q1", "q2"], q322, q504, means, refs, remap, chosen, tmp_path, {"fixed": True})
    first = trial.context_values(*args)
    assert calls == [2, 2, 2]
    hashes = {p.name: trial.digest(p) for p in (tmp_path / "blocks").glob("*.npz")}
    monkeypatch.setattr(trial, "one_query", lambda *args: pytest.fail("committed query was recomputed"))
    resumed = trial.context_values(*args)
    for actual, expected in zip(resumed, first, strict=True):
        np.testing.assert_array_equal(actual, expected)
    assert hashes == {p.name: trial.digest(p) for p in (tmp_path / "blocks").glob("*.npz")}
    with pytest.raises(RuntimeError, match="checkpoint"):
        trial.context_values(*args[:-1], {"fixed": False})
    (tmp_path / "blocks/queries-0000.npz").write_bytes(b"corrupt")
    with pytest.raises(RuntimeError, match="checkpoint"):
        trial.context_values(*args)


def test_only_uncommitted_block_is_repeated_after_interruption(tmp_path, monkeypatch):
    monkeypatch.setitem(trial.SETUP, "checkpoint_queries", 2)
    monkeypatch.setattr(trial, "state", lambda *args, **kwargs: None)
    chosen = np.array([[4], [4], [4]])
    q322, q504, refs = basis(0, 3), basis(1, 3), basis(0)
    means = trial.mean_queries(q322, q504)
    calls = []

    def interrupted(*args):
        calls.append(True)
        if len(calls) == 3:
            raise RuntimeError("interrupted")
        return np.array([.1], np.float32), np.array([.2], np.float32)

    monkeypatch.setattr(trial, "one_query", interrupted)
    args = (None, ["a", "b", "c"], q322, q504, means, refs, np.zeros((3, 1), int), chosen, tmp_path, {})
    with pytest.raises(RuntimeError, match="interrupted"):
        trial.context_values(*args)
    assert (tmp_path / "blocks/queries-0000.json").exists()
    assert not (tmp_path / "blocks/queries-0002.json").exists()
    before = len(calls)
    trial.context_values(*args)
    assert len(calls) - before == 1
