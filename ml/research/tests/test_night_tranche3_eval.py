import json

import numpy as np
import pandas as pd
import pytest

from ml.research import night_tranche3_eval as trial


def test_evaluate_reuses_literal_parent_scores_and_only_computes_new_reference_dots(tmp_path, monkeypatch):
    source, out = tmp_path / 'parent', tmp_path / 'third'
    source.mkdir()
    out.mkdir()
    (out / 'descriptors').mkdir()
    rng = np.random.default_rng(812)
    original = {'top1': rng.normal(size=(2, 120)).astype(np.float32),
                'mean': rng.normal(size=(2, 120)).astype(np.float32)}
    for arm, values in original.items():
        np.save(source / f'{arm}_scores.npy', values, allow_pickle=False)
    contract = {'fixed': 'registered'}
    trial.save(out / 'descriptor_pool.json', {'entries': {'new': 'unused-test-entry'}})
    trial.save(out / 'encode.done.json', {'contract': contract,
        'descriptor_pool_sha256': trial.digest(out / 'descriptor_pool.json'), 'chunks': {}})
    q322, q504 = object(), object()
    new322 = np.array([[.2], [.3]], np.float32)
    new504 = np.array([[.4], [.7]], np.float32)
    added = pd.DataFrame({'id': ['new']})
    calls = []

    def exact(frame, vectors, entries):
        assert frame is added and entries == {'new': 'unused-test-entry'}
        calls.append(vectors)
        return new322.copy() if vectors is q322 else new504.copy()

    captured = {}

    def record(data, arm, scores=None, prefix=None, mean_summary=None):
        if scores is not None:
            captured[arm] = scores.copy()
            prefix = np.argsort(-scores, axis=1, kind='stable')[:, :100]
        return {'arm': arm}, {'top100_gallery_rows': prefix.tolist()}, {'verified': True}

    monkeypatch.setattr(trial.gallery_scale, 'exact_scores', exact)
    monkeypatch.setattr(trial, 'record', record)
    monkeypatch.setattr(trial, 'context_ranking', lambda data, prefix, scores: prefix)
    monkeypatch.setattr(trial, 'state', lambda *args, **kwargs: None)
    data = dict(out=out, source=source, contract=contract, q322=q322, q504=q504, added=added,
                baseline=pd.DataFrame({'id': range(120)}))
    trial.evaluate(data)
    assert calls == [q322, q504]
    for arm in ('top1', 'mean'):
        assert np.array_equal(captured[arm][:, :120], original[arm])
    assert np.array_equal(captured['top1'][:, 120:], new322)
    assert np.array_equal(captured['mean'][:, 120:], .5 * (new322 + new504))
    trial.evaluate(data)
    assert calls == [q322, q504]
    path = out / 'mean_scores.npy'
    altered = np.load(path, allow_pickle=False)
    altered[0, 0] += .1
    np.save(path, altered, allow_pickle=False)
    with pytest.raises(RuntimeError, match='score bytes changed'):
        trial.verify_done(data)


def test_context_recomputes_only_changed_physical_top30_and_validates_resume(tmp_path, monkeypatch):
    out, source = tmp_path / 'third', tmp_path / 'parent'
    out.mkdir()
    source.mkdir()
    baseline = pd.DataFrame({'id': [f'ref-{i}' for i in range(120)]})
    gallery = pd.concat([baseline, pd.DataFrame({'id': ['new']})], ignore_index=True)
    old_chosen = np.tile(np.arange(30, dtype=np.int32), (2, 1))
    old_values = np.tile(np.linspace(-.5, .5, 30, dtype=np.float32), (2, 1))
    parent_contract = {'parent': 'fixed'}
    trial.atomic_npz(source / 'context_evidence.npz', chosen=old_chosen, contextual=old_values)
    trial.save(source / 'context_evidence.json', {
        'inputs': {'contract_sha256': trial.context.signature(parent_contract)},
        'sha256': trial.digest(source / 'context_evidence.npz')})
    prefix = np.tile(np.arange(100, dtype=np.int32), (2, 1))
    prefix[1, 0] = 120
    scores = np.tile(np.linspace(.8, .1, 121, dtype=np.float32), (2, 1))
    calls = []

    class Encoder:
        def __init__(self, *args, **kwargs):
            pass

        def load_state_dict(self, *args, **kwargs):
            pass

        def eval(self):
            pass

    def load_vectors(frame, positions, pool_root):
        assert frame is gallery and pool_root == out
        assert set(positions) == set(prefix[1, :30])
        return np.asarray(positions)[:, None]

    def one_query(encoder, mean, q322, q504, references):
        calls.append(mean)
        assert mean == 1
        positions = references[:, 0].astype(int)
        direct = scores[1, positions]
        return direct.copy(), direct.copy()

    monkeypatch.setattr(trial.evaluator, 'load_vectors', load_vectors)
    monkeypatch.setattr(trial.torch.nn, 'TransformerEncoder', Encoder)
    monkeypatch.setattr(trial.torch.nn, 'TransformerEncoderLayer', Encoder)
    monkeypatch.setattr(trial.torch, 'load', lambda *args, **kwargs: {})
    monkeypatch.setattr(trial.context, 'one_query', one_query)
    monkeypatch.setattr(trial, 'state', lambda *args, **kwargs: None)
    data = dict(out=out, source=source, gallery=gallery, baseline=baseline, checkpoint='unused',
        contract={'parent_contract': parent_contract}, q=pd.DataFrame({'id': ['q0', 'q1']}),
        means=[0, 1], q322=[0, 1], q504=[0, 1])
    ranked = trial.context_ranking(data, prefix, scores)
    assert calls == [1] and np.array_equal(ranked[:, 30:], prefix[:, 30:])
    with np.load(out / 'context_evidence.npz', allow_pickle=False) as saved:
        assert saved['reused'].tolist() == [True, False]
        assert np.array_equal(saved['contextual'][0], old_values[0])
    assert np.array_equal(trial.context_ranking(data, prefix, scores), ranked)
    assert calls == [1]
    receipt = json.loads((out / 'context_evidence.json').read_text())
    assert receipt['computed_queries'] == 1 and receipt['reused_queries'] == 1
    (out / 'context_evidence.npz').write_bytes(b'changed')
    with pytest.raises(RuntimeError, match='context evidence changed'):
        trial.context_ranking(data, prefix, scores)
