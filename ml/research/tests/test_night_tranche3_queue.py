import json

import pytest

from ml.research import night_tranche3_queue as queue


def test_source_pin_rejects_edits_before_child_launch(tmp_path, monkeypatch):
    source = tmp_path / 'worker.py'
    source.write_text('original')
    pins = {str(source): queue.digest(source)}
    monkeypatch.setattr(queue, 'source_contract', lambda: pins)
    expected = queue.register(tmp_path, {'cutoff': 'fixed'})
    queue.assert_sources(expected['sources'])
    source.write_text('changed')
    with pytest.raises(RuntimeError, match='source changed'):
        queue.assert_sources(expected['sources'])


def test_waiting_for_audit_never_counts_as_start_or_launches_after_cutoff(tmp_path, monkeypatch):
    out = tmp_path / 'third'
    out.mkdir()
    monkeypatch.setattr(queue, 'stage', lambda: ({'cutoff': 'past'}, out))
    monkeypatch.setattr(queue, 'source_contract', lambda: {})
    monkeypatch.setattr(queue.evaluator, 'LOCAL', tmp_path)
    monkeypatch.setattr(queue.evaluator, 'allowed_to_start', lambda *args: False)
    monkeypatch.setattr(queue.subprocess, 'Popen', lambda *args, **kwargs: pytest.fail('launched after cutoff'))
    monkeypatch.setattr(queue.os, 'nice', lambda *args: None)
    monkeypatch.setattr(queue, 'state', lambda *args, **kwargs: None)
    queue.run()
    closed = json.loads((out / 'evaluation_queue/closed.json').read_text())
    assert 'cutoff' in closed['reason'] and closed['new_evaluation_started'] is False
    assert not (out / 'evaluation.started.json').exists()


def test_mutable_queue_artifacts_stay_outside_evaluator_root_snapshot(tmp_path, monkeypatch):
    expected = {'sources': {str(queue.Path(queue.evaluation.__file__).resolve()): 'source'}}
    queue.save(tmp_path / 'report.json', {'verified': True})
    queue.save(tmp_path / 'evaluation.done.json', {'contract': {'source_sha256': 'source'},
        'artifacts': {'report.json': queue.digest(tmp_path / 'report.json')}})
    monkeypatch.setattr(queue.evaluator, 'LOCAL', tmp_path / 'local')
    queue.complete(tmp_path, expected)
    assert (tmp_path / 'evaluation_queue/done.json').exists()
    assert (tmp_path / 'evaluation_queue/status.json').exists()
    assert not (tmp_path / 'evaluation_queue.status.json').exists()
    queue.complete(tmp_path, expected)
