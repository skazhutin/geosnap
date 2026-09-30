import json
from datetime import datetime, timedelta

import pandas as pd
import pytest

from ml.research import night_reference_tranche3 as tranche


def gate_fixture(tmp_path, monkeypatch):
    for group in ('first', 'expanded2'):
        directory = tmp_path / 'evaluation' / group
        directory.mkdir(parents=True)
        (directory / 'context_rows.json').write_text('{}')
    rule = {'latest_network_start': (datetime.now().astimezone() + timedelta(hours=1)).isoformat(), 'maximum_new_references': 6000}
    monkeypatch.setitem(tranche.SETUP, 'latest_network_start', rule['latest_network_start'])
    tranche.save(tmp_path / 'expansion3_gate.plan.json', {'rule': rule})
    value = {'eligible': True, 'data_gate_passed': True, 'rule': rule,
             'registered_plan_sha256': tranche.digest(tmp_path / 'expansion3_gate.plan.json'),
             'paired_data_gain': {'gain_pp': .7, 'ci95_pp': [.1, 1.3]},
             'baseline_rows_sha256': tranche.digest(tmp_path / 'evaluation/first/context_rows.json'),
             'candidate_rows_sha256': tranche.digest(tmp_path / 'evaluation/expanded2/context_rows.json')}
    tranche.save(tmp_path / 'expansion3_gate.json', value)
    return value


def test_mandatory_gate_rejects_negative_data_or_changed_comparator(tmp_path, monkeypatch):
    value = gate_fixture(tmp_path, monkeypatch)
    assert tranche.gate(tmp_path, require_time=True)['eligible']
    tranche.save(tmp_path / 'expansion3_gate.json', value | {'eligible': False})
    with pytest.raises(RuntimeError, match='gate did not pass'):
        tranche.gate(tmp_path)
    tranche.save(tmp_path / 'expansion3_gate.json', value)
    (tmp_path / 'evaluation/first/context_rows.json').write_text('{"changed":true}')
    with pytest.raises(RuntimeError, match='input changed'):
        tranche.gate(tmp_path)


def test_local_reuse_requires_one_canonical_path_and_same_nonbenchmark_source_license(tmp_path, monkeypatch):
    store = tmp_path / 'store'
    store.mkdir()
    photo = store / 'photo.jpg'
    photo.write_bytes(b'physical')
    pool = pd.DataFrame([dict(id=str(i), source='mapillary', source_image_id=str(i), image_path='pending') for i in range(4)])
    known = pd.DataFrame([
        dict(id='0', source='mapillary', source_image_id='0', license='CC BY-SA 4.0', image_path=str(photo)),
        dict(id='1', source='msls', source_image_id='1', license='research-only', image_path=str(photo)),
        dict(id='2', source='mapillary', source_image_id='2', license='CC BY-SA 4.0', image_path=str(tmp_path / 'outside.jpg'))])
    (tmp_path / 'outside.jpg').write_bytes(b'outside')
    monkeypatch.setattr(tranche.acquisition, 'existing_names', lambda _: set())
    local, network, excluded = tranche.local_mapping(pool, [known], store)
    assert local.id.tolist() == ['0'] and local.image_path.tolist() == [str(photo)]
    assert network.id.tolist() == ['3']
    assert excluded == {'ambiguous_or_unmapped_existing_physical': 2}


def test_actual_first_http_dispatch_is_gated_and_wrapper_restores_globals(tmp_path, monkeypatch):
    value = gate_fixture(tmp_path, monkeypatch)
    out = tmp_path / 'tranche'
    out.mkdir()
    registered = {'selected_sha256': 'selection', 'gate_sha256': tranche.digest(tmp_path / 'expansion3_gate.json'),
                  'selected_count': 6, 'network_count': 4}
    data = ({}, {}, tmp_path, tmp_path, tmp_path, out, tmp_path)
    sent = []
    monkeypatch.setattr(tranche.requests.Session, 'request', lambda self, *args, **kwargs: sent.append(args))
    originals = tranche.acquisition.stage, tranche.acquisition.prepare, tranche.acquisition.status, tranche.requests.Session.request
    with tranche.download_context(data, registered):
        tranche.requests.Session().request('GET', 'https://example.com/reference')
        assert tranche.acquisition.prepare(6000)['selected_count'] == 4
    assert len(sent) == 1
    receipt = json.loads((out / 'first_http_dispatch.json').read_text())
    assert receipt['selected_sha256'] == 'selection' and 'url' not in receipt
    assert originals == (tranche.acquisition.stage, tranche.acquisition.prepare, tranche.acquisition.status, tranche.requests.Session.request)
    (out / 'first_http_dispatch.json').unlink()
    tranche.save(tmp_path / 'expansion3_gate.json', value | {'eligible': False})
    with tranche.download_context(data, registered), pytest.raises(RuntimeError, match='gate changed'):
        tranche.requests.Session().request('GET', 'https://example.com/reference')
    assert len(sent) == 1
