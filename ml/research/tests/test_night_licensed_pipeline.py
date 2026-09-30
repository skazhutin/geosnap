import json

import numpy as np
import pandas as pd
import pytest

from ml.research import night_licensed_pipeline as licensed


def gallery():
    rows = []
    for i, source in enumerate(("mapillary", "msls", "kartaview")):
        rows.append(dict(id=str(i), source=source, source_image_id=f"p{i}", sequence_id=f"s{i}",
            production_compatible=source != "msls", license="CC BY-SA 4.0" if source != "msls" else "research",
            attribution="provider", source_url="https://example.com/reference", image_path=f"/canonical/{i}.jpg",
            file_sha256=str(i), lat=55., lon=37.))
    return pd.DataFrame(rows)


def test_source_scope_is_single_exact_row_view_and_missing_provenance_fails():
    full = gallery()
    unchanged = full.copy(deep=True)
    selected = licensed.selected_gallery(full)
    pd.testing.assert_frame_equal(selected, full.iloc[[0, 2]].reset_index(drop=True))
    pd.testing.assert_frame_equal(full, unchanged)
    assert licensed.previous_eval.subset_mapping(full, selected).tolist() == [0, 2]
    full.loc[2, "attribution"] = ""
    with pytest.raises(RuntimeError, match="missing provenance"):
        licensed.selected_gallery(full)
    full = gallery()
    full.loc[1, "production_compatible"] = True
    with pytest.raises(RuntimeError, match="contradicts"):
        licensed.selected_gallery(full)


def test_identical_physical_context_reused_without_model_load_then_resume_is_hashed(tmp_path, monkeypatch):
    out, source = tmp_path / "licensed", tmp_path / "expanded"
    out.mkdir()
    source.mkdir()
    old = pd.DataFrame(dict(id=["excluded", "b", "c", "d"]))
    selected = old.iloc[1:].reset_index(drop=True)
    prefix = np.array([[0, 1, 2], [1, 0, 2]], np.int32)
    chosen = np.array([[1, 2], [2, 1]], np.int32)
    contextual = np.array([[.6, .2], [.1, .8]], np.float32)
    scores = np.array([[.8, .7, .2], [.65, .7, .3]], np.float32)
    monkeypatch.setitem(licensed.SETUP, "context_depth", 2)
    monkeypatch.setitem(licensed.context.SETUP, "context_depth", 2)
    old_contract = {"original": True}
    contract = {"source_contract": old_contract}
    licensed.atomic_npz(source / "context_evidence.npz", chosen=chosen, contextual=contextual)
    licensed.save(source / "context_evidence.json", {
        "inputs": {"contract_sha256": licensed.context.signature(old_contract)},
        "sha256": licensed.digest(source / "context_evidence.npz")})
    data = dict(out=out, source=source, g=selected, full=old, contract=contract)
    monkeypatch.setattr(licensed.evaluator, "load_vectors", lambda *a: pytest.fail("reused references loaded"))
    monkeypatch.setattr(licensed.torch, "load", lambda *a, **k: pytest.fail("reused context model loaded"))
    actual = licensed.context_ranking(data, prefix, scores)
    expected = licensed.context.rank_prefix(prefix, contextual, np.take_along_axis(scores, prefix[:, :2], axis=1))
    assert np.array_equal(actual, expected) and np.array_equal(actual[:, 2:], prefix[:, 2:])
    assert np.array_equal(licensed.context_ranking(data, prefix, scores), actual)
    receipt = json.loads((out / "context_evidence.json").read_text())
    assert receipt["reused_queries"] == 2 and receipt["computed_queries"] == 0
    (out / "context_evidence.npz").write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="checkpoint changed"):
        licensed.context_ranking(data, prefix, scores)


def test_waiting_preparation_does_not_bypass_actual_start_cutoff(tmp_path, monkeypatch):
    out = tmp_path / "licensed_pipeline"
    out.mkdir()
    monkeypatch.setattr(licensed, "stage", lambda: ({"cutoff": "past"}, {}, tmp_path, tmp_path, tmp_path, out))
    monkeypatch.setattr(licensed.evaluator, "LOCAL", tmp_path)
    monkeypatch.setattr(licensed.evaluator, "allowed_to_start", lambda *a: False)
    monkeypatch.setattr(licensed, "load", lambda: pytest.fail("inputs loaded after closed cutoff"))
    monkeypatch.setattr(licensed.os, "nice", lambda *a: None)
    licensed.run(wait=True)
    assert (out / "intent.json").exists()
    assert not (out / "licensed_pipeline.started.json").exists()
    assert "cutoff" in json.loads((out / "closed.json").read_text())["reason"]
