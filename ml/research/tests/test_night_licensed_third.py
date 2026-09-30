import json

import numpy as np
import pandas as pd
import pytest

from ml.research import night_licensed_third as trial


def test_selected_third_keeps_prior_licensed_prefix_and_excludes_research_source(monkeypatch):
    rows = [dict(id=str(i), source=source, source_image_id=str(i), sequence_id=f"s{i}",
        production_compatible=source != "msls", license="CC BY-SA 4.0" if source != "msls" else "research",
        attribution="provider", source_url="https://example.test/ref", image_path=f"/canonical/{i}.jpg",
        file_sha256=str(i), lat=55., lon=37.) for i, source in enumerate(("mapillary", "msls", "kartaview", "mapillary"))]
    full = pd.DataFrame(rows)
    old = full.iloc[[0, 2]].reset_index(drop=True)
    for key, value in (("parent_licensed_references", 2), ("new_compatible_references", 1), ("selected_references", 3)):
        monkeypatch.setitem(trial.SETUP, key, value)
    selected, positions = trial.selection(full, old)
    assert selected.id.tolist() == ["0", "2", "3"] and positions.tolist() == [0, 2, 3]
    with pytest.raises(AssertionError):
        trial.selection(full, old.iloc[::-1].reset_index(drop=True))


@pytest.mark.parametrize("changed", [False, True])
def test_context_uses_previous_licensed_evidence_but_explicit_new_descriptor_pool(tmp_path, monkeypatch, changed):
    out, old_root, new_root = [tmp_path / name for name in ("licensed_third", "licensed_previous", "third")]
    for path in (out, old_root, new_root):
        path.mkdir()
    old = pd.DataFrame({"id": ["a", "b"]})
    selected = pd.DataFrame({"id": ["a", "b", "c"]})
    prefix = np.array([[2, 0, 1] if changed else [0, 1, 2]], np.int32)
    scores = np.array([[.5, .4, .6]], np.float32)
    old_contract = {"old_licensed": True}
    trial.licensed.atomic_npz(old_root / "context_evidence.npz", chosen=np.array([[0, 1]]), contextual=np.array([[.3, .2]], np.float32))
    trial.save(old_root / "context_evidence.json", {"inputs": {"contract_sha256": trial.context.signature(old_contract)},
        "sha256": trial.digest(old_root / "context_evidence.npz")})
    monkeypatch.setitem(trial.licensed.SETUP, "context_depth", 2)
    monkeypatch.setitem(trial.context.SETUP, "context_depth", 2)
    monkeypatch.setattr(trial, "state", lambda *a, **k: None)
    original_state = trial.licensed.state
    called = []

    def vectors(frame, indices, pool_root):
        assert changed and pool_root == new_root
        assert frame.id.tolist() == selected.id.tolist()
        called.append(indices.tolist())
        return np.zeros((len(indices), 1), np.float32)

    class Encoder:
        def load_state_dict(self, *a, **k):
            pass

        def eval(self):
            pass

    monkeypatch.setattr(trial.evaluator, "load_vectors", vectors)
    monkeypatch.setattr(trial.licensed.torch, "load", lambda *a, **k: {})
    monkeypatch.setattr(trial.licensed.torch.nn, "TransformerEncoder", lambda *a, **k: Encoder())
    monkeypatch.setattr(trial.licensed.torch.nn, "TransformerEncoderLayer", lambda *a, **k: object())
    new_values = np.array([.1, .7], np.float32)
    monkeypatch.setattr(trial.context, "one_query", lambda *a: (new_values, scores[0, prefix[0, :2]]))
    data = dict(out=out, source=new_root, parent=old_root, context_gallery=old, g=selected,
        full=pd.DataFrame({"id": ["unused-full-gallery"]}), contract={"source_contract": old_contract},
        q=pd.DataFrame({"id": ["q"]}), means=np.zeros((1, 1)), q322=np.zeros((1, 1)), q504=np.zeros((1, 1)), checkpoint="unused")
    ranked = trial.context_ranking(data, prefix, scores)
    values = new_values[None] if changed else np.array([[.3, .2]], np.float32)
    assert np.array_equal(ranked, trial.context.rank_prefix(prefix, values, scores[:, prefix[0, :2]]))
    assert np.array_equal(ranked[:, 2:], prefix[:, 2:])
    assert bool(called) == changed and trial.licensed.state is original_state
    receipt = json.loads((out / "context_evidence.json").read_text())
    assert receipt["computed_queries"] == int(changed)
    assert np.array_equal(trial.context_ranking(data, prefix, scores), ranked)
    (out / "context_evidence.npz").write_bytes(b"tampered")
    with pytest.raises(RuntimeError, match="checkpoint changed"):
        trial.context_ranking(data, prefix, scores)
    assert trial.licensed.state is original_state


def test_waiting_intent_does_not_start_after_cutoff(tmp_path, monkeypatch):
    out = tmp_path / "licensed_third"
    out.mkdir()
    monkeypatch.setattr(trial, "stage", lambda: ({"cutoff": "past"}, {}, tmp_path, tmp_path, tmp_path, out))
    monkeypatch.setattr(trial.evaluator, "LOCAL", tmp_path)
    monkeypatch.setattr(trial, "prepare", lambda: None)
    monkeypatch.setattr(trial, "register_implementation", lambda *a: {})
    monkeypatch.setattr(trial, "assert_sources", lambda *a: None)
    monkeypatch.setattr(trial.evaluator, "allowed_to_start", lambda *a: False)
    monkeypatch.setattr(trial, "load", lambda: pytest.fail("inputs loaded after cutoff"))
    monkeypatch.setattr(trial.os, "nice", lambda *a: None)
    trial.run(wait=True)
    assert (out / "closed.json").exists()
    assert not (out / "licensed_third.started.json").exists()
