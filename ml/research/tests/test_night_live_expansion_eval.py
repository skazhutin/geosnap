import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from ml.research import night_live_expansion_eval as expansion


def test_compatible_existing_descriptors_are_reused_but_identity_mismatch_fails():
    added = pd.DataFrame(dict(id=["a", "b"], file_sha256=["a-sha", "b-sha"]))
    entries = {"a": {"image_sha256": "a-sha"}}
    assert expansion.missing_references(added, entries).id.tolist() == ["b"]
    with pytest.raises(RuntimeError, match="identity"):
        expansion.missing_references(added, {"a": {"image_sha256": "changed"}})


def test_score_merge_preserves_original_float32_scores_and_gallery_order(tmp_path):
    base = np.random.default_rng(1).random((9, 7), dtype=np.float32)
    new = np.random.default_rng(2).random((9, 3), dtype=np.float32)
    original = base.copy()
    path = tmp_path / "scores.npy"
    expansion.merge_scores(base, new, path)
    actual = np.load(path)
    assert actual.dtype == np.float32 and actual.shape == (9, 10)
    np.testing.assert_array_equal(actual[:, :7], original)
    np.testing.assert_array_equal(actual[:, 7:], new)
    np.testing.assert_array_equal(base, original)
    with pytest.raises(RuntimeError, match="rows/dtypes"):
        expansion.merge_scores(base, new[:-1], path)


def test_encoder_skips_existing_reference_and_resume_never_reencodes(tmp_path, monkeypatch):
    image = tmp_path / "new.jpg"
    image.write_bytes(b"fixed synthetic source identity; encoder mocked")
    added = pd.DataFrame([dict(id="cached", file_sha256="old", image_path="never-open-existing"),
                          dict(id="new", file_sha256=expansion.digest(image), image_path=str(image))])
    pool = {"contract_sha256": "descriptor", "entries": {"cached": {"image_sha256": "old", "path": "old.npy", "row": 0}}}
    contract = {"descriptor_contract_sha256": "descriptor", "retriever": {"fixed": True}}
    calls = []

    def embed(images):
        calls.extend(images)
        values = np.zeros((len(images), 8448), np.float32)
        values[:, 0] = 1
        return values

    model = SimpleNamespace(metadata=SimpleNamespace(to_dict=lambda: contract["retriever"]), embed_batch=embed, close=lambda: None)
    monkeypatch.setattr(expansion.gallery_scale, "create_model", lambda cfg: model)
    monkeypatch.setattr(expansion.acquisition, "state", lambda *args, **kwargs: None)
    expansion.encode({}, tmp_path, added, pool, contract)
    assert calls == [str(image)]
    receipt = json.loads((tmp_path / "encode.done.json").read_text())
    assert receipt["newly_encoded"] == receipt["already_compatible_added_descriptors_reused"] == 1
    monkeypatch.setattr(expansion.gallery_scale, "create_model", lambda *_: pytest.fail("resume loaded encoder"))
    expansion.encode({}, tmp_path, added, pool, contract)
    (tmp_path / "descriptors/chunk-0000.npy").write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="descriptor bytes"):
        expansion.encode({}, tmp_path, added, pool, contract)
