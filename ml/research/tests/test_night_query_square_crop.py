import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import torch
from PIL import Image

from ml.research import night_query_square_crop as trial
from ml.retrieval.sage import SageVitBRetriever


def basis(count, index=0):
    result = np.zeros((count, 8448), np.float32)
    result[:, index] = 1
    return result


def test_square_crop_preserves_exact_center_pixels_after_exif_and_original_transform(tmp_path):
    pixels = np.arange(5 * 8 * 3, dtype=np.uint8).reshape(5, 8, 3)
    image = Image.fromarray(pixels)
    np.testing.assert_array_equal(np.asarray(trial.centered_square(image)), pixels[:, 1:6])
    square = image.crop((1, 0, 6, 5))
    transform = SageVitBRetriever(device="cpu")._build_transform()
    assert torch.equal(transform(square), transform(trial.centered_square(square)))
    path = tmp_path / "oriented.png"
    exif = Image.Exif()
    exif[274] = 6
    image.save(path, exif=exif)
    prepared = trial.load_rgb_image(path)
    assert prepared.size == (5, 8)
    expected = np.asarray(prepared)[1:6, :]
    np.testing.assert_array_equal(np.asarray(trial.centered_square(prepared)), expected)


def test_all61_square_queries_reuse_originals_and_resume_rejects_image_or_chunk_changes(tmp_path, monkeypatch):
    square, wide = tmp_path / "square.png", tmp_path / "wide.png"
    Image.new("RGB", (8, 8), "red").save(square)
    Image.new("RGB", (16, 8), "blue").save(wide)
    queries = pd.DataFrame([{"id": f"q{i}", "image_path": str(square if i < 61 else wide),
                             "file_sha256": trial.digest(square if i < 61 else wide)} for i in range(62)])
    original, calls = basis(62), []
    contract = {"retriever": {"pinned": True}}

    def embed(images):
        calls.extend(image.size for image in images)
        return basis(len(images), 1)

    model = SimpleNamespace(metadata=SimpleNamespace(to_dict=lambda: contract["retriever"]),
                            image_size=(322, 322), embed_batch=embed, close=lambda: None)
    monkeypatch.setattr(trial.gallery_scale, "create_model", lambda config: model)
    monkeypatch.setattr(trial, "state", lambda *args, **kwargs: None)
    output = tmp_path / "out"
    output.mkdir()
    vectors = trial.encode({}, queries, original, output, contract)
    np.testing.assert_array_equal(vectors[:61], original[:61])
    np.testing.assert_array_equal(vectors[61:], basis(1, 1))
    assert calls == [(8, 8)]
    receipt = json.loads((output / "descriptors.done.json").read_text())
    assert receipt["square_reused"] == 61 and receipt["new_encodes"] == 1
    before = trial.digest(output / "queries.npz")
    (output / "descriptors/._queries-0000.npz").write_bytes(b"AppleDouble")
    monkeypatch.setattr(trial.gallery_scale, "create_model", lambda *_: pytest.fail("resumed model load"))
    monkeypatch.setattr(trial, "load_rgb_image", lambda *_: pytest.fail("resumed image decode"))
    np.testing.assert_array_equal(trial.encode({}, queries, original, output, contract), vectors)
    assert trial.digest(output / "queries.npz") == before
    with pytest.raises(RuntimeError, match="checkpoint"):
        trial.encode({}, queries, original, output, contract | {"changed": True})
    original_bytes = square.read_bytes()
    square.write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="image bytes"):
        trial.encode({}, queries, original, output, contract)
    square.write_bytes(original_bytes)
    (output / "descriptors/queries-0000.npz").write_bytes(b"corrupt")
    with pytest.raises(RuntimeError, match="checkpoint"):
        trial.encode({}, queries, original, output, contract)


def test_mean3_uses_literal_float32_pair_score_and_gate_is_fixed():
    pair = np.array([[.21345679, -.3123445]], np.float32)
    crop = np.array([[.500235, -.333123]], np.float32)
    original = pair.copy()
    expected = pair.copy()
    expected *= np.float32(2)
    expected += crop
    expected /= np.float32(3)
    np.testing.assert_array_equal(trial.combine_scores(pair, crop), expected)
    np.testing.assert_array_equal(pair, original)
    with pytest.raises(RuntimeError, match="float32"):
        trial.combine_scores(pair.astype(float), crop)
    base = {"raw": {"accuracy_100m": .3}, "recall_at": {"100": .5}}
    for raw, recall, eligible in ((.302, .509, False), (.304, .5, True), (.3, .511, True)):
        result = {"raw": {"accuracy_100m": raw}, "recall_at": {"100": recall}}
        gate = trial.context_gate(base, result)
        assert gate["eligible_for_fixed_context"] is eligible
        assert not gate["context_implemented_or_started"]


def test_streamed_crop_scores_resume_and_pair_prefix_is_replayed(tmp_path, monkeypatch):
    source, before, out = (tmp_path / part for part in ("source", "before", "out"))
    for path in (source, before, out):
        path.mkdir()
    refs = basis(100)
    refs[10:] = basis(90, 1)
    shard = source / "refs.npy"
    np.save(shard, refs)
    gallery = pd.DataFrame({"id": [f"r{i}" for i in range(100)], "file_sha256": [f"sha{i}" for i in range(100)]})
    trial.save(source / "descriptor_pool.json", {"entries": {
        row.id: {"path": str(shard), "row": i, "image_sha256": row.file_sha256} for i, row in enumerate(gallery.itertuples())}})
    qcrop = np.concatenate([basis(1), basis(1, 1)])
    trial.atomic_npz(out / "queries.npz", vectors=qcrop)
    pair = np.tile(np.linspace(.8, -.8, 100, dtype=np.float32), (2, 1))
    np.save(before / "mean_scores.npy", pair)
    trial.atomic_npz(before / "prefix.npz", indices=np.argsort(-pair, kind="stable"))
    monkeypatch.setattr(trial, "state", lambda *a, **kw: None)
    first = trial.infer(source, gallery, before, qcrop, out, {})
    crop_scores = qcrop @ refs.T
    np.testing.assert_array_equal(first[trial.ARMS[0]], np.argsort(-crop_scores, kind="stable"))
    np.testing.assert_array_equal(first[trial.ARMS[1]], np.argsort(-trial.combine_scores(pair, crop_scores), kind="stable"))
    score_hash = trial.digest(out / "scores.npy")
    resumed = trial.infer(source, gallery, before, qcrop, out, {})
    for name in first:
        np.testing.assert_array_equal(resumed[name], first[name])
    assert trial.digest(out / "scores.npy") == score_hash
    trial.atomic_npz(before / "prefix.npz", indices=np.flip(np.argsort(-pair, kind="stable"), axis=1))
    with pytest.raises(RuntimeError, match="prefix replay"):
        trial.infer(source, gallery, before, qcrop, out, {})
    mapped = np.lib.format.open_memmap(out / "scores.npy", mode="r+")
    mapped[0, 0] = .123
    mapped.flush()
    mapped._mmap.close()
    with pytest.raises(RuntimeError, match="score columns"):
        trial.infer(source, gallery, before, qcrop, out, {})
