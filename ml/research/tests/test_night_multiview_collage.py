import json
from copy import deepcopy
from types import SimpleNamespace

import h3
import numpy as np
import pandas as pd
import pytest
from PIL import Image

from ml.research import night_multiview_collage as collage


def test_collage_preserves_all_four_tiles_and_cyclic_case_order():
    colors = [(255, 0, 0), (0, 255, 0), (0, 0, 255), (255, 255, 0)]
    views = [Image.new("RGB", (322, 322), color) for color in colors]
    for start in range(4):
        result = collage.make_collage(views, start)
        assert result.size == (644, 644)
        for position, point in enumerate(((10, 10), (332, 10), (10, 332), (332, 332))):
            assert result.getpixel(point) == colors[(start + position) % 4]
    with pytest.raises(RuntimeError):
        collage.make_collage(views[:3], 0)


def test_population_keeps_exact_existing_exclusions_and_parent_order(monkeypatch):
    monkeypatch.setitem(collage.SETUP, "physical_parents", 1)
    monkeypatch.setitem(collage.SETUP, "eligible_references", 2)
    g = pd.DataFrame(dict(id=["parent", "sibling", "r1", "r2"], sequence_key=["a", "a", "b", "c"],
                          file_sha256=["p", "s", "1", "2"], lat=[55.] * 4, lon=[37.] * 4))
    prepared = {"forbidden_sequences": ["a"], "parents": 1, "direction_queries": 4, "remaining_references": 2}
    records = [dict(gallery_row=0, id="parent", sequence_key="a", sha256="p", h3_group=h3.latlng_to_cell(55., 37., 6))]
    collage.validate_population(g, np.array([0]), np.array([2, 3]), records, prepared)
    with pytest.raises(RuntimeError, match="exclusions"):
        collage.validate_population(g, np.array([0]), np.array([1, 3]), records, prepared)
    with pytest.raises(RuntimeError, match="identity"):
        collage.validate_population(g, np.array([0]), np.array([2, 3]), [records[0] | {"sha256": "changed"}], prepared)


def test_encoding_resume_keeps_query_bytes_and_never_reencodes_parent(tmp_path, monkeypatch):
    image = tmp_path / "parent.png"
    Image.new("RGB", (32, 16), "red").save(image)
    g = pd.DataFrame([dict(id="p", image_path=str(image), file_sha256=collage.digest(image))])
    calls = []
    vectors = np.zeros((4, 8448), np.float32)
    vectors[:, 0] = 1
    contract = {"retriever": {"fixed": True}}

    def load(config):
        calls.append(config)
        return SimpleNamespace(metadata=SimpleNamespace(to_dict=lambda: contract["retriever"]), image_size=(322, 322),
                               embed_batch=lambda images: vectors.copy(), close=lambda: None)

    monkeypatch.setattr(collage.gallery_scale, "create_model", load)
    monkeypatch.setattr(collage.gallery_scale, "perspective", lambda image, yaw, size: Image.new("RGB", (size, size), "blue"))
    monkeypatch.setattr(collage, "state", lambda *a, **kw: None)
    collage.encode({}, g, np.array([0]), tmp_path, contract)
    before = collage.digest(tmp_path / "queries.npz")
    assert calls == [{"device": "mps", "batch_size": 2, "torch_threads": 1}]
    monkeypatch.setattr(collage.gallery_scale, "create_model", lambda *_: pytest.fail("resume reloaded encoder"))
    monkeypatch.setattr(collage.gallery_scale, "perspective", lambda *_: pytest.fail("resume regenerated committed views"))
    collage.encode({}, g, np.array([0]), tmp_path, contract)
    assert collage.digest(tmp_path / "queries.npz") == before
    image.write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="physical panorama"):
        collage.encode({}, g, np.array([0]), tmp_path, contract)


def test_score_resume_reuses_verified_columns_and_rejects_corruption(tmp_path, monkeypatch):
    previous, out = tmp_path / "previous", tmp_path / "out"
    previous.mkdir()
    out.mkdir()
    refs = np.zeros((3, 8448), np.float32)
    refs[np.arange(3), np.arange(3)] = 1
    shard = previous / "vectors.npy"
    np.save(shard, refs)
    g = pd.DataFrame(dict(id=["a", "b", "c"], file_sha256=["a-sha", "b-sha", "c-sha"]))
    collage.save(previous / "descriptor_pool.json", {"entries": {
        row.id: {"path": str(shard), "row": i, "image_sha256": row.file_sha256} for i, row in enumerate(g.itertuples())}})
    qv, eligible = refs[1:].copy(), np.array([1, 2])
    collage.atomic_npz(out / "queries.npz", vectors=qv)
    monkeypatch.setattr(collage, "state", lambda *a, **kw: None)
    collage.compute_scores(previous, g, eligible, qv, out, {})
    np.testing.assert_array_equal(np.load(out / "scores.npy"), np.eye(2, dtype=np.float32))
    original_load = np.load

    def checked_load(path, *args, **kwargs):
        if str(path) == str(shard):
            pytest.fail("committed shard was recomputed")
        return original_load(path, *args, **kwargs)

    monkeypatch.setattr(np, "load", checked_load)
    collage.compute_scores(previous, g, eligible, qv, out, {})
    mapped = np.lib.format.open_memmap(out / "scores.npy", mode="r+")
    mapped[0, 0] = .5
    mapped.flush()
    mapped._mmap.close()
    with pytest.raises(RuntimeError, match="score columns"):
        collage.compute_scores(previous, g, eligible, qv, out, {})


def test_verification_rejects_parent_leak_or_comparator_order_change():
    g = pd.DataFrame(dict(lat=[55.] * 101, lon=[37.] * 101))
    parents, eligible = np.array([0]), np.arange(1, 101)
    scores = np.zeros((4, 100), np.float32)
    rows = [dict(parent=0, yaw=yaw, top100_gallery_rows=eligible.tolist(), error_m=0., positive_rank_through100=1)
            for yaw in collage.SETUP["yaws"]]
    before = [dict(parent=0, yaw=row["yaw"], methods={name: deepcopy(row) for name in ("single", "place_four")}) for row in rows]
    errors, _, groups = collage.verify_rows(g, parents, eligible, rows, scores, before)
    assert errors == [0.] * 4 and len(set(groups)) == 1
    changed = deepcopy(rows)
    changed[0]["top100_gallery_rows"][0] = 0
    with pytest.raises(RuntimeError, match="excluded"):
        collage.verify_rows(g, parents, eligible, changed, scores, before)
    before[0]["yaw"] = 90
    with pytest.raises(RuntimeError, match="order"):
        collage.verify_rows(g, parents, eligible, rows, scores, before)


def test_cutoff_is_rechecked_after_gpu_queues_and_input_validation(tmp_path, monkeypatch):
    night = tmp_path / "night"
    night.mkdir()
    monkeypatch.setattr(collage, "LOCAL", tmp_path)
    monkeypatch.setattr(collage, "paths", lambda: ({}, {}, None, tmp_path, night))
    monkeypatch.setattr(collage.os, "getpriority", lambda *_: 10)
    monkeypatch.setattr(collage, "state", lambda *a, **kw: None)
    locks = []
    monkeypatch.setattr(collage.fcntl, "flock", lambda handle, *_: locks.append(handle.name))
    calls = []

    def cutoff(*args):
        assert len(locks) == 4
        calls.append(True)
        return len(calls) == 1

    monkeypatch.setattr(collage, "allowed_to_start", cutoff)
    monkeypatch.setattr(collage, "load_inputs", lambda *args: (None, None, None, None, None, {}))
    monkeypatch.setattr(collage, "encode", lambda *args: pytest.fail("encoding started after cutoff"))
    collage.run()
    out = night / "aux_multiview_collage"
    assert not (out / "collage.started.json").exists()
    assert "input validation" in json.loads((out / "closed.json").read_text())["reason"]
    assert [p.rsplit("/", 1)[-1] for p in locks] == ["collage.lock", "acquisition_queue.lock", "followthrough.lock", "controller.lock"]
