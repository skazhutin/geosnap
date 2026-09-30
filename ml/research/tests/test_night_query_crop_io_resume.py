import hashlib
import json

import numpy as np
import pandas as pd
import pytest

from ml.research import night_query_crop_io_resume as repair


def inputs(tmp_path, with_legacy):
    source, out = tmp_path / "source", tmp_path / "out"
    source.mkdir()
    out.mkdir()
    random = np.random.default_rng(31)
    refs = random.normal(size=(520, 8448)).astype(np.float32)
    refs /= np.linalg.norm(refs, axis=1, keepdims=True)
    queries = refs[[13, 73, 93]].copy()
    np.savez(out / "queries.npz", vectors=queries)
    gallery = pd.DataFrame({"id": [f"r{i}" for i in range(len(refs))], "file_sha256": [f"sha{i}" for i in range(len(refs))]})
    entries, positions, expected = {}, {}, np.zeros((len(queries), len(refs)), np.float32)
    for shard in range(2):
        dest = np.arange(shard, len(refs), 2)
        path = source / f"refs{shard}.npy"
        values = refs[dest]
        np.save(path, values)
        positions[str(path)] = np.column_stack([dest, np.arange(len(dest))]).astype(np.int64)
        for i, j in enumerate(dest):
            entries[f"r{j}"] = {"path": str(path), "row": i, "image_sha256": f"sha{j}"}
        for start in range(0, len(dest), 256):
            expected[:, dest[start:start + 256]] = queries @ values[start:start + 256].T
    repair.save(source / "descriptor_pool.json", {"entries": entries})
    if with_legacy:
        first = str(source / "refs0.npy")
        pos = positions[first]
        partial = np.full_like(expected, np.nan)
        partial[:, pos[:, 0]] = expected[:, pos[:, 0]]
        np.save(out / "scores.npy", partial)
        repair.save(out / "score_chunks.json", {"inputs": {"contract_sha256": repair.original.hash_json({}),
            "query_sha256": repair.digest(out / "queries.npz")}, "chunks": {first: {
                "source": {"sha256": repair.digest(first), "positions_sha256": hashlib.sha256(pos.tobytes()).hexdigest()},
                "score_sha256": repair.original.score_columns_hash(partial, pos[:, 0])}}})
    return source, out, gallery, queries, expected


@pytest.mark.parametrize("with_legacy", [False, True])
def test_sequential_repair_preserves_exact_math_committed_columns_and_resume(tmp_path, monkeypatch, with_legacy):
    source, out, gallery, queries, expected = inputs(tmp_path, with_legacy)
    monkeypatch.setattr(repair.trial, "state", lambda *a, **kw: None)
    repair.sequential_scores(source, gallery, np.arange(len(gallery)), queries, out, {})
    np.testing.assert_array_equal(np.load(out / "scores.npy"), expected)
    completed = json.loads((out / "io_runtime_repair.completed.json").read_text())
    assert completed["original_committed_shards_reused"] == int(with_legacy)
    if with_legacy:
        assert (out / "scores.before_io_repair.npy").exists()
        assert json.loads((out / "score_parts/part-0000.json").read_text())["reused_original_columns"]

    class NoMatmul(np.ndarray):
        def __matmul__(self, other):
            pytest.fail("committed score parts were recomputed")

    repair.sequential_scores(source, gallery, np.arange(len(gallery)), queries.view(NoMatmul), out, {})
    np.testing.assert_array_equal(np.load(out / "scores.npy"), expected)
    resumed = json.loads((out / "io_runtime_repair.completed.json").read_text())
    assert resumed["original_committed_shards_reused"] == int(with_legacy)
    (out / "score_parts/part-0001.npy").write_bytes(b"corruption")
    with pytest.raises(RuntimeError, match="score part changed"):
        repair.sequential_scores(source, gallery, np.arange(len(gallery)), queries, out, {})
