import numpy as np
import pandas as pd

from ml.research.night_centering import centered_scores


def test_centering_from_cached_cosines_matches_direct_normalized_descriptors():
    rng = np.random.default_rng(91)
    refs, queries = rng.normal(size=(20, 8)), rng.normal(size=(5, 8))
    refs /= np.linalg.norm(refs, axis=1, keepdims=True)
    queries /= np.linalg.norm(queries, axis=1, keepdims=True)
    mean = refs.mean(axis=0)
    scores = queries @ refs.T
    actual = centered_scores(scores, scores.mean(axis=1), refs @ mean, mean @ mean)
    r, q = refs - mean, queries - mean
    r /= np.linalg.norm(r, axis=1, keepdims=True)
    q /= np.linalg.norm(q, axis=1, keepdims=True)
    np.testing.assert_allclose(actual, q @ r.T, atol=1e-14)


def test_zero_mean_preserves_cosine():
    scores = np.array([[.2, .7, -.1]])
    np.testing.assert_array_equal(centered_scores(scores, np.zeros(1), np.zeros(3), 0), scores)


def test_shard_receipts_roundtrip_and_resume_without_recomputing_sums(tmp_path, monkeypatch):
    from ml.research import gallery_scale, night_centering, night_v7
    from ml.research.gallery_scale_storage import digest, save

    previous, night, out = [tmp_path / name for name in ("previous", "night", "center")]
    for p in (previous / "manifests", night, out):
        p.mkdir(parents=True)
    rng = np.random.default_rng(92)
    refs, queries = rng.normal(size=(128, 8448)).astype(np.float32), rng.normal(size=(8, 8448)).astype(np.float32)
    refs /= np.linalg.norm(refs, axis=1, keepdims=True)
    queries /= np.linalg.norm(queries, axis=1, keepdims=True)
    vector_path = previous / "vectors.npy"
    np.save(vector_path, refs)
    g = pd.DataFrame({"id": [f"r{i}" for i in range(len(refs))], "file_sha256": [f"fixture{i}" for i in range(len(refs))]})
    gp, pp = previous / "manifests/G2_smart.parquet", previous / "descriptor_pool.json"
    g.to_parquet(gp, index=False)
    save(pp, {"entries": {r.id: {"path": str(vector_path), "row": i, "image_sha256": r.file_sha256} for i, r in enumerate(g.itertuples())}})
    np.save(night / "base_scores.npy", queries @ refs.T)
    save(night / "input_contract.json", {"gallery_sha256": digest(gp), "pool_sha256": digest(pp)})
    cfg = {"query_count": len(queries)}
    monkeypatch.setattr(gallery_scale, "query_vectors", lambda _: (None, queries))
    monkeypatch.setattr(night_centering, "paths", lambda: (cfg, {}, None, previous, night))
    monkeypatch.setattr(night_v7, "paths", lambda: (cfg, {}, None, previous, night))
    monkeypatch.setattr(night_v7, "record_result", lambda *a, **k: None)
    night_centering.run_trial(cfg, previous, night, out, {})
    receipt = next((out / "sums").glob("*.npz"))
    before = receipt.stat().st_mtime_ns
    with np.load(receipt, allow_pickle=False) as cached:
        assert str(cached["source_path"]) == str(vector_path)
        np.testing.assert_allclose(cached["sum"], refs.sum(axis=0, dtype=float))
    night_centering.run_trial(cfg, previous, night, out, {})
    assert receipt.stat().st_mtime_ns == before
