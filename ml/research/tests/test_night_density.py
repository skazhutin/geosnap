"""Density formula, reference-only sampling and integrity-preserving resumption."""
import numpy as np
import pandas as pd
import pytest

from ml.research import night_density as density


def test_landmark_sampling_is_permutation_invariant_and_missing_sequences_are_singletons():
    gallery = pd.DataFrame({"id": ["b", "a", "c", "d", "e"],
                            "sequence_key": ["mapillary::s", "mapillary::s", None, "mapillary::None", "kartaview::k"]})
    selected = density.select_landmarks(gallery, 4)
    assert set(gallery.id.iloc[selected]) == {"a", "c", "d", "e"}
    shuffled = gallery.iloc[[3, 0, 4, 1, 2]].reset_index(drop=True)
    assert shuffled.id.iloc[density.select_landmarks(shuffled, 4)].tolist() == gallery.id.iloc[selected].tolist()
    assert len(set(density.sequence_labels(gallery)[selected])) == 4
    with pytest.raises(RuntimeError, match="not enough"):
        density.select_landmarks(gallery, 5)


def test_density_excludes_own_sequence_not_only_the_identical_reference():
    landmarks = np.array([[1., 0.], [.96, .28], [.6, .8], [0., 1.]], dtype=np.float32)
    refs = landmarks[[0, 3]]
    values = density.density_values(refs, landmarks, ["a", "c"], ["a", "a", "b", "c"], neighbors=2)
    # First row excludes BOTH high-scoring sequence-a references, including itself.
    np.testing.assert_allclose(values, [.3, (.8 + .28) / 2], atol=1e-7)
    with pytest.raises(RuntimeError, match="insufficient"):
        density.density_values(refs[:1], landmarks, ["a"], ["a", "a", "b", "c"], neighbors=3)


def test_density_correction_matches_csls_ranking_algebra_without_using_query_population():
    cosine = np.array([[.91, .88, .1], [.2, .6, .8]], dtype=np.float32)
    reference_density = np.array([.9, .2, .1], dtype=np.float32)
    corrected = density.corrected_scores(cosine, reference_density)
    arbitrary_query_constant = np.array([.31, .99], dtype=np.float32)
    csls_form = 2 * cosine - reference_density[None, :] - arbitrary_query_constant[:, None]
    np.testing.assert_array_equal(np.argsort(-corrected, axis=1), np.argsort(-csls_form, axis=1))
    assert cosine[0].argmax() == 0 and corrected[0].argmax() == 1


def test_committed_density_partials_resume_without_recomputing_and_detect_corruption(tmp_path, monkeypatch):
    setup = density.SETUP | {"descriptor_dim": 8, "neighbors": 2, "reference_batch_size": 2}
    monkeypatch.setattr(density, "SETUP", setup)
    monkeypatch.setattr(density, "state", lambda *args, **kwargs: None)
    rng = np.random.default_rng(84)
    vectors = rng.normal(size=(8, 8)).astype(np.float32)
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    gallery = pd.DataFrame({"id": [f"r{i}" for i in range(8)], "file_sha256": [f"fp{i}" for i in range(8)],
                            "sequence_key": [f"mapillary::s{i}" for i in range(8)]})
    entries = {}
    paths = [tmp_path / "first.npy", tmp_path / "second.npy"]
    for offset, path in zip([0, 4], paths, strict=True):
        np.save(path, vectors[offset:offset + 4])
        for j in range(offset, offset + 4):
            entries[f"r{j}"] = {"path": str(path), "row": j - offset, "image_sha256": f"fp{j}"}
    selected = np.array([0, 3, 6], dtype=np.int32)
    first = density.calculate_density(gallery, entries, selected, tmp_path, {"fixture": True})
    receipts = list((tmp_path / "partials").glob("*.npz"))
    timestamps = {p: p.stat().st_mtime_ns for p in receipts}
    monkeypatch.setattr(density, "density_values", lambda *args, **kwargs: pytest.fail("completed density was recomputed"))
    resumed = density.calculate_density(gallery, entries, selected, tmp_path, {"fixture": True})
    np.testing.assert_array_equal(first, resumed)
    assert timestamps == {p: p.stat().st_mtime_ns for p in receipts}
    original = receipts[0].read_bytes()
    receipts[0].write_bytes(b"corrupted partial")
    with pytest.raises(RuntimeError, match="checkpoint or source"):
        density.calculate_density(gallery, entries, selected, tmp_path, {"fixture": True})
    receipts[0].write_bytes(original)
    # Change a non-landmark reference while keeping its descriptor unit-normalized.
    changed = vectors[4:].copy()
    changed[3] *= -1
    np.save(paths[1], changed)
    with pytest.raises(RuntimeError, match="checkpoint or source"):
        density.calculate_density(gallery, entries, selected, tmp_path, {"fixture": True})
