import json

import pandas as pd
import pytest

from ml.evaluation.moscow_split import _PerceptualHashIndex
from ml.research import night_quarantine_audit as audit


def references():
    return pd.DataFrame([
        dict(id="keep-a", source="mapillary", source_image_id="1", sequence_id="a", sequence_key="mapillary::a",
             file_sha256="sha-a", pixel_sha256="pixel-a", perceptual_hash="ffffffffffffffff", special=None),
        dict(id="remove", source="kartaview", source_image_id="2", sequence_id="b", sequence_key="kartaview::b",
             file_sha256="sha-b", pixel_sha256="pixel-b", perceptual_hash="5555555555555555", special="  literal  "),
        dict(id="keep-c", source="msls", source_image_id="msls:3", sequence_id="msls:c", sequence_key="mapillary::c",
             file_sha256="sha-c", pixel_sha256="pixel-c", perceptual_hash="aaaaaaaaaaaaaaaa", special="nan"),
    ])


def test_quarantine_is_exact_stable_view_with_reversible_mapping(tmp_path):
    original = references()
    unchanged = original.copy(deep=True)
    clean, mapping = audit.quarantine_view(original, {"kartaview::b"})
    assert mapping == {"original_count": 3, "clean_count": 2, "clean_to_original": [0, 2],
                       "original_to_clean": [0, -1, 1], "excluded_ids": ["remove"]}
    pd.testing.assert_frame_equal(original, unchanged)
    pd.testing.assert_frame_equal(clean, original.iloc[[0, 2]].reset_index(drop=True))
    audit.write_view(clean, tmp_path / "clean.parquet")
    pd.testing.assert_frame_equal(pd.read_parquet(tmp_path / "clean.parquet"), clean)
    corrupt = original.copy()
    corrupt.loc[1, "sequence_key"] = "kartaview::different"
    with pytest.raises(RuntimeError, match="sequence keys"):
        audit.quarantine_view(corrupt, {"kartaview::b"})


def test_original_gate_still_fails_new_baseline_implication_and_clean_gate_excludes_whole_sequence():
    baseline = references().iloc[:2]
    candidates = pd.DataFrame([
        dict(id="match", source="kartaview", source_image_id="10", sequence_key="kartaview::b"),
        dict(id="other-same-sequence", source="kartaview", source_image_id="11", sequence_key="kartaview::b"),
        dict(id="new", source="mapillary", source_image_id="12", sequence_key="mapillary::new"),
    ])
    hashes = ["0000000000000000", "f0f0f0f0f0f0f0f0", "cccccccccccccccc"]
    fps = {r.id: dict(decode_ok=True, file_sha256=r.id, pixel_sha256=r.id + "pixels",
                     perceptual_hash=h, phash_variants=[h])
           for r, h in zip(candidates.itertuples(), hashes, strict=True)}
    index = _PerceptualHashIndex(4)
    index.add(0, 0)
    guard = (set(), set(), set(), {"old-protected::x"}, index)
    with pytest.raises(RuntimeError, match="implicates"):
        audit.addition.filter_fingerprints(candidates, fps, baseline, guard)
    clean, _ = audit.quarantine_view(baseline, {"kartaview::b"})
    broad = (*guard[:3], guard[3] | {"kartaview::b"}, index)
    retained, exclusions = audit.addition.filter_fingerprints(candidates, fps, clean, broad)
    assert [r["id"] for r in retained] == ["new"]
    assert exclusions == {"query_identity_or_phash": 1, "prohibited_sequence": 1}


def test_byte_checkpoint_resume_avoids_rehash_and_rejects_changed_physical_file(tmp_path, monkeypatch):
    image = tmp_path / "reference.jpg"
    image.write_bytes(b"canonical unchanged bytes")
    selected = pd.DataFrame([dict(id="image", image_path=str(image))])
    fingerprints = {"image": {"file_sha256": audit.digest(image)}}
    real_digest = audit.digest
    calls = []

    def digest(path):
        if path == image:
            calls.append(path)
        return real_digest(path)

    monkeypatch.setattr(audit, "digest", digest)
    monkeypatch.setattr(audit, "state", lambda *args, **kwargs: None)
    audit.physical_hashes(tmp_path, selected, fingerprints)
    audit.physical_hashes(tmp_path, selected, fingerprints)
    assert len(calls) == 1
    assert json.loads((tmp_path / "physical_hashes.done.json").read_text())["images"] == 1
    image.write_bytes(b"different bytes")
    with pytest.raises(RuntimeError, match="checkpoint changed"):
        audit.physical_hashes(tmp_path, selected, fingerprints)


def test_identity_check_normalizes_msls_and_keeps_opaque_denylist_spellings():
    gallery = references()
    query = gallery.iloc[[2]].copy()
    query["source"] = "mapillary"
    query["source_image_id"] = "3"
    query["sequence_id"] = "c"
    query["id"] = "different-packaged-id"
    query["file_sha256"] = "reencoded"
    with pytest.raises(RuntimeError, match="development identities"):
        audit.development_checks(query, gallery)
    values = {"msls::msls:c", "kartaview::protected"}
    assert audit.normalized_forbidden(values) == values | {"mapillary::c"}
