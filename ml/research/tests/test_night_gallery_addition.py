import numpy as np
import pandas as pd
import pytest
from PIL import Image

from ml.evaluation.moscow_split import _PerceptualHashIndex
from ml.research import night_gallery_addition as addition
from ml.research.night_gallery_addition import filter_fingerprints, fingerprint_file


def baseline():
    return pd.DataFrame([dict(id="base", source="msls", source_image_id="legacy", sequence_key="mapillary::base",
        file_sha256="sha-base", pixel_sha256="pixel-base", perceptual_hash="0000000000000000")])


def candidate(identity, sequence):
    return dict(id=identity, source="mapillary", source_image_id=identity, sequence_key="mapillary::" + sequence)


def fp(identity, phash="ffffffffffffffff"):
    return dict(decode_ok=True, file_sha256="sha-" + identity, pixel_sha256="pixel-" + identity,
                perceptual_hash=phash, phash_variants=[phash])


def test_query_match_excludes_every_frame_of_sequence_without_query_coordinates():
    rows = pd.DataFrame([candidate("leak", "same"), candidate("sibling", "same"), candidate("good", "other")])
    hashes = {i: fp(i) for i in rows.id}
    guard = ({"sha-leak"}, set(), set(), set(), _PerceptualHashIndex(4))
    kept, counts = filter_fingerprints(rows, hashes, baseline(), guard)
    assert [r["id"] for r in kept] == ["good"]
    assert counts["query_identity_or_phash"] == counts["prohibited_sequence"] == 1


def test_cross_source_identity_sha_and_rotated_phash_are_excluded():
    rows = pd.DataFrame([candidate("legacy", "a"), candidate("sha", "b"), candidate("rotated", "c"), candidate("good", "d")])
    hashes = {i: fp(i) for i in rows.id}
    hashes["sha"]["file_sha256"] = "sha-base"
    hashes["rotated"]["phash_variants"].append("0000000000000000")
    guard = (set(), set(), set(), set(), _PerceptualHashIndex(4))
    kept, counts = filter_fingerprints(rows, hashes, baseline(), guard)
    assert [r["id"] for r in kept] == ["good"]
    assert counts["exact_reference_duplicate"] == 2
    assert counts["reference_phash_duplicate"] == 1


def test_real_fingerprint_contains_rotation_and_mirror_hashes(tmp_path):
    from imagehash import phash

    pixels = np.random.default_rng(5).integers(0, 256, size=(90, 120, 3), dtype=np.uint8)
    path = tmp_path / "reference.png"
    image = Image.fromarray(pixels)
    image.save(path)
    result = fingerprint_file(path)
    assert result["decode_ok"] and len(result["phash_variants"]) == 8
    assert str(phash(image.transpose(Image.Transpose.ROTATE_90))) in result["phash_variants"]
    assert str(phash(image.transpose(Image.Transpose.FLIP_LEFT_RIGHT))) in result["phash_variants"]


def test_new_evidence_implicating_baseline_sequence_fails_instead_of_rewriting_baseline():
    rows = pd.DataFrame([candidate("leak", "base")])
    guard = ({"sha-leak"}, set(), set(), set(), _PerceptualHashIndex(4))
    with pytest.raises(RuntimeError, match="baseline needs scientific review"):
        filter_fingerprints(rows, {"leak": fp("leak")}, baseline(), guard)


def test_legacy_baseline_overlap_still_excludes_new_frames_without_false_new_incident():
    rows = pd.DataFrame([candidate("old-sequence", "base"), candidate("good", "other")])
    guard = (set(), set(), set(), {"mapillary::base"}, _PerceptualHashIndex(4))
    kept, counts = filter_fingerprints(rows, {i: fp(i) for i in rows.id}, baseline(), guard)
    assert [r["id"] for r in kept] == ["good"]
    assert counts["prohibited_sequence"] == 1


def test_rotated_protected_ledger_image_is_rejected_without_opening_heldout_images():
    rows = pd.DataFrame([candidate("rotated", "a"), candidate("sibling", "a")])
    hashes = {i: fp(i) for i in rows.id}
    hashes["rotated"]["phash_variants"].append("aaaaaaaaaaaaaaaa")
    index = _PerceptualHashIndex(4)
    index.add(0, int("aaaaaaaaaaaaaaaa", 16))
    kept, counts = filter_fingerprints(rows, hashes, baseline(), (set(), set(), set(), set(), index))
    assert kept == []
    assert counts["query_identity_or_phash"] == counts["prohibited_sequence"] == 1


def test_audited_gallery_tampering_and_prefix_reordering_fail_before_encoding(tmp_path):
    out, previous, night = (tmp_path / n for n in ("addition", "previous", "night"))
    out.mkdir()
    (previous / "manifests").mkdir(parents=True)
    night.mkdir()
    base = pd.DataFrame(dict(id=["a", "b"], file_sha256=["sha-a", "sha-b"], lat=[55., 56.], lon=[37., 38.]))
    extra = pd.DataFrame(dict(id=["c"], file_sha256=["sha-c"], lat=[55.1], lon=[37.1]))
    base.to_parquet(previous / "manifests/G2_smart.parquet", index=False)
    extra.to_parquet(out / "addition.parquet", index=False)
    pd.concat([base, extra], ignore_index=True).to_parquet(out / "gallery.parquet", index=False)
    addition.save(previous / "descriptor_contract.json", {"model": "test"})
    base_sha = addition.digest(previous / "manifests/G2_smart.parquet")
    addition.save(night / "input_contract.json", {"gallery_sha256": base_sha})
    addition.save(out / "gallery_addition.started.json", {"contract": {"base_sha256": base_sha,
        "source_sha256": addition.digest(addition.__file__),
        "descriptor_contract_sha256": addition.digest(previous / "descriptor_contract.json")}})
    receipt = {"addition_sha256": addition.digest(out / "addition.parquet"), "gallery_sha256": addition.digest(out / "gallery.parquet")}
    addition.save(out / "audit.done.json", receipt)
    addition.verify_audited(out, previous, night)
    pd.concat([base.iloc[::-1], extra], ignore_index=True).to_parquet(out / "gallery.parquet", index=False)
    with pytest.raises(RuntimeError, match="manifest changed"):
        addition.verify_audited(out, previous, night)
    addition.save(out / "audit.done.json", receipt | {"gallery_sha256": addition.digest(out / "gallery.parquet")})
    with pytest.raises(AssertionError):
        addition.verify_audited(out, previous, night)


def test_parent_resolution_is_cached_but_leaf_symlink_escape_still_rejected(tmp_path, monkeypatch):
    from pathlib import Path

    store, outside = tmp_path / "store", tmp_path / "outside"
    parent = store / "provider"
    parent.mkdir(parents=True)
    outside.write_bytes(b"outside")
    paths = [parent / f"{i}.jpg" for i in range(5)]
    for path in paths:
        path.write_bytes(b"owned")
    original, calls = Path.resolve, []

    def tracked(path, *args, **kwargs):
        calls.append(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", tracked)
    addition.verify_canonical_paths(paths, store)
    assert calls == [parent]
    link = parent / "escaped.jpg"
    link.symlink_to(outside)
    with pytest.raises(RuntimeError, match="canonical"):
        addition.verify_canonical_paths([link], store)
