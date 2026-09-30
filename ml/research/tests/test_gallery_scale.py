"""Bounded regressions for the fixed-model gallery experiment."""

import json

import numpy as np
import pandas as pd
import pytest
from PIL import Image

from ml.research.gallery_scale import (
    descriptor_chunks,
    exact_scores,
    fixed_development,
    gallery_role,
    perspective,
    smart_order,
    summarize,
)
from ml.research.gallery_scale_storage import copy_verified, digest, relocate


def test_smart_selection_covers_empty_cells_then_diversifies_sequences():
    base = pd.DataFrame([dict(cell="dense", sequence_key="old", heading_bin=0, season="summer")])
    pool = pd.DataFrame([
        dict(cell="dense", sequence_key="new", heading_bin=1, season="winter", selection_hash="a"),
        dict(cell="empty", sequence_key="old", heading_bin=0, season="summer", selection_hash="b"),
        dict(cell="empty", sequence_key="other", heading_bin=1, season="winter", selection_hash="c"),
    ])
    order = smart_order(pool, base)
    assert order[0] == 2
    assert sorted(order) == [0, 1, 2]
    assert smart_order(pool, base) == order


def test_msls_cannot_evade_mapillary_sequence_role():
    for sequence in ["abc", "legacySequence", "another-sequence"]:
        assert gallery_role("msls", "msls:" + sequence) == gallery_role("mapillary", sequence)


def test_sealed_manifest_is_rejected_before_any_file_read(monkeypatch):
    monkeypatch.setattr("ml.research.gallery_scale.digest", lambda _: pytest.fail("private file was read"))
    with pytest.raises(RuntimeError, match="registered development"):
        fixed_development({"query_manifest": "data/evaluation/moscow_research_v5/prospective/private/final.parquet",
                           "query_sha256": "anything"})


def test_exact_pool_row_selection_and_full_denominator(tmp_path):
    descriptors = np.array([[1, 0], [0, 1], [-1, 0]], dtype=np.float32)
    path = tmp_path / "pool.npy"
    np.save(path, descriptors)
    frame = pd.DataFrame([
        dict(id="west", file_sha256="a", lat=55.75, lon=37.0, heading=0, cell="a", heading_bin=0,
             sequence_key="s1", source="mapillary", production_compatible=True),
        dict(id="east", file_sha256="b", lat=55.75, lon=37.6, heading=0, cell="b", heading_bin=0,
             sequence_key="s2", source="mapillary", production_compatible=True),
    ])
    entries = {"west": dict(path=str(path), row=2, image_sha256="a"),
               "east": dict(path=str(path), row=0, image_sha256="b")}
    q = np.array([[1, 0], [0, 1]], dtype=np.float32)
    scores = exact_scores(frame, q, entries)
    np.testing.assert_array_equal(scores, q @ descriptors[[2, 0]].T)
    queries = pd.DataFrame([dict(id="covered", lat=55.75, lon=37.6, heading=0),
                            dict(id="uncovered", lat=55.75, lon=38.6, heading=0)])
    result, rows = summarize(frame, queries, scores)
    assert result["raw"]["accuracy_100m"] == 0.5
    assert result["retrieval"]["recall_at"]["1"] == 0.5
    assert rows["positive_ranks"] == [1, None]
    assert result["diagnosis"]["no_coverage"] == 1
    assert sum(result["diagnosis"].values()) == 2


def test_perspective_preserves_constant_and_covers_four_directions():
    image = Image.fromarray(np.full((100, 200, 3), 91, dtype=np.uint8))
    for yaw in [0, 90, 180, 270]:
        assert np.all(np.asarray(perspective(image, yaw, 31)) == 91)
    stripes = np.zeros((100, 200, 3), dtype=np.uint8)
    stripes[:, :, 0] = np.arange(200)
    image = Image.fromarray(stripes)
    values = [np.asarray(perspective(image, yaw, 31))[15, 15, 0] for yaw in [0, 90, 180, 270]]
    assert values[0] == 100
    assert values[1] == 150
    assert values[3] == 50
    assert values[2] in [99, 100]  # seam bilinear blend; no black border


def test_storage_relocation_verifies_and_preserves_legacy_path(tmp_path):
    source, target = tmp_path / "internal", tmp_path / "external"
    source.mkdir()
    Image.new("RGB", (20, 20), (1, 2, 3)).save(source / "image.jpg")
    original = (source / "image.jpg").read_bytes()
    receipt = tmp_path / "receipt.json"
    relocate(source, target, receipt)
    assert source.is_symlink()
    assert (source / "image.jpg").read_bytes() == original
    assert (target / "image.jpg").read_bytes() == original
    assert json.loads(receipt.read_text())["status"] == "verified_local_copy_removed"


def test_storage_does_not_delete_source_when_canonical_smoke_fails(tmp_path, monkeypatch):
    source, target = tmp_path / "internal", tmp_path / "external"
    source.mkdir()
    (source / "broken.jpg").write_bytes(b"not an image")
    with pytest.raises(OSError):
        relocate(source, target, tmp_path / "receipt.json")
    assert source.is_dir() and not source.is_symlink()
    assert (source / "broken.jpg").read_bytes() == b"not an image"


@pytest.mark.parametrize("partial,offset", [(b"abcdef", 6), (b"abc", 3), (b"abX", 0), (b"abcdefg", 0)])
def test_archive_copy_resumes_only_verified_prefix(tmp_path, monkeypatch, partial, offset):
    from ml.research import gallery_scale_storage as storage

    source, target = tmp_path / "source.zip", tmp_path / "target.zip"
    source.write_bytes(b"abcdef")
    target.with_name(target.name + ".copying").write_bytes(partial)
    original = storage.shutil.copyfileobj

    def check_resume(incoming, outgoing, length):
        assert incoming.tell() == offset
        original(incoming, outgoing, length)

    monkeypatch.setattr(storage.shutil, "copyfileobj", check_resume)
    copy_verified(source, target)
    assert target.read_bytes() == source.read_bytes() == b"abcdef"
    assert not target.with_name(target.name + ".copying").exists()


def test_descriptor_resume_preserves_unreadable_and_orphaned_files(tmp_path):
    directory = tmp_path / "descriptors"
    directory.mkdir()
    (tmp_path / "descriptor_contract.json").write_text("frozen encoder")
    for i in range(3):
        (directory / f"chunk-{i:06d}.json").write_bytes(b"uninterpreted receipt")
    (directory / "chunk-000004.npy").write_bytes(b"uncommitted vectors")
    (tmp_path / "descriptor_recovery.json").write_text(json.dumps({
        "contract_sha256": digest(tmp_path / "descriptor_contract.json"),
        "excluded_receipts": ["chunk-000001.json"],
    }))
    receipts, serial = descriptor_chunks(tmp_path)
    assert [p.name for p in receipts] == ["chunk-000000.json", "chunk-000002.json"]
    assert serial == 5
    assert (directory / "chunk-000001.json").read_bytes() == b"uninterpreted receipt"
    assert (directory / "chunk-000004.npy").read_bytes() == b"uncommitted vectors"
    (tmp_path / "descriptor_contract.json").write_text("different encoder")
    with pytest.raises(RuntimeError, match="different encoder contract"):
        descriptor_chunks(tmp_path)
