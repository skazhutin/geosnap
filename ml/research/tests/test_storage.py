from ml.research.storage import relocate


def test_transfer_resumes_with_appledouble_sidecars_without_losing_data(tmp_path):
    source = tmp_path / "local"
    source.mkdir()
    (source / "image.jpg").write_bytes(b"actual image bytes")
    destination = tmp_path / "external/gallery"
    staging = destination.with_name("gallery.copying")
    staging.mkdir(parents=True)
    (staging / "image.jpg").write_bytes(b"actual image bytes")
    (staging / "._image.jpg").write_bytes(b"filesystem metadata")
    result = relocate(source, destination)
    assert result["verified_files"] == 1
    assert source.is_symlink()
    assert (source / "image.jpg").read_bytes() == b"actual image bytes"


def test_corrupt_copy_keeps_original(tmp_path):
    import pytest

    source = tmp_path / "local"
    source.mkdir()
    (source / "image.jpg").write_bytes(b"original")
    destination = tmp_path / "external/gallery"
    staging = destination.with_name("gallery.copying")
    staging.mkdir(parents=True)
    (staging / "image.jpg").write_bytes(b"corrupt")
    with pytest.raises(RuntimeError, match="verification failed"):
        relocate(source, destination)
    assert not source.is_symlink()
    assert (source / "image.jpg").read_bytes() == b"original"
