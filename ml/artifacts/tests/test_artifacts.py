from __future__ import annotations

import hashlib
import io
import json
import tarfile
from pathlib import Path

import pytest

from ml.artifacts.ensure_production_artifacts import _extract_archive, ensure_artifact
from ml.artifacts.manifest import (
    ArtifactError,
    ProductionArtifact,
    load_artifact_manifest,
    sha256_tree,
    validate_artifact,
)


def artifact_for(payload: bytes, *, source_type: str = "file", tree_sha256: str | None = None):
    return ProductionArtifact.from_dict(
        {
            "artifact_id": "fixture",
            "version": "1",
            "source_url": "https://example.invalid/fixture",
            "source_type": source_type,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "size_bytes": len(payload),
            "destination": "runtime/fixture" if source_type != "file" else "runtime/fixture.bin",
            "required": True,
            "provenance": "unit test",
            "description": "unit test fixture",
            "strip_components": 1 if source_type != "file" else 0,
            "tree_sha256": tree_sha256,
        }
    )


def test_manifest_rejects_path_traversal(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "manifest_id": "test",
                "version": "1",
                "artifacts": [
                    {
                        "artifact_id": "unsafe",
                        "version": "1",
                        "source_url": "https://example.invalid/file",
                        "source_type": "file",
                        "sha256": "0" * 64,
                        "size_bytes": 1,
                        "destination": "../escape",
                        "required": True,
                        "provenance": "test",
                        "description": "test",
                    }
                ],
            }
        )
    )
    with pytest.raises(ArtifactError, match="safe relative"):
        load_artifact_manifest(path)


def test_already_valid_file_is_not_downloaded(tmp_path: Path, monkeypatch) -> None:
    payload = b"verified"
    artifact = artifact_for(payload)
    destination = artifact.destination_path(tmp_path)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(payload)
    monkeypatch.setattr(
        "ml.artifacts.ensure_production_artifacts._download",
        lambda *_args, **_kwargs: pytest.fail("valid artifact must not be downloaded"),
    )
    assert ensure_artifact(artifact, tmp_path) == "already_valid"


def test_download_is_validated_before_atomic_install(tmp_path: Path, monkeypatch) -> None:
    payload = b"verified"
    artifact = artifact_for(payload)

    def fake_download(_artifact, destination, **_kwargs):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)

    monkeypatch.setattr("ml.artifacts.ensure_production_artifacts._download", fake_download)
    assert ensure_artifact(artifact, tmp_path) == "installed"
    validate_artifact(artifact, tmp_path)


def test_corrupt_file_is_rejected(tmp_path: Path) -> None:
    artifact = artifact_for(b"expected")
    destination = artifact.destination_path(tmp_path)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"corrupt!")
    with pytest.raises(ArtifactError, match="SHA-256 mismatch"):
        validate_artifact(artifact, tmp_path)


def test_archive_rejects_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle:
        info = tarfile.TarInfo("root/../../escape")
        info.size = 1
        bundle.addfile(info, io.BytesIO(b"x"))
    artifact = artifact_for(
        archive.read_bytes(),
        source_type="tar.gz",
        tree_sha256="0" * 64,
    )
    with pytest.raises(ArtifactError, match="unsafe member"):
        _extract_archive(artifact, archive, artifact.destination_path(tmp_path))


def test_tree_hash_changes_when_content_changes(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    (root / "a").write_bytes(b"first")
    before = sha256_tree(root)
    (root / "a").write_bytes(b"second")
    assert sha256_tree(root) != before


def test_archive_root_is_traversable_by_nonroot_runtime(tmp_path: Path) -> None:
    archive = tmp_path / "source.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle:
        info = tarfile.TarInfo("root/hubconf.py")
        payload = b"model = 'pinned'\n"
        info.size = len(payload)
        bundle.addfile(info, io.BytesIO(payload))
    expected = tmp_path / "expected"
    expected.mkdir()
    (expected / "hubconf.py").write_bytes(payload)
    artifact = artifact_for(
        archive.read_bytes(),
        source_type="tar.gz",
        tree_sha256=sha256_tree(expected),
    )
    destination = artifact.destination_path(tmp_path / "installed")

    _extract_archive(artifact, archive, destination)

    assert destination.stat().st_mode & 0o055 == 0o055
