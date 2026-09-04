"""Strict schema and integrity checks for externally distributed artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ARTIFACT_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_SOURCE_TYPES = {"file", "tar.gz"}


class ArtifactError(RuntimeError):
    """An artifact declaration or on-disk artifact is unsafe or invalid."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_tree(root: Path) -> str:
    """Hash a directory deterministically, including relative names and sizes."""

    if not root.is_dir():
        raise ArtifactError(f"artifact directory is missing: {root}")
    digest = hashlib.sha256()
    files = sorted(path for path in root.rglob("*") if path.is_file())
    for path in files:
        relative = path.relative_to(root).as_posix()
        if relative == ".geosnap-artifact.json":
            continue
        encoded = relative.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(path.stat().st_size.to_bytes(8, "big"))
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def _safe_relative(value: str, label: str) -> Path:
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts or "\x00" in value:
        raise ArtifactError(f"{label} must be a safe relative path")
    return path


@dataclass(frozen=True, slots=True)
class ProductionArtifact:
    artifact_id: str
    version: str
    source_url: str
    source_type: str
    sha256: str
    size_bytes: int
    destination: Path
    required: bool
    provenance: str
    description: str
    strip_components: int = 0
    tree_sha256: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ProductionArtifact:
        try:
            artifact = cls(
                artifact_id=str(value["artifact_id"]),
                version=str(value["version"]),
                source_url=str(value["source_url"]),
                source_type=str(value["source_type"]),
                sha256=str(value["sha256"]),
                size_bytes=int(value["size_bytes"]),
                destination=_safe_relative(str(value["destination"]), "destination"),
                required=bool(value["required"]),
                provenance=str(value["provenance"]),
                description=str(value["description"]),
                strip_components=int(value.get("strip_components", 0)),
                tree_sha256=(
                    None if value.get("tree_sha256") is None else str(value["tree_sha256"])
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ArtifactError("artifact entry is missing a required field") from exc
        artifact.validate()
        return artifact

    def validate(self) -> None:
        if not _ARTIFACT_ID.fullmatch(self.artifact_id):
            raise ArtifactError("artifact_id is not a safe identifier")
        if not self.version or len(self.version) > 128:
            raise ArtifactError(f"invalid version for {self.artifact_id}")
        parsed = urlsplit(self.source_url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ArtifactError(f"{self.artifact_id} source must be a public HTTPS URL")
        if self.source_type not in _SOURCE_TYPES:
            raise ArtifactError(f"unsupported source_type for {self.artifact_id}")
        if not _SHA256.fullmatch(self.sha256):
            raise ArtifactError(f"invalid SHA-256 for {self.artifact_id}")
        if self.size_bytes <= 0:
            raise ArtifactError(f"invalid byte size for {self.artifact_id}")
        if self.strip_components < 0 or self.strip_components > 8:
            raise ArtifactError(f"invalid strip_components for {self.artifact_id}")
        if self.source_type == "tar.gz" and not (
            self.tree_sha256 and _SHA256.fullmatch(self.tree_sha256)
        ):
            raise ArtifactError(f"archive {self.artifact_id} requires tree_sha256")
        if self.source_type == "file" and (self.strip_components or self.tree_sha256):
            raise ArtifactError(f"file {self.artifact_id} has archive-only fields")

    def destination_path(self, root: Path) -> Path:
        resolved_root = root.resolve()
        target = (resolved_root / self.destination).resolve()
        if target == resolved_root or resolved_root not in target.parents:
            raise ArtifactError(f"unsafe destination for {self.artifact_id}")
        return target


@dataclass(frozen=True, slots=True)
class ArtifactManifest:
    path: Path
    manifest_id: str
    version: str
    artifacts: tuple[ProductionArtifact, ...]

    @property
    def required(self) -> tuple[ProductionArtifact, ...]:
        return tuple(artifact for artifact in self.artifacts if artifact.required)


def load_artifact_manifest(path: str | Path) -> ArtifactManifest:
    resolved = Path(path).expanduser().resolve()
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ArtifactError(f"cannot read artifact manifest: {resolved}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ArtifactError("artifact manifest must be a schema-version-1 JSON object")
    raw_artifacts = payload.get("artifacts")
    if not isinstance(raw_artifacts, list) or not raw_artifacts:
        raise ArtifactError("artifact manifest must contain artifacts")
    artifacts = tuple(ProductionArtifact.from_dict(item) for item in raw_artifacts)
    ids = [artifact.artifact_id for artifact in artifacts]
    destinations = [artifact.destination.as_posix() for artifact in artifacts]
    if len(ids) != len(set(ids)) or len(destinations) != len(set(destinations)):
        raise ArtifactError("artifact IDs and destinations must be unique")
    return ArtifactManifest(
        path=resolved,
        manifest_id=str(payload.get("manifest_id", "")),
        version=str(payload.get("version", "")),
        artifacts=artifacts,
    )


def production_artifact_root(value: str | Path | None = None) -> Path:
    configured = value or os.environ.get("GEOSNAP_ARTIFACT_DIR") or "data"
    return Path(configured).expanduser().resolve()


def validate_artifact(artifact: ProductionArtifact, root: Path) -> None:
    target = artifact.destination_path(root)
    if artifact.source_type == "file":
        if not target.is_file():
            raise ArtifactError(f"required artifact is missing: {artifact.artifact_id}")
        if target.stat().st_size != artifact.size_bytes:
            raise ArtifactError(f"artifact byte-size mismatch: {artifact.artifact_id}")
        if sha256_file(target) != artifact.sha256:
            raise ArtifactError(f"artifact SHA-256 mismatch: {artifact.artifact_id}")
        return
    if not target.is_dir():
        raise ArtifactError(f"required artifact is missing: {artifact.artifact_id}")
    if sha256_tree(target) != artifact.tree_sha256:
        raise ArtifactError(f"artifact tree SHA-256 mismatch: {artifact.artifact_id}")


def validate_production_artifacts(
    manifest_path: str | Path,
    *,
    artifact_root: str | Path | None = None,
    required_only: bool = True,
) -> dict[str, Any]:
    manifest = load_artifact_manifest(manifest_path)
    root = production_artifact_root(artifact_root)
    selected = manifest.required if required_only else manifest.artifacts
    for artifact in selected:
        validate_artifact(artifact, root)
    return {
        "manifest_id": manifest.manifest_id,
        "version": manifest.version,
        "artifact_root": str(root),
        "artifact_count": len(selected),
        "total_source_bytes": sum(artifact.size_bytes for artifact in selected),
    }
