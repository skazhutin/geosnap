"""Download and atomically install all required production artifacts."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import ssl
import tarfile
import tempfile
import time
import uuid
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

from .manifest import (
    ArtifactError,
    ProductionArtifact,
    load_artifact_manifest,
    production_artifact_root,
    sha256_file,
    sha256_tree,
    validate_artifact,
)


class _HTTPSRedirectHandler(HTTPRedirectHandler):
    max_redirections = 5

    def redirect_request(self, request, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        resolved = urljoin(request.full_url, newurl)
        parsed = urlsplit(resolved)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ArtifactError("artifact download redirected to an unsafe URL")
        return super().redirect_request(request, fp, code, msg, headers, resolved)


def _download(artifact: ProductionArtifact, destination: Path, *, retries: int = 3) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(f".{destination.name}.partial")
    context = ssl.create_default_context()
    opener = build_opener(_HTTPSRedirectHandler(), HTTPSHandler(context=context))
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        offset = partial.stat().st_size if partial.is_file() else 0
        headers = {"User-Agent": "GeoSnap-artifact-provisioner/1"}
        if offset:
            headers["Range"] = f"bytes={offset}-"
        try:
            request = Request(artifact.source_url, headers=headers, method="GET")
            with opener.open(request, timeout=60) as response:
                append = offset > 0 and getattr(response, "status", None) == 206
                mode = "ab" if append else "wb"
                if not append:
                    offset = 0
                downloaded = offset
                next_progress = time.monotonic() + 2.0
                with partial.open(mode) as output:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        output.write(chunk)
                        downloaded += len(chunk)
                        if time.monotonic() >= next_progress:
                            percent = min(100.0, downloaded * 100.0 / artifact.size_bytes)
                            print(f"{artifact.artifact_id}: {downloaded}/{artifact.size_bytes} bytes ({percent:.1f}%)", flush=True)
                            next_progress = time.monotonic() + 2.0
            if partial.stat().st_size != artifact.size_bytes:
                raise ArtifactError(f"downloaded byte-size mismatch for {artifact.artifact_id}")
            if sha256_file(partial) != artifact.sha256:
                raise ArtifactError(f"downloaded SHA-256 mismatch for {artifact.artifact_id}")
            os.replace(partial, destination)
            return
        except (ArtifactError, HTTPError, URLError, OSError, TimeoutError) as exc:
            last_error = exc
            if attempt == retries:
                break
            print(f"{artifact.artifact_id}: transient download failure; retrying ({attempt}/{retries})", flush=True)
            time.sleep(min(2**attempt, 8))
    raise ArtifactError(f"could not download {artifact.artifact_id}: {type(last_error).__name__}") from last_error


def _member_destination(name: str, *, strip_components: int) -> Path | None:
    pure = PurePosixPath(name)
    if pure.is_absolute() or ".." in pure.parts or "\x00" in name:
        raise ArtifactError("archive contains an unsafe member path")
    parts = pure.parts[strip_components:]
    if not parts:
        return None
    return Path(*parts)


def _extract_archive(artifact: ProductionArtifact, archive: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.", dir=str(destination.parent))
    )
    # Provisioning commonly runs as root while the runtime is deliberately
    # unprivileged. The installed archive root must remain traversable.
    temporary.chmod(0o755)
    try:
        with tarfile.open(archive, mode="r:gz") as bundle:
            for member in bundle.getmembers():
                relative = _member_destination(
                    member.name, strip_components=artifact.strip_components
                )
                if relative is None:
                    continue
                target = (temporary / relative).resolve()
                if temporary.resolve() not in target.parents:
                    raise ArtifactError("archive member escapes extraction directory")
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                if not member.isfile():
                    raise ArtifactError("archives may contain only regular files and directories")
                source = bundle.extractfile(member)
                if source is None:
                    raise ArtifactError("archive member could not be read")
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open("wb") as output:
                    shutil.copyfileobj(source, output, length=1024 * 1024)
                target.chmod(member.mode & 0o755 or 0o644)
        actual_tree = sha256_tree(temporary)
        if actual_tree != artifact.tree_sha256:
            raise ArtifactError(f"extracted tree SHA-256 mismatch for {artifact.artifact_id}")
        marker = temporary / ".geosnap-artifact.json"
        marker.write_text(
            json.dumps(
                {"artifact_id": artifact.artifact_id, "source_sha256": artifact.sha256},
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        previous = destination.with_name(f".{destination.name}.previous-{uuid.uuid4().hex}")
        if destination.exists():
            os.replace(destination, previous)
        try:
            os.replace(temporary, destination)
        except Exception:
            if previous.exists():
                os.replace(previous, destination)
            raise
        if previous.exists():
            shutil.rmtree(previous)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def ensure_artifact(artifact: ProductionArtifact, root: Path) -> str:
    destination = artifact.destination_path(root)
    try:
        validate_artifact(artifact, root)
        if artifact.source_type == "tar.gz":
            destination.chmod(0o755)
        return "already_valid"
    except ArtifactError:
        pass
    if artifact.source_type == "file":
        temporary = destination.with_name(f".{destination.name}.download-{uuid.uuid4().hex}")
        try:
            _download(artifact, temporary)
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
    else:
        archive = root / ".archives" / f"{artifact.artifact_id}-{artifact.sha256}.tar.gz"
        if not (
            archive.is_file()
            and archive.stat().st_size == artifact.size_bytes
            and sha256_file(archive) == artifact.sha256
        ):
            temporary = archive.with_name(f".{archive.name}.download-{uuid.uuid4().hex}")
            try:
                _download(artifact, temporary)
                archive.parent.mkdir(parents=True, exist_ok=True)
                os.replace(temporary, archive)
            finally:
                temporary.unlink(missing_ok=True)
        _extract_archive(artifact, archive, destination)
    validate_artifact(artifact, root)
    return "installed"


def run(manifest_path: Path, artifact_root: Path, *, include_optional: bool) -> dict[str, object]:
    manifest = load_artifact_manifest(manifest_path)
    artifact_root.mkdir(parents=True, exist_ok=True)
    statuses: dict[str, str] = {}
    for artifact in manifest.artifacts:
        if not artifact.required and not include_optional:
            statuses[artifact.artifact_id] = "optional_skipped"
            continue
        print(f"{artifact.artifact_id}: validating", flush=True)
        statuses[artifact.artifact_id] = ensure_artifact(artifact, artifact_root)
        print(f"{artifact.artifact_id}: {statuses[artifact.artifact_id]}", flush=True)
    return {
        "manifest_id": manifest.manifest_id,
        "version": manifest.version,
        "artifact_root": str(artifact_root),
        "statuses": statuses,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("configs/production_artifacts.json"),
    )
    parser.add_argument("--artifact-dir", type=Path, default=None)
    parser.add_argument("--include-optional", action="store_true")
    args = parser.parse_args()
    root = production_artifact_root(args.artifact_dir)
    print(json.dumps(run(args.manifest, root, include_optional=args.include_optional), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
