"""Native working matrices and verified sequential copies on external storage."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256

LOCAL = Path("data/embeddings/moscow_research_v5")


def external_root():
    path = ROOT / "storage.json"
    if not path.exists():
        return None
    root = Path(json.loads(path.read_text())["external_root"])
    if not root.is_dir():
        raise RuntimeError("external research volume is unavailable")
    return root


def native_destination(path):
    """Called with exclusive ownership of this experimental output directory."""
    root = external_root()
    if root is None:
        return path
    if path.is_symlink() and path.resolve().is_relative_to(root):
        source = path.resolve()
        temporary = path.with_name(path.name + ".native-copy")
        temporary.mkdir(exist_ok=True)
        # Checkpoint chunks are immutable and their .npz CRC is checked by the
        # existing embedding job before any completed artifact can be committed.
        checkpoint = source / ".embedding-checkpoints"
        if checkpoint.exists():
            target = temporary / ".embedding-checkpoints"
            target.mkdir(exist_ok=True)
            for p in sorted(checkpoint.glob("chunk-*.npz")):
                shutil.copyfile(p, target / p.name)
            shutil.copyfile(checkpoint / "state.json", target / "state.json")
        if (source / "build_metadata.json").exists():
            meta = json.loads((source / "build_metadata.json").read_text())
            for name, expected in meta["artifact_sha256"].items():
                shutil.copyfile(source / name, temporary / name)
                if sha256(temporary / name) != expected:
                    raise RuntimeError("working copy differs from committed artifacts")
            shutil.copyfile(source / "build_metadata.json", temporary / "build_metadata.json")
        path.unlink()
        temporary.rename(path)
    path.mkdir(parents=True, exist_ok=True)
    if root is not None and path.resolve().is_relative_to(root):
        raise RuntimeError("working-matrix parent is still external; migrate its research link first")
    return path


def mirror_artifacts(source):
    root = external_root()
    if root is None:
        return
    relative = source.absolute().relative_to(LOCAL.absolute())
    destination = root / "embeddings" / relative
    destination.mkdir(parents=True, exist_ok=True)
    metadata = json.loads((source / "build_metadata.json").read_text())
    expected = dict(metadata["artifact_sha256"]) | {"build_metadata.json": sha256(source / "build_metadata.json")}
    for name, digest in expected.items():
        src = source / name
        target = destination / name
        if sha256(src) != digest:
            raise RuntimeError("native artifact changed before archival")
        temporary = destination / ("." + name + ".publishing")
        with src.open("rb") as incoming, temporary.open("wb") as outgoing:
            shutil.copyfileobj(incoming, outgoing, length=1024 * 1024)
            outgoing.flush()
            os.fsync(outgoing.fileno())
        if sha256(temporary) != digest:
            raise RuntimeError("external artifact copy failed checksum verification")
        os.replace(temporary, target)
        print("verified external artifact", relative / name, flush=True)
    # Metadata is deliberately last; an interrupted external copy cannot look complete.
    receipt = {
        "source": str(source),
        "external": str(destination),
        "files_sha256": expected,
        "mode": "native working matrices, verified sequential external archive; no external writable mmap",
    }
    (source / "external_archive.json").write_text(json.dumps(receipt, indent=2) + "\n")
