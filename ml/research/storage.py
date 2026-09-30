"""Verify and relocate research-only storage without changing manifest paths."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ml.research.seal import sha256, write_once


def content_manifest(root: Path):
    # macOS creates AppleDouble sidecars on NTFS. They are filesystem metadata,
    # not image/model/manifest bytes and must not masquerade as dataset entries.
    paths = sorted(p for p in root.rglob("*") if not p.name.startswith("._") and p.name != ".DS_Store" and p.is_file())
    with ThreadPoolExecutor(max_workers=4) as pool:
        hashes = list(pool.map(sha256, paths))
    return {str(p.relative_to(root)): h for p, h in zip(paths, hashes, strict=True)}


def relocate(source: Path, target: Path):
    if source.is_symlink():
        if source.resolve() != target.resolve():
            raise ValueError("existing research link points to another destination")
        return {"source": str(source), "target": str(target), "already_linked": True}
    if not source.is_dir() or target.exists():
        raise ValueError("need existing source and unused destination")
    staging = target.with_name(target.name + ".copying")
    target.parent.mkdir(parents=True, exist_ok=True)
    manifest = content_manifest(source)
    if not staging.exists():
        shutil.copytree(source, staging, copy_function=shutil.copyfile)
    print("verifying research file contents", len(manifest), flush=True)
    copied = content_manifest(staging)
    if copied != manifest:
        raise RuntimeError("transfer verification failed; original remains intact")
    # No producer may write into this directory during the transfer.
    if content_manifest(source) != manifest:
        raise RuntimeError("source changed during transfer; original remains intact")
    staging.rename(target)
    link = source.with_name(source.name + ".external-link")
    link.symlink_to(target, target_is_directory=True)
    backup = source.with_name(source.name + ".verified-local-copy")
    source.rename(backup)
    try:
        os.replace(link, source)
    except BaseException:
        backup.rename(source)
        raise
    receipt = {
        "source": str(source),
        "target": str(target),
        "files_sha256": manifest,
        "method": "copy, verify all bytes and unchanged source, replace by symlink, remove verified local duplicate",
        "excluded_filesystem_metadata": ["._* AppleDouble sidecars", ".DS_Store"],
    }
    write_once(target.parent / (target.name + ".transfer.json"), receipt)
    shutil.rmtree(backup)
    return {"source": str(source), "target": str(target), "verified_files": len(manifest)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    root = args.destination.resolve()
    if not root.is_dir():
        raise ValueError("external destination must already exist")
    # Fixed allowlist excludes production, private held-out queries and source repositories.
    source = Path("data/evaluation/moscow_research_v5/gallery_expansion")
    print(json.dumps(relocate(source, root / "gallery_expansion"), ensure_ascii=False), flush=True)
    # NTFS/FUSE writable mmap performed poorly on this host. Experimental
    # extraction uses native working matrices and sequential verified archives.
    # Do not replace existing native embedding directories with external links.
    (root / "embeddings").mkdir(exist_ok=True)
    Path("data/evaluation/moscow_research_v5/storage.json").write_text(
        json.dumps(
            {
                "external_root": str(root),
                "private_calibration_and_final": "remain on internal storage, sealed and unopened",
                "production": "unchanged",
                "external_required_for_gallery_expansion": True,
                "embedding_working_mode": "native working matrices with verified external archives",
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
