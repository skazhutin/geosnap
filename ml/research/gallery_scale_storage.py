"""One-time, allowlisted storage migration with verified bytes and legacy links.

No sealed query manifests or evaluation outputs are interpreted by this module.
Frozen production artifacts are excluded. Legacy paths remain valid via symlinks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from PIL import Image

WORKSPACE = Path(__file__).resolve().parents[2]


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def settings():
    config = json.loads((WORKSPACE / "configs/moscow_gallery_scale.json").read_text())
    value = os.environ.get(config["data_root_env"])
    if not value:
        value = json.loads((WORKSPACE / config["existing_storage_config"]).read_text())["external_root"]
    root = Path(value).expanduser().resolve()
    if not root.is_dir() or not (root / config["canonical_images"]).is_dir():
        raise RuntimeError("GEOSNAP_DATA_ROOT must contain the existing full_mapillary store")
    if root.stat().st_dev == WORKSPACE.stat().st_dev:
        raise RuntimeError("large research data must be on the external volume")
    output = root / config["output_directory"]
    output.mkdir(exist_ok=True)
    return config, root, root / config["canonical_images"], output


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".writing")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    os.replace(temporary, path)


def ordinary_files(root):
    for directory, dirs, names in os.walk(root, followlinks=False):
        dirs[:] = [d for d in dirs if not d.startswith("._")]
        for name in names:
            if name.startswith("._") or name == ".DS_Store":
                continue
            path = Path(directory) / name
            if not path.is_symlink():
                yield path


def copy_verified(source, target, *, durable=True):
    target.parent.mkdir(parents=True, exist_ok=True)
    before = source.stat()
    source_hash = digest(source)
    if not target.exists() or target.stat().st_size != before.st_size or digest(target) != source_hash:
        temporary = target.with_name(target.name + ".copying")
        offset = 0
        if temporary.exists() and temporary.stat().st_size <= before.st_size:
            # A stopped copy may already contain the whole archive. Validate
            # its prefix before appending; size alone never proves identity.
            with source.open("rb") as incoming, temporary.open("rb") as partial:
                for block in iter(lambda: partial.read(4 * 1024 * 1024), b""):
                    if incoming.read(len(block)) != block:
                        offset = 0
                        break
                    offset += len(block)
        with source.open("rb") as incoming, temporary.open("ab" if offset else "wb") as outgoing:
            incoming.seek(offset)
            shutil.copyfileobj(incoming, outgoing, 4 * 1024 * 1024)
            outgoing.flush()
            if durable:
                os.fsync(outgoing.fileno())
        if digest(temporary) != source_hash:
            raise RuntimeError(f"copy checksum mismatch: {source}")
        os.replace(temporary, target)
    after = source.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError(f"source changed while copying: {source}")
    return {"bytes": before.st_size, "sha256": source_hash, "mtime_ns": before.st_mtime_ns}


def relocate(source, target, receipt_path):
    if source.is_symlink():
        if source.resolve() != target.resolve():
            raise RuntimeError(f"unexpected existing link: {source}")
        receipt = json.loads(receipt_path.read_text())
        backup = source.with_name(source.name + ".verified-local-copy")
        if backup.exists():
            # Recover an interruption after the link switch but before removal.
            for name, record in receipt["files"].items():
                if digest(target / name) != record["sha256"]:
                    raise RuntimeError("canonical copy changed during interrupted cleanup")
            for path in ordinary_files(backup):
                name = str(path.relative_to(backup))
                if name not in receipt["files"] or digest(path) != receipt["files"][name]["sha256"]:
                    raise RuntimeError("interrupted local backup has unverified content")
            os.sync()
            shutil.rmtree(backup)
            receipt["status"] = "verified_local_copy_removed"
            save(receipt_path, receipt)
        return receipt
    if not source.is_dir():
        raise RuntimeError(f"missing migration source: {source}")
    paths = list(ordinary_files(source))
    links = {}
    for directory, dirs, names in os.walk(source, followlinks=False):
        for name in dirs + names:
            p = Path(directory) / name
            if p.is_symlink():
                links[str(p.relative_to(source))] = str(p.resolve())
    target.mkdir(parents=True, exist_ok=True)
    print("migrating", source, len(paths), flush=True)
    with ThreadPoolExecutor(max_workers=6) as executor:
        records = dict(zip(
            [str(p.relative_to(source)) for p in paths],
            executor.map(lambda p: copy_verified(p, target / p.relative_to(source), durable=False), paths), strict=True,
        ))
    for name, destination in links.items():
        p = target / name
        p.parent.mkdir(parents=True, exist_ok=True)
        if p.is_symlink():
            if p.resolve() != Path(destination):
                raise RuntimeError("conflicting copied symlink")
        elif not p.exists():
            p.symlink_to(destination, target_is_directory=Path(destination).is_dir())
        else:
            raise RuntimeError("unexpected content at copied symlink")
    # Smoke the canonical files BEFORE replacing or deleting the local copy.
    image_names = [n for n in records if Path(n).suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}]
    smoke = image_names[::max(1, len(image_names) // 12)][:12]
    for name in smoke:
        with Image.open(target / name) as im:
            im.load()
    for p in paths:
        info = p.stat()
        r = records[str(p.relative_to(source))]
        if info.st_size != r["bytes"] or info.st_mtime_ns != r["mtime_ns"]:
            raise RuntimeError("source changed before migration commit")
    if {str(p.relative_to(source)) for p in ordinary_files(source)} != set(records):
        raise RuntimeError("source file membership changed during migration")
    # NTFS/FUSE per-JPEG fsync serializes tens of thousands of small writes.
    # Flush the entire verified directory once, still BEFORE unlinking originals.
    os.sync()
    receipt = {"source": str(source), "canonical": str(target), "files": records,
               "preserved_symlinks": links,
               "verified_bytes": sum(r["bytes"] for r in records.values()),
               "image_smoke": smoke, "legacy_paths": "preserved by directory symlink",
               "durability": "filesystem sync after complete SHA verification, before any local deletion",
               "status": "verified_before_local_removal"}
    save(receipt_path, receipt)
    backup = source.with_name(source.name + ".verified-local-copy")
    if backup.exists():
        raise RuntimeError("unexpected previous migration backup; inspect before continuing")
    source.rename(backup)
    try:
        source.symlink_to(target, target_is_directory=True)
        if smoke:
            with Image.open(source / smoke[0]) as im:
                im.load()
    except BaseException:
        source.unlink(missing_ok=True)
        backup.rename(source)
        raise
    shutil.rmtree(backup)
    receipt["status"] = "verified_local_copy_removed"
    save(receipt_path, receipt)
    print("released GB", round(receipt["verified_bytes"] / 1e9, 3), source, flush=True)
    return receipt


def run():
    _, root, store, output = settings()
    receipts = output / "storage"
    receipts.mkdir(exist_ok=True)
    migrations = []
    # Deliberately keep production v4 descriptors/index and all frozen assets.
    for parent in ("data/embeddings", "data/indexes"):
        for path in sorted((WORKSPACE / parent).iterdir()):
            if path.is_dir() and path.name not in {"moscow_real_v4", "sample"}:
                migrations.append((path, root / "legacy_data" / path.relative_to(WORKSPACE)))
    for relative in ("data/models/research_v5", "data/models/selavprplusplus", "data/models/cricavpr",
                     "data/evaluation/moscow_research_v5/development"):
        path = WORKSPACE / relative
        if path.exists():
            migrations.append((path, root / "legacy_data" / relative))
    for source in ("mapillary", "kartaview", "msls"):
        migrations.append((WORKSPACE / "data/raw/moscow/images" / source, store / "geosnap" / source))
    save(receipts / "plan.json", {"migrations": [[str(a), str(b)] for a, b in migrations],
        "production": "all 42 frozen files kept at original paths; unchanged bytes",
        "private_query_manifests_and_images": "excluded",
        "configs_manifests_and_reports": "preserved; legacy directory paths remain valid"})
    completed = []
    for source, target in migrations:
        name = "__".join(source.relative_to(WORKSPACE).parts) + ".json"
        completed.append(relocate(source, target, receipts / name))
    # Already-external references move on the same filesystem, without any copy.
    old = root / "gallery_expansion/images"
    new = store / "geosnap/gallery_expansion"
    if old.is_dir() and not old.is_symlink():
        old.rename(new)
        old.symlink_to(new, target_is_directory=True)
    save(receipts / "summary.json", {"status": "completed", "internal_bytes_removed": sum(
        r["verified_bytes"] for r in completed), "directories": [{k: r[k] for k in (
            "source", "canonical", "verified_bytes", "status")} for r in completed],
        "protected": "production artifacts, code, git, configs, manifests, benchmark splits, receipts and reports retained"})


if __name__ == "__main__":
    argparse.ArgumentParser(description=__doc__).parse_args()
    run()
