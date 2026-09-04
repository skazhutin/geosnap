"""Build a deterministic, bounded WebP preview set for indexed references."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Any

from PIL import Image, ImageOps, UnidentifiedImageError

from .manifest import sha256_file, sha256_tree

_MAX_SIZE = (480, 320)
_WEBP_QUALITY = 72


def _filename(reference_id: str) -> str:
    return hashlib.sha256(reference_id.encode("utf-8")).hexdigest() + ".webp"


def _write_thumbnail(source: Path, destination: Path) -> None:
    with Image.open(source) as opened:
        opened.load()
        image = ImageOps.exif_transpose(opened).convert("RGB")
    image.thumbnail(_MAX_SIZE, Image.Resampling.LANCZOS, reducing_gap=3.0)
    destination.parent.mkdir(parents=True, exist_ok=True)
    image.save(
        destination,
        format="WEBP",
        quality=_WEBP_QUALITY,
        method=6,
        exact=True,
        exif=b"",
        xmp=b"",
    )


def _deterministic_archive(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    with temporary.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=6) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as bundle:
                for path in sorted(item for item in source.rglob("*") if item.is_file()):
                    relative = path.relative_to(source).as_posix()
                    info = bundle.gettarinfo(str(path), arcname=f"thumbnails/{relative}")
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    info.mtime = 0
                    info.mode = 0o644
                    with path.open("rb") as stream:
                        bundle.addfile(info, stream)
    os.replace(temporary, destination)


def build(metadata_path: Path, output_dir: Path, archive_path: Path) -> dict[str, Any]:
    records: list[tuple[str, Path]] = []
    with metadata_path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            try:
                row = json.loads(line)
                reference_id = str(row["reference_id"])
                source = Path(str(row["metadata"]["image_path"]))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(f"invalid reference metadata at line {line_number}") from exc
            records.append((reference_id, source))
    if len(records) != len({reference_id for reference_id, _ in records}):
        raise RuntimeError("reference metadata contains duplicate IDs")

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=str(output_dir.parent)))
    mapping: dict[str, str] = {}
    try:
        image_dir = temporary / "images"
        failures: list[str] = []
        for index, (reference_id, source) in enumerate(records, start=1):
            filename = _filename(reference_id)
            try:
                _write_thumbnail(source, image_dir / filename)
            except (OSError, SyntaxError, ValueError, UnidentifiedImageError):
                failures.append(reference_id)
                continue
            mapping[reference_id] = f"images/{filename}"
            if index % 500 == 0:
                print(f"thumbnails: {index}/{len(records)}", flush=True)
        if failures:
            raise RuntimeError(
                f"could not build {len(failures)} indexed thumbnails; first ID: {failures[0]}"
            )
        manifest = {
            "schema_version": 1,
            "format": "webp",
            "max_width": _MAX_SIZE[0],
            "max_height": _MAX_SIZE[1],
            "quality": _WEBP_QUALITY,
            "reference_count": len(mapping),
            "references": dict(sorted(mapping.items())),
        }
        (temporary / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n",
            encoding="utf-8",
        )
        tree_sha256 = sha256_tree(temporary)
        previous = output_dir.with_name(f".{output_dir.name}.previous")
        if previous.exists():
            shutil.rmtree(previous)
        if output_dir.exists():
            os.replace(output_dir, previous)
        try:
            os.replace(temporary, output_dir)
        except Exception:
            if previous.exists():
                os.replace(previous, output_dir)
            raise
        if previous.exists():
            shutil.rmtree(previous)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)

    _deterministic_archive(output_dir, archive_path)
    return {
        "reference_count": len(mapping),
        "tree_sha256": tree_sha256,
        "archive": str(archive_path),
        "archive_size_bytes": archive_path.stat().st_size,
        "archive_sha256": sha256_file(archive_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reference-metadata",
        type=Path,
        default=Path("data/indexes/moscow_real_v4/sage-vitb/reference_metadata.jsonl"),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.reference_metadata, args.output_dir, args.archive), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
