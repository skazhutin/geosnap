"""Extract the selected MSLS Moscow gallery from locally downloaded volumes."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import stat
import tempfile
import time
from collections import Counter
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any
from zipfile import BadZipFile, ZipFile, ZipInfo

from ml.ingestion.common import sanitize_error_message, validate_image_file, write_json
from ml.ingestion.msls_moscow import MSLS_SPLIT_VOLUME
from ml.ingestion.schema import read_manifest

logger = logging.getLogger(__name__)

OFFICIAL_VOLUMES: dict[str, dict[str, Any]] = {
    "msls_images_vol_1.zip": {
        "size_bytes": 11_034_252_629,
        "md5": "eef296a420f6e729a5567240f71f976e",
    },
    "msls_images_vol_5.zip": {
        "size_bytes": 10_879_136_492,
        "md5": "fd377d730c8183618973fadb3f998ef8",
    },
}


class MslsExtractionError(RuntimeError):
    """The selected MSLS image set could not be extracted completely."""


def _md5(path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_volume(value: str) -> tuple[str, Path]:
    name, separator, raw_path = value.partition("=")
    if not separator or not name or not raw_path:
        raise argparse.ArgumentTypeError("volume must use NAME=/absolute/path/to/archive.zip")
    if name not in OFFICIAL_VOLUMES:
        raise argparse.ArgumentTypeError(f"unsupported MSLS volume: {name}")
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("MSLS volume path must be absolute")
    return name, path


def _validate_archive(name: str, path: Path, *, verify_md5: bool) -> dict[str, Any]:
    expected = OFFICIAL_VOLUMES[name]
    if not path.is_file():
        raise MslsExtractionError(f"missing MSLS volume: {name}")
    size = path.stat().st_size
    if size != expected["size_bytes"]:
        raise MslsExtractionError(
            f"MSLS volume size mismatch for {name}: {size} != {expected['size_bytes']}"
        )
    checksum = _md5(path) if verify_md5 else None
    if verify_md5 and checksum != expected["md5"]:
        raise MslsExtractionError(f"MSLS volume checksum mismatch for {name}")
    return {
        "path": str(path),
        "size_bytes": size,
        "md5": checksum,
        "official_md5": expected["md5"],
        "official_md5_matches": checksum == expected["md5"] if verify_md5 else None,
    }


def _safe_output_path(raw_path: Any, image_root: Path) -> Path:
    path = Path(str(raw_path))
    root = image_root.resolve()
    resolved = path.resolve()
    if not resolved.is_relative_to(root):
        raise MslsExtractionError(f"image_path escapes image_root: {path}")
    return path


def _expected_member(row: Any) -> tuple[str, str]:
    source_id = str(row.source_image_id)
    split = str(row.msls_original_split)
    volume = str(row.msls_volume)
    if split not in MSLS_SPLIT_VOLUME:
        raise MslsExtractionError(f"invalid MSLS original split: {split}")
    if volume != MSLS_SPLIT_VOLUME[split]:
        raise MslsExtractionError(f"MSLS split/volume mismatch: {split}/{volume}")
    expected = f"train_val/moscow/{split}/images/{source_id}.jpg"
    member = str(row.msls_archive_member)
    posix = PurePosixPath(member)
    if posix.is_absolute() or ".." in posix.parts or member != expected:
        raise MslsExtractionError(f"invalid MSLS archive member for {source_id}")
    return volume, member


def _validate_info(info: ZipInfo, *, max_member_bytes: int) -> None:
    if info.is_dir() or info.file_size < 1 or info.file_size > max_member_bytes:
        raise MslsExtractionError(f"unsafe MSLS member size: {info.filename}")
    if info.flag_bits & 0x1:
        raise MslsExtractionError(f"encrypted MSLS member is unsupported: {info.filename}")
    mode = info.external_attr >> 16
    if mode and stat.S_ISLNK(mode):
        raise MslsExtractionError(f"symlink MSLS member is unsupported: {info.filename}")


def _extract_one(
    archive: ZipFile,
    info: ZipInfo,
    output: Path,
    *,
    min_valid_size_bytes: int,
    min_width: int,
    min_height: int,
    max_member_bytes: int,
) -> tuple[str, int]:
    existing = validate_image_file(
        output,
        min_valid_size_bytes=min_valid_size_bytes,
        min_width=min_width,
        min_height=min_height,
        max_file_size_bytes=max_member_bytes,
    )
    if existing.valid:
        return "skipped", output.stat().st_size

    _validate_info(info, max_member_bytes=max_member_bytes)
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".tmp", dir=output.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as destination, archive.open(info, "r") as source:
            shutil.copyfileobj(source, destination, length=1024 * 1024)
            destination.flush()
            os.fsync(destination.fileno())
        if temporary.stat().st_size != info.file_size:
            raise MslsExtractionError(f"MSLS member size changed while extracting: {info.filename}")
        validation = validate_image_file(
            temporary,
            min_valid_size_bytes=min_valid_size_bytes,
            min_width=min_width,
            min_height=min_height,
            max_file_size_bytes=max_member_bytes,
        )
        if not validation.valid:
            raise MslsExtractionError(
                f"invalid extracted MSLS image ({validation.reason}): {info.filename}"
            )
        os.replace(temporary, output)
        return "extracted", info.file_size
    finally:
        temporary.unlink(missing_ok=True)


def run(
    manifest_path: Path,
    archives: Mapping[str, Path],
    image_root: Path,
    stats_path: Path,
    errors_path: Path,
    *,
    verify_md5: bool = True,
    min_valid_size_bytes: int = 1_000,
    min_width: int = 64,
    min_height: int = 64,
    max_member_bytes: int = 10 * 1024 * 1024,
) -> dict[str, Any]:
    if min_valid_size_bytes < 1 or min_width < 1 or min_height < 1 or max_member_bytes < 1:
        raise ValueError("image validation limits must be positive")
    frame = read_manifest(manifest_path, allow_empty=False)
    required = {
        "msls_original_split",
        "msls_archive_member",
        "msls_volume",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise MslsExtractionError(f"MSLS manifest is missing columns: {', '.join(missing)}")
    if set(frame["source"].astype(str)) != {"msls"}:
        raise MslsExtractionError("MSLS extraction manifest must contain only source=msls")

    rows_by_volume: dict[str, list[tuple[Any, str, Path]]] = {}
    seen_members: set[tuple[str, str]] = set()
    seen_outputs: set[Path] = set()
    for row in frame.itertuples(index=False):
        volume, member = _expected_member(row)
        output = _safe_output_path(row.image_path, image_root)
        identity = (volume, member)
        if identity in seen_members or output.resolve() in seen_outputs:
            raise MslsExtractionError("duplicate MSLS archive member or output path")
        seen_members.add(identity)
        seen_outputs.add(output.resolve())
        rows_by_volume.setdefault(volume, []).append((row, member, output))

    required_volumes = set(rows_by_volume)
    if not required_volumes:
        raise MslsExtractionError("MSLS extraction manifest is empty")
    if not required_volumes.issubset(archives):
        missing_volumes = sorted(required_volumes - set(archives))
        raise MslsExtractionError(f"missing local MSLS archives: {', '.join(missing_volumes)}")

    archive_audit = {
        name: _validate_archive(name, Path(archives[name]), verify_md5=verify_md5)
        for name in sorted(required_volumes)
    }
    started = time.monotonic()
    counts: Counter[str] = Counter()
    bytes_by_status: Counter[str] = Counter()
    per_volume: dict[str, dict[str, int]] = {}
    errors: list[dict[str, str]] = []
    completed = 0
    total = len(frame)

    for volume in sorted(required_volumes):
        volume_counts: Counter[str] = Counter()
        try:
            with ZipFile(archives[volume]) as archive:
                tasks: list[tuple[int, Any, ZipInfo, Path]] = []
                for row, member, output in rows_by_volume[volume]:
                    try:
                        info = archive.getinfo(member)
                    except KeyError as exc:
                        raise MslsExtractionError(f"missing selected MSLS member: {member}") from exc
                    tasks.append((info.header_offset, row, info, output))
                for _, row, info, output in sorted(tasks, key=lambda value: value[0]):
                    try:
                        status, byte_count = _extract_one(
                            archive,
                            info,
                            output,
                            min_valid_size_bytes=min_valid_size_bytes,
                            min_width=min_width,
                            min_height=min_height,
                            max_member_bytes=max_member_bytes,
                        )
                    except Exception as exc:  # noqa: BLE001 - collect every bad member for a resumable rerun
                        status, byte_count = "failed", 0
                        errors.append(
                            {
                                "reference_id": str(row.id),
                                "source_image_id": str(row.source_image_id),
                                "volume": volume,
                                "reason": sanitize_error_message(exc),
                            }
                        )
                    counts[status] += 1
                    volume_counts[status] += 1
                    bytes_by_status[status] += byte_count
                    completed += 1
                    if completed % 1_000 == 0 or completed == total:
                        logger.info(
                            "msls_extract_progress total=%s extracted=%s skipped=%s failed=%s",
                            completed,
                            counts["extracted"],
                            counts["skipped"],
                            counts["failed"],
                        )
        except (BadZipFile, OSError, MslsExtractionError) as exc:
            raise MslsExtractionError(f"cannot read {volume}: {sanitize_error_message(exc)}") from exc
        per_volume[volume] = {
            "requested": len(rows_by_volume[volume]),
            "extracted": volume_counts["extracted"],
            "skipped": volume_counts["skipped"],
            "failed": volume_counts["failed"],
        }

    elapsed = time.monotonic() - started
    stats: dict[str, Any] = {
        "schema_version": 1,
        "complete": counts["failed"] == 0 and completed == total,
        "manifest": str(manifest_path),
        "image_root": str(image_root),
        "images_requested": total,
        "images_extracted": counts["extracted"],
        "already_valid_images_skipped": counts["skipped"],
        "failed_images": counts["failed"],
        "bytes_extracted": bytes_by_status["extracted"],
        "elapsed_seconds": round(elapsed, 6),
        "images_per_second": round(total / elapsed, 6) if elapsed else 0.0,
        "verify_md5": verify_md5,
        "archives": archive_audit,
        "per_volume": per_volume,
        "errors_path": str(errors_path),
    }
    write_json(errors_path, errors)
    write_json(stats_path, stats)
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))
    if not stats["complete"]:
        raise MslsExtractionError(
            f"MSLS extraction incomplete: {counts['failed']} of {total} images failed"
        )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract selected Moscow MSLS images from local ZIP volumes")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--volume",
        action="append",
        type=_parse_volume,
        required=True,
        metavar="NAME=/absolute/path.zip",
    )
    parser.add_argument("--image-root", type=Path, default=Path("data/raw/moscow/images"))
    parser.add_argument("--stats", type=Path, default=Path("data/raw/moscow/msls_extract.stats.json"))
    parser.add_argument("--errors", type=Path, default=Path("data/raw/moscow/msls_extract.errors.json"))
    parser.add_argument("--skip-md5", action="store_true")
    parser.add_argument("--min-valid-size-bytes", type=int, default=1_000)
    parser.add_argument("--min-width", type=int, default=64)
    parser.add_argument("--min-height", type=int, default=64)
    parser.add_argument("--max-member-bytes", type=int, default=10 * 1024 * 1024)
    args = parser.parse_args()
    volumes = dict(args.volume)
    if len(volumes) != len(args.volume):
        parser.error("duplicate --volume name")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    run(
        args.manifest,
        volumes,
        args.image_root,
        args.stats,
        args.errors,
        verify_md5=not args.skip_md5,
        min_valid_size_bytes=args.min_valid_size_bytes,
        min_width=args.min_width,
        min_height=args.min_height,
        max_member_bytes=args.max_member_bytes,
    )


if __name__ == "__main__":
    main()
