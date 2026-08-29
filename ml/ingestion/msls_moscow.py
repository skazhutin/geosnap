"""Import and spatially select the Moscow subset of the MSLS metadata archive.

The MSLS archive contains an historical ``database``/``query`` split.  Those
labels are retained only as provenance: GeoSnap builds its own leakage-safe
evaluation split after combining all reference sources.

This module deliberately does not download or extract image volumes.  It emits
the exact expected archive member for every selected frame and can enrich it
with a separately produced, credential-free volume inventory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections import Counter, defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any
from zipfile import BadZipFile, ZipFile

import numpy as np
import pandas as pd

from ml.enrichment.h3_assign import to_h3, validate_resolution
from ml.ingestion.common import read_json, write_json
from ml.ingestion.merge_sources import haversine_meters
from ml.ingestion.schema import MOSCOW_BOUNDS, canonical_record, manifest_dataframe, write_manifest

MSLS_ARCHIVE_PREFIX = "train_val/moscow"
MSLS_SPLITS = ("database", "query")
MSLS_TABLES = ("raw", "seq_info", "postprocessed")
MSLS_DATASET_URL = "https://www.mapillary.com/dataset/places"
MSLS_LICENSE = "CC BY-NC-SA 4.0"
MSLS_ATTRIBUTION = "Mapillary Street-Level Sequences (MSLS) dataset contributors"
MSLS_SELECTION_SEED = "geosnap-msls-moscow-v1"
DEFAULT_MAX_CSV_MEMBER_BYTES = 512 * 1024 * 1024
MSLS_METADATA_OFFICIAL_MD5 = "4122084a9b0ca041d166f32e11599724"

# Verified against the official MSLS v1.1 image-volume central directories.
# The volume filenames are public dataset identifiers; signed download URLs are
# intentionally never accepted or persisted here.
MSLS_SPLIT_VOLUME = {
    "query": "msls_images_vol_1.zip",
    "database": "msls_images_vol_5.zip",
}
MSLS_RELEASE_VOLUME_AUDIT = {
    "msls_images_vol_1.zip": {
        "full_archive_size_bytes": 11_034_252_629,
        "moscow_frames": 77_496,
        "moscow_compressed_size_bytes": 2_451_077_791,
        "moscow_uncompressed_size_bytes": 2_470_680_709,
        "original_split": "query",
    },
    "msls_images_vol_5.zip": {
        "full_archive_size_bytes": 10_879_136_492,
        "moscow_frames": 171_878,
        "moscow_compressed_size_bytes": 5_237_643_338,
        "moscow_uncompressed_size_bytes": 5_281_515_429,
        "original_split": "database",
    },
}
MSLS_ALL_VOLUME_AUDIT = {
    "msls_images_vol_1.zip": {"full_archive_size_bytes": 11_034_252_629, "moscow_frames": 77_496},
    "msls_images_vol_2.zip": {"full_archive_size_bytes": 11_052_930_529, "moscow_frames": 0},
    "msls_images_vol_3.zip": {"full_archive_size_bytes": 10_995_967_920, "moscow_frames": 0},
    "msls_images_vol_4.zip": {"full_archive_size_bytes": 11_000_744_125, "moscow_frames": 0},
    "msls_images_vol_5.zip": {"full_archive_size_bytes": 10_879_136_492, "moscow_frames": 171_878},
    "msls_images_vol_6.zip": {"full_archive_size_bytes": 1_629_191_243, "moscow_frames": 0},
}

KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")
VOLUME_RE = re.compile(r"^msls_images_vol_[1-9][0-9]*\.zip$")
REQUIRED_COLUMNS = {
    "raw": {"key", "lon", "lat", "ca", "captured_at", "pano"},
    "seq_info": {"key", "sequence_key", "frame_number"},
    "postprocessed": {
        "key",
        "easting",
        "northing",
        "unique_cluster",
        "control_panel",
        "night",
        "view_direction",
    },
}
MSLS_EXTRA_COLUMNS = (
    "msls_original_split",
    "msls_sequence_key",
    "msls_frame_number",
    "msls_pano",
    "msls_night",
    "msls_view_direction",
    "msls_unique_cluster",
    "msls_control_panel",
    "msls_easting",
    "msls_northing",
    "msls_archive_member",
    "msls_volume",
    "msls_compressed_size_bytes",
    "msls_uncompressed_size_bytes",
    "msls_h3_cell",
    "msls_capture_year",
    "msls_season",
)


class MslsMetadataError(ValueError):
    """Raised when the local MSLS metadata cannot be imported safely."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _md5_file(path: Path) -> str:
    # MD5 is used only to match the checksum published with MSLS, never for
    # cryptographic trust. SHA-256 is recorded alongside it.
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_order_key(value: Any, seed: str) -> tuple[str, str]:
    text = str(value)
    digest = hashlib.sha256(f"{seed}:{text}".encode()).hexdigest()
    return digest, text


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_output_paths(metadata_zip: Path, paths: Iterable[Path]) -> None:
    resolved_input = metadata_zip.resolve()
    resolved_outputs = [path.resolve() for path in paths]
    if len(set(resolved_outputs)) != len(resolved_outputs):
        raise MslsMetadataError("MSLS output paths must be distinct")
    if resolved_input in resolved_outputs:
        raise MslsMetadataError("refusing to overwrite the MSLS metadata archive")


def _clean_columns(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[:, [not str(column).startswith("Unnamed:") for column in frame.columns]].copy()


def _read_table(archive: ZipFile, split: str, table: str, max_member_bytes: int) -> pd.DataFrame:
    member = f"{MSLS_ARCHIVE_PREFIX}/{split}/{table}.csv"
    try:
        info = archive.getinfo(member)
    except KeyError as exc:
        raise MslsMetadataError(f"missing required archive member: {member}") from exc
    if info.is_dir() or info.file_size <= 0:
        raise MslsMetadataError(f"required archive member is empty: {member}")
    if info.file_size > max_member_bytes:
        raise MslsMetadataError(
            f"archive member exceeds max_member_bytes ({info.file_size} > {max_member_bytes}): {member}"
        )
    try:
        with archive.open(info) as stream:
            frame = pd.read_csv(
                stream,
                dtype={"key": "string", "sequence_key": "string", "view_direction": "string"},
                low_memory=False,
            )
    except (OSError, UnicodeError, pd.errors.ParserError) as exc:
        raise MslsMetadataError(f"could not parse {member}: {type(exc).__name__}") from exc
    frame = _clean_columns(frame)
    missing = sorted(REQUIRED_COLUMNS[table] - set(frame.columns))
    if missing:
        raise MslsMetadataError(f"{member} is missing columns: {', '.join(missing)}")
    keys = frame["key"].astype("string")
    if keys.isna().any() or (keys.str.strip() == "").any():
        raise MslsMetadataError(f"{member} contains blank keys")
    frame["key"] = keys.str.strip()
    invalid_key = ~frame["key"].str.fullmatch(KEY_RE)
    if invalid_key.any():
        raise MslsMetadataError(f"{member} contains unsafe image keys")
    if frame["key"].duplicated().any():
        raise MslsMetadataError(f"{member} contains duplicate keys")
    return frame


def _parse_bool(series: pd.Series, *, label: str) -> pd.Series:
    mapping = {
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "yes": True,
        "no": False,
    }
    normalized = series.astype("string").str.strip().str.lower().map(mapping)
    if normalized.isna().any():
        raise MslsMetadataError(f"{label} contains invalid boolean values")
    return normalized.astype(bool)


def _parse_numeric(
    frame: pd.DataFrame,
    columns: Sequence[str],
    *,
    split: str,
) -> None:
    for column in columns:
        numeric = pd.to_numeric(frame[column], errors="coerce")
        finite = np.isfinite(numeric.to_numpy(dtype=np.float64, na_value=np.nan))
        if numeric.isna().any() or not bool(finite.all()):
            raise MslsMetadataError(f"train_val/moscow/{split} contains invalid {column} values")
        frame[column] = numeric


def _load_split(archive: ZipFile, split: str, max_member_bytes: int) -> pd.DataFrame:
    tables = {table: _read_table(archive, split, table, max_member_bytes) for table in MSLS_TABLES}
    expected_keys = set(tables["raw"]["key"])
    if not expected_keys:
        raise MslsMetadataError(f"MSLS {split} metadata is empty")
    for table in ("seq_info", "postprocessed"):
        keys = set(tables[table]["key"])
        if keys != expected_keys:
            missing = len(expected_keys - keys)
            extra = len(keys - expected_keys)
            raise MslsMetadataError(f"MSLS {split}/{table}.csv key mismatch: missing={missing}, extra={extra}")

    joined = tables["raw"].merge(tables["seq_info"], on="key", how="inner", validate="one_to_one")
    joined = joined.merge(tables["postprocessed"], on="key", how="inner", validate="one_to_one")
    if len(joined) != len(expected_keys):
        raise MslsMetadataError(f"MSLS {split} join changed row count")

    _parse_numeric(
        joined,
        ("lat", "lon", "ca", "frame_number", "easting", "northing", "unique_cluster"),
        split=split,
    )
    frame_numbers = joined["frame_number"].to_numpy(dtype=np.float64)
    if not bool(np.equal(frame_numbers, np.floor(frame_numbers)).all()):
        raise MslsMetadataError(f"MSLS {split} contains non-integral frame_number values")
    joined["frame_number"] = joined["frame_number"].astype("int64")
    joined["pano"] = _parse_bool(joined["pano"], label=f"MSLS {split} pano")
    joined["night"] = _parse_bool(joined["night"], label=f"MSLS {split} night")
    joined["control_panel"] = _parse_bool(joined["control_panel"], label=f"MSLS {split} control_panel")

    sequences = joined["sequence_key"].astype("string").str.strip()
    if sequences.isna().any() or (sequences == "").any():
        raise MslsMetadataError(f"MSLS {split} contains blank sequence_key values")
    if (~sequences.str.fullmatch(KEY_RE)).any():
        raise MslsMetadataError(f"MSLS {split} contains unsafe sequence_key values")
    joined["sequence_key"] = sequences
    directions = joined["view_direction"].astype("string").str.strip()
    if directions.isna().any() or (directions == "").any():
        raise MslsMetadataError(f"MSLS {split} contains blank view_direction values")
    joined["view_direction"] = directions

    timestamps = pd.to_datetime(joined["captured_at"], errors="coerce", utc=True)
    if timestamps.isna().any():
        raise MslsMetadataError(f"MSLS {split} contains invalid captured_at values")
    joined["msls_capture_timestamp"] = timestamps
    joined["msls_original_split"] = split
    joined["msls_archive_member"] = [f"{MSLS_ARCHIVE_PREFIX}/{split}/images/{key}.jpg" for key in joined["key"]]
    return joined


def load_moscow_metadata(
    metadata_zip: Path,
    *,
    strict_aoi: bool = True,
    max_member_bytes: int = DEFAULT_MAX_CSV_MEMBER_BYTES,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load and exactly join both Moscow metadata splits directly from the ZIP."""

    if max_member_bytes < 1:
        raise ValueError("max_member_bytes must be >= 1")
    if not metadata_zip.is_file():
        raise FileNotFoundError(metadata_zip)
    try:
        with ZipFile(metadata_zip) as archive:
            frames = [_load_split(archive, split, max_member_bytes) for split in MSLS_SPLITS]
    except BadZipFile as exc:
        raise MslsMetadataError(f"invalid MSLS metadata ZIP: {metadata_zip}") from exc

    combined = pd.concat(frames, ignore_index=True)
    if combined["key"].duplicated().any():
        raise MslsMetadataError("MSLS image keys overlap across original database/query splits")
    if combined.duplicated(["sequence_key", "frame_number"]).any():
        raise MslsMetadataError("MSLS sequence/frame pairs are not unique")

    min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
    inside = combined["lat"].between(min_lat, max_lat) & combined["lon"].between(min_lon, max_lon)
    outside_count = int((~inside).sum())
    if outside_count and strict_aoi:
        raise MslsMetadataError(f"MSLS Moscow metadata contains {outside_count} rows outside configured Moscow bounds")
    combined = combined.loc[inside].copy()
    if combined.empty:
        raise MslsMetadataError("no valid Moscow MSLS rows remain after AOI filtering")

    combined["msls_capture_year"] = combined["msls_capture_timestamp"].dt.year.astype("int64")
    combined["msls_season"] = combined["msls_capture_timestamp"].dt.month.map(_season_for_month)
    audit = {
        "archive_rows": sum(len(frame) for frame in frames),
        "rows_by_original_split": {split: int(len(frame)) for split, frame in zip(MSLS_SPLITS, frames, strict=True)},
        "outside_moscow_rows": outside_count,
        "valid_moscow_rows": int(len(combined)),
        "unique_image_keys": int(combined["key"].nunique()),
        "unique_provider_sequences": int(combined["sequence_key"].nunique()),
        "cross_original_split_sequences": int(
            (combined.groupby("sequence_key")["msls_original_split"].nunique() > 1).sum()
        ),
    }
    return combined.reset_index(drop=True), audit


def _season_for_month(month: int) -> str:
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "autumn"


def _normalize_member(value: Any) -> str:
    member = str(value or "").strip().replace("\\", "/").lstrip("/")
    path = PurePosixPath(member)
    if not member or path.is_absolute() or ".." in path.parts:
        raise MslsMetadataError("volume inventory contains an unsafe archive member")
    if not member.startswith(f"{MSLS_ARCHIVE_PREFIX}/") or path.suffix.lower() not in {".jpg", ".jpeg"}:
        raise MslsMetadataError(f"volume inventory member is not an MSLS Moscow image: {member}")
    return member


def _normalize_volume(value: Any) -> str:
    volume = str(value or "").strip()
    if "://" in volume or any(character in volume for character in "?#/\\"):
        raise MslsMetadataError("volume inventory must use a credential-free volume filename")
    if not VOLUME_RE.fullmatch(volume):
        raise MslsMetadataError(f"invalid MSLS image volume filename: {volume}")
    return volume


def _optional_size(entry: Mapping[str, Any], *keys: str) -> int | None:
    value = next((entry[key] for key in keys if key in entry and entry[key] is not None), None)
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise MslsMetadataError(f"invalid volume inventory byte size: {value}") from exc
    if parsed < 0:
        raise MslsMetadataError("volume inventory byte sizes must be >= 0")
    return parsed


def _iter_volume_entries(payload: Any) -> Iterable[tuple[Mapping[str, Any], Any]]:
    if isinstance(payload, list):
        for entry in payload:
            if not isinstance(entry, Mapping):
                raise MslsMetadataError("volume inventory entries must be objects")
            yield entry, None
        return
    if not isinstance(payload, Mapping):
        raise MslsMetadataError("volume inventory must be an object or array")
    entries = payload.get("entries")
    if isinstance(entries, list):
        for entry in entries:
            if not isinstance(entry, Mapping):
                raise MslsMetadataError("volume inventory entries must be objects")
            yield entry, None
        return
    members = payload.get("members")
    if isinstance(members, Mapping):
        for member, value in members.items():
            if isinstance(value, Mapping):
                yield {"archive_member": member, **value}, None
            else:
                yield {"archive_member": member, "volume": value}, None
        return
    volumes = payload.get("volumes")
    if isinstance(volumes, list):
        for volume_entry in volumes:
            if not isinstance(volume_entry, Mapping):
                raise MslsMetadataError("volume inventory volumes must be objects")
            default_volume = volume_entry.get("volume") or volume_entry.get("name") or volume_entry.get("filename")
            nested = volume_entry.get("entries") or volume_entry.get("members")
            if not isinstance(nested, list):
                raise MslsMetadataError("each volume inventory volume must contain an entries/members array")
            for entry in nested:
                if isinstance(entry, str):
                    yield {"archive_member": entry}, default_volume
                elif isinstance(entry, Mapping):
                    yield entry, default_volume
                else:
                    raise MslsMetadataError("volume inventory entries must be strings or objects")
        return
    raise MslsMetadataError("volume inventory has no supported entries, members, or volumes field")


def load_volume_index(path: Path | None) -> dict[str, dict[str, Any]]:
    """Load safe member-to-volume metadata without retaining signed URLs."""

    if path is None:
        return {}
    if not path.is_file():
        raise FileNotFoundError(path)
    payload = read_json(path, default=None)
    lookup: dict[str, dict[str, Any]] = {}
    for raw_entry, default_volume in _iter_volume_entries(payload):
        member_value = raw_entry.get("archive_member") or raw_entry.get("member") or raw_entry.get("path")
        if not member_value and raw_entry.get("key") and raw_entry.get("split"):
            member_value = f"{MSLS_ARCHIVE_PREFIX}/{raw_entry['split']}/images/{raw_entry['key']}.jpg"
        member = _normalize_member(member_value)
        volume = _normalize_volume(raw_entry.get("volume") or default_volume)
        normalized = {
            "volume": volume,
            "compressed_size_bytes": _optional_size(
                raw_entry, "compressed_size_bytes", "compressed_size", "compress_size"
            ),
            "uncompressed_size_bytes": _optional_size(
                raw_entry, "uncompressed_size_bytes", "uncompressed_size", "file_size", "size"
            ),
        }
        previous = lookup.get(member)
        if previous is not None and previous != normalized:
            raise MslsMetadataError(f"conflicting volume inventory entries for {member}")
        lookup[member] = normalized
    return lookup


def _attach_volume_index(frame: pd.DataFrame, lookup: Mapping[str, Mapping[str, Any]]) -> pd.DataFrame:
    result = frame.copy()
    matches = [lookup.get(member, {}) for member in result["msls_archive_member"]]
    expected_volumes = [MSLS_SPLIT_VOLUME[str(split)] for split in result["msls_original_split"]]
    for member, expected, match in zip(result["msls_archive_member"], expected_volumes, matches, strict=True):
        supplied = match.get("volume")
        if supplied is not None and supplied != expected:
            raise MslsMetadataError(
                f"volume inventory conflicts with verified MSLS release mapping for {member}: "
                f"expected {expected}, found {supplied}"
            )
    result["msls_volume"] = pd.Series(expected_volumes, dtype="string", index=result.index)
    result["msls_compressed_size_bytes"] = pd.Series(
        [match.get("compressed_size_bytes") for match in matches], dtype="Int64", index=result.index
    )
    result["msls_uncompressed_size_bytes"] = pd.Series(
        [match.get("uncompressed_size_bytes") for match in matches], dtype="Int64", index=result.index
    )
    return result


def _heading_delta(first: float, second: float) -> float:
    delta = abs((float(first) - float(second)) % 360.0)
    return min(delta, 360.0 - delta)


def _evenly_limit(indices: list[int], limit: int | None) -> list[int]:
    if limit is None or len(indices) <= limit:
        return indices
    if limit == 1:
        return [indices[len(indices) // 2]]
    positions = [round(position * (len(indices) - 1) / (limit - 1)) for position in range(limit)]
    return [indices[position] for position in positions]


def suppress_near_identical_sequence_frames(
    frame: pd.DataFrame,
    *,
    min_spacing_m: float,
    heading_change_deg: float,
    min_heading_spacing_m: float,
    max_per_sequence: int | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Keep spatially or directionally novel frames along each provider sequence."""

    if min_spacing_m < 0 or min_heading_spacing_m < 0:
        raise ValueError("spacing thresholds must be >= 0")
    if min_heading_spacing_m > min_spacing_m:
        raise ValueError("min_heading_spacing_m must be <= min_spacing_m")
    if not 0 <= heading_change_deg <= 180:
        raise ValueError("heading_change_deg must be in [0, 180]")
    if max_per_sequence is not None and max_per_sequence < 1:
        raise ValueError("max_per_sequence must be >= 1 when set")

    kept: list[int] = []
    before_sequence_limit = 0
    for _, group in frame.groupby("sequence_key", sort=True):
        ordered = group.sort_values(["frame_number", "key", "msls_original_split"], kind="stable")
        sequence_indices: list[int] = []
        previous: Any | None = None
        for row in ordered.itertuples():
            if previous is None:
                sequence_indices.append(int(row.Index))
                previous = row
                continue
            distance = haversine_meters(float(previous.lat), float(previous.lon), float(row.lat), float(row.lon))
            direction_changed = str(previous.view_direction) != str(row.view_direction)
            heading_changed = _heading_delta(float(previous.ca), float(row.ca)) >= heading_change_deg
            viewpoint_keep = (direction_changed or heading_changed) and distance >= min_heading_spacing_m
            if distance >= min_spacing_m or viewpoint_keep:
                sequence_indices.append(int(row.Index))
                previous = row
        before_sequence_limit += len(sequence_indices)
        kept.extend(_evenly_limit(sequence_indices, max_per_sequence))

    selected = frame.loc[kept].copy()
    report = {
        "input_records": int(len(frame)),
        "after_near_identical_suppression": int(before_sequence_limit),
        "near_identical_frames_removed": int(len(frame) - before_sequence_limit),
        "after_optional_sequence_limit": int(len(selected)),
        "optional_sequence_limit_removed": int(before_sequence_limit - len(selected)),
    }
    return selected, report


def _round_robin(groups: Mapping[str, Sequence[int]], *, seed: str) -> list[int]:
    queues = {key: deque(values) for key, values in groups.items() if values}
    keys = sorted(queues, key=lambda key: _stable_order_key(key, seed))
    result: list[int] = []
    while queues:
        for key in list(keys):
            queue = queues.get(key)
            if queue is None:
                continue
            result.append(queue.popleft())
            if not queue:
                del queues[key]
        keys = [key for key in keys if key in queues]
    return result


def _diversity_bucket(row: Any) -> str:
    return "|".join(
        (
            str(row.msls_capture_year),
            str(row.msls_season),
            str(row.view_direction).strip().lower(),
            "night" if bool(row.night) else "day",
            "pano" if bool(row.pano) else "perspective",
            str(row.msls_original_split),
        )
    )


def balanced_selection_order(frame: pd.DataFrame, *, seed: str) -> list[int]:
    """Round-robin cells, diversity buckets, and sequences deterministically."""

    by_cell_bucket_sequence: dict[str, dict[str, dict[str, list[int]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    ordered = frame.sort_values(["msls_h3_cell", "frame_number", "key", "msls_original_split"], kind="stable")
    for row in ordered.itertuples():
        by_cell_bucket_sequence[str(row.msls_h3_cell)][_diversity_bucket(row)][str(row.sequence_key)].append(
            int(row.Index)
        )

    cell_queues: dict[str, list[int]] = {}
    for cell, buckets in by_cell_bucket_sequence.items():
        bucket_queues = {
            bucket: _round_robin(sequences, seed=f"{seed}:sequence:{cell}:{bucket}")
            for bucket, sequences in buckets.items()
        }
        cell_queues[cell] = _round_robin(bucket_queues, seed=f"{seed}:bucket:{cell}")
    return _round_robin(cell_queues, seed=f"{seed}:cell")


def _heading_octant(value: float) -> str:
    labels = ("N", "NE", "E", "SE", "S", "SW", "W", "NW")
    return labels[int(((float(value) % 360.0) + 22.5) // 45.0) % 8]


def _counter(series: Iterable[Any]) -> dict[str, int]:
    return dict(sorted(Counter(str(value) for value in series).items()))


def _numeric_summary(values: Iterable[Any]) -> dict[str, float | int | None]:
    array = np.asarray(list(values), dtype=np.float64)
    array = array[np.isfinite(array)]
    if len(array) == 0:
        return {"count": 0, "min": None, "p50": None, "p90": None, "p99": None, "max": None, "mean": None}
    return {
        "count": int(len(array)),
        "min": float(array.min()),
        "p50": float(np.percentile(array, 50)),
        "p90": float(np.percentile(array, 90)),
        "p99": float(np.percentile(array, 99)),
        "max": float(array.max()),
        "mean": float(array.mean()),
    }


def _fixed_grid_stats(frame: pd.DataFrame, *, lat_bins: int = 40, lon_bins: int = 60) -> dict[str, Any]:
    counts, _, _ = np.histogram2d(
        frame["lat"].to_numpy(dtype=np.float64),
        frame["lon"].to_numpy(dtype=np.float64),
        bins=(lat_bins, lon_bins),
        range=((MOSCOW_BOUNDS[0], MOSCOW_BOUNDS[1]), (MOSCOW_BOUNDS[2], MOSCOW_BOUNDS[3])),
    )
    occupied = int(np.count_nonzero(counts))
    total = int(counts.size)
    return {
        "lat_bins": lat_bins,
        "lon_bins": lon_bins,
        "occupied_cells": occupied,
        "empty_cells": total - occupied,
        "occupancy_ratio": occupied / total,
        "references_per_occupied_cell_mean": float(counts.sum() / occupied) if occupied else 0.0,
    }


def _volume_stats(frame: pd.DataFrame) -> dict[str, Any]:
    mapped = frame["msls_volume"].notna()
    per_volume: dict[str, Any] = {}
    for volume, subset in frame.loc[mapped].groupby("msls_volume", sort=True):
        release = MSLS_RELEASE_VOLUME_AUDIT.get(str(volume))
        estimated_compressed = None
        estimated_uncompressed = None
        if release:
            estimated_compressed = round(
                len(subset) * int(release["moscow_compressed_size_bytes"]) / int(release["moscow_frames"])
            )
            estimated_uncompressed = round(
                len(subset) * int(release["moscow_uncompressed_size_bytes"]) / int(release["moscow_frames"])
            )
        per_volume[str(volume)] = {
            "frames": int(len(subset)),
            "compressed_size_bytes": int(subset["msls_compressed_size_bytes"].sum(min_count=1))
            if subset["msls_compressed_size_bytes"].notna().any()
            else None,
            "uncompressed_size_bytes": int(subset["msls_uncompressed_size_bytes"].sum(min_count=1))
            if subset["msls_uncompressed_size_bytes"].notna().any()
            else None,
            "estimated_compressed_size_bytes_from_release_mean": estimated_compressed,
            "estimated_uncompressed_size_bytes_from_release_mean": estimated_uncompressed,
            "full_archive_size_bytes_if_downloaded": int(release["full_archive_size_bytes"]) if release else None,
        }
    required_archives = sorted(per_volume)
    return {
        "mapped_frames": int(mapped.sum()),
        "unmapped_frames": int((~mapped).sum()),
        "mapping_coverage_ratio": float(mapped.mean()) if len(frame) else 0.0,
        "mapping_method": "verified_msls_v1.1_central_directory_split_mapping",
        "required_image_volumes": required_archives,
        "full_archive_download_size_bytes": sum(
            int(MSLS_RELEASE_VOLUME_AUDIT[volume]["full_archive_size_bytes"])
            for volume in required_archives
            if volume in MSLS_RELEASE_VOLUME_AUDIT
        ),
        "per_volume": per_volume,
    }


def describe_coverage(frame: pd.DataFrame, *, h3_resolution: int) -> dict[str, Any]:
    sequence_counts = frame.groupby("sequence_key", sort=False).size()
    cell_counts = frame.groupby("msls_h3_cell", sort=False).size()
    dates = frame["msls_capture_timestamp"]
    return {
        "records": int(len(frame)),
        "geographic_coverage": {
            "bounds": {
                "min_lat": float(frame["lat"].min()),
                "max_lat": float(frame["lat"].max()),
                "min_lon": float(frame["lon"].min()),
                "max_lon": float(frame["lon"].max()),
            },
            "h3_resolution": h3_resolution,
            "unique_h3_cells": int(frame["msls_h3_cell"].nunique()),
            "fixed_moscow_grid": _fixed_grid_stats(frame),
        },
        "spatial_density": {
            "references_per_occupied_h3_cell": _numeric_summary(cell_counts),
            "sequences_per_occupied_h3_cell": _numeric_summary(frame.groupby("msls_h3_cell")["sequence_key"].nunique()),
        },
        "sequence_diversity": {
            "unique_provider_sequences": int(frame["sequence_key"].nunique()),
            "cross_original_split_sequences": int(
                (frame.groupby("sequence_key")["msls_original_split"].nunique() > 1).sum()
            ),
            "references_per_sequence": _numeric_summary(sequence_counts),
        },
        "viewpoint_diversity": {
            "view_direction": _counter(frame["view_direction"]),
            "heading_octants": _counter(_heading_octant(value) for value in frame["ca"]),
            "pano": {"true": int(frame["pano"].sum()), "false": int((~frame["pano"]).sum())},
        },
        "temporal_seasonal_diversity": {
            "capture_date_min": dates.min().date().isoformat(),
            "capture_date_max": dates.max().date().isoformat(),
            "unique_capture_dates": int(dates.dt.date.nunique()),
            "years": _counter(frame["msls_capture_year"]),
            "seasons": _counter(frame["msls_season"]),
            "night": {"true": int(frame["night"].sum()), "false": int((~frame["night"]).sum())},
        },
        "original_msls_split_provenance": _counter(frame["msls_original_split"]),
        "volume_mapping": _volume_stats(frame),
    }


def _coverage_cells(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    grouped = frame.groupby("msls_h3_cell", sort=True).agg(
        frames=("key", "size"), sequences=("sequence_key", "nunique")
    )
    cells: list[dict[str, Any]] = []
    for cell, row in grouped.iterrows():
        import h3

        if hasattr(h3, "cell_to_latlng"):
            lat, lon = h3.cell_to_latlng(str(cell))
        else:
            lat, lon = h3.h3_to_geo(str(cell))
        cells.append(
            {
                "h3_cell": str(cell),
                "center_lat": float(lat),
                "center_lon": float(lon),
                "frames": int(row["frames"]),
                "sequences": int(row["sequences"]),
            }
        )
    return cells


def _coverage_payload(all_rows: pd.DataFrame, selected: pd.DataFrame, h3_resolution: int) -> dict[str, Any]:
    layers = {
        "all_msls_moscow": _coverage_cells(all_rows),
        "selected_msls_moscow": _coverage_cells(selected),
    }
    for split in MSLS_SPLITS:
        layers[f"selected_{split}"] = _coverage_cells(selected[selected["msls_original_split"] == split])
    return {
        "schema_version": 1,
        "source": "msls",
        "city_id": "moscow",
        "h3_resolution": h3_resolution,
        "moscow_bounds": {
            "min_lat": MOSCOW_BOUNDS[0],
            "max_lat": MOSCOW_BOUNDS[1],
            "min_lon": MOSCOW_BOUNDS[2],
            "max_lon": MOSCOW_BOUNDS[3],
        },
        "layers": layers,
    }


def _python_optional_int(value: Any) -> int | None:
    return None if pd.isna(value) else int(value)


def _build_canonical_records(frame: pd.DataFrame, *, image_root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in frame.itertuples(index=False):
        key = str(row.key)
        original_split = str(row.msls_original_split)
        sequence_key = str(row.sequence_key)
        volume = None if pd.isna(row.msls_volume) else str(row.msls_volume)
        compressed_size = _python_optional_int(row.msls_compressed_size_bytes)
        uncompressed_size = _python_optional_int(row.msls_uncompressed_size_bytes)
        archive_member = str(row.msls_archive_member)
        metadata = {
            "dataset": "Mapillary Street-Level Sequences (MSLS)",
            "original_split": original_split,
            "sequence_key": sequence_key,
            "frame_number": int(row.frame_number),
            "pano": bool(row.pano),
            "night": bool(row.night),
            "view_direction": str(row.view_direction),
            "unique_cluster": int(row.unique_cluster),
            "control_panel": bool(row.control_panel),
            "easting": float(row.easting),
            "northing": float(row.northing),
            "archive_member": archive_member,
            "image_volume": volume,
            "compressed_size_bytes": compressed_size,
            "uncompressed_size_bytes": uncompressed_size,
        }
        record = canonical_record(
            source="msls",
            source_image_id=key,
            city_id="moscow",
            lat=float(row.lat),
            lon=float(row.lon),
            image_path=str(image_root / "msls" / original_split / f"{key}.jpg"),
            sequence_id=f"msls:{sequence_key}",
            captured_at=row.msls_capture_timestamp.isoformat(),
            heading=float(row.ca),
            quality_score=None,
            license_name=MSLS_LICENSE,
            attribution=MSLS_ATTRIBUTION,
            source_url=MSLS_DATASET_URL,
            metadata=metadata,
            download_url="",
        )
        record.update(
            {
                "msls_original_split": original_split,
                "msls_sequence_key": sequence_key,
                "msls_frame_number": int(row.frame_number),
                "msls_pano": bool(row.pano),
                "msls_night": bool(row.night),
                "msls_view_direction": str(row.view_direction),
                "msls_unique_cluster": int(row.unique_cluster),
                "msls_control_panel": bool(row.control_panel),
                "msls_easting": float(row.easting),
                "msls_northing": float(row.northing),
                "msls_archive_member": archive_member,
                "msls_volume": volume,
                "msls_compressed_size_bytes": compressed_size,
                "msls_uncompressed_size_bytes": uncompressed_size,
                "msls_h3_cell": str(row.msls_h3_cell),
                "msls_capture_year": int(row.msls_capture_year),
                "msls_season": str(row.msls_season),
            }
        )
        records.append(record)
    return records


def _coerce_msls_output_schema(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep source-specific Parquet types stable even when optional sizes are all null."""

    result = frame.copy()
    for column in (
        "msls_frame_number",
        "msls_unique_cluster",
        "msls_compressed_size_bytes",
        "msls_uncompressed_size_bytes",
        "msls_capture_year",
    ):
        result[column] = pd.to_numeric(result[column], errors="raise").astype("Int64")
    for column in ("msls_pano", "msls_night", "msls_control_panel"):
        result[column] = result[column].astype("boolean")
    for column in ("msls_easting", "msls_northing"):
        result[column] = pd.to_numeric(result[column], errors="raise").astype("float64")
    for column in (
        "msls_original_split",
        "msls_sequence_key",
        "msls_view_direction",
        "msls_archive_member",
        "msls_volume",
        "msls_h3_cell",
        "msls_season",
    ):
        result[column] = result[column].astype("string")
    return result


def _write_markdown(path: Path, stats: Mapping[str, Any]) -> None:
    full = stats["coverage_and_diversity"]["all_moscow_metadata"]
    selected = stats["coverage_and_diversity"]["selected_gallery"]
    volume = selected["volume_mapping"]
    lines = [
        "# MSLS Moscow metadata selection",
        "",
        f"- Complete: {stats['complete']}",
        f"- Moscow metadata frames: {stats['metadata_audit']['valid_moscow_rows']}",
        f"- Provider sequences: {stats['metadata_audit']['unique_provider_sequences']}",
        f"- Cross-original-split sequences: {stats['metadata_audit']['cross_original_split_sequences']}",
        f"- Selected frames: {stats['selection']['selected_records']}",
        f"- Selected sequences: {selected['sequence_diversity']['unique_provider_sequences']}",
        f"- Selected H3 cells (r{stats['selection']['h3_resolution']}): "
        f"{selected['geographic_coverage']['unique_h3_cells']}",
        f"- Full H3 cells (r{stats['selection']['h3_resolution']}): {full['geographic_coverage']['unique_h3_cells']}",
        f"- H3-cell retention: {stats['selection_retention']['h3_cell_retention_ratio']:.4f}",
        f"- Provider-sequence retention: {stats['selection_retention']['provider_sequence_retention_ratio']:.4f}",
        f"- Volume-mapped selected frames: {volume['mapped_frames']}",
        f"- Volume-unmapped selected frames: {volume['unmapped_frames']}",
        "",
        "## Original split is provenance only",
        "",
        "The MSLS database/query labels are not used as the final GeoSnap evaluation split.",
        "",
        "## Selected volume distribution",
        "",
        "| Volume | Frames | Estimated compressed bytes | Estimated uncompressed bytes | Full ZIP bytes |",
        "|---|---:|---:|---:|---:|",
    ]
    for name, values in volume["per_volume"].items():
        lines.append(
            f"| {name} | {values['frames']} | "
            f"{values['estimated_compressed_size_bytes_from_release_mean']} | "
            f"{values['estimated_uncompressed_size_bytes_from_release_mean']} | "
            f"{values['full_archive_size_bytes_if_downloaded']} |"
        )
    _atomic_text(path, "\n".join(lines) + "\n")


def run(
    metadata_zip: Path,
    output_parquet: Path,
    output_json: Path,
    stats_path: Path,
    *,
    coverage_path: Path | None = None,
    image_root: Path = Path("data/raw/moscow/images"),
    volume_index_path: Path | None = None,
    h3_resolution: int = 10,
    min_sequence_spacing_m: float = 15.0,
    heading_change_deg: float = 45.0,
    min_heading_spacing_m: float = 3.0,
    max_records: int | None = None,
    max_per_sequence: int | None = None,
    selection_seed: str = MSLS_SELECTION_SEED,
    strict_aoi: bool = True,
    max_member_bytes: int = DEFAULT_MAX_CSV_MEMBER_BYTES,
) -> dict[str, Any]:
    """Import MSLS Moscow metadata and publish a deterministic canonical selection."""

    validate_resolution(h3_resolution)
    if max_records is not None and max_records < 1:
        raise ValueError("max_records must be >= 1 when set")
    if not selection_seed.strip():
        raise ValueError("selection_seed must not be blank")
    coverage_target = coverage_path or stats_path.with_name(f"{stats_path.stem}.coverage.json")
    markdown_target = stats_path.with_suffix(".md")
    _validate_output_paths(
        metadata_zip,
        (output_parquet, output_json, stats_path, coverage_target, markdown_target),
    )

    all_rows, metadata_audit = load_moscow_metadata(
        metadata_zip,
        strict_aoi=strict_aoi,
        max_member_bytes=max_member_bytes,
    )
    volume_lookup = load_volume_index(volume_index_path)
    all_rows = _attach_volume_index(all_rows, volume_lookup)
    all_rows["msls_h3_cell"] = [
        to_h3(float(lat), float(lon), h3_resolution) for lat, lon in zip(all_rows["lat"], all_rows["lon"], strict=True)
    ]

    candidates, suppression = suppress_near_identical_sequence_frames(
        all_rows,
        min_spacing_m=min_sequence_spacing_m,
        heading_change_deg=heading_change_deg,
        min_heading_spacing_m=min_heading_spacing_m,
        max_per_sequence=max_per_sequence,
    )
    balanced_order = balanced_selection_order(candidates, seed=selection_seed)
    if max_records is not None:
        balanced_order = balanced_order[:max_records]
    selected = candidates.loc[balanced_order].copy().reset_index(drop=True)
    if selected.empty:
        raise MslsMetadataError("MSLS selection is empty; refusing to publish")

    records = _build_canonical_records(selected, image_root=image_root)
    manifest = _coerce_msls_output_schema(manifest_dataframe(records, extra_columns=MSLS_EXTRA_COLUMNS))
    coverage_payload = _coverage_payload(all_rows, selected, h3_resolution)
    content_hash = hashlib.sha256("\n".join(str(record["id"]) for record in records).encode("utf-8")).hexdigest()
    config = {
        "h3_resolution": h3_resolution,
        "min_sequence_spacing_m": min_sequence_spacing_m,
        "heading_change_deg": heading_change_deg,
        "min_heading_spacing_m": min_heading_spacing_m,
        "max_records": max_records,
        "max_per_sequence": max_per_sequence,
        "selection_seed": selection_seed,
        "strict_aoi": strict_aoi,
    }
    selection_stats = {
        **suppression,
        "selected_records": int(len(selected)),
        "global_cap_applied": bool(max_records is not None and len(candidates) > max_records),
        "global_cap_removed": int(max(0, len(candidates) - len(selected))),
        "selected_content_sha256": content_hash,
        **config,
    }

    # The report is the completion marker and is written only after all data
    # artifacts.  File hashes make a stale report detectable after interruption.
    write_manifest(manifest, output_parquet, allow_empty=False)
    write_json(output_json, records)
    write_json(coverage_target, coverage_payload)
    metadata_md5 = _md5_file(metadata_zip)
    all_coverage = describe_coverage(all_rows, h3_resolution=h3_resolution)
    selected_coverage = describe_coverage(selected, h3_resolution=h3_resolution)
    all_h3_cells = int(all_coverage["geographic_coverage"]["unique_h3_cells"])
    selected_h3_cells = int(selected_coverage["geographic_coverage"]["unique_h3_cells"])
    all_sequences = int(all_coverage["sequence_diversity"]["unique_provider_sequences"])
    selected_sequences = int(selected_coverage["sequence_diversity"]["unique_provider_sequences"])
    stats: dict[str, Any] = {
        "schema_version": 1,
        "complete": True,
        "source": "msls",
        "city_id": "moscow",
        "metadata_archive": {
            "path": str(metadata_zip),
            "size_bytes": metadata_zip.stat().st_size,
            "sha256": _sha256_file(metadata_zip),
            "md5": metadata_md5,
            "official_md5": MSLS_METADATA_OFFICIAL_MD5,
            "official_md5_matches": metadata_md5 == MSLS_METADATA_OFFICIAL_MD5,
        },
        "license": MSLS_LICENSE,
        "attribution": MSLS_ATTRIBUTION,
        "source_url": MSLS_DATASET_URL,
        "original_split_policy": "provenance_only_not_geosnap_evaluation_split",
        "metadata_audit": metadata_audit,
        "volume_inventory": {
            "path": str(volume_index_path) if volume_index_path else None,
            "inventory_moscow_members": len(volume_lookup),
            "verified_release_volume_audit": MSLS_ALL_VOLUME_AUDIT,
            "patch_v1_1_moscow_frames": 0,
        },
        "selection": selection_stats,
        "selection_retention": {
            "frame_retention_ratio": len(selected) / len(all_rows),
            "h3_cell_retention_ratio": selected_h3_cells / all_h3_cells,
            "provider_sequence_retention_ratio": selected_sequences / all_sequences,
        },
        "coverage_and_diversity": {
            "all_moscow_metadata": all_coverage,
            "selected_gallery": selected_coverage,
        },
        "artifacts": {
            "canonical_parquet": str(output_parquet),
            "canonical_json": str(output_json),
            "coverage_cells_json": str(coverage_target),
            "markdown": str(markdown_target),
            "canonical_parquet_sha256": _sha256_file(output_parquet),
            "canonical_json_sha256": _sha256_file(output_json),
            "coverage_cells_json_sha256": _sha256_file(coverage_target),
        },
    }
    write_json(stats_path, stats)
    _write_markdown(markdown_target, stats)
    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import and diversity-balance the MSLS Moscow metadata ZIP without downloading image volumes"
    )
    parser.add_argument("--metadata-zip", type=Path, required=True)
    parser.add_argument("--output-parquet", type=Path, default=Path("data/raw/moscow/msls_selected.parquet"))
    parser.add_argument("--output-json", type=Path, default=Path("data/raw/moscow/msls_selected.json"))
    parser.add_argument("--stats", type=Path, default=Path("data/raw/moscow/msls_selection.stats.json"))
    parser.add_argument("--coverage", type=Path)
    parser.add_argument("--image-root", type=Path, default=Path("data/raw/moscow/images"))
    parser.add_argument("--volume-index", type=Path)
    parser.add_argument("--h3-resolution", type=int, default=10)
    parser.add_argument("--min-sequence-spacing-m", type=float, default=15.0)
    parser.add_argument("--heading-change-deg", type=float, default=45.0)
    parser.add_argument("--min-heading-spacing-m", type=float, default=3.0)
    parser.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Optional disk-budget cap after balancing; 0 keeps every useful frame (default)",
    )
    parser.add_argument(
        "--max-per-sequence",
        type=int,
        default=0,
        help="Optional per-sequence cap after spacing; 0 disables the cap (default)",
    )
    parser.add_argument("--selection-seed", default=MSLS_SELECTION_SEED)
    parser.add_argument(
        "--allow-aoi-exclusions",
        action="store_true",
        help="Exclude and report out-of-bounds rows instead of rejecting the archive",
    )
    parser.add_argument("--max-csv-member-bytes", type=int, default=DEFAULT_MAX_CSV_MEMBER_BYTES)
    args = parser.parse_args()
    if args.max_records < 0 or args.max_per_sequence < 0:
        parser.error("--max-records and --max-per-sequence must be >= 0")

    run(
        metadata_zip=args.metadata_zip,
        output_parquet=args.output_parquet,
        output_json=args.output_json,
        stats_path=args.stats,
        coverage_path=args.coverage,
        image_root=args.image_root,
        volume_index_path=args.volume_index,
        h3_resolution=args.h3_resolution,
        min_sequence_spacing_m=args.min_sequence_spacing_m,
        heading_change_deg=args.heading_change_deg,
        min_heading_spacing_m=args.min_heading_spacing_m,
        max_records=args.max_records or None,
        max_per_sequence=args.max_per_sequence or None,
        selection_seed=args.selection_seed,
        strict_aoi=not args.allow_aoi_exclusions,
        max_member_bytes=args.max_csv_member_bytes,
    )


if __name__ == "__main__":
    main()
