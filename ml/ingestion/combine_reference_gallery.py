"""Fail-closed canonical gallery combination and Moscow coverage reporting.

This is the publication boundary between source-specific ingestion and the
cleaning/embedding pipeline.  It combines canonical Parquet inputs without
discarding source-specific columns, enforces source provenance and stable
identities, and writes metadata-only coverage maps.  It never downloads or
copies image bytes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import uuid
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import parse_qs, urlsplit

import numpy as np
import pandas as pd
from shapely import covers, points

from ml.cleaning.check_dataset import nearest_reference_distances
from ml.enrichment.h3_assign import to_h3, validate_resolution
from ml.ingestion.common import validate_image_file, write_json
from ml.ingestion.mapillary_citywide import AoiBoundary, load_aoi_boundary
from ml.ingestion.msls_moscow import MSLS_ATTRIBUTION, MSLS_DATASET_URL, MSLS_LICENSE
from ml.ingestion.schema import (
    MOSCOW_BOUNDS,
    PIPELINE_COLUMNS,
    ManifestSchemaError,
    coerce_manifest_schema,
    read_manifest,
    stable_reference_id,
    validate_manifest_schema,
    write_manifest,
)

DEFAULT_REQUIRED_SOURCES = ("mapillary", "kartaview", "msls")
SOURCE_COLORS = {
    "mapillary": "#e91e63",
    "kartaview": "#168aad",
    "msls": "#f2a900",
}
SOURCE_LICENSES = {
    "mapillary": "CC BY-SA 4.0",
    "kartaview": "CC BY-SA 4.0",
    "msls": MSLS_LICENSE,
}
ALLOWED_IMAGE_SUFFIXES = frozenset({".jpg", ".jpeg", ".png", ".webp"})
CoverageBounds = tuple[float, float, float, float]


class GalleryCombineError(ValueError):
    """A candidate combined reference gallery failed publication gates."""


@dataclass(frozen=True)
class _BundleArtifact:
    """One externally visible path backed by a versioned generation file."""

    name: str
    staged_path: Path
    destination: Path
    generation_relative_path: Path


@dataclass(frozen=True)
class _EndpointOrigin:
    """Enough state to restore a CLI endpoint if migration is interrupted."""

    destination: Path
    kind: str
    symlink_target: str | None = None
    backup_path: Path | None = None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def _atomic_figure(figure: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.stem}.{os.getpid()}.tmp.png")
    try:
        figure.savefig(temporary, format="png", dpi=180, bbox_inches="tight")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_artifact_paths(input_paths: Sequence[Path], artifact_paths: Sequence[Path]) -> None:
    resolved_inputs = [path.resolve() for path in input_paths]
    if len(set(resolved_inputs)) != len(resolved_inputs):
        raise GalleryCombineError("input manifest paths must be distinct")
    resolved_artifacts = [path.resolve() for path in artifact_paths]
    if len(set(resolved_artifacts)) != len(resolved_artifacts):
        raise GalleryCombineError("output artifact paths must be distinct")
    overlap = set(resolved_inputs) & set(resolved_artifacts)
    if overlap:
        raise GalleryCombineError("refusing to overwrite an input manifest")


def _lexical_absolute(path: Path) -> Path:
    """Return an absolute CLI endpoint without dereferencing an existing symlink."""

    return Path(os.path.abspath(os.fspath(path)))


def _bundle_root_for(report_json: Path, artifacts: Sequence[tuple[str, Path]]) -> Path:
    endpoint_key = "\0".join(
        f"{name}={_lexical_absolute(path)}" for name, path in sorted(artifacts, key=lambda item: item[0])
    )
    suffix = hashlib.sha256(endpoint_key.encode("utf-8")).hexdigest()[:16]
    report_parent = _lexical_absolute(report_json).parent.resolve(strict=False)
    return report_parent / f".{report_json.stem}.bundle-{suffix}"


@contextmanager
def _publication_lock(bundle_root: Path) -> Iterator[BinaryIO]:
    """Serialize the single-pointer commit for one set of CLI endpoints."""

    import fcntl

    bundle_root.mkdir(parents=True, exist_ok=True)
    with (bundle_root / "publication.lock").open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield lock
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _managed_endpoint_target(bundle_root: Path, relative_path: Path) -> str:
    return str(bundle_root / "current" / relative_path)


def _is_managed_endpoint(artifact: _BundleArtifact, bundle_root: Path) -> bool:
    if not artifact.destination.is_symlink():
        return False
    try:
        return os.readlink(artifact.destination) == _managed_endpoint_target(
            bundle_root,
            artifact.generation_relative_path,
        )
    except OSError:
        return False


def _replace_symlink(path: Path, target: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        os.symlink(target, temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _valid_generation_name(name: str) -> bool:
    for prefix in ("generation-", "legacy-"):
        if not name.startswith(prefix):
            continue
        suffix = name.removeprefix(prefix)
        return len(suffix) == 32 and all(character in "0123456789abcdef" for character in suffix)
    return False


def _current_generation_name(bundle_root: Path) -> str | None:
    current = bundle_root / "current"
    if not current.is_symlink():
        return None
    try:
        target = Path(os.readlink(current))
    except OSError:
        return None
    if target.is_absolute() or len(target.parts) != 2 or target.parts[0] != "generations":
        return None
    name = target.parts[1]
    return name if _valid_generation_name(name) else None


def _remove_owned_generation(bundle_root: Path, generation_name: str) -> None:
    if not _valid_generation_name(generation_name):
        raise GalleryCombineError(f"unsafe_generation_cleanup_name:{generation_name}")
    if _current_generation_name(bundle_root) == generation_name:
        return
    generations = bundle_root / "generations"
    target = generations / generation_name
    if target.parent != generations:
        raise GalleryCombineError(f"unsafe_generation_cleanup_path:{target}")
    if target.is_symlink():
        target.unlink(missing_ok=True)
    elif target.exists():
        shutil.rmtree(target)


def _select_safe_previous_generation(
    bundle_root: Path,
    *,
    current_name: str,
    preferred_name: str | None,
) -> str | None:
    generations = bundle_root / "generations"
    if (
        preferred_name is not None
        and preferred_name != current_name
        and _valid_generation_name(preferred_name)
        and (generations / preferred_name).is_dir()
        and not (generations / preferred_name).is_symlink()
    ):
        return preferred_name
    # Never promote an arbitrary complete-but-unpublished orphan to
    # "previous" merely because it is recent.  Zero predecessors is safer
    # than retaining a generation that was not observed as current or created
    # explicitly as the byte-identical legacy migration point.
    return None


def _prune_generations_locked(
    bundle_root: Path,
    *,
    preferred_previous: str | None,
) -> None:
    """Retain the active generation and at most one complete predecessor.

    The caller must hold ``publication.lock`` so no pointer switch can race the
    selection or removal.
    """

    current_name = _current_generation_name(bundle_root)
    if current_name is None:
        raise GalleryCombineError("cannot_prune_without_managed_current_generation")
    previous_name = _select_safe_previous_generation(
        bundle_root,
        current_name=current_name,
        preferred_name=preferred_previous,
    )
    keep = {current_name}
    if previous_name is not None:
        keep.add(previous_name)
    generations = bundle_root / "generations"
    for path in list(generations.iterdir()):
        if path.name in keep or not _valid_generation_name(path.name):
            continue
        _remove_owned_generation(bundle_root, path.name)


def _snapshot_endpoint_origins(
    artifacts: Sequence[_BundleArtifact],
    *,
    transaction_id: str,
) -> list[_EndpointOrigin]:
    origins: list[_EndpointOrigin] = []
    try:
        for artifact in artifacts:
            destination = artifact.destination
            if destination.is_symlink():
                origins.append(
                    _EndpointOrigin(
                        destination=destination,
                        kind="symlink",
                        symlink_target=os.readlink(destination),
                    )
                )
                continue
            if not destination.exists():
                origins.append(_EndpointOrigin(destination=destination, kind="missing"))
                continue
            if not destination.is_file():
                raise GalleryCombineError(f"output_endpoint_not_file:{destination}")
            backup = destination.with_name(f".{destination.name}.{transaction_id}.backup")
            try:
                os.link(destination, backup)
            except OSError:
                shutil.copy2(destination, backup)
            origins.append(
                _EndpointOrigin(
                    destination=destination,
                    kind="file",
                    backup_path=backup,
                )
            )
    except Exception:
        for origin in origins:
            if origin.backup_path is not None:
                origin.backup_path.unlink(missing_ok=True)
        raise
    return origins


def _snapshot_current_pointer(current: Path) -> _EndpointOrigin:
    if current.is_symlink():
        return _EndpointOrigin(destination=current, kind="symlink", symlink_target=os.readlink(current))
    if current.exists():
        raise GalleryCombineError(f"bundle_current_not_symlink:{current}")
    return _EndpointOrigin(destination=current, kind="missing")


def _restore_origin(origin: _EndpointOrigin) -> None:
    if origin.kind == "missing":
        if not origin.destination.exists() and not origin.destination.is_symlink():
            return
        origin.destination.unlink(missing_ok=True)
        return
    if origin.kind == "symlink":
        assert origin.symlink_target is not None
        if origin.destination.is_symlink():
            try:
                if os.readlink(origin.destination) == origin.symlink_target:
                    return
            except OSError:
                pass
        _replace_symlink(origin.destination, origin.symlink_target)
        return
    if origin.kind == "file":
        if origin.backup_path is None or not origin.backup_path.is_file():
            raise GalleryCombineError(f"missing_publication_backup:{origin.destination}")
        try:
            if origin.destination.is_file() and os.path.samefile(origin.backup_path, origin.destination):
                origin.backup_path.unlink(missing_ok=True)
                return
        except OSError:
            pass
        os.replace(origin.backup_path, origin.destination)
        return
    raise AssertionError(f"unsupported endpoint origin kind: {origin.kind}")


def _restore_origins(origins: Sequence[_EndpointOrigin]) -> None:
    failures: list[str] = []
    for origin in origins:
        try:
            _restore_origin(origin)
        except Exception as exc:  # pragma: no cover - catastrophic filesystem failure
            failures.append(f"{origin.destination}:{type(exc).__name__}")
    if failures:
        raise GalleryCombineError(f"publication_rollback_failed:{','.join(failures)}")


def _cleanup_origin_backups(origins: Sequence[_EndpointOrigin]) -> None:
    for origin in origins:
        if origin.backup_path is not None:
            origin.backup_path.unlink(missing_ok=True)


def _copy_legacy_generation(
    artifacts: Sequence[_BundleArtifact],
    *,
    bundle_root: Path,
    transaction_id: str,
) -> str:
    generation_name = f"legacy-{transaction_id}"
    temporary = bundle_root / "staging" / generation_name
    final = bundle_root / "generations" / generation_name
    try:
        for artifact in artifacts:
            target = temporary / artifact.generation_relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(artifact.destination, target, follow_symlinks=True)
        os.replace(temporary, final)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return generation_name


def _publish_generation(
    *,
    bundle_root: Path,
    staging_root: Path,
    generation_name: str,
    artifacts: Sequence[_BundleArtifact],
) -> None:
    """Publish all artifacts through one atomically replaced generation pointer.

    CLI endpoints are stable symlinks through ``bundle_root/current``.  Legacy
    regular-file endpoints are first migrated to an identical generation; a
    failed migration is rolled back to the original file/symlink state.
    """

    if not artifacts:
        raise ValueError("at least one bundle artifact is required")
    relative_paths = [artifact.generation_relative_path for artifact in artifacts]
    if len(set(relative_paths)) != len(relative_paths):
        raise GalleryCombineError("duplicate_generation_relative_path")
    for artifact in artifacts:
        relative = artifact.generation_relative_path
        if relative.is_absolute() or ".." in relative.parts:
            raise GalleryCombineError(f"unsafe_generation_relative_path:{relative}")
        if not artifact.staged_path.is_file():
            raise GalleryCombineError(f"missing_staged_artifact:{artifact.name}")

    generations = bundle_root / "generations"
    generations.mkdir(parents=True, exist_ok=True)
    final_generation = generations / generation_name
    if final_generation.exists():
        raise GalleryCombineError(f"generation_already_exists:{generation_name}")

    with _publication_lock(bundle_root):
        finalized = False
        published = False
        legacy_name: str | None = None
        previous_name: str | None = None
        origins: list[_EndpointOrigin] = []
        try:
            os.replace(staging_root, final_generation)
            finalized = True
            current = bundle_root / "current"
            current_origin = _snapshot_current_pointer(current)
            previous_name = _current_generation_name(bundle_root)
            managed_ready = (
                current.is_symlink()
                and all(_is_managed_endpoint(artifact, bundle_root) for artifact in artifacts)
                and all(artifact.destination.is_file() for artifact in artifacts)
            )
            if managed_ready:
                _replace_symlink(current, f"generations/{generation_name}")
                published = True
                _prune_generations_locked(
                    bundle_root,
                    preferred_previous=previous_name,
                )
                return

            transaction_id = uuid.uuid4().hex
            origins = _snapshot_endpoint_origins(artifacts, transaction_id=transaction_id)
            old_complete = all(artifact.destination.is_file() for artifact in artifacts)
            try:
                if old_complete:
                    legacy_name = _copy_legacy_generation(
                        artifacts,
                        bundle_root=bundle_root,
                        transaction_id=transaction_id,
                    )
                    _replace_symlink(current, f"generations/{legacy_name}")

                for artifact in artifacts:
                    if _is_managed_endpoint(artifact, bundle_root):
                        continue
                    _replace_symlink(
                        artifact.destination,
                        _managed_endpoint_target(bundle_root, artifact.generation_relative_path),
                    )
                _replace_symlink(current, f"generations/{generation_name}")
                published = True
                _prune_generations_locked(
                    bundle_root,
                    preferred_previous=legacy_name if old_complete else previous_name,
                )
            except Exception as publication_error:
                rollback_error: Exception | None = None
                if not published:
                    try:
                        _restore_origins(origins)
                        _restore_origin(current_origin)
                    except Exception as exc:  # pragma: no cover - catastrophic filesystem failure
                        rollback_error = exc
                if rollback_error is not None:
                    raise GalleryCombineError(
                        f"publication_failed_and_rollback_failed:{type(publication_error).__name__}:"
                        f"{type(rollback_error).__name__}"
                    ) from rollback_error
                raise
        finally:
            try:
                _cleanup_origin_backups(origins)
            finally:
                if finalized and not published:
                    _remove_owned_generation(bundle_root, generation_name)
                    if legacy_name is not None:
                        _remove_owned_generation(bundle_root, legacy_name)
                    if _current_generation_name(bundle_root) is not None:
                        _prune_generations_locked(
                            bundle_root,
                            preferred_previous=previous_name,
                        )


def _dtype_family(series: pd.Series) -> str:
    from pandas.api.types import is_bool_dtype, is_float_dtype, is_integer_dtype

    if is_bool_dtype(series.dtype):
        return "bool"
    if is_integer_dtype(series.dtype):
        return "integer"
    if is_float_dtype(series.dtype):
        return "float"
    non_null = series.dropna()
    if len(non_null) == 0:
        # An all-null source column carries no evidence about its semantic
        # type.  Ignoring its storage dtype lets a populated input determine
        # the lossless union type instead of producing a false conflict.
        return "unknown"
    families: set[str] = set()
    for value in non_null.head(1000):
        if isinstance(value, (bool, np.bool_)):
            families.add("bool")
        elif isinstance(value, (int, np.integer)):
            families.add("integer")
        elif isinstance(value, (float, np.floating)):
            families.add("float")
        elif isinstance(value, str):
            families.add("string")
        else:
            families.add("unsupported")
    if families <= {"integer", "float"}:
        return "float" if "float" in families else "integer"
    if len(families) == 1:
        return next(iter(families))
    return "+".join(sorted(families))


def _coerce_union_extra_columns(
    combined: pd.DataFrame,
    inputs: Sequence[pd.DataFrame],
    extras: Sequence[str],
) -> pd.DataFrame:
    result = combined.copy()
    for column in extras:
        families = {
            _dtype_family(frame[column])
            for frame in inputs
            if column in frame.columns and _dtype_family(frame[column]) != "unknown"
        }
        if not families:
            families = {"string"}
        if families <= {"integer"}:
            result[column] = pd.to_numeric(result[column], errors="raise").astype("Int64")
        elif families <= {"integer", "float"}:
            result[column] = pd.to_numeric(result[column], errors="raise").astype("float64")
        elif families == {"bool"}:
            result[column] = result[column].astype("boolean")
        elif families == {"string"}:
            result[column] = result[column].astype("string")
        else:
            raise GalleryCombineError(f"extra_column_type_conflict:{column}:{','.join(sorted(families))}")
    return result


def _load_and_union(input_paths: Sequence[Path]) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if not input_paths:
        raise GalleryCombineError("at least one --input-manifest is required")
    frames: list[pd.DataFrame] = []
    input_audit: list[dict[str, Any]] = []
    for path in input_paths:
        if not path.is_file():
            raise GalleryCombineError(f"missing_input_manifest:{path}")
        try:
            frame = read_manifest(path, allow_empty=False)
        except (ManifestSchemaError, OSError, ValueError) as exc:
            raise GalleryCombineError(f"invalid_input_manifest:{path}:{exc}") from exc
        frames.append(frame)
        input_audit.append(
            {
                "path": str(path),
                "rows": int(len(frame)),
                "sources": dict(sorted(Counter(str(value) for value in frame["source"]).items())),
                "sha256": _sha256_file(path),
            }
        )

    union_columns = set().union(*(set(frame.columns) for frame in frames))
    extras = sorted(union_columns - set(PIPELINE_COLUMNS))
    # Do not let all-null physical dtypes participate in concat inference.
    # They carry no semantic type information and Pandas is changing this
    # inference behavior; populated inputs are coerced explicitly below.
    concat_frames = [frame.drop(columns=frame.columns[frame.isna().all()]) for frame in frames]
    combined = pd.concat(concat_frames, ignore_index=True, sort=False)
    for column in union_columns - set(combined.columns):
        combined[column] = pd.NA
    combined = coerce_manifest_schema(combined)
    combined = _coerce_union_extra_columns(combined, frames, extras)
    return combined[[*PIPELINE_COLUMNS, *extras]], input_audit


def _valid_https_url(value: Any, *, allowed_hosts: set[str]) -> Any | None:
    try:
        parsed = urlsplit(str(value).strip())
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme != "https"
        or (parsed.hostname or "").lower() not in allowed_hosts
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
    ):
        return None
    return parsed


def _valid_source_provenance(source: str, row: Any) -> bool:
    license_name = str(row.license)
    attribution = str(row.attribution)
    source_url = str(row.source_url)
    source_id = str(row.source_image_id)
    if license_name != SOURCE_LICENSES[source]:
        return False
    if source == "mapillary":
        parsed = _valid_https_url(source_url, allowed_hosts={"mapillary.com", "www.mapillary.com"})
        if parsed is None or parsed.path.rstrip("/") != "/app":
            return False
        query = parse_qs(parsed.query, keep_blank_values=True)
        pkeys = query.get("pKey", [])
        return (
            attribution.startswith("Mapillary image by ")
            and bool(attribution.removeprefix("Mapillary image by ").strip())
            and pkeys == [source_id]
            and set(query) <= {"focus", "pKey"}
            and query.get("focus", ["photo"]) == ["photo"]
            and not parsed.fragment
        )
    if source == "kartaview":
        parsed = _valid_https_url(source_url, allowed_hosts={"kartaview.org", "www.kartaview.org"})
        path_parts = parsed.path.split("/") if parsed is not None else []
        return (
            parsed is not None
            and path_parts[:2] == ["", "details"]
            and len(path_parts) == 5
            and path_parts[2] == str(row.sequence_id)
            and path_parts[3].isdigit()
            and path_parts[4] == "track-info"
            and not parsed.query
            and not parsed.fragment
            and attribution == "© Grab and KartaView Contributors"
        )
    if source == "msls":
        parsed = _valid_https_url(source_url, allowed_hosts={"mapillary.com", "www.mapillary.com"})
        expected = urlsplit(MSLS_DATASET_URL)
        return (
            parsed is not None
            and parsed.path.rstrip("/") == expected.path.rstrip("/")
            and not parsed.query
            and not parsed.fragment
            and attribution == MSLS_ATTRIBUTION
        )
    return False


def _resolved_image_path(value: Any, image_base: Path) -> Path:
    path = Path(str(value))
    return (path if path.is_absolute() else image_base / path).resolve(strict=False)


def _validated_image_paths(
    values: Iterable[Any],
    *,
    image_base: Path,
) -> tuple[list[Path | None], Counter[str]]:
    paths: list[Path | None] = []
    failures: Counter[str] = Counter()
    for value in values:
        text = str(value).strip()
        if not text or "\x00" in text:
            paths.append(None)
            failures["blank_or_nul"] += 1
            continue
        candidate = Path(text)
        if any(part == ".." for part in candidate.parts):
            paths.append(None)
            failures["parent_traversal"] += 1
            continue
        if candidate.suffix.lower() not in ALLOWED_IMAGE_SUFFIXES:
            paths.append(None)
            failures["unsupported_suffix"] += 1
            continue
        try:
            paths.append(_resolved_image_path(text, image_base))
        except (OSError, RuntimeError, ValueError):
            paths.append(None)
            failures["unresolvable"] += 1
    return paths, failures


def _resolved_image_boundary(image_base: Path) -> Path:
    try:
        boundary = image_base.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise GalleryCombineError(f"invalid_image_base:{image_base}") from exc
    if not boundary.is_dir():
        raise GalleryCombineError(f"invalid_image_base_not_directory:{image_base}")
    return boundary


def _coverage_bounds(aoi_boundary: AoiBoundary | None) -> CoverageBounds:
    if aoi_boundary is None:
        return MOSCOW_BOUNDS
    return (
        aoi_boundary.bounds.south,
        aoi_boundary.bounds.north,
        aoi_boundary.bounds.west,
        aoi_boundary.bounds.east,
    )


def _aoi_mask(frame: pd.DataFrame, aoi_boundary: AoiBoundary) -> pd.Series:
    lat = pd.to_numeric(frame["lat"], errors="coerce").to_numpy(dtype=np.float64)
    lon = pd.to_numeric(frame["lon"], errors="coerce").to_numpy(dtype=np.float64)
    finite = np.isfinite(lat) & np.isfinite(lon)
    inside = np.zeros(len(frame), dtype=bool)
    if finite.any():
        inside[finite] = np.asarray(
            covers(aoi_boundary.geometry, points(lon[finite], lat[finite])),
            dtype=bool,
        )
    return pd.Series(inside, index=frame.index, dtype=bool)


def validate_gallery(
    frame: pd.DataFrame,
    *,
    required_sources: Sequence[str] = DEFAULT_REQUIRED_SOURCES,
    require_images: bool = False,
    image_base: Path = Path("."),
    aoi_boundary: AoiBoundary | None = None,
) -> dict[str, Any]:
    """Validate the combined gallery, collecting all independent gate failures."""

    required = tuple(dict.fromkeys(str(source).strip().lower() for source in required_sources))
    if not required or any(not source for source in required):
        raise ValueError("required_sources must contain nonblank source names")
    structural = validate_manifest_schema(frame, allow_empty=False)
    errors = list(structural)
    if len(frame) == 0:
        raise GalleryCombineError("empty_combined_gallery")

    sources = frame["source"].astype("string")
    actual_sources = set(str(value) for value in sources.dropna())
    missing = sorted(set(required) - actual_sources)
    unexpected = sorted(actual_sources - set(required))
    if missing:
        errors.append(f"missing_required_sources:{','.join(missing)}")
    if unexpected:
        errors.append(f"unexpected_sources:{','.join(unexpected)}")

    duplicate_ids = frame["id"].astype("string").duplicated(keep=False)
    if duplicate_ids.any():
        errors.append(f"duplicate_uuid:{int(duplicate_ids.sum())}")
    duplicate_identity = frame.duplicated(["source", "source_image_id"], keep=False)
    if duplicate_identity.any():
        errors.append(f"duplicate_source_identity:{int(duplicate_identity.sum())}")

    stable = [
        str(reference_id) == stable_reference_id(str(source), str(source_id))
        for reference_id, source, source_id in zip(frame["id"], frame["source"], frame["source_image_id"], strict=True)
    ]
    unstable_count = len(stable) - sum(stable)
    if unstable_count:
        errors.append(f"unstable_reference_id:{unstable_count}")

    image_boundary = _resolved_image_boundary(image_base) if require_images else None
    resolved_paths, invalid_paths = _validated_image_paths(frame["image_path"], image_base=image_base)
    if invalid_paths:
        formatted = ",".join(f"{reason}={count}" for reason, count in sorted(invalid_paths.items()))
        errors.append(f"invalid_image_path:{sum(invalid_paths.values())}:{formatted}")
    # casefold supplements normcase on macOS, where the default filesystem is
    # commonly case-insensitive even though Python exposes POSIX path rules.
    normalized_paths = pd.Series(
        [os.path.normcase(str(path)).casefold() if path is not None else pd.NA for path in resolved_paths],
        dtype="string",
    )
    duplicate_paths = normalized_paths.duplicated(keep=False)
    if duplicate_paths.any():
        errors.append(f"duplicate_image_path:{int(duplicate_paths.sum())}")

    city = frame["city_id"].astype("string").str.strip().str.lower()
    bad_city = city != "moscow"
    if bad_city.any():
        errors.append(f"non_moscow_city_id:{int(bad_city.sum())}")
    lat = pd.to_numeric(frame["lat"], errors="coerce")
    lon = pd.to_numeric(frame["lon"], errors="coerce")
    if aoi_boundary is None:
        min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
        outside = ~lat.between(min_lat, max_lat) | ~lon.between(min_lon, max_lon)
        outside_error = "coordinate_outside_moscow_bounds"
    else:
        outside = ~_aoi_mask(frame, aoi_boundary)
        outside_error = "coordinate_outside_moscow_aoi"
    if outside.any():
        errors.append(f"{outside_error}:{int(outside.sum())}")

    sequence = frame["sequence_id"].astype("string")
    missing_sequence = sequence.isna() | (sequence.str.strip() == "")
    if missing_sequence.any():
        errors.append(f"missing_sequence_id:{int(missing_sequence.sum())}")

    bad_provenance = 0
    for row in frame.itertuples(index=False):
        source = str(row.source)
        if source not in SOURCE_LICENSES or not _valid_source_provenance(source, row):
            bad_provenance += 1
    if bad_provenance:
        errors.append(f"invalid_source_provenance:{bad_provenance}")

    image_validation_failures: Counter[str] = Counter()
    outside_image_boundary = 0
    if require_images:
        for path in resolved_paths:
            if path is None:
                continue
            assert image_boundary is not None
            if not path.is_relative_to(image_boundary):
                outside_image_boundary += 1
                continue
            validation = validate_image_file(path)
            if not validation.valid:
                image_validation_failures[str(validation.reason or "unknown")] += 1
        if image_validation_failures:
            formatted = ",".join(f"{reason}={count}" for reason, count in sorted(image_validation_failures.items()))
            missing_count = image_validation_failures.get("missing_file", 0)
            if missing_count:
                errors.append(f"missing_image_files:{missing_count}")
            errors.append(f"invalid_image_files:{sum(image_validation_failures.values())}:{formatted}")
        if outside_image_boundary:
            errors.append(f"image_path_outside_image_base:{outside_image_boundary}")

    if errors:
        raise GalleryCombineError("; ".join(dict.fromkeys(errors)))
    return {
        "status": "passed",
        "required_sources": list(required),
        "actual_sources": sorted(actual_sources),
        "stable_ids_checked": int(len(frame)),
        "unique_source_identities": int(frame[["source", "source_image_id"]].drop_duplicates().shape[0]),
        "unique_image_paths": int(normalized_paths.nunique(dropna=True)),
        "moscow_aoi_rows_checked": int(len(frame)),
        "moscow_aoi_mode": "geojson_polygon" if aoi_boundary is not None else "administrative_bbox",
        "moscow_aoi_sha256": aoi_boundary.sha256 if aoi_boundary is not None else None,
        "provenance_rows_checked": int(len(frame)),
        "require_images": require_images,
        "image_base": str(image_boundary) if image_boundary is not None else None,
        "image_files_checked": int(len(frame)) if require_images else 0,
        "image_validation_failures": dict(sorted(image_validation_failures.items())),
    }


def _provider_sequence(source: Any, sequence_id: Any) -> str:
    source_name = str(source).strip().lower()
    sequence = str(sequence_id).strip()
    prefix = f"{source_name}:"
    return sequence if sequence.lower().startswith(prefix) else f"{source_name}:{sequence}"


def _season(month: int) -> str:
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "autumn"


def _heading_octant(value: float) -> str:
    labels = ("N", "NE", "E", "SE", "S", "SW", "W", "NW")
    return labels[int(((float(value) % 360.0) + 22.5) // 45.0) % 8]


def _counts(values: Iterable[Any]) -> dict[str, int]:
    return dict(sorted(Counter(str(value) for value in values).items()))


def _numeric_summary(values: Iterable[Any]) -> dict[str, Any]:
    array = np.asarray(list(values), dtype=np.float64)
    array = array[np.isfinite(array)]
    if len(array) == 0:
        return {
            "count": 0,
            "min": None,
            "p50": None,
            "p90": None,
            "p99": None,
            "max": None,
            "mean": None,
        }
    return {
        "count": int(len(array)),
        "min": float(array.min()),
        "p50": float(np.percentile(array, 50)),
        "p90": float(np.percentile(array, 90)),
        "p99": float(np.percentile(array, 99)),
        "max": float(array.max()),
        "mean": float(array.mean()),
    }


def _fixed_grid(
    frame: pd.DataFrame,
    *,
    lat_bins: int,
    lon_bins: int,
    coverage_bounds: CoverageBounds,
) -> dict[str, Any]:
    counts, _, _ = np.histogram2d(
        frame["lat"].to_numpy(dtype=np.float64),
        frame["lon"].to_numpy(dtype=np.float64),
        bins=(lat_bins, lon_bins),
        range=((coverage_bounds[0], coverage_bounds[1]), (coverage_bounds[2], coverage_bounds[3])),
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


def _optional_string_distribution(frame: pd.DataFrame, columns: Sequence[str]) -> dict[str, Any]:
    combined = pd.Series(pd.NA, index=frame.index, dtype="string")
    fields: list[str] = []
    for column in columns:
        if column not in frame.columns:
            continue
        candidate = frame[column].astype("string").str.strip()
        candidate = candidate.mask(candidate == "")
        usable = combined.isna() & candidate.notna()
        if not usable.any():
            continue
        combined.loc[usable] = candidate.loc[usable]
        fields.append(column)
    values = combined.dropna()
    return {
        "fields": fields,
        "present": int(len(values)),
        "missing": int(len(frame) - len(values)),
        "counts": _counts(values),
    }


def _optional_boolean_distribution(frame: pd.DataFrame, columns: Sequence[str]) -> dict[str, Any]:
    combined = pd.Series(pd.NA, index=frame.index, dtype="boolean")
    fields: list[str] = []
    for column in columns:
        if column not in frame.columns:
            continue
        candidate = frame[column].astype("boolean")
        usable = combined.isna() & candidate.notna()
        if not usable.any():
            continue
        combined.loc[usable] = candidate.loc[usable]
        fields.append(column)
    values = combined.dropna()
    return {
        "fields": fields,
        "present": int(len(values)),
        "missing": int(len(frame) - len(values)),
        "true": int(values.sum()),
        "false": int((~values).sum()),
    }


def _with_diagnostic_columns(frame: pd.DataFrame, *, h3_resolution: int) -> pd.DataFrame:
    result = frame.copy()
    result["_coverage_h3"] = [
        to_h3(float(lat), float(lon), h3_resolution) for lat, lon in zip(result["lat"], result["lon"], strict=True)
    ]
    result["_provider_sequence"] = [
        _provider_sequence(source, sequence)
        for source, sequence in zip(result["source"], result["sequence_id"], strict=True)
    ]
    # Inputs legitimately mix second- and millisecond-precision ISO strings.
    # Pandas otherwise infers one strict format from the first row and silently
    # turns the other source's valid timestamps into NaT.
    result["_captured_timestamp"] = pd.to_datetime(
        result["captured_at"],
        errors="coerce",
        utc=True,
        format="mixed",
    )
    return result


def describe_layer(
    frame: pd.DataFrame,
    *,
    h3_resolution: int,
    grid_lat_bins: int,
    grid_lon_bins: int,
    coverage_bounds: CoverageBounds = MOSCOW_BOUNDS,
) -> dict[str, Any]:
    h3_counts = frame.groupby("_coverage_h3", sort=False).size()
    sequence_counts = frame.groupby("_provider_sequence", sort=False).size()
    distances = nearest_reference_distances(frame[["lat", "lon"]].reset_index(drop=True))
    headings = pd.to_numeric(frame["heading"], errors="coerce").dropna()
    timestamps = frame["_captured_timestamp"].dropna()
    years = timestamps.dt.year.astype(int)
    months = timestamps.dt.month.astype(int)
    bounds = {
        "min_lat": float(frame["lat"].min()),
        "max_lat": float(frame["lat"].max()),
        "min_lon": float(frame["lon"].min()),
        "max_lon": float(frame["lon"].max()),
    }
    return {
        "references": int(len(frame)),
        "geographic_coverage": {
            "bounds": bounds,
            "h3_resolution": h3_resolution,
            "unique_h3_cells": int(frame["_coverage_h3"].nunique()),
            "fixed_moscow_grid": _fixed_grid(
                frame,
                lat_bins=grid_lat_bins,
                lon_bins=grid_lon_bins,
                coverage_bounds=coverage_bounds,
            ),
        },
        "spatial_density": {
            "references_per_occupied_h3_cell": _numeric_summary(h3_counts),
            "provider_sequences_per_occupied_h3_cell": _numeric_summary(
                frame.groupby("_coverage_h3")["_provider_sequence"].nunique()
            ),
            "nearest_reference_distance_m": _numeric_summary(distances),
        },
        "sequence_diversity": {
            "namespace_policy": "source:sequence_id (existing source prefix is not duplicated)",
            "unique_provider_sequences": int(frame["_provider_sequence"].nunique()),
            "references_per_provider_sequence": _numeric_summary(sequence_counts),
            "provider_sequences_per_source": dict(
                sorted(frame.groupby("source")["_provider_sequence"].nunique().astype(int).items())
            ),
        },
        "heading_viewpoint_diversity": {
            "heading_present": int(len(headings)),
            "heading_missing": int(len(frame) - len(headings)),
            "heading_present_ratio": len(headings) / len(frame),
            "heading_octants": _counts(_heading_octant(value) for value in headings),
            "semantic_view_direction": _optional_string_distribution(frame, ("msls_view_direction", "view_direction")),
            "panorama": _optional_boolean_distribution(frame, ("msls_pano", "pano")),
        },
        "temporal_seasonal_diversity": {
            "captured_at_present": int(len(timestamps)),
            "captured_at_missing": int(len(frame) - len(timestamps)),
            "capture_date_min": timestamps.min().isoformat() if len(timestamps) else None,
            "capture_date_max": timestamps.max().isoformat() if len(timestamps) else None,
            "unique_capture_dates": int(timestamps.dt.date.nunique()) if len(timestamps) else 0,
            "years": _counts(years),
            "months": _counts(months),
            "seasons": _counts(_season(month) for month in months),
            "night": _optional_boolean_distribution(frame, ("msls_night", "night")),
        },
    }


def build_diagnostics(
    frame: pd.DataFrame,
    *,
    h3_resolution: int,
    grid_lat_bins: int,
    grid_lon_bins: int,
    coverage_bounds: CoverageBounds = MOSCOW_BOUNDS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    enriched = _with_diagnostic_columns(frame, h3_resolution=h3_resolution)
    by_source = {
        source: describe_layer(
            enriched[enriched["source"] == source].copy(),
            h3_resolution=h3_resolution,
            grid_lat_bins=grid_lat_bins,
            grid_lon_bins=grid_lon_bins,
            coverage_bounds=coverage_bounds,
        )
        for source in sorted(str(value) for value in enriched["source"].unique())
    }
    combined = describe_layer(
        enriched,
        h3_resolution=h3_resolution,
        grid_lat_bins=grid_lat_bins,
        grid_lon_bins=grid_lon_bins,
        coverage_bounds=coverage_bounds,
    )
    source_count_per_cell = enriched.groupby("_coverage_h3")["source"].nunique()
    overlap = {
        "union_h3_cells": int(enriched["_coverage_h3"].nunique()),
        "single_source_h3_cells": int((source_count_per_cell == 1).sum()),
        "multi_source_h3_cells": int((source_count_per_cell > 1).sum()),
        "h3_cells_by_number_of_sources": _counts(source_count_per_cell),
        "per_source_h3_cells": {
            source: int(values["geographic_coverage"]["unique_h3_cells"]) for source, values in by_source.items()
        },
    }
    return enriched, {"combined": combined, "by_source": by_source, "source_overlap": overlap}


def _source_provenance_summary(frame: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for source, subset in frame.groupby("source", sort=True):
        hosts = []
        for value in subset["source_url"]:
            try:
                hosts.append((urlsplit(str(value)).hostname or "").lower())
            except ValueError:
                hosts.append("invalid")
        summary[str(source)] = {
            "rows": int(len(subset)),
            "required_license": SOURCE_LICENSES[str(source)],
            "license_counts": _counts(subset["license"]),
            "unique_attributions": int(subset["attribution"].nunique()),
            "unique_source_urls": int(subset["source_url"].nunique()),
            "source_url_hosts": _counts(hosts),
        }
    return summary


def _draw_aoi_outline(axis: Any, aoi_boundary: AoiBoundary | None) -> None:
    if aoi_boundary is None:
        return
    polygons = (
        aoi_boundary.geometry.geoms
        if aoi_boundary.geometry.geom_type == "MultiPolygon"
        else (aoi_boundary.geometry,)
    )
    for polygon in polygons:
        exterior_x, exterior_y = polygon.exterior.xy
        axis.plot(exterior_x, exterior_y, color="#202020", linewidth=0.7, alpha=0.9, zorder=5)
        for interior in polygon.interiors:
            interior_x, interior_y = interior.xy
            axis.plot(interior_x, interior_y, color="#555555", linewidth=0.45, alpha=0.75, zorder=5)


def _configure_map_axis(
    axis: Any,
    title: str,
    *,
    coverage_bounds: CoverageBounds,
    aoi_boundary: AoiBoundary | None,
) -> None:
    min_lat, max_lat, min_lon, max_lon = coverage_bounds
    axis.set(
        title=title,
        xlabel="Longitude",
        ylabel="Latitude",
        xlim=(min_lon, max_lon),
        ylim=(min_lat, max_lat),
    )
    mean_lat = (min_lat + max_lat) / 2
    axis.set_aspect(1.0 / math.cos(math.radians(mean_lat)))
    axis.grid(alpha=0.18, linewidth=0.5)
    _draw_aoi_outline(axis, aoi_boundary)


def generate_coverage_maps(
    frame: pd.DataFrame,
    *,
    maps_dir: Path,
    required_sources: Sequence[str],
    coverage_bounds: CoverageBounds = MOSCOW_BOUNDS,
    aoi_boundary: AoiBoundary | None = None,
) -> dict[str, str]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise GalleryCombineError("matplotlib is required for coverage maps") from exc

    paths = {source: maps_dir / f"{source}_coverage.png" for source in required_sources}
    paths["combined_sources"] = maps_dir / "combined_coverage_sources.png"
    paths["combined_density"] = maps_dir / "combined_coverage_density.png"

    for source in required_sources:
        subset = frame[frame["source"] == source]
        figure, axis = plt.subplots(figsize=(8.5, 7.5))
        try:
            density = axis.hexbin(
                subset["lon"],
                subset["lat"],
                gridsize=(72, 48),
                bins="log",
                mincnt=1,
                cmap="viridis",
            )
            figure.colorbar(density, ax=axis, label="Reference count (log scale)")
            _configure_map_axis(
                axis,
                f"{source} Moscow coverage — {len(subset):,} references",
                coverage_bounds=coverage_bounds,
                aoi_boundary=aoi_boundary,
            )
            _atomic_figure(figure, paths[source])
        finally:
            plt.close(figure)

    figure, axis = plt.subplots(figsize=(8.5, 7.5))
    try:
        source_counts = frame["source"].value_counts()
        for source in sorted(required_sources, key=lambda value: (-int(source_counts[value]), value)):
            subset = frame[frame["source"] == source]
            is_large = len(subset) > 20_000
            axis.scatter(
                subset["lon"],
                subset["lat"],
                s=1.2 if is_large else 5.0,
                alpha=0.18 if is_large else 0.62,
                linewidths=0,
                color=SOURCE_COLORS.get(source),
                label=f"{source} ({len(subset):,})",
                rasterized=True,
            )
        axis.legend(markerscale=4, framealpha=0.9)
        _configure_map_axis(
            axis,
            f"Combined Moscow coverage by source — {len(frame):,} references",
            coverage_bounds=coverage_bounds,
            aoi_boundary=aoi_boundary,
        )
        _atomic_figure(figure, paths["combined_sources"])
    finally:
        plt.close(figure)

    figure, axis = plt.subplots(figsize=(8.5, 7.5))
    try:
        density = axis.hexbin(
            frame["lon"],
            frame["lat"],
            gridsize=(72, 48),
            bins="log",
            mincnt=1,
            cmap="magma",
        )
        figure.colorbar(density, ax=axis, label="Reference count (log scale)")
        _configure_map_axis(
            axis,
            f"Combined Moscow reference density — {len(frame):,} references",
            coverage_bounds=coverage_bounds,
            aoi_boundary=aoi_boundary,
        )
        _atomic_figure(figure, paths["combined_density"])
    finally:
        plt.close(figure)
    return {key: str(path) for key, path in paths.items()}


def _write_markdown(path: Path, report: Mapping[str, Any]) -> None:
    combined = report["coverage"]["combined"]
    lines = [
        "# Canonical Moscow reference gallery",
        "",
        f"- Complete: {report['complete']}",
        f"- References: {report['rows']}",
        f"- Sources: {', '.join(report['validation']['actual_sources'])}",
        f"- Required images checked: {report['validation']['require_images']}",
        f"- AOI mode: {report['aoi']['mode']}",
        f"- AOI SHA-256: {report['aoi']['sha256']}",
        f"- Rows excluded outside AOI: {report['aoi']['rows_excluded']}",
        f"- Combined H3 cells (r{combined['geographic_coverage']['h3_resolution']}): "
        f"{combined['geographic_coverage']['unique_h3_cells']}",
        f"- Combined provider sequences: {combined['sequence_diversity']['unique_provider_sequences']}",
        "",
        "## Source coverage",
        "",
        "| Source | References | H3 cells | Provider sequences | NN p50 (m) | Heading present |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for source, values in sorted(report["coverage"]["by_source"].items()):
        lines.append(
            f"| {source} | {values['references']} | "
            f"{values['geographic_coverage']['unique_h3_cells']} | "
            f"{values['sequence_diversity']['unique_provider_sequences']} | "
            f"{values['spatial_density']['nearest_reference_distance_m']['p50']} | "
            f"{values['heading_viewpoint_diversity']['heading_present']} |"
        )
    lines.extend(
        [
            "",
            "## Coverage maps",
            "",
            *[f"- {key}: `{value}`" for key, value in sorted(report["maps"].items())],
            "",
            "## Source policy",
            "",
            "MSLS database/query labels remain provenance only. Provider sequence metrics are namespaced by source.",
        ]
    )
    _atomic_text(path, "\n".join(lines) + "\n")


def run(
    input_manifests: Sequence[Path],
    output_manifest: Path,
    report_json: Path,
    report_markdown: Path,
    maps_dir: Path,
    *,
    require_images: bool = False,
    image_base: Path = Path("."),
    h3_resolution: int = 10,
    grid_lat_bins: int = 40,
    grid_lon_bins: int = 60,
    aoi_geojson: Path | None = None,
    filter_outside_aoi: bool = False,
) -> dict[str, Any]:
    """Combine canonical source manifests and publish coverage diagnostics."""

    validate_resolution(h3_resolution)
    if grid_lat_bins < 1 or grid_lon_bins < 1:
        raise ValueError("grid bin counts must be >= 1")
    if filter_outside_aoi and aoi_geojson is None:
        raise ValueError("filter_outside_aoi requires aoi_geojson")
    aoi_boundary = load_aoi_boundary(aoi_geojson) if aoi_geojson is not None else None
    coverage_bounds = _coverage_bounds(aoi_boundary)
    required = DEFAULT_REQUIRED_SOURCES
    map_destinations = {source: maps_dir / f"{source}_coverage.png" for source in required}
    map_destinations["combined_sources"] = maps_dir / "combined_coverage_sources.png"
    map_destinations["combined_density"] = maps_dir / "combined_coverage_density.png"
    protected_inputs = [*input_manifests, *([aoi_geojson] if aoi_geojson is not None else [])]
    _validate_artifact_paths(
        protected_inputs,
        [output_manifest, report_json, report_markdown, *map_destinations.values()],
    )

    combined, input_audit = _load_and_union(input_manifests)
    rows_before_aoi_filter = len(combined)
    excluded_by_source: dict[str, int] = {}
    excluded_rows = 0
    if aoi_boundary is not None:
        inside_aoi = _aoi_mask(combined, aoi_boundary)
        excluded = combined.loc[~inside_aoi]
        excluded_rows = int(len(excluded))
        excluded_by_source = dict(
            sorted(Counter(str(value) for value in excluded["source"]).items())
        )
        if filter_outside_aoi:
            combined = combined.loc[inside_aoi].reset_index(drop=True)
    validation = validate_gallery(
        combined,
        required_sources=required,
        require_images=require_images,
        image_base=image_base,
        aoi_boundary=aoi_boundary,
    )
    enriched, coverage = build_diagnostics(
        combined,
        h3_resolution=h3_resolution,
        grid_lat_bins=grid_lat_bins,
        grid_lon_bins=grid_lon_bins,
        coverage_bounds=coverage_bounds,
    )
    endpoint_specs = [
        ("canonical_parquet", output_manifest),
        ("report_json", report_json),
        ("report_markdown", report_markdown),
        *((f"map_{key}", path) for key, path in map_destinations.items()),
    ]
    bundle_root = _bundle_root_for(report_json, endpoint_specs)
    generation_name = f"generation-{uuid.uuid4().hex}"
    staging_root = bundle_root / "staging" / generation_name
    staged_manifest = staging_root / "canonical.parquet"
    staged_report_json = staging_root / "report.json"
    staged_report_markdown = staging_root / "report.md"
    staged_maps_dir = staging_root / "maps"
    staging_root.mkdir(parents=True, exist_ok=False)

    try:
        staged_maps = generate_coverage_maps(
            enriched,
            maps_dir=staged_maps_dir,
            required_sources=required,
            coverage_bounds=coverage_bounds,
            aoi_boundary=aoi_boundary,
        )

        # Derived diagnostic columns stay in reports only. The canonical output
        # is the exact union of source columns.
        write_manifest(combined, staged_manifest, allow_empty=False)
        import pyarrow.parquet as pq

        arrow_schema = pq.read_schema(staged_manifest)
        published_maps = {key: str(path) for key, path in map_destinations.items()}
        report: dict[str, Any] = {
            "schema_version": 1,
            "complete": True,
            "city_id": "moscow",
            "rows": int(len(combined)),
            "source_counts": dict(sorted(Counter(str(value) for value in combined["source"]).items())),
            "inputs": input_audit,
            "aoi": {
                "mode": "geojson_polygon" if aoi_boundary is not None else "administrative_bbox",
                "geojson": str(aoi_boundary.path) if aoi_boundary is not None else None,
                "sha256": aoi_boundary.sha256 if aoi_boundary is not None else None,
                "bounds_min_lat_max_lat_min_lon_max_lon": list(coverage_bounds),
                "filter_outside_aoi": filter_outside_aoi,
                "rows_before_filter": int(rows_before_aoi_filter),
                "rows_excluded": excluded_rows,
                "rows_excluded_by_source": excluded_by_source,
                "rows_after_filter": int(len(combined)),
            },
            "validation": validation,
            "source_provenance": _source_provenance_summary(combined),
            "output_schema": [
                {"name": field.name, "type": str(field.type), "nullable": field.nullable} for field in arrow_schema
            ],
            "coverage": coverage,
            "maps": published_maps,
            "artifacts": {
                "canonical_parquet": str(output_manifest),
                "canonical_parquet_sha256": _sha256_file(staged_manifest),
                "report_json": str(report_json),
                "report_markdown": str(report_markdown),
                "maps_sha256": {
                    key: _sha256_file(Path(value)) for key, value in sorted(staged_maps.items())
                },
            },
        }
        _write_markdown(staged_report_markdown, report)
        report["artifacts"]["report_markdown_sha256"] = _sha256_file(staged_report_markdown)
        write_json(staged_report_json, report)

        artifacts = [
            _BundleArtifact(
                name="canonical_parquet",
                staged_path=staged_manifest,
                destination=_lexical_absolute(output_manifest),
                generation_relative_path=Path("canonical.parquet"),
            ),
            _BundleArtifact(
                name="report_json",
                staged_path=staged_report_json,
                destination=_lexical_absolute(report_json),
                generation_relative_path=Path("report.json"),
            ),
            _BundleArtifact(
                name="report_markdown",
                staged_path=staged_report_markdown,
                destination=_lexical_absolute(report_markdown),
                generation_relative_path=Path("report.md"),
            ),
            *[
                _BundleArtifact(
                    name=f"map_{key}",
                    staged_path=Path(staged_maps[key]),
                    destination=_lexical_absolute(map_destinations[key]),
                    generation_relative_path=Path("maps") / map_destinations[key].name,
                )
                for key in map_destinations
            ],
        ]
        _publish_generation(
            bundle_root=bundle_root,
            staging_root=staging_root,
            generation_name=generation_name,
            artifacts=artifacts,
        )
    except Exception:
        shutil.rmtree(staging_root, ignore_errors=True)
        raise

    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Combine canonical Moscow source Parquets and generate fail-closed coverage diagnostics"
    )
    parser.add_argument("--input-manifest", action="append", type=Path, required=True)
    parser.add_argument(
        "--output-manifest",
        type=Path,
        default=Path("data/processed/moscow/canonical_reference_gallery.parquet"),
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=Path("data/processed/moscow/reports/canonical_reference_gallery.json"),
    )
    parser.add_argument(
        "--report-markdown",
        type=Path,
        default=Path("data/processed/moscow/reports/canonical_reference_gallery.md"),
    )
    parser.add_argument(
        "--maps-dir",
        type=Path,
        default=Path("data/processed/moscow/reports/coverage_maps"),
    )
    parser.add_argument("--require-images", action="store_true")
    parser.add_argument("--image-base", type=Path, default=Path("."))
    parser.add_argument("--h3-resolution", type=int, default=10)
    parser.add_argument("--grid-lat-bins", type=int, default=40)
    parser.add_argument("--grid-lon-bins", type=int, default=60)
    parser.add_argument("--aoi-geojson", type=Path)
    parser.add_argument(
        "--filter-outside-aoi",
        action="store_true",
        help="explicitly exclude input rows outside --aoi-geojson before validation and publication",
    )
    args = parser.parse_args()
    run(
        input_manifests=args.input_manifest,
        output_manifest=args.output_manifest,
        report_json=args.report_json,
        report_markdown=args.report_markdown,
        maps_dir=args.maps_dir,
        require_images=args.require_images,
        image_base=args.image_base,
        h3_resolution=args.h3_resolution,
        grid_lat_bins=args.grid_lat_bins,
        grid_lon_bins=args.grid_lon_bins,
        aoi_geojson=args.aoi_geojson,
        filter_outside_aoi=args.filter_outside_aoi,
    )


if __name__ == "__main__":
    main()
