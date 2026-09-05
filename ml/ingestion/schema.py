"""Canonical reference-manifest schema and serialization helpers.

Every persisted pipeline stage uses the same required columns.  Source-specific
fields may be appended, but required fields are never silently removed.
"""

from __future__ import annotations

import json
import math
import os
import uuid
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

REFERENCE_ID_NAMESPACE = uuid.UUID("58e0f6ca-e665-4fb6-8427-8e9205467f52")

CANONICAL_COLUMNS = [
    "id",
    "city_id",
    "source",
    "source_image_id",
    "sequence_id",
    "image_path",
    "lat",
    "lon",
    "captured_at",
    "heading",
    "quality_score",
    "license",
    "attribution",
    "source_url",
    "metadata_json",
]

# The downloader needs a refreshable/direct URL, but it is deliberately not
# part of the public reference contract returned by the product.
PIPELINE_COLUMNS = [*CANONICAL_COLUMNS, "download_url"]

STRING_COLUMNS = {
    "id",
    "city_id",
    "source",
    "source_image_id",
    "sequence_id",
    "image_path",
    "captured_at",
    "license",
    "attribution",
    "source_url",
    "metadata_json",
    "download_url",
    "h3_coarse",
    "h3_fine",
}
FLOAT_COLUMNS = {
    "lat",
    "lon",
    "heading",
    "quality_score",
    "blur_score",
    "brightness",
    "exposure_score",
}
INTEGER_COLUMNS = {"width", "height"}

# Bounding box of the official OSM administrative Moscow relation 102269.
# Exact final-gallery scope is enforced with the pinned relation polygon; the
# bbox remains the portable coarse gate used by generic schema utilities.
MOSCOW_BOUNDS = (55.1421745, 56.0212238, 36.8031012, 37.9674277)
MOSCOW_LEGACY_CORE_BOUNDS = (55.55, 55.95, 37.30, 37.90)


class ManifestSchemaError(ValueError):
    """Raised when a manifest cannot satisfy the canonical contract."""


def is_missing_value(value: Any) -> bool:
    if value is None or value.__class__.__name__ == "NAType":
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def stable_reference_id(source: str, source_image_id: str | int) -> str:
    key = f"{str(source).strip().lower()}:{str(source_image_id).strip()}"
    return str(uuid.uuid5(REFERENCE_ID_NAMESPACE, key))


def normalize_heading(value: Any) -> float | None:
    if is_missing_value(value) or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number % 360.0


def normalize_captured_at(value: Any) -> str | None:
    """Return a UTC ISO-8601 string, accepting epoch seconds/milliseconds."""
    if is_missing_value(value) or value == "":
        return None
    if isinstance(value, bool):
        return None

    parsed: datetime | None = None
    if isinstance(value, (int, float)):
        number = float(value)
        if not math.isfinite(number):
            return None
        # Current street-imagery APIs commonly expose milliseconds.
        if abs(number) >= 100_000_000_000:
            number /= 1000.0
        try:
            parsed = datetime.fromtimestamp(number, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            numeric = float(text)
        except ValueError:
            numeric = None
        if numeric is not None and math.isfinite(numeric):
            return normalize_captured_at(numeric)
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            # KartaView has historically used this format.
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    parsed = datetime.strptime(text, fmt).replace(tzinfo=UTC)
                    break
                except ValueError:
                    continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    parsed = parsed.astimezone(UTC)
    if parsed.microsecond:
        return parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")


def _json_safe(value: Any) -> Any:
    if is_missing_value(value):
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        values = [_json_safe(item) for item in value]
        return sorted(values, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (TypeError, ValueError):
            pass
    return str(value)


def normalize_metadata_json(value: Any) -> str:
    """Encode metadata as deterministic JSON text for a stable Arrow schema."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = {"raw_value": value}
    if is_missing_value(value):
        value = {}
    return json.dumps(_json_safe(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def canonical_record(
    *,
    source: str,
    source_image_id: str | int,
    lat: Any,
    lon: Any,
    image_path: str,
    city_id: str = "moscow",
    sequence_id: Any = None,
    captured_at: Any = None,
    heading: Any = None,
    quality_score: Any = None,
    license_name: str = "",
    attribution: str = "",
    source_url: str = "",
    metadata: Any = None,
    download_url: str = "",
    reference_id: str | None = None,
) -> dict[str, Any]:
    source_name = str(source).strip().lower()
    source_id = str(source_image_id).strip()
    if not source_name or not source_id:
        raise ManifestSchemaError("source and source_image_id are required")
    try:
        latitude = float(lat)
        longitude = float(lon)
    except (TypeError, ValueError) as exc:
        raise ManifestSchemaError("lat/lon must be numeric") from exc
    if not math.isfinite(latitude) or not math.isfinite(longitude):
        raise ManifestSchemaError("lat/lon must be finite")
    if not (-90.0 <= latitude <= 90.0 and -180.0 <= longitude <= 180.0):
        raise ManifestSchemaError("lat/lon outside world bounds")
    quality: float | None
    if is_missing_value(quality_score) or quality_score == "":
        quality = None
    else:
        try:
            quality = float(quality_score)
        except (TypeError, ValueError) as exc:
            raise ManifestSchemaError("quality_score must be numeric or null") from exc
        if not math.isfinite(quality):
            quality = None

    return {
        "id": reference_id or stable_reference_id(source_name, source_id),
        "city_id": str(city_id).strip().lower() or "moscow",
        "source": source_name,
        "source_image_id": source_id,
        "sequence_id": None if is_missing_value(sequence_id) or sequence_id == "" else str(sequence_id),
        "image_path": str(image_path),
        "lat": latitude,
        "lon": longitude,
        "captured_at": normalize_captured_at(captured_at),
        "heading": normalize_heading(heading),
        "quality_score": quality,
        "license": str(license_name or ""),
        "attribution": str(attribution or ""),
        "source_url": str(source_url or ""),
        "metadata_json": normalize_metadata_json(metadata),
        "download_url": str(download_url or ""),
    }


def _empty_series(column: str):
    import pandas as pd

    if column in FLOAT_COLUMNS:
        return pd.Series(dtype="float64")
    if column in INTEGER_COLUMNS:
        return pd.Series(dtype="Int64")
    return pd.Series(dtype="string")


def manifest_dataframe(records: Iterable[Mapping[str, Any]], extra_columns: Iterable[str] = ()):
    """Build a schema-preserving DataFrame, including for zero records."""
    import pandas as pd

    rows = [dict(row) for row in records]
    ordered_columns = list(dict.fromkeys([*PIPELINE_COLUMNS, *extra_columns, *(key for row in rows for key in row)]))
    if not rows:
        return pd.DataFrame({column: _empty_series(column) for column in ordered_columns})
    df = pd.DataFrame(rows)
    for column in ordered_columns:
        if column not in df.columns:
            df[column] = _empty_series(column)
    return coerce_manifest_schema(df)[ordered_columns]


def coerce_manifest_schema(df):
    """Return a copy with canonical columns and deterministic metadata JSON."""
    import pandas as pd

    result = df.copy()
    for column in PIPELINE_COLUMNS:
        if column not in result.columns:
            result[column] = _empty_series(column)

    if len(result):
        result["metadata_json"] = result["metadata_json"].map(normalize_metadata_json)
        result["captured_at"] = result["captured_at"].map(normalize_captured_at)
        result["heading"] = pd.to_numeric(result["heading"], errors="coerce").map(
            lambda value: normalize_heading(value) if not pd.isna(value) else None
        )
        result["quality_score"] = pd.to_numeric(result["quality_score"], errors="coerce")
        result["lat"] = pd.to_numeric(result["lat"], errors="coerce")
        result["lon"] = pd.to_numeric(result["lon"], errors="coerce")
    return result


def _blank_string_mask(series):
    values = series.astype("string")
    return values.isna() | (values.str.strip() == "")


def validate_manifest_schema(
    df,
    *,
    allow_empty: bool = True,
    strict_reference: bool = False,
) -> list[str]:
    """Validate the stable structural contract, plus deployable-reference rules.

    Intermediate manifests may have null heading/quality while those values are
    being derived. ``strict_reference`` is the final-gallery boundary: it also
    enforces Moscow scope, bounded quality, and complete provenance.
    """

    import pandas as pd

    errors: list[str] = []
    missing = [column for column in CANONICAL_COLUMNS if column not in df.columns]
    if missing:
        return [f"missing_column:{column}" for column in missing]
    if len(df) == 0:
        return [] if allow_empty else ["empty_manifest"]

    ids = df["id"].astype("string")
    if ids.isna().any() or (ids.str.strip() == "").any():
        errors.append("missing_id")
    if ids.duplicated().any():
        errors.append("duplicate_id")

    for column in ("city_id", "source", "source_image_id", "image_path"):
        if _blank_string_mask(df[column]).any():
            errors.append(f"missing_{column}")

    lat = pd.to_numeric(df["lat"], errors="coerce")
    lon = pd.to_numeric(df["lon"], errors="coerce")
    if lat.isna().any() or (~lat.between(-90.0, 90.0)).any():
        errors.append("invalid_lat")
    if lon.isna().any() or (~lon.between(-180.0, 180.0)).any():
        errors.append("invalid_lon")

    heading = pd.to_numeric(df["heading"], errors="coerce")
    heading_present = ~df["heading"].isna()
    if (heading_present & (heading.isna() | ~heading.between(0.0, 360.0, inclusive="left"))).any():
        errors.append("invalid_heading")

    quality = pd.to_numeric(df["quality_score"], errors="coerce")
    quality_present = ~df["quality_score"].isna()
    if (quality_present & (quality.isna() | ~quality.between(0.0, 1.0))).any():
        errors.append("invalid_quality_score")

    if strict_reference:
        for column in ("license", "attribution", "source_url"):
            if _blank_string_mask(df[column]).any():
                errors.append(f"missing_{column}")
        if quality.isna().any() or (~quality.between(0.0, 1.0)).any():
            errors.append("missing_or_invalid_quality_score")
        city = df["city_id"].astype("string").str.strip().str.lower()
        moscow = city == "moscow"
        min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
        if (moscow & (~lat.between(min_lat, max_lat) | ~lon.between(min_lon, max_lon))).any():
            errors.append("coordinate_outside_moscow_bounds")
        for index, value in df["source_url"].items():
            if is_missing_value(value):
                continue
            parsed_url = urlsplit(str(value).strip())
            if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
                errors.append(f"invalid_source_url:{index}")

    for index, value in df["metadata_json"].items():
        if not isinstance(value, str):
            errors.append(f"metadata_json_not_string:{index}")
            continue
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            errors.append(f"metadata_json_invalid:{index}")
            continue
        if normalize_metadata_json(parsed) != value:
            errors.append(f"metadata_json_not_canonical:{index}")
    return errors


def write_manifest(
    df,
    path: Path,
    *,
    allow_empty: bool = True,
    strict_reference: bool = False,
) -> None:
    """Validate and atomically persist a Parquet manifest."""
    normalized = coerce_manifest_schema(df)
    errors = validate_manifest_schema(
        normalized,
        allow_empty=allow_empty,
        strict_reference=strict_reference,
    )
    if errors:
        raise ManifestSchemaError("; ".join(errors))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        normalized.to_parquet(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_manifest(
    path: Path,
    *,
    allow_empty: bool = True,
    strict_reference: bool = False,
):
    import pandas as pd

    df = coerce_manifest_schema(pd.read_parquet(path))
    errors = validate_manifest_schema(
        df,
        allow_empty=allow_empty,
        strict_reference=strict_reference,
    )
    if errors:
        raise ManifestSchemaError("; ".join(errors))
    return df
