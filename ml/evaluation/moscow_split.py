"""Build a leakage-resistant Moscow street-view evaluation split.

The splitter is intentionally offline: it consumes a canonical, already
downloaded Parquet manifest and never refreshes source metadata or images.
Query membership is sequence-level, while individual query frames are kept
only when a different gallery sequence provides a geographic positive.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import tempfile
from collections import Counter, defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import imagehash
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image, UnidentifiedImageError

from ml.enrichment.h3_assign import to_h3
from ml.ingestion.common import write_json
from ml.ingestion.schema import (
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    MOSCOW_BOUNDS,
    STRING_COLUMNS,
    coerce_manifest_schema,
    is_missing_value,
    read_manifest,
    stable_reference_id,
    validate_manifest_schema,
)
from ml.localization.geo import EARTH_RADIUS_M, haversine_m

AREA_H3_RESOLUTION = 8
SAMPLE_H3_RESOLUTION = 10
PHASH_BITS = 64
SPLIT_SCHEMA_VERSION = 2
# v3 replaces the per-candidate gallery rebuild with an incremental positive
# support graph and records the bounded initial candidate policy in the audit.
ALGORITHM_VERSION = "sequence-holdout-v3"
DEFAULT_CALIBRATION_TEST_EMBARGO_M = 100.0
DEFAULT_MINIMUM_GALLERY_FRACTION = 0.5
INDEPENDENT_QUERY_TARGET = 100
DEFAULT_HOLDOUT_CANDIDATE_MULTIPLIER = 4
BUNDLE_MANIFEST_NAME = "bundle_manifest.json"

SequenceKey = tuple[str, str]


class MoscowSplitError(RuntimeError):
    """The input cannot produce a defensible held-out evaluation split."""


@dataclass(frozen=True, slots=True)
class Fingerprint:
    file_sha256: str
    perceptual_hash: str
    perceptual_hash_int: int


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _query_sequence_exclusions(paths: Sequence[Path]) -> tuple[set[SequenceKey], list[dict[str, Any]]]:
    """Load historical query manifests without removing their rows from the gallery pool."""

    sequences: set[SequenceKey] = set()
    evidence: list[dict[str, Any]] = []
    for path in paths:
        frame = read_manifest(path, allow_empty=True)
        current = {
            (_text(row.source).lower(), _text(row.sequence_id))
            for row in frame.itertuples()
            if _text(row.source) and _text(row.sequence_id)
        }
        sequences.update(current)
        evidence.append(
            {
                "name": path.name,
                "rows": len(frame),
                "provider_sequences": len(current),
                "sha256": _sha256_file(path),
            }
        )
    return sequences, sorted(evidence, key=lambda item: (str(item["name"]), str(item["sha256"])))


def _stable_score(seed: int, *parts: object) -> str:
    payload = "\0".join([str(seed), *(str(part) for part in parts)]).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _text(value: Any) -> str:
    return "" if is_missing_value(value) else str(value).strip()


def _sequence_key(frame: pd.DataFrame, index: int) -> SequenceKey:
    """Return provider-namespaced sequence identity for one manifest row."""

    return (
        _text(frame.at[index, "source"]).lower(),
        _text(frame.at[index, "sequence_id"]),
    )


def _sequence_label(key: SequenceKey) -> str:
    """Encode a provider sequence for human-readable, unambiguous audits."""

    return json.dumps(list(key), ensure_ascii=False, separators=(",", ":"))


def _hash_band_keys(value: int, threshold: int) -> list[tuple[int, int]]:
    """Return LSH keys with no false negatives for Hamming <= threshold."""

    if threshold >= PHASH_BITS:
        return [(0, 0)]
    bands = threshold + 1
    base_width, remainder = divmod(PHASH_BITS, bands)
    result: list[tuple[int, int]] = []
    offset = 0
    for band in range(bands):
        width = base_width + (1 if band < remainder else 0)
        mask = (1 << width) - 1
        result.append((band, (value >> offset) & mask))
        offset += width
    return result


class _PerceptualHashIndex:
    def __init__(self, threshold: int) -> None:
        self.threshold = threshold
        self._bands: dict[tuple[int, int], list[tuple[int, int]]] = defaultdict(list)

    def add(self, row_index: int, value: int) -> None:
        for key in _hash_band_keys(value, self.threshold):
            self._bands[key].append((row_index, value))

    def matches(self, value: int) -> list[tuple[int, int]]:
        candidates: dict[int, int] = {}
        for key in _hash_band_keys(value, self.threshold):
            for row_index, candidate in self._bands.get(key, ()):
                candidates[row_index] = candidate
        return [
            (row_index, int((value ^ candidate).bit_count()))
            for row_index, candidate in candidates.items()
            if (value ^ candidate).bit_count() <= self.threshold
        ]


class _SpatialIndex:
    """Small local metric grid followed by exact haversine checks."""

    def __init__(self, frame: pd.DataFrame, indices: Iterable[int], radius_m: float) -> None:
        self.frame = frame
        self.radius_m = radius_m
        self.reference_lat = float(frame["lat"].mean()) if len(frame) else 55.75
        self._cells: dict[tuple[int, int], list[int]] = defaultdict(list)
        for index in indices:
            self._cells[self._cell(float(frame.at[index, "lat"]), float(frame.at[index, "lon"]))].append(index)

    def _xy(self, lat: float, lon: float) -> tuple[float, float]:
        return (
            math.radians(lon) * EARTH_RADIUS_M * math.cos(math.radians(self.reference_lat)),
            math.radians(lat) * EARTH_RADIUS_M,
        )

    def _cell(self, lat: float, lon: float) -> tuple[int, int]:
        x, y = self._xy(lat, lon)
        return math.floor(x / self.radius_m), math.floor(y / self.radius_m)

    def nearby(self, lat: float, lon: float) -> Iterable[int]:
        cell_x, cell_y = self._cell(lat, lon)
        # +/-2 is deliberately conservative around cell boundaries and the
        # small equirectangular projection distortion across Moscow.
        for dx in range(-2, 3):
            for dy in range(-2, 3):
                yield from self._cells.get((cell_x + dx, cell_y + dy), ())

    def nearest(
        self,
        row_index: int,
        *,
        different_sequence_from: SequenceKey | None = None,
    ) -> float | None:
        lat = float(self.frame.at[row_index, "lat"])
        lon = float(self.frame.at[row_index, "lon"])
        best: float | None = None
        for candidate in self.nearby(lat, lon):
            if candidate == row_index:
                continue
            if different_sequence_from is not None:
                candidate_sequence = _sequence_key(self.frame, candidate)
                if candidate_sequence == different_sequence_from:
                    continue
            distance = haversine_m(
                lat,
                lon,
                float(self.frame.at[candidate, "lat"]),
                float(self.frame.at[candidate, "lon"]),
            )
            if distance <= self.radius_m and (best is None or distance < best):
                best = distance
        return best


def _validate_parameters(
    *,
    max_queries: int,
    min_query_spacing_m: float,
    positive_distance_m: float,
    phash_distance_threshold: int,
    calibration_test_embargo_m: float,
    minimum_queries_per_split: int,
    minimum_sequences_per_split: int,
    minimum_areas_per_split: int,
    minimum_gallery_rows: int,
    minimum_gallery_sequences: int,
    minimum_gallery_areas: int,
    minimum_gallery_fraction: float,
    holdout_candidate_multiplier: int,
) -> None:
    if max_queries < 1:
        raise ValueError("max_queries must be >= 1")
    if not math.isfinite(min_query_spacing_m) or min_query_spacing_m < 0:
        raise ValueError("min_query_spacing_m must be finite and >= 0")
    if not math.isfinite(positive_distance_m) or positive_distance_m <= 0:
        raise ValueError("positive_distance_m must be finite and > 0")
    if not 0 <= phash_distance_threshold <= PHASH_BITS:
        raise ValueError("phash_distance_threshold must be in [0, 64]")
    if not math.isfinite(calibration_test_embargo_m) or calibration_test_embargo_m < 0:
        raise ValueError("calibration_test_embargo_m must be finite and >= 0")
    integer_minimums = {
        "minimum_queries_per_split": minimum_queries_per_split,
        "minimum_sequences_per_split": minimum_sequences_per_split,
        "minimum_areas_per_split": minimum_areas_per_split,
        "minimum_gallery_rows": minimum_gallery_rows,
        "minimum_gallery_sequences": minimum_gallery_sequences,
        "minimum_gallery_areas": minimum_gallery_areas,
    }
    for name, value in integer_minimums.items():
        if value < 0:
            raise ValueError(f"{name} must be >= 0")
    if not math.isfinite(minimum_gallery_fraction) or not 0 <= minimum_gallery_fraction <= 1:
        raise ValueError("minimum_gallery_fraction must be finite and in [0, 1]")
    if holdout_candidate_multiplier < 1:
        raise ValueError("holdout_candidate_multiplier must be >= 1")
    if minimum_queries_per_split * 2 > max_queries:
        raise ValueError("max_queries cannot satisfy minimum_queries_per_split for both query splits")


def _validate_moscow_manifest(frame: pd.DataFrame) -> None:
    if len(frame) == 0:
        raise MoscowSplitError("input manifest is empty")
    bad_ids: list[str] = []
    stable_source_ids: list[str] = []
    for _, row in frame.iterrows():
        source = _text(row["source"]).lower()
        source_image_id = _text(row["source_image_id"])
        expected = stable_reference_id(source, source_image_id)
        actual = _text(row["id"])
        if not source or not source_image_id or actual != expected:
            bad_ids.append(actual or "<missing>")
        stable_source_ids.append(f"{source}:{source_image_id}")
    if bad_ids:
        preview = ", ".join(bad_ids[:5])
        raise MoscowSplitError(f"non-stable reference IDs detected ({len(bad_ids)}): {preview}")
    if len(set(stable_source_ids)) != len(stable_source_ids):
        raise MoscowSplitError("duplicate stable source IDs detected")

    city = frame["city_id"].astype("string").str.strip().str.lower()
    if not bool((city == "moscow").all()):
        raise MoscowSplitError("all records must have city_id=moscow")
    min_lat, max_lat, min_lon, max_lon = MOSCOW_BOUNDS
    latitude = pd.to_numeric(frame["lat"], errors="coerce")
    longitude = pd.to_numeric(frame["lon"], errors="coerce")
    if latitude.isna().any() or longitude.isna().any():
        raise MoscowSplitError("manifest contains invalid coordinates")
    if (~latitude.between(min_lat, max_lat) | ~longitude.between(min_lon, max_lon)).any():
        raise MoscowSplitError("manifest contains coordinates outside the configured Moscow bounds")


def _fingerprint_images(frame: pd.DataFrame) -> dict[int, Fingerprint]:
    fingerprints: dict[int, Fingerprint] = {}
    failures: list[str] = []
    for index, row in frame.iterrows():
        image_path = Path(_text(row["image_path"]))
        try:
            file_sha256 = _sha256_file(image_path)
            with Image.open(image_path) as opened:
                opened.load()
                perceptual_hash = str(imagehash.phash(opened.convert("RGB"), hash_size=8))
        except (OSError, ValueError, UnidentifiedImageError):
            failures.append(_text(row["id"]))
            continue
        fingerprints[int(index)] = Fingerprint(
            file_sha256=file_sha256,
            perceptual_hash=perceptual_hash,
            perceptual_hash_int=int(perceptual_hash, 16),
        )
    if failures:
        preview = ", ".join(failures[:5])
        raise MoscowSplitError(f"cannot fingerprint {len(failures)} downloaded images: {preview}")
    return fingerprints


def _round_robin_groups(groups: Mapping[str, Sequence[int]], *, seed: int, namespace: str) -> list[int]:
    queues = {
        key: deque(sorted(values, key=lambda value: _stable_score(seed, namespace, key, value)))
        for key, values in groups.items()
        if values
    }
    keys = sorted(queues, key=lambda key: _stable_score(seed, namespace, "group", key))
    result: list[int] = []
    while keys:
        next_keys: list[str] = []
        for key in keys:
            queue = queues[key]
            if queue:
                result.append(queue.popleft())
            if queue:
                next_keys.append(key)
        keys = next_keys
    return result


def _eligible_frames(
    frame: pd.DataFrame,
    usable_indices: Sequence[int],
    *,
    positive_distance_m: float,
) -> tuple[set[int], dict[str, int]]:
    sequences_by_area: dict[str, set[SequenceKey]] = defaultdict(set)
    for index in usable_indices:
        sequences_by_area[str(frame.at[index, "evaluation_area_h3"])].add(_sequence_key(frame, index))
    multi_sequence_areas = {area for area, sequences in sequences_by_area.items() if len(sequences) >= 2}
    spatial = _SpatialIndex(frame, usable_indices, positive_distance_m)
    eligible: set[int] = set()
    area_rejected = 0
    no_positive = 0
    for index in usable_indices:
        if str(frame.at[index, "evaluation_area_h3"]) not in multi_sequence_areas:
            area_rejected += 1
            continue
        sequence = _sequence_key(frame, index)
        if spatial.nearest(index, different_sequence_from=sequence) is None:
            no_positive += 1
            continue
        eligible.add(index)
    return eligible, {
        "areas_total": len(sequences_by_area),
        "areas_with_at_least_two_sequences": len(multi_sequence_areas),
        "rows_excluded_single_sequence_area": area_rejected,
        "rows_excluded_without_cross_sequence_positive": no_positive,
    }


def _query_distances(
    frame: pd.DataFrame,
    eligible_indices: set[int],
    held_out_sequences: set[SequenceKey],
    usable_indices: Sequence[int],
    *,
    positive_distance_m: float,
) -> tuple[dict[int, float], list[int]]:
    gallery = [index for index in usable_indices if _sequence_key(frame, index) not in held_out_sequences]
    if not gallery:
        return {}, gallery
    spatial = _SpatialIndex(frame, gallery, positive_distance_m)
    distances: dict[int, float] = {}
    for index in eligible_indices:
        sequence = _sequence_key(frame, index)
        if sequence not in held_out_sequences:
            continue
        distance = spatial.nearest(index)
        if distance is not None:
            distances[index] = distance
    return distances, gallery


def _sequence_order(frame: pd.DataFrame, eligible_indices: set[int], seed: int) -> list[SequenceKey]:
    sequences_by_area: dict[str, list[SequenceKey]] = defaultdict(list)
    for index in eligible_indices:
        sequences_by_area[str(frame.at[index, "evaluation_area_h3"])].append(_sequence_key(frame, index))
    groups: dict[str, Sequence[int]] = {}
    sequence_numbers: dict[SequenceKey, int] = {}
    reverse: dict[int, SequenceKey] = {}
    for number, sequence in enumerate(sorted({item for values in sequences_by_area.values() for item in values})):
        sequence_numbers[sequence] = number
        reverse[number] = sequence
    for area, sequences in sequences_by_area.items():
        groups[area] = [sequence_numbers[value] for value in sorted(set(sequences))]
    interleaved = _round_robin_groups(groups, seed=seed, namespace="sequence")
    result: list[SequenceKey] = []
    seen: set[SequenceKey] = set()
    for number in interleaved:
        sequence = reverse[number]
        if sequence not in seen:
            seen.add(sequence)
            result.append(sequence)
    return result


def _choose_held_out_sequences(
    frame: pd.DataFrame,
    eligible_indices: set[int],
    usable_indices: Sequence[int],
    *,
    seed: int,
    positive_distance_m: float,
    max_queries: int,
    minimum_gallery_rows: int,
    minimum_gallery_sequences: int,
    minimum_gallery_areas: int,
    minimum_gallery_fraction: float,
    holdout_candidate_multiplier: int,
) -> tuple[list[SequenceKey], dict[str, int | bool]]:
    """Choose a bounded, sequence-held-out candidate pool without quadratic scans.

    The former implementation rebuilt a complete gallery and recomputed every
    held-out positive for every candidate sequence.  That is correct but
    becomes quadratic when providers expose mostly one-image sequence IDs.  We
    instead build the sequence-level positive-support graph once.  Removing a
    candidate then updates only query frames supported by that sequence, so an
    already chosen sequence can never lose its final gallery positive.

    The initial pool intentionally oversamples the query cap.  The downstream
    fixed-point selection still performs the definitive image/hash/spacing
    leakage checks and can only reduce this pool; it never publishes an
    unverified candidate.
    """

    ordered_sequences = _sequence_order(frame, eligible_indices, seed)
    sequence_by_index = {int(index): _sequence_key(frame, int(index)) for index in usable_indices}
    area_by_index = {int(index): str(frame.at[index, "evaluation_area_h3"]) for index in usable_indices}

    sequence_rows: dict[SequenceKey, list[int]] = defaultdict(list)
    sequence_areas: dict[SequenceKey, set[str]] = defaultdict(set)
    area_sequence_counts: Counter[str] = Counter()
    for index in usable_indices:
        row_index = int(index)
        sequence = sequence_by_index[row_index]
        sequence_rows[sequence].append(row_index)
        sequence_areas[sequence].add(area_by_index[row_index])
    for areas in sequence_areas.values():
        for area in areas:
            area_sequence_counts[area] += 1

    # Each eligible frame tracks the *provider sequences* that can still be a
    # gallery positive.  One sequence is sufficient regardless of how many of
    # its frames appear nearby, matching `_query_distances` semantics.
    spatial = _SpatialIndex(frame, usable_indices, positive_distance_m)
    support_count_by_index: dict[int, int] = {}
    frames_supported_by_sequence: dict[SequenceKey, list[int]] = defaultdict(list)
    viable_frames_by_sequence: Counter[SequenceKey] = Counter()
    for index in sorted(eligible_indices):
        row_index = int(index)
        sequence = sequence_by_index[row_index]
        lat = float(frame.at[row_index, "lat"])
        lon = float(frame.at[row_index, "lon"])
        supporter_sequences: set[SequenceKey] = set()
        for candidate in spatial.nearby(lat, lon):
            candidate_index = int(candidate)
            candidate_sequence = sequence_by_index[candidate_index]
            if candidate_index == row_index or candidate_sequence == sequence:
                continue
            distance = haversine_m(
                lat,
                lon,
                float(frame.at[candidate_index, "lat"]),
                float(frame.at[candidate_index, "lon"]),
            )
            if distance <= positive_distance_m:
                supporter_sequences.add(candidate_sequence)
        if not supporter_sequences:
            # `_eligible_frames` should already have rejected this row. Keep
            # the graph defensive so a future eligibility change fails closed.
            continue
        support_count_by_index[row_index] = len(supporter_sequences)
        viable_frames_by_sequence[sequence] += 1
        for supporter in supporter_sequences:
            frames_supported_by_sequence[supporter].append(row_index)

    reserve_rows = max(
        minimum_gallery_rows,
        math.ceil(len(usable_indices) * minimum_gallery_fraction),
    )
    candidate_cap = min(
        len(ordered_sequences),
        max_queries * holdout_candidate_multiplier,
    )
    chosen: list[SequenceKey] = []
    chosen_set: set[SequenceKey] = set()
    gallery_rows = len(usable_indices)
    gallery_sequences = len(sequence_rows)
    rejected_no_positive = 0
    rejected_gallery_reserve = 0

    for sequence in ordered_sequences:
        if len(chosen) >= candidate_cap:
            break
        rows = sequence_rows.get(sequence, [])
        areas = sequence_areas.get(sequence, set())
        if (
            gallery_rows - len(rows) < reserve_rows
            or gallery_sequences - 1 < minimum_gallery_sequences
            or len(area_sequence_counts) - sum(area_sequence_counts[area] == 1 for area in areas)
            < minimum_gallery_areas
        ):
            rejected_gallery_reserve += 1
            continue
        if viable_frames_by_sequence[sequence] < 1:
            rejected_no_positive += 1
            continue

        lost_viable_frames: Counter[SequenceKey] = Counter()
        for frame_index in frames_supported_by_sequence.get(sequence, []):
            if support_count_by_index[frame_index] == 1:
                lost_viable_frames[sequence_by_index[frame_index]] += 1
        if any(
            affected in chosen_set
            and viable_frames_by_sequence[affected] <= lost_count
            for affected, lost_count in lost_viable_frames.items()
        ):
            rejected_no_positive += 1
            continue

        chosen.append(sequence)
        chosen_set.add(sequence)
        gallery_rows -= len(rows)
        gallery_sequences -= 1
        for area in areas:
            area_sequence_counts[area] -= 1
            if area_sequence_counts[area] == 0:
                del area_sequence_counts[area]
        for frame_index in frames_supported_by_sequence.get(sequence, []):
            if support_count_by_index[frame_index] == 1:
                viable_frames_by_sequence[sequence_by_index[frame_index]] -= 1
            support_count_by_index[frame_index] -= 1

    if not chosen:
        raise MoscowSplitError(
            "insufficient coverage: no sequence can be held out while retaining "
            "a gallery positive and the configured gallery reserve"
        )
    return chosen, {
        "ordered_eligible_sequence_count": len(ordered_sequences),
        "candidate_multiplier": holdout_candidate_multiplier,
        "candidate_sequence_cap": candidate_cap,
        "candidate_sequences_selected": len(chosen),
        "candidate_rows_selected": len(usable_indices) - gallery_rows,
        "candidate_cap_reached": len(chosen) >= candidate_cap,
        "candidate_sequences_rejected_no_gallery_positive": rejected_no_positive,
        "candidate_sequences_rejected_gallery_reserve": rejected_gallery_reserve,
    }


def _filter_gallery_leakage(
    candidates: Iterable[int],
    gallery: Sequence[int],
    fingerprints: Mapping[int, Fingerprint],
    *,
    phash_distance_threshold: int,
) -> tuple[list[int], dict[str, int]]:
    exact_gallery = {fingerprints[index].file_sha256 for index in gallery}
    phash_gallery = _PerceptualHashIndex(phash_distance_threshold)
    for index in gallery:
        phash_gallery.add(index, fingerprints[index].perceptual_hash_int)
    kept: list[int] = []
    exact_rejected = 0
    near_rejected = 0
    for index in candidates:
        fingerprint = fingerprints[index]
        if fingerprint.file_sha256 in exact_gallery:
            exact_rejected += 1
            continue
        if phash_gallery.matches(fingerprint.perceptual_hash_int):
            near_rejected += 1
            continue
        kept.append(index)
    return kept, {
        "query_frames_rejected_exact_file_match_with_gallery": exact_rejected,
        "query_frames_rejected_phash_near_duplicate_with_gallery": near_rejected,
    }


def _downsample_queries(
    frame: pd.DataFrame,
    candidates: Sequence[int],
    *,
    seed: int,
    min_query_spacing_m: float,
) -> tuple[list[int], int]:
    if min_query_spacing_m == 0:
        return list(candidates), 0
    by_sequence_and_cell: dict[str, list[int]] = defaultdict(list)
    for index in candidates:
        sequence = _sequence_label(_sequence_key(frame, index))
        cell = str(frame.at[index, "evaluation_sample_h3"])
        by_sequence_and_cell[f"{sequence}\0{cell}"].append(index)

    kept: list[int] = []
    removed = 0
    reference_lat = float(frame["lat"].mean())

    def spacing_cell(index: int) -> tuple[int, int]:
        x = math.radians(float(frame.at[index, "lon"])) * EARTH_RADIUS_M * math.cos(math.radians(reference_lat))
        y = math.radians(float(frame.at[index, "lat"])) * EARTH_RADIUS_M
        return math.floor(x / min_query_spacing_m), math.floor(y / min_query_spacing_m)

    order = _round_robin_groups(by_sequence_and_cell, seed=seed, namespace="downsample-global")
    kept_by_cell: dict[tuple[int, int], list[int]] = defaultdict(list)
    for index in order:
        cell_x, cell_y = spacing_cell(index)
        nearby_kept = (
            other
            for dx in range(-2, 3)
            for dy in range(-2, 3)
            for other in kept_by_cell.get((cell_x + dx, cell_y + dy), ())
        )
        if any(
            haversine_m(
                float(frame.at[index, "lat"]),
                float(frame.at[index, "lon"]),
                float(frame.at[other, "lat"]),
                float(frame.at[other, "lon"]),
            )
            < min_query_spacing_m
            for other in nearby_kept
        ):
            removed += 1
            continue
        kept_by_cell[(cell_x, cell_y)].append(index)
        kept.append(index)
    return kept, removed


def _assign_sequences(
    frame: pd.DataFrame,
    candidates: Sequence[int],
    preferred_order: Sequence[SequenceKey],
    *,
    seed: int,
    calibration_test_embargo_m: float,
    representative_balance: bool = False,
) -> tuple[dict[SequenceKey, str], dict[SequenceKey, str], dict[str, Any]]:
    """Assign connected geographic components wholly to calibration or test."""

    by_sequence: dict[SequenceKey, list[int]] = defaultdict(list)
    for index in candidates:
        by_sequence[_sequence_key(frame, index)].append(index)

    parent: dict[SequenceKey, SequenceKey] = {key: key for key in by_sequence}

    def find(key: SequenceKey) -> SequenceKey:
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    def union(left: SequenceKey, right: SequenceKey) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        if left_root < right_root:
            parent[right_root] = left_root
        else:
            parent[left_root] = right_root

    area_owner: dict[str, SequenceKey] = {}
    for sequence, indices in by_sequence.items():
        for index in indices:
            area = str(frame.at[index, "evaluation_area_h3"])
            if area in area_owner:
                union(sequence, area_owner[area])
            else:
                area_owner[area] = sequence

    if calibration_test_embargo_m > 0 and candidates:
        spatial = _SpatialIndex(frame, candidates, calibration_test_embargo_m)
        candidate_set = set(candidates)
        for index in candidates:
            for other in spatial.nearby(float(frame.at[index, "lat"]), float(frame.at[index, "lon"])):
                if other not in candidate_set or other <= index:
                    continue
                distance = haversine_m(
                    float(frame.at[index, "lat"]),
                    float(frame.at[index, "lon"]),
                    float(frame.at[other, "lat"]),
                    float(frame.at[other, "lon"]),
                )
                if distance < calibration_test_embargo_m:
                    union(_sequence_key(frame, index), _sequence_key(frame, other))

    components: dict[SequenceKey, list[SequenceKey]] = defaultdict(list)
    for sequence in by_sequence:
        components[find(sequence)].append(sequence)
    preferred_rank = {sequence: position for position, sequence in enumerate(preferred_order)}

    def component_order(root: SequenceKey) -> tuple[int, str]:
        rank = min((preferred_rank.get(sequence, len(preferred_rank)) for sequence in components[root]), default=0)
        labels = sorted(_sequence_label(sequence) for sequence in components[root])
        return rank, _stable_score(seed, "geo-component", *labels)

    ordered_components = sorted(components, key=component_order)
    assignment: dict[SequenceKey, str] = {}
    geo_group_by_sequence: dict[SequenceKey, str] = {}
    row_counts = {"calibration": 0, "test": 0}
    sequence_counts = {"calibration": 0, "test": 0}
    area_counts = {"calibration": 0, "test": 0}
    feature_counts: dict[str, Counter[str]] = {
        "calibration": Counter(),
        "test": Counter(),
    }
    component_rows: dict[SequenceKey, list[int]] = {
        root: [index for sequence in sequences for index in by_sequence[sequence]]
        for root, sequences in components.items()
    }
    for position, root in enumerate(ordered_components):
        sequences = components[root]
        indices = component_rows[root]
        areas = {str(frame.at[index, "evaluation_area_h3"]) for index in indices}
        component_features: Counter[str] = Counter()
        if representative_balance:
            for index in indices:
                source = _text(frame.at[index, "source"]).lower() or "unknown"
                component_features[f"provider:{source}"] += 1
                component_features[f"area:{frame.at[index, 'evaluation_area_h3']}"] += 1
                try:
                    long_side = max(int(frame.at[index, "width"]), int(frame.at[index, "height"]))
                except (KeyError, TypeError, ValueError):
                    resolution = "unknown"
                else:
                    resolution = "lt1600" if long_side < 1600 else ("1600_2499" if long_side < 2500 else "ge2500")
                component_features[f"resolution:{resolution}"] += 1
        if position == 0:
            split = "calibration"
        elif position == 1:
            split = "test"
        elif representative_balance:

            def balance_score(
                candidate_split: str,
                *,
                current_features: Counter[str] = component_features,
                current_indices: tuple[int, ...] = tuple(indices),
                current_root: SequenceKey = root,
            ) -> tuple[float, int, int, int, str]:
                other_split = "test" if candidate_split == "calibration" else "calibration"
                feature_keys = set(feature_counts[candidate_split]) | set(feature_counts[other_split]) | set(
                    current_features
                )
                feature_imbalance = 0.0
                for key in feature_keys:
                    left = feature_counts[candidate_split][key] + current_features[key]
                    right = feature_counts[other_split][key]
                    feature_imbalance += abs(left - right) / max(left + right, 1)
                row_left = row_counts[candidate_split] + len(current_indices)
                row_right = row_counts[other_split]
                row_imbalance = abs(row_left - row_right) / max(row_left + row_right, 1)
                return (
                    row_imbalance + feature_imbalance / max(len(feature_keys), 1),
                    row_counts[candidate_split],
                    sequence_counts[candidate_split],
                    area_counts[candidate_split],
                    _stable_score(
                        seed,
                        "assign-representative-component",
                        _sequence_label(current_root),
                        candidate_split,
                    ),
                )

            split = min(("calibration", "test"), key=balance_score)
        else:
            split = min(
                ("calibration", "test"),
                key=lambda candidate_split: (
                    row_counts[candidate_split],
                    sequence_counts[candidate_split],
                    area_counts[candidate_split],
                    _stable_score(seed, "assign-component", _sequence_label(root), candidate_split),
                ),
            )
        component_payload = "\0".join(sorted(_sequence_label(sequence) for sequence in sequences))
        group_id = hashlib.sha256(component_payload.encode("utf-8")).hexdigest()
        for sequence in sequences:
            assignment[sequence] = split
            geo_group_by_sequence[sequence] = group_id
        row_counts[split] += len(indices)
        sequence_counts[split] += len(sequences)
        area_counts[split] += len(areas)
        feature_counts[split].update(component_features)
    diagnostics: dict[str, Any] = {
        "component_count": len(components),
        "component_row_counts": sorted(len(indices) for indices in component_rows.values()),
        "component_sequence_counts": sorted(len(sequences) for sequences in components.values()),
        "representative_balance": representative_balance,
        "feature_counts": {split: dict(sorted(counts.items())) for split, counts in feature_counts.items()},
    }
    return assignment, geo_group_by_sequence, diagnostics


def _bounded_query_order(
    frame: pd.DataFrame,
    candidates: Sequence[int],
    assignment: Mapping[SequenceKey, str],
    *,
    seed: int,
) -> list[int]:
    split_groups: dict[str, dict[str, list[int]]] = {"calibration": defaultdict(list), "test": defaultdict(list)}
    for index in candidates:
        sequence = _sequence_key(frame, index)
        sample_cell = str(frame.at[index, "evaluation_sample_h3"])
        split_groups[assignment[sequence]][f"{_sequence_label(sequence)}\0{sample_cell}"].append(index)
    queues: dict[str, deque[int]] = {}
    for split in ("calibration", "test"):
        sequence_ordered = _round_robin_groups(
            split_groups[split],
            seed=seed,
            namespace=f"query-order:{split}",
        )
        queues[split] = deque(sequence_ordered)
    result: list[int] = []
    while queues["calibration"] or queues["test"]:
        for split in ("calibration", "test"):
            if queues[split]:
                result.append(queues[split].popleft())
    return result


def _deduplicate_query_union(
    ordered_candidates: Sequence[int],
    frame: pd.DataFrame,
    assignment: Mapping[SequenceKey, str],
    fingerprints: Mapping[int, Fingerprint],
    *,
    max_queries: int,
    phash_distance_threshold: int,
) -> tuple[list[int], dict[str, int]]:
    """Greedily enforce exact and perceptual uniqueness across all queries."""

    accepted: dict[str, list[int]] = {"calibration": [], "test": []}
    exact_owner: dict[str, str] = {}
    phash = _PerceptualHashIndex(phash_distance_threshold)
    accepted_split_by_index: dict[int, str] = {}
    exact_cross_rejected = 0
    exact_within_rejected = 0
    near_cross_rejected = 0
    near_within_rejected = 0
    capped = 0
    for position, index in enumerate(ordered_candidates):
        if sum(len(values) for values in accepted.values()) >= max_queries:
            capped = len(ordered_candidates) - position
            break
        sequence = _sequence_key(frame, index)
        split = assignment[sequence]
        fingerprint = fingerprints[index]
        if fingerprint.file_sha256 in exact_owner:
            if exact_owner[fingerprint.file_sha256] == split:
                exact_within_rejected += 1
            else:
                exact_cross_rejected += 1
            continue
        matches = phash.matches(fingerprint.perceptual_hash_int)
        if matches:
            if any(accepted_split_by_index[row_index] != split for row_index, _ in matches):
                near_cross_rejected += 1
            else:
                near_within_rejected += 1
            continue
        accepted[split].append(index)
        exact_owner[fingerprint.file_sha256] = split
        phash.add(index, fingerprint.perceptual_hash_int)
        accepted_split_by_index[index] = split
    return [*accepted["calibration"], *accepted["test"]], {
        "query_frames_rejected_exact_file_match_across_query_splits": exact_cross_rejected,
        "query_frames_rejected_exact_file_match_within_query_split": exact_within_rejected,
        "query_frames_rejected_exact_file_match_in_query_union": (exact_cross_rejected + exact_within_rejected),
        "query_frames_rejected_phash_near_duplicate_across_query_splits": near_cross_rejected,
        "query_frames_rejected_phash_near_duplicate_within_query_split": near_within_rejected,
        "query_frames_rejected_phash_near_duplicate_in_query_union": (near_cross_rejected + near_within_rejected),
        "query_frames_removed_by_max_queries": capped,
    }


def _finalize_query_selection(
    frame: pd.DataFrame,
    eligible_indices: set[int],
    usable_indices: Sequence[int],
    fingerprints: Mapping[int, Fingerprint],
    initial_sequences: Sequence[SequenceKey],
    *,
    seed: int,
    max_queries: int,
    min_query_spacing_m: float,
    positive_distance_m: float,
    phash_distance_threshold: int,
    calibration_test_embargo_m: float,
    representative_balance: bool = False,
) -> tuple[
    list[int],
    list[int],
    dict[int, float],
    dict[SequenceKey, str],
    dict[SequenceKey, str],
    dict[str, int],
    dict[str, Any],
]:
    held_out = set(initial_sequences)
    diagnostics: Counter[str] = Counter()
    final_selected: list[int] = []
    final_gallery: list[int] = []
    final_distances: dict[int, float] = {}
    final_assignment: dict[SequenceKey, str] = {}
    final_geo_group_by_sequence: dict[SequenceKey, str] = {}
    final_geo_diagnostics: dict[str, Any] = {}
    for _ in range(len(initial_sequences) + 2):
        distances, gallery = _query_distances(
            frame,
            eligible_indices,
            held_out,
            usable_indices,
            positive_distance_m=positive_distance_m,
        )
        ordered_candidates = sorted(
            distances,
            key=lambda index: _stable_score(
                seed,
                "candidate",
                frame.at[index, "evaluation_area_h3"],
                frame.at[index, "evaluation_sample_h3"],
                frame.at[index, "id"],
            ),
        )
        filtered, leakage = _filter_gallery_leakage(
            ordered_candidates,
            gallery,
            fingerprints,
            phash_distance_threshold=phash_distance_threshold,
        )
        sampled, spacing_removed = _downsample_queries(
            frame,
            filtered,
            seed=seed,
            min_query_spacing_m=min_query_spacing_m,
        )
        assignment, geo_group_by_sequence, geo_diagnostics = _assign_sequences(
            frame,
            sampled,
            initial_sequences,
            seed=seed,
            calibration_test_embargo_m=calibration_test_embargo_m,
            representative_balance=representative_balance,
        )
        bounded_order = _bounded_query_order(frame, sampled, assignment, seed=seed)
        selected, query_dedup = _deduplicate_query_union(
            bounded_order,
            frame,
            assignment,
            fingerprints,
            max_queries=max_queries,
            phash_distance_threshold=phash_distance_threshold,
        )
        active_sequences = {_sequence_key(frame, index) for index in selected}
        diagnostics = Counter(leakage)
        diagnostics.update(query_dedup)
        diagnostics["query_frames_removed_by_min_spacing"] = spacing_removed
        final_selected = selected
        final_gallery = gallery
        final_distances = {index: distances[index] for index in selected}
        final_assignment = assignment
        final_geo_group_by_sequence = geo_group_by_sequence
        final_geo_diagnostics = geo_diagnostics
        if active_sequences == held_out:
            break
        held_out = active_sequences
        if not held_out:
            break
    if not final_selected:
        raise MoscowSplitError(
            "no defensible query frames remain after positive-distance, exact-hash, pHash, and spacing checks"
        )
    active_sequences = {_sequence_key(frame, index) for index in final_selected}
    final_gallery = [index for index in usable_indices if _sequence_key(frame, index) not in active_sequences]
    # Expanded gallery membership after an inactive sequence returns must not
    # create a latent leak. The fixed-point loop above normally guarantees it;
    # this assertion keeps future algorithm changes fail-closed.
    filtered, leakage = _filter_gallery_leakage(
        final_selected,
        final_gallery,
        fingerprints,
        phash_distance_threshold=phash_distance_threshold,
    )
    if filtered != final_selected or any(leakage.values()):
        raise MoscowSplitError("query/gallery leakage appeared after final sequence membership was resolved")
    spatial = _SpatialIndex(frame, final_gallery, positive_distance_m)
    final_distances = {}
    for index in final_selected:
        distance = spatial.nearest(index)
        if distance is None:
            raise MoscowSplitError("a final query has no gallery positive within the configured distance")
        final_distances[index] = distance
    final_assignment = {sequence: split for sequence, split in final_assignment.items() if sequence in active_sequences}
    final_geo_group_by_sequence = {
        sequence: group_id for sequence, group_id in final_geo_group_by_sequence.items() if sequence in active_sequences
    }
    return (
        final_selected,
        final_gallery,
        final_distances,
        final_assignment,
        final_geo_group_by_sequence,
        dict(diagnostics),
        final_geo_diagnostics,
    )


def _with_split_columns(
    frame: pd.DataFrame,
    indices: Sequence[int],
    split: str,
    fingerprints: Mapping[int, Fingerprint],
    distances: Mapping[int, float],
    geo_groups: Mapping[int, str],
) -> pd.DataFrame:
    result = frame.loc[list(indices)].copy() if indices else frame.iloc[0:0].copy()
    result["evaluation_split"] = split
    result["nearest_gallery_distance_m"] = [
        float(distances[index]) if index in distances else float("nan") for index in indices
    ]
    result["file_sha256"] = [fingerprints[index].file_sha256 for index in indices]
    result["perceptual_hash"] = [fingerprints[index].perceptual_hash for index in indices]
    result["evaluation_geo_group_id"] = [geo_groups.get(index, pd.NA) for index in indices]
    return result.reset_index(drop=True)


def _align_output_schemas(frames: Sequence[pd.DataFrame]) -> list[pd.DataFrame]:
    """Give empty and non-empty outputs identical Arrow-compatible dtypes."""

    if not frames:
        return []
    columns = list(frames[0].columns)
    combined = pd.concat(frames, ignore_index=True).reindex(columns=columns)
    string_columns = {
        *STRING_COLUMNS,
        "evaluation_area_h3",
        "evaluation_sample_h3",
        "evaluation_split",
        "evaluation_geo_group_id",
        "file_sha256",
        "perceptual_hash",
    }
    float_columns = {*FLOAT_COLUMNS, "nearest_gallery_distance_m"}
    aligned: list[pd.DataFrame] = []
    for source in frames:
        result = source.reindex(columns=columns).copy()
        for column in columns:
            if column in string_columns:
                result[column] = result[column].astype("string")
            elif column in float_columns:
                result[column] = pd.to_numeric(result[column], errors="coerce").astype("float64")
            elif column in INTEGER_COLUMNS:
                result[column] = pd.to_numeric(result[column], errors="coerce").astype("Int64")
            elif pd.api.types.is_bool_dtype(combined[column].dtype):
                result[column] = result[column].astype("boolean")
            elif pd.api.types.is_integer_dtype(combined[column].dtype):
                result[column] = pd.to_numeric(result[column], errors="coerce").astype("Int64")
            elif pd.api.types.is_numeric_dtype(combined[column].dtype):
                result[column] = pd.to_numeric(result[column], errors="coerce").astype("float64")
            elif combined[column].dropna().map(lambda value: isinstance(value, str)).all():
                result[column] = result[column].astype("string")
        aligned.append(result)
    return aligned


def _write_split_outputs(frames_and_paths: Sequence[tuple[pd.DataFrame, Path]]) -> None:
    """Validate and atomically write every split with one identical Arrow schema."""

    normalized: list[pd.DataFrame] = []
    for frame, _ in frames_and_paths:
        current = coerce_manifest_schema(frame)
        errors = validate_manifest_schema(current, allow_empty=True)
        if errors:
            raise MoscowSplitError("invalid output manifest: " + "; ".join(errors))
        normalized.append(current)
    combined = pd.concat(normalized, ignore_index=True)
    shared_schema = pa.Table.from_pandas(combined, preserve_index=False).schema
    for current, (_, path) in zip(normalized, frames_and_paths, strict=True):
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            table = pa.Table.from_pandas(current, schema=shared_schema, preserve_index=False, safe=True)
            pq.write_table(table, temporary)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def _set_values(frame: pd.DataFrame, column: str) -> set[str]:
    return {_text(value) for value in frame[column] if _text(value)}


def _provider_sequence_values(frame: pd.DataFrame) -> set[SequenceKey]:
    return {
        (_text(row.source).lower(), _text(row.sequence_id))
        for row in frame.itertuples()
        if _text(row.source) and _text(row.sequence_id)
    }


def _near_pair_count(left: pd.DataFrame, right: pd.DataFrame, threshold: int) -> int:
    index = _PerceptualHashIndex(threshold)
    for row_index, value in enumerate(right["perceptual_hash"]):
        index.add(row_index, int(str(value), 16))
    return sum(len(index.matches(int(str(value), 16))) for value in left["perceptual_hash"])


def _pairwise_audit(left: pd.DataFrame, right: pd.DataFrame, threshold: int) -> dict[str, int]:
    stable_source_overlap = len(
        {f"{_text(row.source).lower()}:{_text(row.source_image_id)}" for row in left.itertuples()}
        & {f"{_text(row.source).lower()}:{_text(row.source_image_id)}" for row in right.itertuples()}
    )
    return {
        "id_overlap": len(_set_values(left, "id") & _set_values(right, "id")),
        "source_id_overlap": stable_source_overlap,
        "stable_source_id_overlap": stable_source_overlap,
        "source_url_overlap": len(_set_values(left, "source_url") & _set_values(right, "source_url")),
        "image_path_overlap": len(_set_values(left, "image_path") & _set_values(right, "image_path")),
        "file_sha256_overlap": len(_set_values(left, "file_sha256") & _set_values(right, "file_sha256")),
        "sequence_id_overlap": len(_provider_sequence_values(left) & _provider_sequence_values(right)),
        "geo_group_id_overlap": len(
            _set_values(left, "evaluation_geo_group_id") & _set_values(right, "evaluation_geo_group_id")
        ),
        "phash_near_duplicate_pairs": _near_pair_count(left, right, threshold),
    }


def _query_union_audit(
    calibration: pd.DataFrame,
    test: pd.DataFrame,
    *,
    phash_distance_threshold: int,
    min_query_spacing_m: float,
    calibration_test_embargo_m: float,
) -> dict[str, Any]:
    query = pd.concat([calibration, test], ignore_index=True)
    exact_counts = Counter(_text(value).lower() for value in query["file_sha256"] if _text(value))
    exact_pairs = sum(count * (count - 1) // 2 for count in exact_counts.values())
    phash_index = _PerceptualHashIndex(phash_distance_threshold)
    phash_pairs = 0
    for index, value in enumerate(query["perceptual_hash"]):
        encoded = int(str(value), 16)
        phash_pairs += len(phash_index.matches(encoded))
        phash_index.add(index, encoded)

    minimum_query_distance: float | None = None
    spacing_violations = 0
    minimum_cross_split_distance: float | None = None
    embargo_violations = 0
    for left in range(len(query)):
        for right in range(left + 1, len(query)):
            distance = haversine_m(
                float(query.at[left, "lat"]),
                float(query.at[left, "lon"]),
                float(query.at[right, "lat"]),
                float(query.at[right, "lon"]),
            )
            if minimum_query_distance is None or distance < minimum_query_distance:
                minimum_query_distance = distance
            if distance < min_query_spacing_m:
                spacing_violations += 1
            if query.at[left, "evaluation_split"] != query.at[right, "evaluation_split"]:
                if minimum_cross_split_distance is None or distance < minimum_cross_split_distance:
                    minimum_cross_split_distance = distance
                if distance < calibration_test_embargo_m:
                    embargo_violations += 1

    area_overlap = sorted(_set_values(calibration, "evaluation_area_h3") & _set_values(test, "evaluation_area_h3"))
    group_overlap = sorted(
        _set_values(calibration, "evaluation_geo_group_id") & _set_values(test, "evaluation_geo_group_id")
    )
    return {
        "query_rows": len(query),
        "exact_file_duplicate_pairs": exact_pairs,
        "phash_near_duplicate_pairs": phash_pairs,
        "minimum_query_pair_distance_m": minimum_query_distance,
        "query_spacing_violation_pairs": spacing_violations,
        "calibration_test_area_h3_overlap": area_overlap,
        "calibration_test_geo_group_overlap": group_overlap,
        "minimum_calibration_test_distance_m": minimum_cross_split_distance,
        "calibration_test_embargo_violation_pairs": embargo_violations,
    }


def _distance_summary(values: Sequence[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min": None, "median": None, "p90": None, "max": None}
    series = pd.Series(values, dtype="float64")
    return {
        "count": len(values),
        "min": float(series.min()),
        "median": float(series.median()),
        "p90": float(series.quantile(0.9)),
        "max": float(series.max()),
    }


def _counts(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "rows": len(frame),
        "sequences": len(_provider_sequence_values(frame)),
        "areas": int(frame["evaluation_area_h3"].astype("string").nunique()),
        "h3_sample_cells": int(frame["evaluation_sample_h3"].astype("string").nunique()),
        "geo_groups": len(_set_values(frame, "evaluation_geo_group_id")),
        "by_source": {str(key): int(value) for key, value in frame["source"].value_counts().sort_index().items()},
        "by_area": {
            str(key): int(value) for key, value in frame["evaluation_area_h3"].value_counts().sort_index().items()
        },
    }


def _canonical_ledger_hash(ledger: Sequence[Mapping[str, Any]]) -> str:
    encoded = json.dumps(list(ledger), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _coverage_gate(
    gallery: pd.DataFrame,
    calibration: pd.DataFrame,
    test: pd.DataFrame,
    *,
    usable_row_count: int,
    minimum_queries_per_split: int,
    minimum_sequences_per_split: int,
    minimum_areas_per_split: int,
    minimum_gallery_rows: int,
    minimum_gallery_sequences: int,
    minimum_gallery_areas: int,
    minimum_gallery_fraction: float,
) -> dict[str, Any]:
    profiles = {
        "gallery": _counts(gallery),
        "calibration": _counts(calibration),
        "test": _counts(test),
    }
    minimum_gallery_rows_effective = max(
        minimum_gallery_rows,
        math.ceil(usable_row_count * minimum_gallery_fraction),
    )
    requirements = {
        "minimum_queries_per_split": minimum_queries_per_split,
        "minimum_sequences_per_split": minimum_sequences_per_split,
        "minimum_areas_per_split": minimum_areas_per_split,
        "minimum_gallery_rows": minimum_gallery_rows,
        "minimum_gallery_rows_effective": minimum_gallery_rows_effective,
        "minimum_gallery_sequences": minimum_gallery_sequences,
        "minimum_gallery_areas": minimum_gallery_areas,
        "minimum_gallery_fraction": minimum_gallery_fraction,
    }
    checks = {
        "calibration_rows": profiles["calibration"]["rows"] >= minimum_queries_per_split,
        "test_rows": profiles["test"]["rows"] >= minimum_queries_per_split,
        "calibration_sequences": profiles["calibration"]["sequences"] >= minimum_sequences_per_split,
        "test_sequences": profiles["test"]["sequences"] >= minimum_sequences_per_split,
        "calibration_areas": profiles["calibration"]["areas"] >= minimum_areas_per_split,
        "test_areas": profiles["test"]["areas"] >= minimum_areas_per_split,
        "gallery_rows": profiles["gallery"]["rows"] >= minimum_gallery_rows_effective,
        "gallery_sequences": profiles["gallery"]["sequences"] >= minimum_gallery_sequences,
        "gallery_areas": profiles["gallery"]["areas"] >= minimum_gallery_areas,
    }
    failures = sorted(name for name, passed in checks.items() if not passed)
    return {
        "passed": not failures,
        "requirements": requirements,
        "checks": checks,
        "failures": failures,
        "observed": profiles,
    }


def _bundle_fingerprint(
    *,
    input_manifest_sha256: str,
    parameters: Mapping[str, Any],
    ledger_sha256: str,
) -> str:
    payload = {
        "algorithm_version": ALGORITHM_VERSION,
        "input_manifest_sha256": input_manifest_sha256,
        "ledger_sha256": ledger_sha256,
        "parameters": dict(parameters),
        "schema_version": SPLIT_SCHEMA_VERSION,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _verified_existing_bundle(output_dir: Path, bundle_fingerprint: str) -> dict[str, Any] | None:
    manifest_path = output_dir / BUNDLE_MANIFEST_NAME
    audit_path = output_dir / "split_audit.json"
    if not manifest_path.is_file() or not audit_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if manifest.get("bundle_fingerprint") != bundle_fingerprint:
        return None
    if audit.get("bundle", {}).get("fingerprint") != bundle_fingerprint:
        raise MoscowSplitError("existing split bundle fingerprint disagrees with its audit")
    files = manifest.get("files")
    if not isinstance(files, Mapping):
        raise MoscowSplitError("existing split bundle manifest has no file hash mapping")
    for name, expected in files.items():
        path = output_dir / str(name)
        if not path.is_file() or _sha256_file(path) != str(expected):
            raise MoscowSplitError(f"existing split bundle failed integrity verification: {name}")
    return audit


def _publish_staged_bundle(staging_dir: Path, output_dir: Path, *, replace_existing: bool) -> None:
    if output_dir.exists() and not replace_existing:
        raise MoscowSplitError(
            f"output directory already contains a different or unverifiable split bundle: {output_dir}; "
            "use replace_existing=True/--replace-existing to replace it"
        )
    if not output_dir.exists():
        os.replace(staging_dir, output_dir)
        return
    if not output_dir.is_dir():
        raise MoscowSplitError(f"output path exists and is not a directory: {output_dir}")
    backup = output_dir.with_name(f".{output_dir.name}.{os.getpid()}.backup")
    if backup.exists():
        raise MoscowSplitError(f"cannot replace split bundle while recovery backup exists: {backup}")
    os.replace(output_dir, backup)
    try:
        os.replace(staging_dir, output_dir)
    except Exception:
        os.replace(backup, output_dir)
        raise
    shutil.rmtree(backup)


def _write_markdown(path: Path, audit: Mapping[str, Any]) -> None:
    counts = audit["sample_counts"]
    overlaps = audit["overlap_audit"]
    distances = audit["positive_distance_summary_m"]
    independence = audit["query_union_independence_audit"]
    coverage = audit["coverage_gate"]
    sample_size = audit["sample_size_assessment"]
    lines = [
        "# Moscow evaluation split audit",
        "",
        f"- Algorithm: `{audit['algorithm_version']}`",
        f"- Bundle fingerprint: `{audit['bundle']['fingerprint']}`",
        f"- Seed: `{audit['parameters']['seed']}`",
        f"- Input rows: {audit['input']['rows']}",
        f"- Gallery: {counts['gallery']['rows']} rows / {counts['gallery']['sequences']} sequences",
        (f"- Calibration: {counts['calibration']['rows']} rows / {counts['calibration']['sequences']} sequences"),
        f"- Test: {counts['test']['rows']} rows / {counts['test']['sequences']} sequences",
        f"- Positive threshold: {audit['parameters']['positive_distance_m']:.3f} m",
        f"- Query positive-distance p90: {distances['all_queries']['p90']}",
        f"- pHash rejection threshold: {audit['parameters']['phash_distance_threshold']} bits",
        f"- Coverage gate: **{'PASS' if coverage['passed'] else 'FAIL'}**",
        (
            f"- Independent-query target: {sample_size['selected_query_count']} / "
            f"{sample_size['independent_query_target']} "
            f"(**{'MET' if sample_size['target_met'] else 'NOT MET'}**)"
        ),
        f"- Global query minimum spacing: {independence['minimum_query_pair_distance_m']} m",
        f"- Calibration/test minimum distance: {independence['minimum_calibration_test_distance_m']} m",
        (f"- Calibration/test geographic embargo: {audit['parameters']['calibration_test_embargo_m']:.3f} m"),
        "",
        "## Pairwise leakage audit",
        "",
        "| pair | ID | source ID | file SHA-256 | sequence | pHash near pairs |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for pair, values in overlaps.items():
        lines.append(
            f"| {pair} | {values['id_overlap']} | {values['stable_source_id_overlap']} | "
            f"{values['file_sha256_overlap']} | {values['sequence_id_overlap']} | "
            f"{values['phash_near_duplicate_pairs']} |"
        )
    lines.extend(
        [
            "",
            "## Query-union independence",
            "",
            f"- Exact duplicate pairs: {independence['exact_file_duplicate_pairs']}",
            f"- pHash-near duplicate pairs: {independence['phash_near_duplicate_pairs']}",
            f"- Spacing violation pairs: {independence['query_spacing_violation_pairs']}",
            f"- Calibration/test area overlap: {independence['calibration_test_area_h3_overlap']}",
            f"- Calibration/test geo-group overlap: {independence['calibration_test_geo_group_overlap']}",
            (f"- Calibration/test embargo violation pairs: {independence['calibration_test_embargo_violation_pairs']}"),
            "",
            "## Coverage gate",
            "",
            *[f"- {key}: {'PASS' if value else 'FAIL'}" for key, value in coverage["checks"].items()],
            "",
            "## Exclusions",
            "",
            *[f"- {key}: {value}" for key, value in audit["excluded_counts"].items()],
            "",
            "The JSON companion contains the complete per-file SHA-256/pHash ledger, area/source counts, "
            "distance distributions, parameters, and artifact hashes.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def run(
    input_manifest: Path,
    output_dir: Path,
    *,
    seed: int = 20260815,
    max_queries: int = 1000,
    min_query_spacing_m: float = 20.0,
    positive_distance_m: float = 100.0,
    phash_distance_threshold: int = 4,
    calibration_test_embargo_m: float = DEFAULT_CALIBRATION_TEST_EMBARGO_M,
    minimum_queries_per_split: int = 1,
    minimum_sequences_per_split: int = 1,
    minimum_areas_per_split: int = 1,
    minimum_gallery_rows: int = 1,
    minimum_gallery_sequences: int = 1,
    minimum_gallery_areas: int = 1,
    minimum_gallery_fraction: float = DEFAULT_MINIMUM_GALLERY_FRACTION,
    holdout_candidate_multiplier: int = DEFAULT_HOLDOUT_CANDIDATE_MULTIPLIER,
    replace_existing: bool = False,
    representative_balance: bool = False,
    exclude_query_manifests: Sequence[Path] = (),
) -> dict[str, Any]:
    """Create gallery/calibration/test Parquets and a fail-closed audit."""

    _validate_parameters(
        max_queries=max_queries,
        min_query_spacing_m=min_query_spacing_m,
        positive_distance_m=positive_distance_m,
        phash_distance_threshold=phash_distance_threshold,
        calibration_test_embargo_m=calibration_test_embargo_m,
        minimum_queries_per_split=minimum_queries_per_split,
        minimum_sequences_per_split=minimum_sequences_per_split,
        minimum_areas_per_split=minimum_areas_per_split,
        minimum_gallery_rows=minimum_gallery_rows,
        minimum_gallery_sequences=minimum_gallery_sequences,
        minimum_gallery_areas=minimum_gallery_areas,
        minimum_gallery_fraction=minimum_gallery_fraction,
        holdout_candidate_multiplier=holdout_candidate_multiplier,
    )
    frame = read_manifest(input_manifest, allow_empty=True).reset_index(drop=True)
    _validate_moscow_manifest(frame)
    fingerprints = _fingerprint_images(frame)
    frame["evaluation_area_h3"] = [
        to_h3(float(lat), float(lon), AREA_H3_RESOLUTION) for lat, lon in zip(frame["lat"], frame["lon"], strict=True)
    ]
    frame["evaluation_sample_h3"] = [
        to_h3(float(lat), float(lon), SAMPLE_H3_RESOLUTION) for lat, lon in zip(frame["lat"], frame["lon"], strict=True)
    ]

    missing_sequence_indices = [index for index, value in frame["sequence_id"].items() if not _text(value)]
    usable_indices = [index for index in frame.index if index not in set(missing_sequence_indices)]
    if not usable_indices:
        raise MoscowSplitError("no records have a stable sequence_id")
    eligible_indices, eligibility = _eligible_frames(
        frame,
        usable_indices,
        positive_distance_m=positive_distance_m,
    )
    excluded_query_sequences, exclusion_evidence = _query_sequence_exclusions(
        tuple(Path(path) for path in exclude_query_manifests)
    )
    eligible_before_historical_exclusion = len(eligible_indices)
    eligible_indices = {
        index
        for index in eligible_indices
        if _sequence_key(frame, index) not in excluded_query_sequences
    }
    eligibility["eligible_before_historical_query_sequence_exclusion"] = (
        eligible_before_historical_exclusion
    )
    eligibility["historical_query_provider_sequences_excluded"] = len(
        excluded_query_sequences
    )
    eligibility["eligible_removed_by_historical_query_sequence_exclusion"] = (
        eligible_before_historical_exclusion - len(eligible_indices)
    )
    if not eligible_indices:
        raise MoscowSplitError("no query candidates exist in multi-sequence neighborhoods with gallery positives")
    initial_sequences, initial_holdout_selection = _choose_held_out_sequences(
        frame,
        eligible_indices,
        usable_indices,
        seed=seed,
        positive_distance_m=positive_distance_m,
        max_queries=max_queries,
        minimum_gallery_rows=minimum_gallery_rows,
        minimum_gallery_sequences=minimum_gallery_sequences,
        minimum_gallery_areas=minimum_gallery_areas,
        minimum_gallery_fraction=minimum_gallery_fraction,
        holdout_candidate_multiplier=holdout_candidate_multiplier,
    )
    (
        selected,
        gallery_indices,
        distances,
        assignment,
        geo_group_by_sequence,
        diagnostics,
        geo_diagnostics,
    ) = _finalize_query_selection(
        frame,
        eligible_indices,
        usable_indices,
        fingerprints,
        initial_sequences,
        seed=seed,
        max_queries=max_queries,
        min_query_spacing_m=min_query_spacing_m,
        positive_distance_m=positive_distance_m,
        phash_distance_threshold=phash_distance_threshold,
        calibration_test_embargo_m=calibration_test_embargo_m,
        representative_balance=representative_balance,
    )
    calibration_indices = [index for index in selected if assignment[_sequence_key(frame, index)] == "calibration"]
    test_indices = [index for index in selected if assignment[_sequence_key(frame, index)] == "test"]
    geo_groups = {index: geo_group_by_sequence[_sequence_key(frame, index)] for index in selected}

    gallery, calibration, test = _align_output_schemas(
        [
            _with_split_columns(frame, gallery_indices, "gallery", fingerprints, {}, {}),
            _with_split_columns(
                frame,
                calibration_indices,
                "calibration",
                fingerprints,
                distances,
                geo_groups,
            ),
            _with_split_columns(frame, test_indices, "test", fingerprints, distances, geo_groups),
        ]
    )

    overlap_audit = {
        "gallery__calibration": _pairwise_audit(gallery, calibration, phash_distance_threshold),
        "gallery__test": _pairwise_audit(gallery, test, phash_distance_threshold),
        "calibration__test": _pairwise_audit(calibration, test, phash_distance_threshold),
    }
    if any(any(value != 0 for value in result.values()) for result in overlap_audit.values()):
        raise MoscowSplitError(f"non-zero cross-split overlap detected: {overlap_audit}")
    query_union_audit = _query_union_audit(
        calibration,
        test,
        phash_distance_threshold=phash_distance_threshold,
        min_query_spacing_m=min_query_spacing_m,
        calibration_test_embargo_m=calibration_test_embargo_m,
    )
    query_union_failures = {
        key: query_union_audit[key]
        for key in (
            "exact_file_duplicate_pairs",
            "phash_near_duplicate_pairs",
            "query_spacing_violation_pairs",
            "calibration_test_area_h3_overlap",
            "calibration_test_geo_group_overlap",
            "calibration_test_embargo_violation_pairs",
        )
        if query_union_audit[key]
    }
    if query_union_failures:
        raise MoscowSplitError(f"query-union independence audit failed: {query_union_failures}")
    coverage_gate = _coverage_gate(
        gallery,
        calibration,
        test,
        usable_row_count=len(usable_indices),
        minimum_queries_per_split=minimum_queries_per_split,
        minimum_sequences_per_split=minimum_sequences_per_split,
        minimum_areas_per_split=minimum_areas_per_split,
        minimum_gallery_rows=minimum_gallery_rows,
        minimum_gallery_sequences=minimum_gallery_sequences,
        minimum_gallery_areas=minimum_gallery_areas,
        minimum_gallery_fraction=minimum_gallery_fraction,
    )
    if not coverage_gate["passed"]:
        raise MoscowSplitError(
            "insufficient coverage for defensible split after all leakage and spacing filters: "
            + ", ".join(coverage_gate["failures"])
        )

    gallery_set = set(gallery_indices)
    calibration_set = set(calibration_indices)
    test_set = set(test_indices)
    missing_sequence_set = set(missing_sequence_indices)
    ledger: list[dict[str, Any]] = []
    for index, row in frame.iterrows():
        if index in gallery_set:
            disposition = "gallery"
        elif index in calibration_set:
            disposition = "calibration"
        elif index in test_set:
            disposition = "test"
        elif index in missing_sequence_set:
            disposition = "excluded_missing_sequence_id"
        else:
            disposition = "excluded_held_out_not_sampled"
        ledger.append(
            {
                "disposition": disposition,
                "file_sha256": fingerprints[index].file_sha256,
                "id": _text(row["id"]),
                "image_path": _text(row["image_path"]),
                "perceptual_hash": fingerprints[index].perceptual_hash,
                "sequence_id": _text(row["sequence_id"]),
                "provider_sequence_id": (
                    _sequence_label(_sequence_key(frame, index)) if index not in missing_sequence_set else None
                ),
                "evaluation_geo_group_id": geo_groups.get(index),
                "source": _text(row["source"]).lower(),
                "source_image_id": _text(row["source_image_id"]),
                "stable_source_id": f"{_text(row['source']).lower()}:{_text(row['source_image_id'])}",
            }
        )
    ledger.sort(key=lambda row: (str(row["disposition"]), str(row["id"])))
    active_query_sequences = {_sequence_key(frame, index) for index in selected}
    selected_set = set(selected)
    held_out_non_query = sum(
        1
        for index in usable_indices
        if _sequence_key(frame, index) in active_query_sequences and index not in selected_set
    )
    excluded_total = len(frame) - len(gallery) - len(calibration) - len(test)
    all_query_distances = [distances[index] for index in selected]
    selected_query_count = len(calibration) + len(test)
    sample_size_assessment = {
        "independent_query_target": INDEPENDENT_QUERY_TARGET,
        "selected_query_count": selected_query_count,
        "target_met": selected_query_count >= INDEPENDENT_QUERY_TARGET,
        "configured_cap_below_target": max_queries < INDEPENDENT_QUERY_TARGET,
        "status": (
            "target_met"
            if selected_query_count >= INDEPENDENT_QUERY_TARGET
            else "below_target_requires_more_coverage_or_explicit_policy_review"
        ),
    }
    input_manifest_sha256 = _sha256_file(input_manifest)
    ledger_sha256 = _canonical_ledger_hash(ledger)
    parameters = {
        "seed": seed,
        "max_queries": max_queries,
        "min_query_spacing_m": min_query_spacing_m,
        "positive_distance_m": positive_distance_m,
        "phash_distance_threshold": phash_distance_threshold,
        "calibration_test_embargo_m": calibration_test_embargo_m,
        "area_h3_resolution": AREA_H3_RESOLUTION,
        "sample_h3_resolution": SAMPLE_H3_RESOLUTION,
        "minimum_queries_per_split": minimum_queries_per_split,
        "minimum_sequences_per_split": minimum_sequences_per_split,
        "minimum_areas_per_split": minimum_areas_per_split,
        "minimum_gallery_rows": minimum_gallery_rows,
        "minimum_gallery_sequences": minimum_gallery_sequences,
        "minimum_gallery_areas": minimum_gallery_areas,
        "minimum_gallery_fraction": minimum_gallery_fraction,
        "holdout_candidate_multiplier": holdout_candidate_multiplier,
        "representative_balance": representative_balance,
        "excluded_query_manifests": exclusion_evidence,
        "excluded_query_sequence_fingerprint": hashlib.sha256(
            "\n".join(sorted(_sequence_label(key) for key in excluded_query_sequences)).encode("utf-8")
        ).hexdigest(),
    }
    bundle_fingerprint = _bundle_fingerprint(
        input_manifest_sha256=input_manifest_sha256,
        parameters=parameters,
        ledger_sha256=ledger_sha256,
    )
    audit: dict[str, Any] = {
        "schema_version": SPLIT_SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "input": {
            "path": str(input_manifest),
            "rows": len(frame),
            "sha256": input_manifest_sha256,
        },
        "parameters": parameters,
        "eligibility": {
            **eligibility,
            "historical_query_exclusion_manifests": exclusion_evidence,
            "eligible_query_frames_before_holdout": len(eligible_indices),
            "eligible_sequences_before_holdout": len({_sequence_key(frame, index) for index in eligible_indices}),
            "initial_holdout_selection": initial_holdout_selection,
            "initial_held_out_provider_sequences": sorted(_sequence_label(sequence) for sequence in initial_sequences),
            "final_held_out_provider_sequences": sorted(
                _sequence_label(sequence) for sequence in active_query_sequences
            ),
            "initial_held_out_sequences": sorted({sequence_id for _, sequence_id in initial_sequences}),
            "final_held_out_sequences": sorted({sequence_id for _, sequence_id in active_query_sequences}),
            "calibration_sequence_ids": sorted(_set_values(calibration, "sequence_id")),
            "test_sequence_ids": sorted(_set_values(test, "sequence_id")),
            "calibration_provider_sequence_ids": sorted(
                _sequence_label(sequence) for sequence in _provider_sequence_values(calibration)
            ),
            "test_provider_sequence_ids": sorted(
                _sequence_label(sequence) for sequence in _provider_sequence_values(test)
            ),
        },
        "excluded_counts": {
            "excluded_from_all_outputs": excluded_total,
            "rows_missing_sequence_id": len(missing_sequence_indices),
            "held_out_sequence_rows_not_sampled_as_queries": held_out_non_query,
            **diagnostics,
        },
        "sample_counts": {
            "gallery": _counts(gallery),
            "calibration": _counts(calibration),
            "test": _counts(test),
        },
        "coverage_gate": coverage_gate,
        "sample_size_assessment": sample_size_assessment,
        "geographic_partition": {
            "partition_column": "evaluation_area_h3",
            "calibration_test_embargo_m": calibration_test_embargo_m,
            **geo_diagnostics,
        },
        "positive_distance_summary_m": {
            "all_queries": _distance_summary(all_query_distances),
            "calibration": _distance_summary([distances[index] for index in calibration_indices]),
            "test": _distance_summary([distances[index] for index in test_indices]),
        },
        "overlap_audit": overlap_audit,
        "query_union_independence_audit": query_union_audit,
        "hashes": {
            "input_manifest_sha256": input_manifest_sha256,
            "file_phash_ledger_sha256": ledger_sha256,
        },
        "bundle": {
            "fingerprint": bundle_fingerprint,
            "publication": (
                "verified_staging_directory_publish; existing bundles require an explicit recovery-backup swap"
            ),
        },
        "file_phash_ledger": ledger,
    }

    existing = _verified_existing_bundle(output_dir, bundle_fingerprint) if output_dir.is_dir() else None
    if existing is not None:
        print(
            json.dumps(
                {
                    "audit": str(output_dir / "split_audit.json"),
                    "bundle_fingerprint": bundle_fingerprint,
                    "calibration_queries": len(calibration),
                    "gallery": len(gallery),
                    "publication": "reused_verified_bundle",
                    "test_queries": len(test),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return existing
    if output_dir.exists() and not replace_existing:
        raise MoscowSplitError(
            f"output directory already contains a different or unverifiable split bundle: {output_dir}; "
            "use replace_existing=True/--replace-existing to replace it"
        )

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging_dir = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.{os.getpid()}.", dir=output_dir.parent))
    try:
        output_paths = {
            "gallery": staging_dir / "gallery.parquet",
            "calibration": staging_dir / "calibration_queries.parquet",
            "test": staging_dir / "test_queries.parquet",
        }
        _write_split_outputs(
            [
                (gallery, output_paths["gallery"]),
                (calibration, output_paths["calibration"]),
                (test, output_paths["test"]),
            ]
        )
        audit["hashes"].update(
            {
                "gallery_parquet_sha256": _sha256_file(output_paths["gallery"]),
                "calibration_queries_parquet_sha256": _sha256_file(output_paths["calibration"]),
                "test_queries_parquet_sha256": _sha256_file(output_paths["test"]),
            }
        )
        markdown_path = staging_dir / "split_audit.md"
        audit_path = staging_dir / "split_audit.json"
        _write_markdown(markdown_path, audit)
        write_json(audit_path, audit)
        bundle_files = {
            path.name: _sha256_file(path)
            for path in (
                output_paths["gallery"],
                output_paths["calibration"],
                output_paths["test"],
                audit_path,
                markdown_path,
            )
        }
        write_json(
            staging_dir / BUNDLE_MANIFEST_NAME,
            {
                "bundle_fingerprint": bundle_fingerprint,
                "files": bundle_files,
                "schema_version": SPLIT_SCHEMA_VERSION,
            },
        )
        if _verified_existing_bundle(staging_dir, bundle_fingerprint) is None:
            raise MoscowSplitError("staged split bundle failed integrity verification")
        _publish_staged_bundle(staging_dir, output_dir, replace_existing=replace_existing)
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir)
    print(
        json.dumps(
            {
                "gallery": len(gallery),
                "calibration_queries": len(calibration),
                "test_queries": len(test),
                "audit": str(output_dir / "split_audit.json"),
                "bundle_fingerprint": bundle_fingerprint,
                "publication": "published_new_bundle",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a leakage-resistant Moscow evaluation split")
    parser.add_argument("--manifest", type=Path, required=True, help="Canonical downloaded Parquet manifest")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20260815)
    parser.add_argument("--max-queries", type=int, default=1000)
    parser.add_argument("--min-query-spacing-m", type=float, default=20.0)
    parser.add_argument("--positive-distance-m", type=float, default=100.0)
    parser.add_argument("--phash-distance-threshold", type=int, default=4)
    parser.add_argument(
        "--calibration-test-embargo-m",
        type=float,
        default=DEFAULT_CALIBRATION_TEST_EMBARGO_M,
    )
    parser.add_argument("--minimum-queries-per-split", type=int, default=1)
    parser.add_argument("--minimum-sequences-per-split", type=int, default=1)
    parser.add_argument("--minimum-areas-per-split", type=int, default=1)
    parser.add_argument("--minimum-gallery-rows", type=int, default=1)
    parser.add_argument("--minimum-gallery-sequences", type=int, default=1)
    parser.add_argument("--minimum-gallery-areas", type=int, default=1)
    parser.add_argument(
        "--minimum-gallery-fraction",
        type=float,
        default=DEFAULT_MINIMUM_GALLERY_FRACTION,
    )
    parser.add_argument(
        "--holdout-candidate-multiplier",
        type=int,
        default=DEFAULT_HOLDOUT_CANDIDATE_MULTIPLIER,
        help="bounded initial holdout pool as a multiple of --max-queries",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Replace the entire prior split bundle after a verified staged build",
    )
    parser.add_argument(
        "--representative-balance",
        action="store_true",
        help="balance calibration/test provider, H3-area, and resolution distributions for v2",
    )
    parser.add_argument(
        "--exclude-query-manifest",
        action="append",
        default=[],
        type=Path,
        help="historical query manifest whose provider-scoped sequences cannot become new queries",
    )
    args = parser.parse_args()
    run(
        input_manifest=args.manifest,
        output_dir=args.output_dir,
        seed=args.seed,
        max_queries=args.max_queries,
        min_query_spacing_m=args.min_query_spacing_m,
        positive_distance_m=args.positive_distance_m,
        phash_distance_threshold=args.phash_distance_threshold,
        calibration_test_embargo_m=args.calibration_test_embargo_m,
        minimum_queries_per_split=args.minimum_queries_per_split,
        minimum_sequences_per_split=args.minimum_sequences_per_split,
        minimum_areas_per_split=args.minimum_areas_per_split,
        minimum_gallery_rows=args.minimum_gallery_rows,
        minimum_gallery_sequences=args.minimum_gallery_sequences,
        minimum_gallery_areas=args.minimum_gallery_areas,
        minimum_gallery_fraction=args.minimum_gallery_fraction,
        holdout_candidate_multiplier=args.holdout_candidate_multiplier,
        replace_existing=args.replace_existing,
        representative_balance=args.representative_balance,
        exclude_query_manifests=args.exclude_query_manifest,
    )


if __name__ == "__main__":
    main()
