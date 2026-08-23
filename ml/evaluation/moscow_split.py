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
SPLIT_SCHEMA_VERSION = 1
ALGORITHM_VERSION = "sequence-holdout-v1"


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


def _stable_score(seed: int, *parts: object) -> str:
    payload = "\0".join([str(seed), *(str(part) for part in parts)]).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _text(value: Any) -> str:
    return "" if is_missing_value(value) else str(value).strip()


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
        different_sequence_from: str | None = None,
    ) -> float | None:
        lat = float(self.frame.at[row_index, "lat"])
        lon = float(self.frame.at[row_index, "lon"])
        best: float | None = None
        for candidate in self.nearby(lat, lon):
            if candidate == row_index:
                continue
            if different_sequence_from is not None:
                candidate_sequence = _text(self.frame.at[candidate, "sequence_id"])
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
) -> None:
    if max_queries < 1:
        raise ValueError("max_queries must be >= 1")
    if not math.isfinite(min_query_spacing_m) or min_query_spacing_m < 0:
        raise ValueError("min_query_spacing_m must be finite and >= 0")
    if not math.isfinite(positive_distance_m) or positive_distance_m <= 0:
        raise ValueError("positive_distance_m must be finite and > 0")
    if not 0 <= phash_distance_threshold <= PHASH_BITS:
        raise ValueError("phash_distance_threshold must be in [0, 64]")


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
    sequences_by_area: dict[str, set[str]] = defaultdict(set)
    for index in usable_indices:
        sequences_by_area[str(frame.at[index, "evaluation_area_h3"])].add(_text(frame.at[index, "sequence_id"]))
    multi_sequence_areas = {area for area, sequences in sequences_by_area.items() if len(sequences) >= 2}
    spatial = _SpatialIndex(frame, usable_indices, positive_distance_m)
    eligible: set[int] = set()
    area_rejected = 0
    no_positive = 0
    for index in usable_indices:
        if str(frame.at[index, "evaluation_area_h3"]) not in multi_sequence_areas:
            area_rejected += 1
            continue
        sequence = _text(frame.at[index, "sequence_id"])
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
    held_out_sequences: set[str],
    usable_indices: Sequence[int],
    *,
    positive_distance_m: float,
) -> tuple[dict[int, float], list[int]]:
    gallery = [index for index in usable_indices if _text(frame.at[index, "sequence_id"]) not in held_out_sequences]
    if not gallery:
        return {}, gallery
    spatial = _SpatialIndex(frame, gallery, positive_distance_m)
    distances: dict[int, float] = {}
    for index in eligible_indices:
        sequence = _text(frame.at[index, "sequence_id"])
        if sequence not in held_out_sequences:
            continue
        distance = spatial.nearest(index)
        if distance is not None:
            distances[index] = distance
    return distances, gallery


def _sequence_order(frame: pd.DataFrame, eligible_indices: set[int], seed: int) -> list[str]:
    sequences_by_area: dict[str, list[str]] = defaultdict(list)
    for index in eligible_indices:
        sequences_by_area[str(frame.at[index, "evaluation_area_h3"])].append(_text(frame.at[index, "sequence_id"]))
    groups: dict[str, Sequence[int]] = {}
    sequence_numbers: dict[str, int] = {}
    reverse: dict[int, str] = {}
    for number, sequence in enumerate(sorted({item for values in sequences_by_area.values() for item in values})):
        sequence_numbers[sequence] = number
        reverse[number] = sequence
    for area, sequences in sequences_by_area.items():
        groups[area] = [sequence_numbers[value] for value in sorted(set(sequences))]
    interleaved = _round_robin_groups(groups, seed=seed, namespace="sequence")
    result: list[str] = []
    seen: set[str] = set()
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
    max_queries: int,
    positive_distance_m: float,
) -> list[str]:
    chosen: list[str] = []
    for sequence in _sequence_order(frame, eligible_indices, seed):
        trial = {*chosen, sequence}
        distances, _ = _query_distances(
            frame,
            eligible_indices,
            trial,
            usable_indices,
            positive_distance_m=positive_distance_m,
        )
        represented = {_text(frame.at[index, "sequence_id"]) for index in distances}
        if represented != trial:
            continue
        chosen.append(sequence)
        # Retain two sequence opportunities for disjoint calibration/test
        # whenever the data supports them; one remains a valid tiny split.
        if len(chosen) >= 2 and len(distances) >= max_queries:
            break
    if not chosen:
        raise MoscowSplitError("no sequence can be held out while retaining a gallery positive")
    return chosen


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
    by_sequence_and_cell: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    for index in candidates:
        sequence = _text(frame.at[index, "sequence_id"])
        cell = str(frame.at[index, "evaluation_sample_h3"])
        by_sequence_and_cell[sequence][cell].append(index)

    kept: list[int] = []
    removed = 0
    reference_lat = float(frame["lat"].mean())

    def spacing_cell(index: int) -> tuple[int, int]:
        x = math.radians(float(frame.at[index, "lon"])) * EARTH_RADIUS_M * math.cos(math.radians(reference_lat))
        y = math.radians(float(frame.at[index, "lat"])) * EARTH_RADIUS_M
        return math.floor(x / min_query_spacing_m), math.floor(y / min_query_spacing_m)

    for sequence in sorted(by_sequence_and_cell, key=lambda value: _stable_score(seed, "downsample-seq", value)):
        order = _round_robin_groups(
            by_sequence_and_cell[sequence],
            seed=seed,
            namespace=f"downsample:{sequence}",
        )
        sequence_kept_by_cell: dict[tuple[int, int], list[int]] = defaultdict(list)
        for index in order:
            cell_x, cell_y = spacing_cell(index)
            nearby_kept = (
                other
                for dx in range(-2, 3)
                for dy in range(-2, 3)
                for other in sequence_kept_by_cell.get((cell_x + dx, cell_y + dy), ())
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
            sequence_kept_by_cell[(cell_x, cell_y)].append(index)
            kept.append(index)
    return kept, removed


def _assign_sequences(
    frame: pd.DataFrame,
    candidates: Sequence[int],
    preferred_order: Sequence[str],
    *,
    seed: int,
) -> dict[str, str]:
    by_sequence: dict[str, list[int]] = defaultdict(list)
    for index in candidates:
        by_sequence[_text(frame.at[index, "sequence_id"])].append(index)
    ordered = [sequence for sequence in preferred_order if sequence in by_sequence]
    ordered.extend(
        sorted(
            set(by_sequence) - set(ordered),
            key=lambda value: _stable_score(seed, "assign-extra", value),
        )
    )
    assignment: dict[str, str] = {}
    area_sets = {"calibration": set(), "test": set()}
    cell_sets = {"calibration": set(), "test": set()}
    row_counts = {"calibration": 0, "test": 0}
    for position, sequence in enumerate(ordered):
        areas = {str(frame.at[index, "evaluation_area_h3"]) for index in by_sequence[sequence]}
        cells = {str(frame.at[index, "evaluation_sample_h3"]) for index in by_sequence[sequence]}
        if position == 0:
            split = "calibration"
        elif position == 1:
            split = "test"
        else:
            choices: list[tuple[tuple[int, int, int, str], str]] = []
            for candidate_split in ("calibration", "test"):
                score = (
                    len(areas - area_sets[candidate_split]),
                    len(cells - cell_sets[candidate_split]),
                    -row_counts[candidate_split],
                    _stable_score(seed, "assign", sequence, candidate_split),
                )
                choices.append((score, candidate_split))
            split = max(choices)[1]
        assignment[sequence] = split
        area_sets[split].update(areas)
        cell_sets[split].update(cells)
        row_counts[split] += len(by_sequence[sequence])
    return assignment


def _bounded_query_order(
    frame: pd.DataFrame,
    candidates: Sequence[int],
    assignment: Mapping[str, str],
    *,
    seed: int,
) -> list[int]:
    split_groups: dict[str, dict[str, list[int]]] = {"calibration": defaultdict(list), "test": defaultdict(list)}
    for index in candidates:
        sequence = _text(frame.at[index, "sequence_id"])
        sample_cell = str(frame.at[index, "evaluation_sample_h3"])
        split_groups[assignment[sequence]][f"{sequence}\0{sample_cell}"].append(index)
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


def _reject_cross_query_leakage(
    ordered_candidates: Sequence[int],
    frame: pd.DataFrame,
    assignment: Mapping[str, str],
    fingerprints: Mapping[int, Fingerprint],
    *,
    max_queries: int,
    phash_distance_threshold: int,
) -> tuple[list[int], dict[str, int]]:
    accepted: dict[str, list[int]] = {"calibration": [], "test": []}
    exact: dict[str, set[str]] = {"calibration": set(), "test": set()}
    phash = {
        "calibration": _PerceptualHashIndex(phash_distance_threshold),
        "test": _PerceptualHashIndex(phash_distance_threshold),
    }
    exact_rejected = 0
    near_rejected = 0
    capped = 0
    for position, index in enumerate(ordered_candidates):
        if sum(len(values) for values in accepted.values()) >= max_queries:
            capped = len(ordered_candidates) - position
            break
        sequence = _text(frame.at[index, "sequence_id"])
        split = assignment[sequence]
        other = "test" if split == "calibration" else "calibration"
        fingerprint = fingerprints[index]
        if fingerprint.file_sha256 in exact[other]:
            exact_rejected += 1
            continue
        if phash[other].matches(fingerprint.perceptual_hash_int):
            near_rejected += 1
            continue
        accepted[split].append(index)
        exact[split].add(fingerprint.file_sha256)
        phash[split].add(index, fingerprint.perceptual_hash_int)
    return [*accepted["calibration"], *accepted["test"]], {
        "query_frames_rejected_exact_file_match_across_query_splits": exact_rejected,
        "query_frames_rejected_phash_near_duplicate_across_query_splits": near_rejected,
        "query_frames_removed_by_max_queries": capped,
    }


def _finalize_query_selection(
    frame: pd.DataFrame,
    eligible_indices: set[int],
    usable_indices: Sequence[int],
    fingerprints: Mapping[int, Fingerprint],
    initial_sequences: Sequence[str],
    *,
    seed: int,
    max_queries: int,
    min_query_spacing_m: float,
    positive_distance_m: float,
    phash_distance_threshold: int,
) -> tuple[list[int], list[int], dict[int, float], dict[str, str], dict[str, int]]:
    held_out = set(initial_sequences)
    diagnostics: Counter[str] = Counter()
    final_selected: list[int] = []
    final_gallery: list[int] = []
    final_distances: dict[int, float] = {}
    final_assignment: dict[str, str] = {}
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
        assignment = _assign_sequences(frame, sampled, initial_sequences, seed=seed)
        bounded_order = _bounded_query_order(frame, sampled, assignment, seed=seed)
        selected, cross = _reject_cross_query_leakage(
            bounded_order,
            frame,
            assignment,
            fingerprints,
            max_queries=max_queries,
            phash_distance_threshold=phash_distance_threshold,
        )
        active_sequences = {_text(frame.at[index, "sequence_id"]) for index in selected}
        diagnostics = Counter(leakage)
        diagnostics.update(cross)
        diagnostics["query_frames_removed_by_min_spacing"] = spacing_removed
        final_selected = selected
        final_gallery = gallery
        final_distances = {index: distances[index] for index in selected}
        final_assignment = assignment
        if active_sequences == held_out:
            break
        held_out = active_sequences
        if not held_out:
            break
    if not final_selected:
        raise MoscowSplitError(
            "no defensible query frames remain after positive-distance, exact-hash, pHash, and spacing checks"
        )
    active_sequences = {_text(frame.at[index, "sequence_id"]) for index in final_selected}
    final_gallery = [index for index in usable_indices if _text(frame.at[index, "sequence_id"]) not in active_sequences]
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
    return final_selected, final_gallery, final_distances, final_assignment, dict(diagnostics)


def _with_split_columns(
    frame: pd.DataFrame,
    indices: Sequence[int],
    split: str,
    fingerprints: Mapping[int, Fingerprint],
    distances: Mapping[int, float],
) -> pd.DataFrame:
    result = frame.loc[list(indices)].copy() if indices else frame.iloc[0:0].copy()
    result["evaluation_split"] = split
    result["nearest_gallery_distance_m"] = [
        float(distances[index]) if index in distances else float("nan") for index in indices
    ]
    result["file_sha256"] = [fingerprints[index].file_sha256 for index in indices]
    result["perceptual_hash"] = [fingerprints[index].perceptual_hash for index in indices]
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


def _near_pair_count(left: pd.DataFrame, right: pd.DataFrame, threshold: int) -> int:
    index = _PerceptualHashIndex(threshold)
    for row_index, value in enumerate(right["perceptual_hash"]):
        index.add(row_index, int(str(value), 16))
    return sum(len(index.matches(int(str(value), 16))) for value in left["perceptual_hash"])


def _pairwise_audit(left: pd.DataFrame, right: pd.DataFrame, threshold: int) -> dict[str, int]:
    stable_source_overlap = len(
        {f"{_text(row.source)}:{_text(row.source_image_id)}" for row in left.itertuples()}
        & {f"{_text(row.source)}:{_text(row.source_image_id)}" for row in right.itertuples()}
    )
    return {
        "id_overlap": len(_set_values(left, "id") & _set_values(right, "id")),
        "source_id_overlap": stable_source_overlap,
        "stable_source_id_overlap": stable_source_overlap,
        "file_sha256_overlap": len(_set_values(left, "file_sha256") & _set_values(right, "file_sha256")),
        "sequence_id_overlap": len(_set_values(left, "sequence_id") & _set_values(right, "sequence_id")),
        "phash_near_duplicate_pairs": _near_pair_count(left, right, threshold),
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
        "sequences": int(frame["sequence_id"].dropna().astype("string").nunique()),
        "areas": int(frame["evaluation_area_h3"].astype("string").nunique()),
        "h3_sample_cells": int(frame["evaluation_sample_h3"].astype("string").nunique()),
        "by_source": {str(key): int(value) for key, value in frame["source"].value_counts().sort_index().items()},
        "by_area": {
            str(key): int(value) for key, value in frame["evaluation_area_h3"].value_counts().sort_index().items()
        },
    }


def _canonical_ledger_hash(ledger: Sequence[Mapping[str, Any]]) -> str:
    encoded = json.dumps(list(ledger), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_markdown(path: Path, audit: Mapping[str, Any]) -> None:
    counts = audit["sample_counts"]
    overlaps = audit["overlap_audit"]
    distances = audit["positive_distance_summary_m"]
    lines = [
        "# Moscow evaluation split audit",
        "",
        f"- Algorithm: `{audit['algorithm_version']}`",
        f"- Seed: `{audit['parameters']['seed']}`",
        f"- Input rows: {audit['input']['rows']}",
        f"- Gallery: {counts['gallery']['rows']} rows / {counts['gallery']['sequences']} sequences",
        (f"- Calibration: {counts['calibration']['rows']} rows / {counts['calibration']['sequences']} sequences"),
        f"- Test: {counts['test']['rows']} rows / {counts['test']['sequences']} sequences",
        f"- Positive threshold: {audit['parameters']['positive_distance_m']:.3f} m",
        f"- Query positive-distance p90: {distances['all_queries']['p90']}",
        f"- pHash rejection threshold: {audit['parameters']['phash_distance_threshold']} bits",
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
) -> dict[str, Any]:
    """Create gallery/calibration/test Parquets and a fail-closed audit."""

    _validate_parameters(
        max_queries=max_queries,
        min_query_spacing_m=min_query_spacing_m,
        positive_distance_m=positive_distance_m,
        phash_distance_threshold=phash_distance_threshold,
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
    if not eligible_indices:
        raise MoscowSplitError("no query candidates exist in multi-sequence neighborhoods with gallery positives")
    initial_sequences = _choose_held_out_sequences(
        frame,
        eligible_indices,
        usable_indices,
        seed=seed,
        max_queries=max_queries,
        positive_distance_m=positive_distance_m,
    )
    selected, gallery_indices, distances, assignment, diagnostics = _finalize_query_selection(
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
    )
    calibration_indices = [
        index for index in selected if assignment[_text(frame.at[index, "sequence_id"])] == "calibration"
    ]
    test_indices = [index for index in selected if assignment[_text(frame.at[index, "sequence_id"])] == "test"]

    gallery, calibration, test = _align_output_schemas(
        [
            _with_split_columns(frame, gallery_indices, "gallery", fingerprints, {}),
            _with_split_columns(frame, calibration_indices, "calibration", fingerprints, distances),
            _with_split_columns(frame, test_indices, "test", fingerprints, distances),
        ]
    )

    overlap_audit = {
        "gallery__calibration": _pairwise_audit(gallery, calibration, phash_distance_threshold),
        "gallery__test": _pairwise_audit(gallery, test, phash_distance_threshold),
        "calibration__test": _pairwise_audit(calibration, test, phash_distance_threshold),
    }
    if any(any(value != 0 for value in result.values()) for result in overlap_audit.values()):
        raise MoscowSplitError(f"non-zero cross-split overlap detected: {overlap_audit}")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = {
        "gallery": output_dir / "gallery.parquet",
        "calibration": output_dir / "calibration_queries.parquet",
        "test": output_dir / "test_queries.parquet",
    }
    _write_split_outputs(
        [
            (gallery, output_paths["gallery"]),
            (calibration, output_paths["calibration"]),
            (test, output_paths["test"]),
        ]
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
                "source": _text(row["source"]),
                "source_image_id": _text(row["source_image_id"]),
                "stable_source_id": f"{_text(row['source'])}:{_text(row['source_image_id'])}",
            }
        )
    ledger.sort(key=lambda row: (str(row["disposition"]), str(row["id"])))
    active_query_sequences = {_text(frame.at[index, "sequence_id"]) for index in selected}
    held_out_non_query = sum(
        1
        for index in usable_indices
        if _text(frame.at[index, "sequence_id"]) in active_query_sequences and index not in set(selected)
    )
    excluded_total = len(frame) - len(gallery) - len(calibration) - len(test)
    all_query_distances = [distances[index] for index in selected]
    audit: dict[str, Any] = {
        "schema_version": SPLIT_SCHEMA_VERSION,
        "algorithm_version": ALGORITHM_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "input": {
            "path": str(input_manifest),
            "rows": len(frame),
            "sha256": _sha256_file(input_manifest),
        },
        "parameters": {
            "seed": seed,
            "max_queries": max_queries,
            "min_query_spacing_m": min_query_spacing_m,
            "positive_distance_m": positive_distance_m,
            "phash_distance_threshold": phash_distance_threshold,
            "area_h3_resolution": AREA_H3_RESOLUTION,
            "sample_h3_resolution": SAMPLE_H3_RESOLUTION,
        },
        "eligibility": {
            **eligibility,
            "eligible_query_frames_before_holdout": len(eligible_indices),
            "eligible_sequences_before_holdout": len(
                {_text(frame.at[index, "sequence_id"]) for index in eligible_indices}
            ),
            "initial_held_out_sequences": initial_sequences,
            "final_held_out_sequences": sorted(active_query_sequences),
            "calibration_sequence_ids": sorted(_set_values(calibration, "sequence_id")),
            "test_sequence_ids": sorted(_set_values(test, "sequence_id")),
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
        "positive_distance_summary_m": {
            "all_queries": _distance_summary(all_query_distances),
            "calibration": _distance_summary([distances[index] for index in calibration_indices]),
            "test": _distance_summary([distances[index] for index in test_indices]),
        },
        "overlap_audit": overlap_audit,
        "hashes": {
            "input_manifest_sha256": _sha256_file(input_manifest),
            "gallery_parquet_sha256": _sha256_file(output_paths["gallery"]),
            "calibration_queries_parquet_sha256": _sha256_file(output_paths["calibration"]),
            "test_queries_parquet_sha256": _sha256_file(output_paths["test"]),
            "file_phash_ledger_sha256": _canonical_ledger_hash(ledger),
        },
        "file_phash_ledger": ledger,
    }
    audit_path = output_dir / "split_audit.json"
    write_json(audit_path, audit)
    _write_markdown(output_dir / "split_audit.md", audit)
    print(
        json.dumps(
            {
                "gallery": len(gallery),
                "calibration_queries": len(calibration),
                "test_queries": len(test),
                "audit": str(audit_path),
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
    args = parser.parse_args()
    run(
        input_manifest=args.manifest,
        output_dir=args.output_dir,
        seed=args.seed,
        max_queries=args.max_queries,
        min_query_spacing_m=args.min_query_spacing_m,
        positive_distance_m=args.positive_distance_m,
        phash_distance_threshold=args.phash_distance_threshold,
    )


if __name__ == "__main__":
    main()
