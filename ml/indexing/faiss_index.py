"""Correctness-first normalized ``faiss.IndexFlatIP`` persistence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

from ml.retrieval.base import DescriptorError, l2_normalize
from ml.retrieval.embedding_job import load_embedding_artifacts

INDEX_SCHEMA_VERSION = 2
_ARTIFACT_FILENAMES = (
    "index.faiss",
    "id_mapping.json",
    "reference_metadata.jsonl",
)
_COMMIT_METADATA_KEYS = frozenset({"artifact_generation", "artifact_sha256"})


class FaissIndexError(RuntimeError):
    """A FAISS artifact is malformed or an index operation is invalid."""


class FaissUnavailableError(FaissIndexError):
    """The required FAISS runtime is unavailable."""


def _import_faiss() -> Any:
    # Current macOS wheels for torch and faiss may bundle distinct OpenMP
    # runtimes. Importing both can abort the interpreter before an exception is
    # possible. The production service uses FaissIndexWorker on Darwin.
    if sys.platform == "darwin" and "torch" in sys.modules and "faiss" not in sys.modules:
        raise FaissUnavailableError(
            "loading FAISS into a process that already imported PyTorch is unsafe with "
            "common macOS wheels; use FaissIndexWorker/LocalizationService process isolation"
        )
    try:
        import faiss
    except (ImportError, OSError) as exc:
        raise FaissUnavailableError(
            "faiss-cpu (or a compatible FAISS build) is required for retrieval; no NumPy production fallback is used"
        ) from exc
    return faiss


def _write_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(_json_safe(payload), handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_json_safe(row), ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if np.isfinite(value) else None
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _validate_reference_ids(reference_ids: Sequence[str]) -> list[str]:
    """Validate stable IDs without changing their manifest order."""

    ids = [str(value) for value in reference_ids]
    if not ids:
        raise FaissIndexError("cannot build a FAISS index with zero references")
    if any(not value for value in ids):
        raise FaissIndexError("reference IDs must be non-empty strings")
    if len(ids) != len(set(ids)):
        raise FaissIndexError("reference IDs must be unique")
    return ids


def _prepare_reference_metadata(
    ids: Sequence[str],
    reference_metadata: Sequence[Mapping[str, Any]] | None,
    *,
    index_id: str,
    city_id: str | None,
) -> list[dict[str, Any]]:
    """Validate and scope ordered metadata for prevalidated reference IDs."""

    if reference_metadata is None:
        references = [{} for _ in ids]
    else:
        references = [dict(_json_safe(value)) for value in reference_metadata]
        if len(references) != len(ids):
            raise FaissIndexError("reference metadata count does not match IDs")
    for reference_id, metadata in zip(ids, references, strict=True):
        row_index_id = metadata.get("index_id")
        if row_index_id is not None and str(row_index_id) != str(index_id):
            raise FaissIndexError(
                f"reference {reference_id!r} index_id {row_index_id!r} does not match build index_id {index_id!r}"
            )
        metadata["index_id"] = index_id
        row_city_id = metadata.get("city_id")
        if city_id is not None and row_city_id is not None and str(row_city_id) != str(city_id):
            raise FaissIndexError(
                f"reference {reference_id!r} city_id {row_city_id!r} does not match build city_id {city_id!r}"
            )
        if city_id is not None:
            metadata["city_id"] = city_id
    return references


def _prepare_reference_rows(
    reference_ids: Sequence[str],
    reference_metadata: Sequence[Mapping[str, Any]] | None,
    *,
    index_id: str,
    city_id: str | None,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Validate and scope one ordered set of reference sidecars."""

    ids = _validate_reference_ids(reference_ids)
    references = _prepare_reference_metadata(
        ids,
        reference_metadata,
        index_id=index_id,
        city_id=city_id,
    )
    return ids, references


def _build_metadata(
    *,
    index_id: str,
    city_id: str | None,
    gallery_size: int,
    descriptor_dim: int,
    retriever_metadata: Mapping[str, Any] | None,
    extra_metadata: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Create the common immutable metadata used by one-shot and streamed builds."""

    metadata: dict[str, Any] = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "built_at": datetime.now(UTC).isoformat(),
        "index_id": index_id,
        "city_id": city_id,
        "gallery_size": gallery_size,
        "descriptor_dim": descriptor_dim,
        "normalization": "L2",
        "similarity": "cosine_via_inner_product",
        "faiss_index_type": "IndexFlatIP",
        "retriever": dict(retriever_metadata or {}),
    }
    if extra_metadata:
        reserved = (set(metadata) | _COMMIT_METADATA_KEYS).intersection(extra_metadata)
        if reserved:
            raise FaissIndexError(f"extra_metadata cannot override reserved keys: {sorted(reserved)}")
        metadata.update(dict(extra_metadata))
    return metadata


def _read_committed_sidecars(
    directory: Path,
) -> tuple[list[str], list[dict[str, Any]], dict[str, Any]]:
    """Read one committed artifact generation without importing FAISS."""

    paths = {
        "index.faiss": directory / "index.faiss",
        "id_mapping.json": directory / "id_mapping.json",
        "reference_metadata.jsonl": directory / "reference_metadata.jsonl",
        "index_metadata.json": directory / "index_metadata.json",
    }
    missing = [name for name, path in paths.items() if not path.is_file()]
    if missing:
        raise FaissIndexError(f"incomplete FAISS artifact directory; missing {missing}")

    try:
        with paths["index_metadata.json"].open("r", encoding="utf-8") as handle:
            metadata = json.load(handle)
        if not isinstance(metadata, dict):
            raise FaissIndexError("index metadata must be a JSON object")
        if metadata.get("schema_version") != INDEX_SCHEMA_VERSION:
            raise FaissIndexError(
                f"unsupported index schema version {metadata.get('schema_version')!r}; expected {INDEX_SCHEMA_VERSION}"
            )
        generation = metadata.get("artifact_generation")
        if not isinstance(generation, str) or not generation:
            raise FaissIndexError("index metadata does not declare an artifact generation")
        expected_hashes = metadata.get("artifact_sha256")
        if not isinstance(expected_hashes, dict) or set(expected_hashes) != set(_ARTIFACT_FILENAMES):
            raise FaissIndexError("index metadata must declare SHA-256 for every persisted artifact")

        mapping_bytes = paths["id_mapping.json"].read_bytes()
        reference_bytes = paths["reference_metadata.jsonl"].read_bytes()
        actual_hashes = {
            "index.faiss": _sha256_file(paths["index.faiss"]),
            "id_mapping.json": hashlib.sha256(mapping_bytes).hexdigest(),
            "reference_metadata.jsonl": hashlib.sha256(reference_bytes).hexdigest(),
        }
        for filename, actual_hash in actual_hashes.items():
            expected_hash = expected_hashes.get(filename)
            if not isinstance(expected_hash, str) or expected_hash.lower() != actual_hash:
                raise FaissIndexError(f"SHA-256 mismatch for persisted artifact {filename}")

        mapping = json.loads(mapping_bytes)
        if not isinstance(mapping, list) or not all(isinstance(item, dict) for item in mapping):
            raise FaissIndexError("ID mapping must be a JSON list of objects")
        if [item.get("row") for item in mapping] != list(range(len(mapping))):
            raise FaissIndexError("ID mapping rows must be contiguous and ordered from zero")
        if any(item.get("artifact_generation") != generation for item in mapping):
            raise FaissIndexError("ID mapping artifact generation does not match index metadata")
        ids = [str(item["reference_id"]) for item in mapping]

        reference_rows: list[dict[str, Any]] = []
        for line in reference_bytes.decode("utf-8").splitlines():
            if line.strip():
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise FaissIndexError("reference metadata rows must be JSON objects")
                reference_rows.append(row)
        if [row.get("reference_id") for row in reference_rows] != ids:
            raise FaissIndexError("reference metadata order does not match ID mapping")
        if any(row.get("artifact_generation") != generation for row in reference_rows):
            raise FaissIndexError("reference metadata artifact generation does not match index metadata")
        references = [dict(row.get("metadata", {})) for row in reference_rows]

        if metadata.get("faiss_index_type") != "IndexFlatIP":
            raise FaissIndexError("metadata does not declare IndexFlatIP")
        if metadata.get("normalization") != "L2":
            raise FaissIndexError("metadata does not declare L2 normalization")
        if int(metadata.get("gallery_size", -1)) != len(ids):
            raise FaissIndexError("metadata gallery size does not match ID mapping")
    except FaissIndexError:
        raise
    except Exception as exc:
        raise FaissIndexError(f"failed to load FAISS artifacts from {directory}: {exc}") from exc
    return ids, references, metadata


@dataclass(frozen=True, slots=True)
class RetrievalResult:
    """One ranked FAISS match with its stable ID and display/localization metadata."""

    reference_id: str
    score: float
    rank: int
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def lat(self) -> float | None:
        value = self.metadata.get("lat")
        return None if value is None else float(value)

    @property
    def lon(self) -> float | None:
        value = self.metadata.get("lon")
        return None if value is None else float(value)

    @property
    def city_id(self) -> str | None:
        value = self.metadata.get("city_id")
        return None if value is None else str(value)

    @property
    def source(self) -> str | None:
        value = self.metadata.get("source")
        return None if value is None else str(value)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class FaissExactIndex:
    """Normalized cosine search backed only by FAISS ``IndexFlatIP``."""

    def __init__(
        self,
        index: Any,
        reference_ids: Sequence[str],
        reference_metadata: Sequence[Mapping[str, Any]],
        build_metadata: Mapping[str, Any],
    ) -> None:
        self._index = index
        self.reference_ids = tuple(str(value) for value in reference_ids)
        self.reference_metadata = tuple(dict(value) for value in reference_metadata)
        self.build_metadata = dict(build_metadata)
        self._validate_loaded_state()

    @classmethod
    def build(
        cls,
        descriptors: np.ndarray,
        reference_ids: Sequence[str],
        *,
        reference_metadata: Sequence[Mapping[str, Any]] | None = None,
        retriever_metadata: Mapping[str, Any] | None = None,
        index_id: str = "default",
        city_id: str | None = None,
        extra_metadata: Mapping[str, Any] | None = None,
    ) -> FaissExactIndex:
        faiss = _import_faiss()
        ids = _validate_reference_ids(reference_ids)
        try:
            matrix = l2_normalize(descriptors)
        except DescriptorError as exc:
            raise FaissIndexError(str(exc)) from exc
        if matrix.shape[0] != len(ids):
            raise FaissIndexError(f"descriptor rows ({matrix.shape[0]}) do not match IDs ({len(ids)})")
        references = _prepare_reference_metadata(
            ids,
            reference_metadata,
            index_id=index_id,
            city_id=city_id,
        )

        index = faiss.IndexFlatIP(int(matrix.shape[1]))
        index.add(matrix)
        metadata = _build_metadata(
            index_id=index_id,
            city_id=city_id,
            gallery_size=len(ids),
            descriptor_dim=int(matrix.shape[1]),
            retriever_metadata=retriever_metadata,
            extra_metadata=extra_metadata,
        )
        return cls(index, ids, references, metadata)

    @property
    def size(self) -> int:
        return int(self._index.ntotal)

    @property
    def descriptor_dim(self) -> int:
        return int(self._index.d)

    @property
    def is_ready(self) -> bool:
        return self.size > 0 and len(self.reference_ids) == self.size

    def _validate_loaded_state(self) -> None:
        if type(self._index).__name__ != "IndexFlatIP":
            raise FaissIndexError(f"unsupported FAISS type {type(self._index).__name__}; expected IndexFlatIP")
        if len(self.reference_ids) != self.size:
            raise FaissIndexError("FAISS ntotal does not match persisted ID mapping")
        if len(self.reference_metadata) != self.size:
            raise FaissIndexError("reference metadata count does not match persisted ID mapping")
        if len(set(self.reference_ids)) != len(self.reference_ids):
            raise FaissIndexError("persisted ID mapping contains duplicates")
        expected_dim = self.build_metadata.get("descriptor_dim")
        if expected_dim is not None and int(expected_dim) != self.descriptor_dim:
            raise FaissIndexError("persisted metadata descriptor dimension is inconsistent")
        expected_size = self.build_metadata.get("gallery_size")
        if expected_size is not None and int(expected_size) != self.size:
            raise FaissIndexError("persisted metadata gallery size is inconsistent")
        if self.build_metadata.get("faiss_index_type") != "IndexFlatIP":
            raise FaissIndexError("metadata does not declare IndexFlatIP")
        if self.build_metadata.get("normalization") != "L2":
            raise FaissIndexError("metadata does not declare L2 normalization")

    def search(self, queries: np.ndarray, *, k: int = 10) -> list[list[RetrievalResult]]:
        if k < 1:
            raise ValueError("k must be >= 1")
        if not self.is_ready:
            raise FaissIndexError("FAISS index is not ready")
        array = np.asarray(queries, dtype=np.float32)
        if array.ndim == 1:
            array = array.reshape(1, -1)
        if array.ndim != 2 or array.shape[1] != self.descriptor_dim:
            raise FaissIndexError(f"query shape must be (N, {self.descriptor_dim}), got {array.shape}")
        try:
            normalized = l2_normalize(array)
        except DescriptorError as exc:
            raise FaissIndexError(str(exc)) from exc
        result_k = min(k, self.size)
        scores, rows = self._index.search(normalized, result_k)
        batches: list[list[RetrievalResult]] = []
        for query_scores, query_rows in zip(scores, rows, strict=True):
            matches: list[RetrievalResult] = []
            for rank, (score, row) in enumerate(zip(query_scores, query_rows, strict=True), start=1):
                row_index = int(row)
                if row_index < 0:  # defensive; should not occur after k is clamped
                    continue
                matches.append(
                    RetrievalResult(
                        reference_id=self.reference_ids[row_index],
                        score=float(score),
                        rank=rank,
                        metadata=self.reference_metadata[row_index],
                    )
                )
            batches.append(matches)
        return batches

    def search_one(self, query: np.ndarray, *, k: int = 10) -> list[RetrievalResult]:
        return self.search(query, k=k)[0]

    def save(self, directory: str | Path) -> Path:
        faiss = _import_faiss()
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        generation = uuid.uuid4().hex
        final_paths = {filename: directory / filename for filename in (*_ARTIFACT_FILENAMES, "index_metadata.json")}
        temporary_paths = {filename: directory / f".{filename}.{generation}.tmp" for filename in final_paths}
        try:
            faiss.write_index(self._index, str(temporary_paths["index.faiss"]))
            with temporary_paths["index.faiss"].open("rb") as handle:
                os.fsync(handle.fileno())
            _write_json(
                temporary_paths["id_mapping.json"],
                [
                    {
                        "row": row,
                        "reference_id": reference_id,
                        "artifact_generation": generation,
                    }
                    for row, reference_id in enumerate(self.reference_ids)
                ],
            )
            _write_jsonl(
                temporary_paths["reference_metadata.jsonl"],
                [
                    {
                        "reference_id": reference_id,
                        "metadata": metadata,
                        "artifact_generation": generation,
                    }
                    for reference_id, metadata in zip(self.reference_ids, self.reference_metadata, strict=True)
                ],
            )
            artifact_hashes = {filename: _sha256_file(temporary_paths[filename]) for filename in _ARTIFACT_FILENAMES}
            committed_metadata = {
                key: value for key, value in self.build_metadata.items() if key not in _COMMIT_METADATA_KEYS
            }
            committed_metadata.update(
                {
                    "schema_version": INDEX_SCHEMA_VERSION,
                    "artifact_generation": generation,
                    "artifact_sha256": artifact_hashes,
                }
            )
            _write_json(temporary_paths["index_metadata.json"], committed_metadata)

            for filename in _ARTIFACT_FILENAMES:
                os.replace(temporary_paths[filename], final_paths[filename])
            _fsync_directory(directory)
            # The metadata rename is the commit point: readers either validate the
            # previous generation or the complete generation written above.
            os.replace(
                temporary_paths["index_metadata.json"],
                final_paths["index_metadata.json"],
            )
            _fsync_directory(directory)
        except Exception as exc:
            for temporary_path in temporary_paths.values():
                temporary_path.unlink(missing_ok=True)
            raise FaissIndexError(f"failed to save FAISS artifacts to {directory}: {exc}") from exc
        self.build_metadata = committed_metadata
        return directory

    @classmethod
    def load(cls, directory: str | Path) -> FaissExactIndex:
        directory = Path(directory)
        ids, references, metadata = _read_committed_sidecars(directory)
        faiss = _import_faiss()
        try:
            index = faiss.read_index(str(directory / "index.faiss"))
            expected_index_hash = metadata["artifact_sha256"]["index.faiss"]
            if _sha256_file(directory / "index.faiss") != expected_index_hash:
                raise FaissIndexError("SHA-256 mismatch for persisted artifact index.faiss")
        except FaissIndexError:
            raise
        except Exception as exc:
            raise FaissIndexError(f"failed to load FAISS artifacts from {directory}: {exc}") from exc
        return cls(index, ids, references, metadata)


class FaissExactIndexBuilder:
    """One-use, bounded-memory builder for an exact normalized FAISS index.

    This is deliberately separate from :class:`FaissExactIndex`: persisted and
    serving indexes remain immutable, while an evaluation worker can append
    small descriptor batches without materializing the complete gallery in the
    PyTorch process or in one IPC payload.
    """

    def __init__(
        self,
        *,
        descriptor_dim: int,
        expected_size: int,
        retriever_metadata: Mapping[str, Any] | None = None,
        index_id: str = "default",
        city_id: str | None = None,
        extra_metadata: Mapping[str, Any] | None = None,
    ) -> None:
        try:
            dimension = int(descriptor_dim)
            expected = int(expected_size)
        except (TypeError, ValueError) as exc:
            raise FaissIndexError("descriptor_dim and expected_size must be integers") from exc
        if dimension < 1:
            raise FaissIndexError("descriptor_dim must be positive")
        if expected < 1:
            raise FaissIndexError("expected_size must be positive")
        faiss = _import_faiss()
        self._index = faiss.IndexFlatIP(dimension)
        self._descriptor_dim = dimension
        self._expected_size = expected
        self._retriever_metadata = dict(retriever_metadata or {})
        self._index_id = index_id
        self._city_id = city_id
        self._extra_metadata = dict(extra_metadata or {})
        self._reference_ids: list[str] = []
        self._reference_metadata: list[dict[str, Any]] = []
        self._seen_reference_ids: set[str] = set()
        self._finished = False

    @property
    def size(self) -> int:
        return int(self._index.ntotal)

    def add_batch(
        self,
        descriptors: np.ndarray,
        reference_ids: Sequence[str],
        *,
        reference_metadata: Sequence[Mapping[str, Any]] | None = None,
    ) -> None:
        """Validate, normalize, and append one manifest-ordered descriptor batch."""

        if self._finished:
            raise FaissIndexError("cannot add descriptors after the FAISS build is finalized")
        ids, references = _prepare_reference_rows(
            reference_ids,
            reference_metadata,
            index_id=self._index_id,
            city_id=self._city_id,
        )
        duplicate_ids = self._seen_reference_ids.intersection(ids)
        if duplicate_ids:
            raise FaissIndexError(
                f"reference IDs must be unique across streamed batches: {sorted(duplicate_ids)[:10]}"
            )
        if len(self._reference_ids) + len(ids) > self._expected_size:
            raise FaissIndexError(
                "streamed descriptor count exceeds the declared expected_size"
            )
        try:
            matrix = l2_normalize(descriptors)
        except DescriptorError as exc:
            raise FaissIndexError(str(exc)) from exc
        if matrix.shape[0] != len(ids):
            raise FaissIndexError(f"descriptor rows ({matrix.shape[0]}) do not match IDs ({len(ids)})")
        if matrix.shape[1] != self._descriptor_dim:
            raise FaissIndexError(
                f"descriptor dimension ({matrix.shape[1]}) does not match builder dimension ({self._descriptor_dim})"
            )
        self._index.add(matrix)
        self._reference_ids.extend(ids)
        self._reference_metadata.extend(references)
        self._seen_reference_ids.update(ids)

    def finish(self) -> FaissExactIndex:
        """Freeze a complete build and return normal ``FaissExactIndex`` semantics."""

        if self._finished:
            raise FaissIndexError("FAISS build is already finalized")
        if self.size != self._expected_size:
            raise FaissIndexError(
                f"streamed descriptor count ({self.size}) does not match expected_size ({self._expected_size})"
            )
        metadata = _build_metadata(
            index_id=self._index_id,
            city_id=self._city_id,
            gallery_size=self.size,
            descriptor_dim=self._descriptor_dim,
            retriever_metadata=self._retriever_metadata,
            extra_metadata=self._extra_metadata,
        )
        index = FaissExactIndex(
            self._index,
            self._reference_ids,
            self._reference_metadata,
            metadata,
        )
        self._finished = True
        return index


def build_index_from_embedding_artifacts(
    embedding_dir: str | Path,
    output_dir: str | Path,
    *,
    index_id: str = "default",
    city_id: str | None = None,
) -> FaissExactIndex:
    descriptors, ids, references, embedding_metadata = load_embedding_artifacts(embedding_dir)
    metadata_by_id = {
        row["reference_id"]: dict(row.get("metadata", {})) | {"image_path": row.get("image_path")} for row in references
    }
    reference_metadata = [metadata_by_id[reference_id] for reference_id in ids]
    index = FaissExactIndex.build(
        descriptors,
        ids,
        reference_metadata=reference_metadata,
        retriever_metadata=embedding_metadata.get("retriever", {}),
        index_id=index_id,
        city_id=city_id,
        extra_metadata={"embedding_input_signature": embedding_metadata.get("input_signature")},
    )
    index.save(output_dir)
    return index


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a normalized exact FAISS index")
    parser.add_argument("--embeddings", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--index-id", default="default")
    parser.add_argument("--city-id")
    args = parser.parse_args()
    index = build_index_from_embedding_artifacts(
        args.embeddings,
        args.output_dir,
        index_id=args.index_id,
        city_id=args.city_id,
    )
    print(json.dumps(index.build_metadata, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
