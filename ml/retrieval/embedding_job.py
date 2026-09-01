"""Restart-safe offline reference embedding extraction."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import shutil
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

from .base import BaseRetriever
from .image_io import load_rgb_image
from .registry import create_retriever

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2
_COMMITTED_ARTIFACTS = (
    "descriptors.npy",
    "id_mapping.json",
    "reference_metadata.jsonl",
    "failures.jsonl",
)


class EmbeddingJobError(RuntimeError):
    """The embedding artifact set cannot be built safely."""


@dataclass(frozen=True, slots=True)
class ReferenceImage:
    """One stable manifest row used by the embedding job."""

    reference_id: str
    image_path: Path
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(
        cls,
        row: Mapping[str, Any],
        *,
        image_root: str | Path | None = None,
    ) -> ReferenceImage:
        raw_id = row.get("id", row.get("reference_id"))
        raw_path = row.get("image_path")
        if raw_id is None or not str(raw_id).strip():
            raise EmbeddingJobError("manifest row is missing a non-empty id/reference_id")
        if raw_path is None or not str(raw_path).strip():
            raise EmbeddingJobError(f"reference {raw_id!r} is missing image_path")
        path = Path(str(raw_path))
        if image_root is not None and not path.is_absolute():
            path = Path(image_root) / path
        metadata = {str(key): _json_safe(value) for key, value in row.items()}
        return cls(reference_id=str(raw_id), image_path=path, metadata=metadata)


@dataclass(frozen=True, slots=True)
class EmbeddingFailure:
    reference_id: str
    image_path: str
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class EmbeddingProgress:
    processed_inputs: int
    total_inputs: int
    successful_descriptors: int
    failures: int
    checkpoint_path: Path


@dataclass(frozen=True, slots=True)
class EmbeddingArtifacts:
    root: Path
    descriptors_path: Path
    id_mapping_path: Path
    reference_metadata_path: Path
    build_metadata_path: Path
    failures_path: Path
    descriptor_count: int
    failure_count: int


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
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    return str(value)


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_npy(path: Path, array: np.ndarray) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as handle:
        np.save(handle, array, allow_pickle=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


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


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_json_safe(row), ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _input_signature(
    records: Sequence[ReferenceImage],
    retriever: BaseRetriever,
    *,
    reuse_identity: Mapping[str, Any] | None = None,
) -> str:
    digest = hashlib.sha256()
    digest.update(f"embedding-schema:{SCHEMA_VERSION}\n".encode())
    retriever_metadata = retriever.metadata.to_dict()
    # A resumed job may move between devices; checkpoint, revision, and
    # preprocessing identity may not change underneath completed chunks.
    retriever_metadata.pop("device", None)
    digest.update(
        json.dumps(
            _json_safe(retriever_metadata), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    )
    digest.update(b"\n")
    if reuse_identity is not None:
        digest.update(b"reuse:")
        digest.update(
            json.dumps(_json_safe(reuse_identity), sort_keys=True, separators=(",", ":")).encode("utf-8")
        )
        digest.update(b"\n")
    for record in records:
        digest.update(record.reference_id.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(record.image_path).encode("utf-8"))
        digest.update(b"\0")
        digest.update(
            json.dumps(
                _json_safe(record.metadata), sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        )
        try:
            digest.update(b"\0sha256:")
            digest.update(_sha256_file(record.image_path).encode("ascii"))
        except OSError:
            digest.update(b"\0missing")
        digest.update(b"\n")
    return digest.hexdigest()


class EmbeddingJob:
    """Extract descriptors in batches and checkpoint every completed batch.

    Checkpoints are immutable NPZ chunks plus an atomically replaced state file.
    A restart validates the ordered manifest/model signature before continuing,
    preventing descriptor rows from being attached to the wrong reference IDs.
    """

    def __init__(
        self,
        retriever: BaseRetriever,
        output_dir: str | Path,
        *,
        batch_size: int = 32,
        progress_callback: Callable[[EmbeddingProgress], None] | None = None,
        reuse_from: str | Path | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self.retriever = retriever
        self.output_dir = Path(output_dir)
        self.batch_size = batch_size
        self.progress_callback = progress_callback
        self.reuse_from = None if reuse_from is None else Path(reuse_from)

    @property
    def _checkpoint_dir(self) -> Path:
        return self.output_dir / ".embedding-checkpoints"

    @property
    def _state_path(self) -> Path:
        return self._checkpoint_dir / "state.json"

    def _new_state(self, signature: str, total: int) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "input_signature": signature,
            "model_name": self.retriever.model_name,
            "descriptor_dim": self.retriever.descriptor_dim,
            "total_inputs": total,
            "next_input_index": 0,
            "chunks": [],
            "failures": [],
            "successful_count": 0,
            "reused_descriptor_count": 0,
            "computed_descriptor_count": 0,
        }

    def _load_reusable_descriptors(
        self,
        records: Sequence[ReferenceImage],
    ) -> tuple[np.ndarray | None, dict[str, int], dict[str, Any] | None]:
        if self.reuse_from is None:
            return None, {}, None
        descriptors, ids, references, metadata = load_embedding_artifacts(self.reuse_from)
        expected_model = dict(self.retriever.metadata.to_dict())
        actual_model = dict(metadata.get("retriever", {}))
        expected_model.pop("device", None)
        actual_model.pop("device", None)
        if actual_model != expected_model:
            raise EmbeddingJobError("reusable embedding artifact does not match the loaded retriever")

        base_rows = {reference_id: row for reference_id, row in zip(ids, references, strict=True)}
        reusable: dict[str, int] = {}
        row_by_id = {reference_id: row for row, reference_id in enumerate(ids)}
        for record in records:
            prior = base_rows.get(record.reference_id)
            if prior is None:
                continue
            if Path(str(prior.get("image_path", ""))).resolve() != record.image_path.resolve():
                continue
            prior_metadata = prior.get("metadata")
            if isinstance(prior_metadata, Mapping):
                for key in ("source", "source_image_id"):
                    before = prior_metadata.get(key)
                    after = record.metadata.get(key)
                    if before is not None and after is not None and str(before) != str(after):
                        break
                else:
                    reusable[record.reference_id] = row_by_id[record.reference_id]
            else:
                reusable[record.reference_id] = row_by_id[record.reference_id]
        identity = {
            "root": str(self.reuse_from.resolve()),
            "artifact_generation": metadata["artifact_generation"],
            "artifact_sha256": metadata["artifact_sha256"],
            "eligible_descriptor_count": len(reusable),
        }
        return descriptors, reusable, identity

    def _load_or_create_state(
        self,
        signature: str,
        total: int,
        *,
        resume: bool,
    ) -> dict[str, Any]:
        self._checkpoint_dir.mkdir(parents=True, exist_ok=True)
        if self._state_path.exists() and resume:
            with self._state_path.open("r", encoding="utf-8") as handle:
                state = json.load(handle)
            if state.get("input_signature") != signature:
                raise EmbeddingJobError(
                    "existing embedding checkpoint belongs to a different ordered manifest/model; "
                    "use a new output directory or run with resume=False"
                )
            if state.get("descriptor_dim") != self.retriever.descriptor_dim:
                raise EmbeddingJobError("checkpoint descriptor dimension does not match retriever")
            for name in state.get("chunks", []):
                if not (self._checkpoint_dir / name).is_file():
                    raise EmbeddingJobError(f"checkpoint references missing chunk {name!r}")
            if "successful_count" not in state:
                count = 0
                for name in state.get("chunks", []):
                    with np.load(self._checkpoint_dir / name, allow_pickle=False) as chunk:
                        count += int(chunk["descriptors"].shape[0])
                state["successful_count"] = count
            return state
        if self._checkpoint_dir.exists() and not resume:
            shutil.rmtree(self._checkpoint_dir)
            self._checkpoint_dir.mkdir(parents=True, exist_ok=True)
        state = self._new_state(signature, total)
        _atomic_json(self._state_path, state)
        return state

    def _embed_new_isolating_failures(
        self,
        batch: Sequence[ReferenceImage],
    ) -> tuple[list[str], np.ndarray, list[EmbeddingFailure]]:
        valid_records: list[ReferenceImage] = []
        decoded_images: list[Any] = []
        failures: list[EmbeddingFailure] = []
        for record in batch:
            try:
                decoded_images.append(load_rgb_image(record.image_path))
                valid_records.append(record)
            except Exception as exc:  # one corrupt/missing image must not stop the gallery
                failures.append(
                    EmbeddingFailure(
                        reference_id=record.reference_id,
                        image_path=str(record.image_path),
                        error_type=type(exc).__name__,
                        message=str(exc)[:1000],
                    )
                )

        if not valid_records:
            return [], np.empty((0, self.retriever.descriptor_dim), dtype=np.float32), failures

        try:
            matrix = self.retriever.embed_batch(decoded_images)
            matrix = self.retriever.validate_descriptors(
                matrix, expected_rows=len(valid_records), normalize=True
            )
            return [record.reference_id for record in valid_records], matrix, failures
        except Exception as batch_exc:
            logger.warning("batch embedding failed; isolating images one by one: %s", batch_exc)

        ids: list[str] = []
        rows: list[np.ndarray] = []
        for record, image in zip(valid_records, decoded_images, strict=True):
            try:
                descriptor = self.retriever.embed_query(image)
                ids.append(record.reference_id)
                rows.append(descriptor)
            except Exception as exc:
                failures.append(
                    EmbeddingFailure(
                        reference_id=record.reference_id,
                        image_path=str(record.image_path),
                        error_type=type(exc).__name__,
                        message=str(exc)[:1000],
                    )
                )
        matrix = (
            np.stack(rows).astype(np.float32, copy=False)
            if rows
            else np.empty((0, self.retriever.descriptor_dim), dtype=np.float32)
        )
        return ids, matrix, failures

    def _embed_isolating_failures(
        self,
        batch: Sequence[ReferenceImage],
        *,
        reusable_descriptors: np.ndarray | None = None,
        reusable_rows: Mapping[str, int] | None = None,
    ) -> tuple[list[str], np.ndarray, list[EmbeddingFailure], int]:
        reusable_rows = reusable_rows or {}
        missing = [record for record in batch if record.reference_id not in reusable_rows]
        new_ids, new_matrix, failures = self._embed_new_isolating_failures(missing)
        new_by_id = {reference_id: new_matrix[row] for row, reference_id in enumerate(new_ids)}
        ids: list[str] = []
        rows: list[np.ndarray] = []
        reused = 0
        for record in batch:
            reuse_row = reusable_rows.get(record.reference_id)
            if reuse_row is not None:
                if reusable_descriptors is None:
                    raise EmbeddingJobError("reusable descriptor mapping has no descriptor matrix")
                ids.append(record.reference_id)
                rows.append(np.asarray(reusable_descriptors[reuse_row], dtype=np.float32))
                reused += 1
            elif record.reference_id in new_by_id:
                ids.append(record.reference_id)
                rows.append(new_by_id[record.reference_id])
        matrix = (
            np.stack(rows).astype(np.float32, copy=False)
            if rows
            else np.empty((0, self.retriever.descriptor_dim), dtype=np.float32)
        )
        return ids, matrix, failures, reused

    def _completed_artifacts_for_signature(self, signature: str) -> EmbeddingArtifacts | None:
        """Reuse a committed generation after its resumability chunks are pruned.

        A completed artifact remains the authoritative result for the exact
        ordered manifest/model signature.  We still validate it through the
        normal artifact loader instead of treating the commit metadata as a
        blind cache hit.
        """

        metadata_path = self.output_dir / "build_metadata.json"
        if not metadata_path.is_file():
            return None
        try:
            raw_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise EmbeddingJobError("embedding build metadata is missing or invalid") from exc
        if not isinstance(raw_metadata, dict) or raw_metadata.get("status") != "complete":
            return None
        if raw_metadata.get("input_signature") != signature:
            raise EmbeddingJobError(
                "existing embedding artifact belongs to a different ordered manifest/model; "
                "use a new output directory or run with resume=False"
            )

        _, ids, _, metadata = load_embedding_artifacts(self.output_dir)
        return EmbeddingArtifacts(
            root=self.output_dir,
            descriptors_path=self.output_dir / "descriptors.npy",
            id_mapping_path=self.output_dir / "id_mapping.json",
            reference_metadata_path=self.output_dir / "reference_metadata.jsonl",
            build_metadata_path=metadata_path,
            failures_path=self.output_dir / "failures.jsonl",
            descriptor_count=len(ids),
            failure_count=int(metadata["failure_count"]),
        )

    def _save_chunk(self, number: int, ids: Sequence[str], descriptors: np.ndarray) -> str:
        name = f"chunk-{number:08d}.npz"
        path = self._checkpoint_dir / name
        temporary = path.with_name(path.name + ".tmp")
        with temporary.open("wb") as handle:
            np.savez_compressed(
                handle,
                reference_ids=np.asarray(ids, dtype=np.str_),
                descriptors=np.asarray(descriptors, dtype=np.float32),
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        return name

    def _finalize(
        self,
        records: Sequence[ReferenceImage],
        signature: str,
        state: Mapping[str, Any],
    ) -> EmbeddingArtifacts:
        ids: list[str] = []
        for name in state["chunks"]:
            with np.load(self._checkpoint_dir / name, allow_pickle=False) as chunk:
                chunk_ids = [str(value) for value in chunk["reference_ids"].tolist()]
                matrix = np.asarray(chunk["descriptors"], dtype=np.float32)
                if matrix.shape != (len(chunk_ids), self.retriever.descriptor_dim):
                    raise EmbeddingJobError(f"invalid descriptor shape in checkpoint chunk {name}")
            ids.extend(chunk_ids)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        failures_path = self.output_dir / "failures.jsonl"
        failures = [EmbeddingFailure(**item) for item in state.get("failures", [])]
        if not ids:
            generation = uuid.uuid4().hex
            _write_jsonl(failures_path, (asdict(failure) for failure in failures))
            _atomic_json(
                self.output_dir / "build_metadata.json",
                {
                    "schema_version": SCHEMA_VERSION,
                    "built_at": datetime.now(UTC).isoformat(),
                    "artifact_generation": generation,
                    "input_signature": signature,
                    "input_count": len(records),
                    "descriptor_count": 0,
                    "failure_count": len(failures),
                    "status": "failed_no_descriptors",
                    "retriever": self.retriever.metadata.to_dict(),
                },
            )
            _fsync_directory(self.output_dir)
            raise EmbeddingJobError(
                "embedding job produced zero descriptors; inspect failures.jsonl/checkpoint state"
            )
        if len(ids) != len(set(ids)):
            raise EmbeddingJobError("checkpoint contains duplicate reference IDs")

        descriptors_path = self.output_dir / "descriptors.npy"
        id_mapping_path = self.output_dir / "id_mapping.json"
        references_path = self.output_dir / "reference_metadata.jsonl"
        metadata_path = self.output_dir / "build_metadata.json"
        generation = uuid.uuid4().hex
        final_paths = {
            "descriptors.npy": descriptors_path,
            "id_mapping.json": id_mapping_path,
            "reference_metadata.jsonl": references_path,
            "failures.jsonl": failures_path,
            "build_metadata.json": metadata_path,
        }
        temporary_paths = {
            name: self.output_dir / f".{name}.{generation}.tmp"
            for name in final_paths
        }
        by_id = {record.reference_id: record for record in records}
        try:
            descriptors = np.lib.format.open_memmap(
                temporary_paths["descriptors.npy"],
                mode="w+",
                dtype=np.float32,
                shape=(len(ids), self.retriever.descriptor_dim),
            )
            offset = 0
            for name in state["chunks"]:
                with np.load(self._checkpoint_dir / name, allow_pickle=False) as chunk:
                    chunk_ids = [str(value) for value in chunk["reference_ids"].tolist()]
                    matrix = self.retriever.validate_descriptors(
                        np.asarray(chunk["descriptors"], dtype=np.float32),
                        expected_rows=len(chunk_ids),
                        normalize=True,
                    )
                descriptors[offset : offset + len(chunk_ids)] = matrix
                offset += len(chunk_ids)
            if offset != len(ids):
                raise EmbeddingJobError("checkpoint descriptor count changed during finalization")
            descriptors.flush()
            del descriptors
            with temporary_paths["descriptors.npy"].open("rb") as handle:
                os.fsync(handle.fileno())

            _atomic_json(
                temporary_paths["id_mapping.json"],
                [
                    {
                        "row": row,
                        "reference_id": reference_id,
                        "artifact_generation": generation,
                    }
                    for row, reference_id in enumerate(ids)
                ],
            )
            _write_jsonl(
                temporary_paths["reference_metadata.jsonl"],
                (
                    {
                        "reference_id": reference_id,
                        "image_path": str(by_id[reference_id].image_path),
                        "metadata": by_id[reference_id].metadata,
                        "artifact_generation": generation,
                    }
                    for reference_id in ids
                ),
            )
            _write_jsonl(
                temporary_paths["failures.jsonl"],
                (
                    asdict(failure) | {"artifact_generation": generation}
                    for failure in failures
                ),
            )
            artifact_hashes = {
                name: _sha256_file(temporary_paths[name])
                for name in _COMMITTED_ARTIFACTS
            }
            committed_metadata = {
                "schema_version": SCHEMA_VERSION,
                "status": "complete",
                "built_at": datetime.now(UTC).isoformat(),
                "artifact_generation": generation,
                "artifact_sha256": artifact_hashes,
                "input_signature": signature,
                "input_count": len(records),
                "descriptor_count": len(ids),
                "descriptor_dim": self.retriever.descriptor_dim,
                "failure_count": len(failures),
                "reused_descriptor_count": int(state.get("reused_descriptor_count", 0)),
                "computed_descriptor_count": int(state.get("computed_descriptor_count", len(ids))),
                "reuse_provenance": state.get("reuse_provenance"),
                "normalized": True,
                "dtype": "float32",
                "retriever": self.retriever.metadata.to_dict(),
            }
            _atomic_json(temporary_paths["build_metadata.json"], committed_metadata)

            for name in _COMMITTED_ARTIFACTS:
                os.replace(temporary_paths[name], final_paths[name])
            _fsync_directory(self.output_dir)
            # Metadata is the commit point. Readers reject any mixed generation
            # or hash instead of pairing stale vectors with new IDs.
            os.replace(
                temporary_paths["build_metadata.json"],
                final_paths["build_metadata.json"],
            )
            _fsync_directory(self.output_dir)
        except Exception as exc:
            for path in temporary_paths.values():
                path.unlink(missing_ok=True)
            if isinstance(exc, EmbeddingJobError):
                raise
            raise EmbeddingJobError(f"failed to commit embedding artifacts: {type(exc).__name__}") from exc

        # The metadata commit above makes the checkpoint chunks redundant.
        # Retaining them would roughly double descriptor storage for a completed
        # gallery, while failed or interrupted jobs keep them for safe resume.
        # Cleanup is deliberately best-effort: a valid committed generation must
        # remain usable even if the filesystem refuses this space reclamation.
        try:
            shutil.rmtree(self._checkpoint_dir)
            _fsync_directory(self.output_dir)
        except OSError as exc:
            logger.warning("unable to remove completed embedding checkpoints: %s", exc)
        return EmbeddingArtifacts(
            root=self.output_dir,
            descriptors_path=descriptors_path,
            id_mapping_path=id_mapping_path,
            reference_metadata_path=references_path,
            build_metadata_path=metadata_path,
            failures_path=failures_path,
            descriptor_count=len(ids),
            failure_count=len(failures),
        )

    def run(
        self,
        records: Sequence[ReferenceImage],
        *,
        resume: bool = True,
    ) -> EmbeddingArtifacts:
        records = list(records)
        if not records:
            raise EmbeddingJobError("cannot embed an empty reference manifest")
        reference_ids = [record.reference_id for record in records]
        if len(reference_ids) != len(set(reference_ids)):
            raise EmbeddingJobError("reference manifest contains duplicate stable IDs")

        self.retriever.load()  # exactly once; model implementations are idempotent
        reusable_descriptors, reusable_rows, reuse_identity = self._load_reusable_descriptors(records)
        signature = _input_signature(records, self.retriever, reuse_identity=reuse_identity)
        if resume:
            completed = self._completed_artifacts_for_signature(signature)
            if completed is not None:
                return completed
        state = self._load_or_create_state(signature, len(records), resume=resume)
        next_index = int(state["next_input_index"])
        if not 0 <= next_index <= len(records):
            raise EmbeddingJobError("checkpoint next_input_index is outside the manifest")

        for start in range(next_index, len(records), self.batch_size):
            batch = records[start : start + self.batch_size]
            ids, descriptors, failures, reused = self._embed_isolating_failures(
                batch,
                reusable_descriptors=reusable_descriptors,
                reusable_rows=reusable_rows,
            )
            chunk_name = self._save_chunk(len(state["chunks"]), ids, descriptors)
            state["chunks"].append(chunk_name)
            state["failures"].extend(asdict(failure) for failure in failures)
            state["successful_count"] = int(state.get("successful_count", 0)) + len(ids)
            state["reused_descriptor_count"] = int(state.get("reused_descriptor_count", 0)) + reused
            state["computed_descriptor_count"] = int(state.get("computed_descriptor_count", 0)) + len(ids) - reused
            state["reuse_provenance"] = reuse_identity
            state["next_input_index"] = start + len(batch)
            _atomic_json(self._state_path, state)
            progress = EmbeddingProgress(
                processed_inputs=int(state["next_input_index"]),
                total_inputs=len(records),
                successful_descriptors=int(state["successful_count"]),
                failures=len(state["failures"]),
                checkpoint_path=self._state_path,
            )
            logger.info(
                "reference embedding checkpoint",
                extra={
                    "processed": progress.processed_inputs,
                    "total": progress.total_inputs,
                    "successful": progress.successful_descriptors,
                    "failures": progress.failures,
                },
            )
            if self.progress_callback is not None:
                self.progress_callback(progress)

        return self._finalize(records, signature, state)


def load_embedding_artifacts(root: str | Path) -> tuple[np.ndarray, list[str], list[dict[str, Any]], dict[str, Any]]:
    """Load and cross-check finalized embedding artifacts."""

    root = Path(root)
    metadata_path = root / "build_metadata.json"
    try:
        with metadata_path.open("r", encoding="utf-8") as handle:
            metadata = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise EmbeddingJobError("embedding build metadata is missing or invalid") from exc
    if not isinstance(metadata, dict):
        raise EmbeddingJobError("embedding build metadata must be a JSON object")
    if metadata.get("schema_version") != SCHEMA_VERSION:
        raise EmbeddingJobError(
            f"unsupported embedding schema version {metadata.get('schema_version')!r}; expected {SCHEMA_VERSION}"
        )
    if metadata.get("status") != "complete":
        raise EmbeddingJobError(
            f"embedding artifacts are not complete: {metadata.get('status', 'unknown')}"
        )
    generation = metadata.get("artifact_generation")
    if not isinstance(generation, str) or not generation:
        raise EmbeddingJobError("embedding metadata has no artifact generation")
    expected_hashes = metadata.get("artifact_sha256")
    if not isinstance(expected_hashes, dict) or set(expected_hashes) != set(_COMMITTED_ARTIFACTS):
        raise EmbeddingJobError("embedding metadata must hash every committed artifact")
    paths = {name: root / name for name in _COMMITTED_ARTIFACTS}
    missing = [name for name, path in paths.items() if not path.is_file()]
    if missing:
        raise EmbeddingJobError(f"incomplete embedding artifact directory; missing {missing}")
    for name, path in paths.items():
        if expected_hashes.get(name) != _sha256_file(path):
            raise EmbeddingJobError(f"SHA-256 mismatch for embedding artifact {name}")

    descriptors = np.load(paths["descriptors.npy"], allow_pickle=False, mmap_mode="r")
    with paths["id_mapping.json"].open("r", encoding="utf-8") as handle:
        mapping = json.load(handle)
    if not isinstance(mapping, list) or not all(isinstance(item, dict) for item in mapping):
        raise EmbeddingJobError("id_mapping must be a JSON list of objects")
    ids = [str(item["reference_id"]) for item in mapping]
    if [item["row"] for item in mapping] != list(range(len(mapping))):
        raise EmbeddingJobError("id_mapping rows must be contiguous and ordered from zero")
    if any(item.get("artifact_generation") != generation for item in mapping):
        raise EmbeddingJobError("id_mapping generation does not match embedding metadata")
    expected_shape = (len(ids), int(metadata.get("descriptor_dim", -1)))
    if descriptors.ndim != 2 or descriptors.shape != expected_shape:
        raise EmbeddingJobError("descriptor row count does not match id_mapping")
    if descriptors.dtype != np.float32 or metadata.get("dtype") != "float32":
        raise EmbeddingJobError("descriptors must use float32 storage")
    if int(metadata.get("descriptor_count", -1)) != len(ids):
        raise EmbeddingJobError("embedding metadata descriptor count does not match id_mapping")
    if metadata.get("normalized") is not True:
        raise EmbeddingJobError("embedding metadata does not declare normalized descriptors")
    for start in range(0, len(ids), 4096):
        block = np.asarray(descriptors[start : start + 4096])
        if not np.isfinite(block).all():
            raise EmbeddingJobError("descriptors contain NaN or infinite values")
        if not np.allclose(np.linalg.norm(block, axis=1), 1.0, atol=1e-4):
            raise EmbeddingJobError("descriptors are not L2-normalized")
    references: list[dict[str, Any]] = []
    with paths["reference_metadata.jsonl"].open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                references.append(json.loads(line))
    if [row["reference_id"] for row in references] != ids:
        raise EmbeddingJobError("reference metadata order does not match id_mapping")
    if any(row.get("artifact_generation") != generation for row in references):
        raise EmbeddingJobError("reference metadata generation does not match embedding metadata")
    return np.asarray(descriptors, dtype=np.float32), ids, references, metadata


def _read_manifest(path: Path, image_root: Path) -> list[ReferenceImage]:
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        try:
            from ml.ingestion.schema import read_manifest
        except ImportError as exc:
            raise EmbeddingJobError("pandas/pyarrow are required to read a Parquet manifest") from exc
        try:
            rows = read_manifest(
                path,
                allow_empty=False,
                strict_reference=True,
            ).to_dict(orient="records")
        except ValueError as exc:
            raise EmbeddingJobError(f"invalid final reference manifest: {exc}") from exc
    elif suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    elif suffix == ".jsonl":
        with path.open("r", encoding="utf-8") as handle:
            rows = [json.loads(line) for line in handle if line.strip()]
    elif suffix == ".json":
        with path.open("r", encoding="utf-8") as handle:
            rows = json.load(handle)
    else:
        raise EmbeddingJobError("manifest must be Parquet, CSV, JSON, or JSONL")
    return [ReferenceImage.from_mapping(row, image_root=image_root) for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build restart-safe reference descriptors")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--image-root", type=Path, default=Path("."))
    parser.add_argument("--retriever", choices=["megaloc", "dinov2-salad"], default="megaloc")
    parser.add_argument("--device", choices=["auto", "cuda", "mps", "cpu"], default="auto")
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--model-cache", type=Path)
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument(
        "--reuse-from",
        type=Path,
        help="reuse same-model descriptors when stable ID, image path, and source identity match",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    retriever = create_retriever(
        args.retriever,
        device=args.device,
        batch_size=args.batch_size,
        cache_dir=args.model_cache,
    )
    records = _read_manifest(args.manifest, args.image_root)
    artifacts = EmbeddingJob(
        retriever,
        args.output_dir,
        batch_size=args.batch_size,
        reuse_from=args.reuse_from,
    ).run(records, resume=not args.no_resume)
    print(json.dumps(asdict(artifacts), default=str, indent=2))


if __name__ == "__main__":
    main()
