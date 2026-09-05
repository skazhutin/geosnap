"""Process-isolated FAISS search for runtimes with incompatible OpenMP wheels."""

from __future__ import annotations

import multiprocessing as mp
import threading
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, NoReturn

import numpy as np

from .faiss_index import (
    FaissExactIndex,
    FaissIndexError,
    RetrievalResult,
    _read_committed_sidecars,
)


def _worker_main(directory: str, connection: Any) -> None:
    """Child entrypoint: imports/loads FAISS once and serves exact searches."""

    try:
        index = FaissExactIndex.load(directory)
        connection.send(
            {
                "kind": "ready",
                "size": index.size,
                "descriptor_dim": index.descriptor_dim,
                "artifact_generation": index.build_metadata.get("artifact_generation"),
            }
        )
    except BaseException as exc:  # report startup failure before the child exits
        connection.send(
            {
                "kind": "error",
                "error_type": type(exc).__name__,
                "message": str(exc),
            }
        )
        connection.close()
        return
    try:
        while True:
            request = connection.recv()
            if request.get("operation") == "close":
                connection.send({"kind": "closed"})
                return
            request_id = request.get("request_id")
            try:
                if request.get("operation") != "search":
                    raise ValueError("unknown worker operation")
                batches = index.search(request["queries"], k=int(request["k"]))
                connection.send(
                    {
                        "kind": "result",
                        "request_id": request_id,
                        "batches": [[item.to_dict() for item in batch] for batch in batches],
                    }
                )
            except BaseException as exc:
                connection.send(
                    {
                        "kind": "error",
                        "request_id": request_id,
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                    }
                )
    except (EOFError, BrokenPipeError, KeyboardInterrupt):
        return
    finally:
        connection.close()


def _read_sidecars(directory: Path) -> tuple[list[str], list[dict[str, Any]], dict[str, Any]]:
    return _read_committed_sidecars(directory)


class FaissIndexWorker:
    """Expose ``FaissExactIndex`` semantics while FAISS lives in one child process."""

    def __init__(self, directory: str | Path, *, timeout_seconds: float = 60.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.directory = Path(directory)
        self.timeout_seconds = timeout_seconds
        ids, references, metadata = _read_sidecars(self.directory)
        self.reference_ids = tuple(ids)
        self.reference_metadata = tuple(references)
        self.build_metadata = metadata
        self._context = mp.get_context("spawn")
        self._parent_connection: Any | None = None
        self._process: Any | None = None
        self._lock = threading.Lock()
        self._ready = False

    @property
    def size(self) -> int:
        return len(self.reference_ids)

    @property
    def descriptor_dim(self) -> int:
        return int(self.build_metadata["descriptor_dim"])

    @property
    def is_ready(self) -> bool:
        return bool(self._ready and self._process is not None and self._process.is_alive())

    def start(self) -> FaissIndexWorker:
        if self.is_ready:
            return self
        ids, references, metadata = _read_sidecars(self.directory)
        self.reference_ids = tuple(ids)
        self.reference_metadata = tuple(references)
        self.build_metadata = metadata
        parent, child = self._context.Pipe(duplex=True)
        process = self._context.Process(
            target=_worker_main,
            args=(str(self.directory), child),
            name="geosnap-faiss-index",
            daemon=True,
        )
        process.start()
        child.close()
        self._parent_connection = parent
        self._process = process
        if not parent.poll(self.timeout_seconds):
            self.close(force=True)
            raise FaissIndexError("timed out while starting the FAISS index worker")
        try:
            response = parent.recv()
        except (BrokenPipeError, EOFError, OSError) as exc:
            self.close(force=True)
            raise FaissIndexError("FAISS worker closed during startup") from exc
        if not isinstance(response, Mapping):
            self.close(force=True)
            raise FaissIndexError("FAISS worker returned a malformed startup response")
        if response.get("kind") != "ready":
            self.close(force=True)
            raise FaissIndexError(
                f"FAISS worker failed to start: {response.get('error_type')}: {response.get('message')}"
            )
        try:
            worker_size = int(response["size"])
            worker_dimension = int(response["descriptor_dim"])
        except (KeyError, TypeError, ValueError) as exc:
            self.close(force=True)
            raise FaissIndexError("FAISS worker returned a malformed startup response") from exc
        if worker_size != self.size or worker_dimension != self.descriptor_dim:
            self.close(force=True)
            raise FaissIndexError("FAISS worker index disagrees with persisted sidecar metadata")
        if response.get("artifact_generation") != self.build_metadata["artifact_generation"]:
            self.close(force=True)
            raise FaissIndexError("FAISS worker loaded a different artifact generation")
        self._ready = True
        return self

    def _restart_and_raise(self, message: str, *, cause: BaseException | None = None) -> NoReturn:
        self.close(force=True)
        try:
            self.start()
        except Exception as restart_error:
            raise FaissIndexError(f"{message}; worker restart failed: {restart_error}") from restart_error
        error = FaissIndexError(f"{message}; worker was restarted")
        if cause is not None:
            raise error from cause
        raise error

    def search(self, queries: np.ndarray, *, k: int = 10) -> list[list[RetrievalResult]]:
        request_id = uuid.uuid4().hex
        with self._lock:
            if not self.is_ready or self._parent_connection is None:
                raise FaissIndexError("FAISS index worker is not ready")
            connection = self._parent_connection
            try:
                connection.send(
                    {
                        "operation": "search",
                        "request_id": request_id,
                        "queries": np.asarray(queries, dtype=np.float32),
                        "k": int(k),
                    }
                )
                if not connection.poll(self.timeout_seconds):
                    self._restart_and_raise("FAISS search worker timed out")
                response = connection.recv()
            except (BrokenPipeError, EOFError, OSError) as exc:
                self._restart_and_raise("FAISS search worker transport failed", cause=exc)
            if not isinstance(response, Mapping):
                self._restart_and_raise("FAISS search worker protocol mismatch")
            if response.get("request_id") != request_id:
                self._restart_and_raise("FAISS search worker protocol mismatch")
            if response.get("kind") == "error":
                if not isinstance(response.get("error_type"), str) or not isinstance(response.get("message"), str):
                    self._restart_and_raise("FAISS search worker protocol mismatch")
                raise FaissIndexError(
                    f"FAISS worker search failed: {response.get('error_type')}: {response.get('message')}"
                )
            if response.get("kind") != "result":
                self._restart_and_raise("FAISS search worker protocol mismatch")
            try:
                return [[RetrievalResult(**item) for item in batch] for batch in response["batches"]]
            except Exception as exc:
                self._restart_and_raise("FAISS search worker protocol mismatch", cause=exc)

    def search_one(self, query: np.ndarray, *, k: int = 10) -> list[RetrievalResult]:
        return self.search(query, k=k)[0]

    def close(self, *, force: bool = False) -> None:
        process = self._process
        connection = self._parent_connection
        self._ready = False
        if process is not None and process.is_alive() and connection is not None and not force:
            try:
                connection.send({"operation": "close"})
                if connection.poll(min(self.timeout_seconds, 5.0)):
                    connection.recv()
                process.join(timeout=5.0)
            except (BrokenPipeError, EOFError, OSError):
                pass
        if process is not None and process.is_alive():
            process.terminate()
            process.join(timeout=5.0)
        if connection is not None:
            try:
                connection.close()
            except OSError:
                pass
        self._process = None
        self._parent_connection = None

    def __enter__(self) -> FaissIndexWorker:
        return self.start()

    def __exit__(self, *args: Any) -> None:
        self.close()
