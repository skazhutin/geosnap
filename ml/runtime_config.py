"""Fail-closed loader for the single configuration shared by benchmark and service."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class RuntimeConfigError(RuntimeError):
    """A frozen runtime configuration is missing, inconsistent, or stale."""


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class FrozenRuntimeConfig:
    path: Path
    payload: dict[str, Any]
    sha256: str

    @classmethod
    def load(cls, path: str | Path, *, verify_index: bool = False) -> FrozenRuntimeConfig:
        resolved = Path(path).expanduser().resolve()
        try:
            loaded = json.loads(resolved.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeConfigError(f"cannot read frozen runtime config: {resolved}") from exc
        if not isinstance(loaded, dict) or loaded.get("schema_version") != 1:
            raise RuntimeConfigError("runtime config must be a schema-version-1 JSON object")
        if loaded.get("status") not in {
            "frozen_before_final_test",
            "production_selection_from_prefrozen_finalists",
        }:
            raise RuntimeConfigError("runtime config does not have a supported frozen status")
        actual_sha256 = sha256_file(resolved)
        if loaded.get("status") == "production_selection_from_prefrozen_finalists":
            hash_path = resolved.with_suffix(".sha256")
            try:
                declared_sha256 = hash_path.read_text(encoding="ascii").strip().split()[0]
            except (OSError, IndexError) as exc:
                raise RuntimeConfigError(
                    f"production runtime config hash file is missing or malformed: {hash_path}"
                ) from exc
            if declared_sha256 != actual_sha256:
                raise RuntimeConfigError("production runtime config SHA-256 does not match")
        try:
            top_k = int(loaded["retrieval"]["top_k"])
            threshold = float(loaded["localization"]["confidence_threshold"])
            retriever = str(loaded["retriever"]["name"])
            estimator = str(loaded["localization"]["estimator"])
            Path(str(loaded["index"]["directory"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeConfigError("runtime config is missing a required field") from exc
        if top_k < 5 or not 0.0 <= threshold <= 1.0 or not retriever or not estimator:
            raise RuntimeConfigError("runtime config values are outside their supported ranges")
        instance = cls(resolved, loaded, actual_sha256)
        instance.verify_confidence_model()
        if verify_index:
            instance.verify_gallery()
            instance.verify_index()
        return instance

    @property
    def retriever(self) -> str:
        return str(self.payload["retriever"]["name"])

    @property
    def top_k(self) -> int:
        return int(self.payload["retrieval"]["top_k"])

    @property
    def query_aggregation(self) -> str:
        return str(self.payload["retrieval"].get("query_aggregation", "single"))

    @property
    def estimator(self) -> str:
        return str(self.payload["localization"]["estimator"])

    @property
    def confidence_threshold(self) -> float:
        return float(self.payload["localization"]["confidence_threshold"])

    @property
    def index_dir(self) -> Path:
        path = Path(str(self.payload["index"]["directory"]))
        return self._runtime_artifact_path(path)

    @property
    def confidence_model_path(self) -> Path | None:
        value = self.payload.get("confidence", {}).get("artifact")
        if not value:
            return None
        path = Path(str(value))
        return path if path.is_absolute() else (self.path.parent.parent / path).resolve()

    @property
    def gallery_manifest_path(self) -> Path:
        value = self.payload.get("dataset", {}).get("gallery_manifest")
        if not value:
            raise RuntimeConfigError("frozen runtime config does not bind a gallery manifest")
        path = Path(str(value))
        return self._runtime_artifact_path(path)

    def _runtime_artifact_path(self, path: Path) -> Path:
        """Map frozen ``data/...`` paths into an operational artifact volume.

        The mapping changes storage only. Hashes and all frozen ML identities
        remain authoritative and are still checked before use.
        """

        if path.is_absolute():
            return path
        artifact_root = os.environ.get("GEOSNAP_ARTIFACT_DIR")
        if artifact_root and self.payload.get("status") == "production_selection_from_prefrozen_finalists":
            parts = path.parts[1:] if path.parts and path.parts[0] == "data" else path.parts
            return (Path(artifact_root).expanduser().resolve() / Path(*parts)).resolve()
        return (self.path.parent.parent / path).resolve()

    @property
    def city_id(self) -> str:
        return str(self.payload["index"]["city_id"])

    @property
    def index_id(self) -> str:
        return str(self.payload["index"]["index_id"])

    def verify_index(self) -> None:
        metadata = self.index_dir / "index_metadata.json"
        if not metadata.is_file():
            raise RuntimeConfigError(f"frozen index metadata is missing: {metadata}")
        expected = str(self.payload["index"].get("index_metadata_sha256", ""))
        if not expected or sha256_file(metadata) != expected:
            raise RuntimeConfigError("frozen index metadata SHA-256 does not match")

    def verify_gallery(self) -> None:
        path = self.gallery_manifest_path
        if not path.is_file():
            raise RuntimeConfigError(f"frozen gallery manifest is missing: {path}")
        expected = str(self.payload.get("dataset", {}).get("gallery_sha256", ""))
        if not expected or sha256_file(path) != expected:
            raise RuntimeConfigError("frozen gallery manifest SHA-256 does not match")

    def verify_confidence_model(self) -> None:
        path = self.confidence_model_path
        if path is None:
            return
        if not path.is_file():
            raise RuntimeConfigError(f"frozen confidence model is missing: {path}")
        expected = str(self.payload.get("confidence", {}).get("artifact_sha256", ""))
        if not expected or sha256_file(path) != expected:
            raise RuntimeConfigError("frozen confidence model SHA-256 does not match")

    def assert_benchmark_contract(
        self,
        *,
        retriever: str,
        top_k: int,
        estimator: str,
        confidence_threshold: float,
        query_aggregation: str,
        gallery_manifest: str | Path,
        localization: Mapping[str, Any] | None = None,
    ) -> None:
        actual = {
            "retriever": retriever,
            "top_k": int(top_k),
            "estimator": estimator,
            "confidence_threshold": float(confidence_threshold),
            "query_aggregation": query_aggregation,
            "gallery_sha256": sha256_file(gallery_manifest),
        }
        expected = {
            "retriever": self.retriever,
            "top_k": self.top_k,
            "estimator": self.estimator,
            "confidence_threshold": self.confidence_threshold,
            "query_aggregation": self.query_aggregation,
            "gallery_sha256": str(self.payload["dataset"]["gallery_sha256"]),
        }
        if actual != expected:
            raise RuntimeConfigError(f"benchmark/runtime configuration mismatch: {actual} != {expected}")
        if localization is not None and dict(localization) != dict(self.payload["localization"]):
            raise RuntimeConfigError(
                "benchmark/runtime localization configuration mismatch: "
                f"{dict(localization)} != {self.payload['localization']}"
            )
