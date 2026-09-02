from __future__ import annotations

import json
from pathlib import Path

import pytest

from ml.runtime_config import FrozenRuntimeConfig, RuntimeConfigError, sha256_file


def _config(tmp_path: Path, *, top_k: int = 20) -> tuple[Path, Path]:
    index = tmp_path / "index"
    index.mkdir(parents=True)
    metadata = index / "index_metadata.json"
    metadata.write_text('{"fixture": true}\n', encoding="utf-8")
    gallery = tmp_path / "gallery.parquet"
    gallery.write_bytes(b"gallery")
    config = tmp_path / "runtime.json"
    config.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "frozen_before_final_test",
                "retriever": {"name": "megaloc"},
                "retrieval": {"top_k": top_k},
                "localization": {"estimator": "weighted_medoid", "confidence_threshold": 0.8},
                "dataset": {"gallery_sha256": sha256_file(gallery)},
                "index": {
                    "directory": str(index),
                    "city_id": "moscow",
                    "index_id": "moscow-real-v2",
                    "index_metadata_sha256": sha256_file(metadata),
                },
            }
        ),
        encoding="utf-8",
    )
    return config, gallery


def test_frozen_config_verifies_index_and_benchmark_contract(tmp_path: Path) -> None:
    config_path, gallery = _config(tmp_path)
    config = FrozenRuntimeConfig.load(config_path, verify_index=True)
    config.assert_benchmark_contract(
        retriever="megaloc",
        top_k=20,
        estimator="weighted_medoid",
        confidence_threshold=0.8,
        query_aggregation="single",
        gallery_manifest=gallery,
    )
    assert config.city_id == "moscow"


def test_frozen_config_rejects_contract_or_index_drift(tmp_path: Path) -> None:
    config_path, gallery = _config(tmp_path)
    config = FrozenRuntimeConfig.load(config_path, verify_index=True)
    with pytest.raises(RuntimeConfigError, match="mismatch"):
        config.assert_benchmark_contract(
            retriever="megaloc",
            top_k=10,
            estimator="weighted_medoid",
            confidence_threshold=0.8,
            query_aggregation="single",
            gallery_manifest=gallery,
        )
    (config.index_dir / "index_metadata.json").write_text("changed", encoding="utf-8")
    with pytest.raises(RuntimeConfigError, match="SHA-256"):
        config.verify_index()


def test_frozen_config_k_contract_allows_preregistered_minimum(tmp_path: Path) -> None:
    config_path, _ = _config(tmp_path, top_k=5)
    assert FrozenRuntimeConfig.load(config_path).top_k == 5

    invalid_path, _ = _config(tmp_path / "invalid", top_k=4)
    with pytest.raises(RuntimeConfigError, match="outside"):
        FrozenRuntimeConfig.load(invalid_path)


def test_frozen_config_verifies_confidence_artifact(tmp_path: Path) -> None:
    config_path, _ = _config(tmp_path)
    artifact = tmp_path / "confidence.json"
    artifact.write_text('{"schema_version": 1}\n', encoding="utf-8")
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    payload["confidence"] = {
        "artifact": str(artifact),
        "artifact_sha256": sha256_file(artifact),
    }
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    assert FrozenRuntimeConfig.load(config_path).confidence_model_path == artifact

    artifact.write_text("changed", encoding="utf-8")
    with pytest.raises(RuntimeConfigError, match="confidence model SHA-256"):
        FrozenRuntimeConfig.load(config_path)
