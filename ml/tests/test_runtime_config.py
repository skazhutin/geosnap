from __future__ import annotations

import json
from pathlib import Path

import pytest

from ml.runtime_config import FrozenRuntimeConfig, RuntimeConfigError, sha256_file


def _config(tmp_path: Path) -> tuple[Path, Path]:
    index = tmp_path / "index"
    index.mkdir()
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
                "retrieval": {"top_k": 20},
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
