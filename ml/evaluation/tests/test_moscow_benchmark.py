from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from PIL import Image

from ml.evaluation.moscow_benchmark import (
    MoscowBenchmarkError,
    load_moscow_benchmark,
    run_moscow_benchmark,
)
from ml.indexing.faiss_index import RetrievalResult
from ml.ingestion.schema import canonical_record, manifest_dataframe
from ml.retrieval.base import BaseRetriever, ImageInput, RetrieverMetadata, l2_normalize


class _MockRetriever(BaseRetriever):
    model_name = "mock-street-view"
    descriptor_dim = 3

    @property
    def metadata(self) -> RetrieverMetadata:
        return RetrieverMetadata(
            model_name=self.model_name,
            descriptor_dim=self.descriptor_dim,
            device=self.device,
            preprocessing="fixture-mean-rgb",
            checkpoint="fixture-only",
            revision="fixture-revision",
            extra={"test_only": True},
        )

    def load(self) -> _MockRetriever:
        self._device = "cpu"
        self._loaded = True
        return self

    def embed_batch(self, images: Sequence[ImageInput]) -> np.ndarray:
        self.require_loaded()
        rows: list[np.ndarray] = []
        for value in images:
            if isinstance(value, Image.Image):
                image = value.convert("RGB")
            else:
                with Image.open(value) as opened:
                    image = opened.convert("RGB")
            rows.append(np.asarray(image, dtype=np.float32).mean(axis=(0, 1)))
        return l2_normalize(np.stack(rows))


class _NumpyExactSearch:
    def __init__(self) -> None:
        self.descriptors: np.ndarray | None = None
        self.ids: list[str] = []
        self.metadata: list[dict[str, Any]] = []

    def __enter__(self) -> _NumpyExactSearch:
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def build(
        self,
        descriptors: np.ndarray,
        reference_ids: Sequence[str],
        reference_metadata: Sequence[Mapping[str, Any]],
        retriever_metadata: Mapping[str, Any],
    ) -> None:
        del retriever_metadata
        self.descriptors = l2_normalize(descriptors)
        self.ids = list(reference_ids)
        self.metadata = [dict(row) | {"index_id": "fixture"} for row in reference_metadata]

    def search_one(self, query: np.ndarray, *, k: int) -> list[RetrievalResult]:
        assert self.descriptors is not None
        scores = self.descriptors @ l2_normalize(query)[0]
        order = np.argsort(-scores)[:k]
        return [
            RetrievalResult(
                reference_id=self.ids[int(index)],
                score=float(scores[int(index)]),
                rank=rank,
                metadata=self.metadata[int(index)],
            )
            for rank, index in enumerate(order, start=1)
        ]


def _write_image(path: Path, color: tuple[int, int, int]) -> str:
    Image.new("RGB", (96, 64), color).save(path, format="PNG")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _record(
    *,
    reference_id: str,
    source_image_id: str,
    sequence_id: str,
    image_path: Path,
    image_sha256: str,
    lat: float,
    lon: float,
    area_id: str,
) -> dict[str, Any]:
    return canonical_record(
        source="kartaview",
        source_image_id=source_image_id,
        sequence_id=sequence_id,
        lat=lat,
        lon=lon,
        image_path=str(image_path),
        captured_at="2024-01-01T12:00:00Z",
        heading=90.0,
        quality_score=0.9,
        license_name="CC BY-SA 4.0",
        attribution=f"Fixture author {source_image_id}",
        source_url=f"https://kartaview.example/images/{source_image_id}",
        metadata={"fixture": True},
        reference_id=reference_id,
    ) | {
        "area_id": area_id,
        "image_sha256": image_sha256,
    }


def _write_fixture(tmp_path: Path) -> tuple[Path, Path]:
    images = tmp_path / "images"
    images.mkdir(parents=True)
    gallery_rows: list[dict[str, Any]] = []
    query_rows: list[dict[str, Any]] = []

    for index in range(10):
        is_red = index < 5
        color = (230 - index * 2, 20 + index, 18) if is_red else (18, 20 + index, 230 - index * 2)
        image_path = images / f"gallery-{index}.png"
        image_sha256 = _write_image(image_path, color)
        lat = 55.7500 + (index % 5) * 0.00002 if is_red else 55.8000 + (index % 5) * 0.00002
        lon = 37.6100 + (index % 5) * 0.00002 if is_red else 37.7000 + (index % 5) * 0.00002
        gallery_rows.append(
            _record(
                reference_id=f"gallery-{index}",
                source_image_id=f"gallery-source-{index}",
                sequence_id=f"gallery-sequence-{index}",
                image_path=image_path,
                image_sha256=image_sha256,
                lat=lat,
                lon=lon,
                area_id="red-area" if is_red else "blue-area",
            )
        )

    query_values = [
        ((225, 25, 20), 55.75002, 37.61002, "red-area"),
        ((220, 30, 22), 55.75004, 37.61004, "red-area"),
        ((20, 25, 225), 55.80002, 37.70002, "blue-area"),
        ((22, 30, 220), 55.80004, 37.70004, "blue-area"),
    ]
    for index, (color, lat, lon, area_id) in enumerate(query_values):
        image_path = images / f"query-{index}.png"
        image_sha256 = _write_image(image_path, color)
        query_rows.append(
            _record(
                reference_id=f"query-{index}",
                source_image_id=f"query-source-{index}",
                sequence_id=f"query-sequence-{index}",
                image_path=image_path,
                image_sha256=image_sha256,
                lat=lat,
                lon=lon,
                area_id=area_id,
            )
        )

    gallery_path = tmp_path / "gallery.parquet"
    query_path = tmp_path / "queries.parquet"
    manifest_dataframe(gallery_rows).to_parquet(gallery_path, index=False)
    manifest_dataframe(query_rows).to_parquet(query_path, index=False)
    return gallery_path, query_path


def test_real_manifest_benchmark_reports_metrics_provenance_and_storage(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    payload, json_path, markdown_path = run_moscow_benchmark(
        gallery_manifest_path=gallery_path,
        query_manifest_path=query_path,
        retriever=_MockRetriever(),
        output_dir=tmp_path / "reports",
        report_stem="fixture",
        top_k=10,
        estimator="weighted_medoid",
        confidence_threshold=0.0,
        search_factory=_NumpyExactSearch,
    )

    assert payload["benchmark_kind"] == "real_moscow_street_view"
    assert payload["dataset"]["gallery"]["count"] == 10
    assert payload["dataset"]["queries"]["count"] == 4
    assert payload["dataset"]["gallery"]["source_counts"] == {"kartaview": 10}
    assert payload["dataset"]["queries"]["area_counts"]["area_id"] == {
        "blue-area": 2,
        "red-area": 2,
    }
    assert payload["dataset"]["gallery"]["sequence_count"] == 10
    assert len(payload["dataset"]["fingerprint_sha256"]) == 64
    assert payload["leakage_audit"]["passed"] is True
    assert payload["primary"]["retrieval"]["recall_at"] == {
        "1": 1.0,
        "5": 1.0,
        "10": 1.0,
    }
    assert payload["primary"]["localization"]["accuracy_within_m"]["100"] == 1.0
    assert payload["primary"]["localization"]["answer_rate"] == 1.0
    assert payload["primary"]["localization"]["low_confidence_rate"] == 0.0
    assert len(payload["primary"]["confidence_buckets"]) == 4
    assert payload["primary"]["false_confident_errors"]["count"] == 0
    assert payload["runtime"]["descriptor_dimension"] == 3
    assert payload["runtime"]["descriptor_dtype"] == "float32"
    assert payload["runtime"]["gallery_descriptor_storage_bytes"] == 10 * 3 * 4
    assert payload["runtime"]["query_descriptor_storage_bytes"] == 4 * 3 * 4
    assert payload["runtime"]["exact_faiss_vector_storage_bytes"] == 10 * 3 * 4
    assert len(payload["per_query"]) == 4
    assert json.loads(json_path.read_text(encoding="utf-8"))["robustness"] is None
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Leakage audit: **PASS**" in markdown
    assert "False-confident errors" in markdown


def test_default_backend_runs_process_isolated_exact_faiss_without_network(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    payload, _, _ = run_moscow_benchmark(
        gallery_manifest_path=gallery_path,
        query_manifest_path=query_path,
        retriever=_MockRetriever(),
        output_dir=tmp_path / "reports",
        top_k=10,
        confidence_threshold=0.0,
    )

    assert payload["primary"]["retrieval"]["recall_at"]["10"] == 1.0
    assert payload["runtime"]["exact_search_backend"].endswith("isolated process)")


def test_robustness_derivatives_are_separate_from_primary_denominator(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    payload, _, _ = run_moscow_benchmark(
        gallery_manifest_path=gallery_path,
        query_manifest_path=query_path,
        retriever=_MockRetriever(),
        output_dir=tmp_path / "reports",
        top_k=10,
        confidence_threshold=0.0,
        robustness=True,
        search_factory=_NumpyExactSearch,
    )

    assert payload["primary"]["retrieval"]["query_count"] == 4
    assert payload["robustness"]["source_query_count"] == 4
    assert payload["robustness"]["derived_query_count"] == 36
    assert payload["robustness"]["variants_per_query"] == 9
    assert len(payload["robustness"]["per_query"]) == 36


def test_nullable_sequence_fields_do_not_create_synthetic_overlap(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    gallery = pd.read_parquet(gallery_path)
    queries = pd.read_parquet(query_path)
    gallery["sequence_id"] = None
    queries["sequence_id"] = None
    gallery.to_parquet(gallery_path, index=False)
    queries.to_parquet(query_path, index=False)

    loaded = load_moscow_benchmark(gallery_path, query_path)
    assert loaded.leakage_audit["passed"] is True
    assert loaded.leakage_audit["checks"]["sequence_identity"] is False


@pytest.mark.parametrize(
    ("leakage_kind", "expected_fragment"),
    [
        ("id", '"id"'),
        ("source_image_identity", '"source_image_identity"'),
        ("source_url", '"source_url"'),
        ("sequence_identity", '"sequence_identity"'),
        ("declared_hash", "does not match actual image bytes"),
        ("computed_hash", '"computed_image_sha256"'),
    ],
)
def test_query_gallery_leakage_fails_closed(
    tmp_path: Path,
    leakage_kind: str,
    expected_fragment: str,
) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    gallery = pd.read_parquet(gallery_path)
    queries = pd.read_parquet(query_path)
    if leakage_kind == "id":
        queries.at[0, "id"] = gallery.at[0, "id"]
    elif leakage_kind == "source_image_identity":
        queries.at[0, "source"] = gallery.at[0, "source"]
        queries.at[0, "source_image_id"] = gallery.at[0, "source_image_id"]
    elif leakage_kind == "source_url":
        queries.at[0, "source_url"] = gallery.at[0, "source_url"]
    elif leakage_kind == "sequence_identity":
        queries.at[0, "source"] = gallery.at[0, "source"]
        queries.at[0, "sequence_id"] = gallery.at[0, "sequence_id"]
    elif leakage_kind == "declared_hash":
        queries.at[0, "image_sha256"] = gallery.at[0, "image_sha256"]
    else:
        query_image = Path(str(queries.at[0, "image_path"]))
        gallery_image = Path(str(gallery.at[0, "image_path"]))
        query_image.write_bytes(gallery_image.read_bytes())
        queries.at[0, "image_sha256"] = hashlib.sha256(query_image.read_bytes()).hexdigest()
    queries.to_parquet(query_path, index=False)

    with pytest.raises(MoscowBenchmarkError, match=expected_fragment):
        load_moscow_benchmark(gallery_path, query_path)


def test_missing_image_and_missing_canonical_schema_fail_before_model_load(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    queries = pd.read_parquet(query_path)
    Path(str(queries.at[0, "image_path"])).unlink()
    with pytest.raises(MoscowBenchmarkError, match="missing image"):
        load_moscow_benchmark(gallery_path, query_path)

    gallery_path, query_path = _write_fixture(tmp_path / "second")
    malformed = pd.read_parquet(query_path).drop(columns=["lon"])
    malformed.to_parquet(query_path, index=False)
    with pytest.raises(MoscowBenchmarkError, match="missing canonical columns"):
        load_moscow_benchmark(gallery_path, query_path)

    gallery_path, query_path = _write_fixture(tmp_path / "third")
    outside = pd.read_parquet(query_path)
    outside.at[0, "lat"] = 40.0
    outside.to_parquet(query_path, index=False)
    with pytest.raises(MoscowBenchmarkError, match="outside Moscow bounds"):
        load_moscow_benchmark(gallery_path, query_path)


def test_configured_model_mismatch_fails_before_retriever_load(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    retriever = _MockRetriever()
    with pytest.raises(MoscowBenchmarkError, match="configured model"):
        run_moscow_benchmark(
            gallery_manifest_path=gallery_path,
            query_manifest_path=query_path,
            retriever=retriever,
            expected_model="megaloc",
            output_dir=tmp_path / "reports",
            search_factory=_NumpyExactSearch,
        )
    assert retriever.is_loaded is False
