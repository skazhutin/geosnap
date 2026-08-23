from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from ml.evaluation.commons_benchmark import run_commons_benchmark
from ml.indexing.faiss_index import RetrievalResult
from ml.retrieval.base import BaseRetriever, ImageInput, RetrieverMetadata, l2_normalize


class _MockRetriever(BaseRetriever):
    model_name = "mock-retriever"
    descriptor_dim = 3

    @property
    def metadata(self) -> RetrieverMetadata:
        return RetrieverMetadata(
            model_name=self.model_name,
            descriptor_dim=self.descriptor_dim,
            device=self.device,
            preprocessing="fixture-dominant-rgb",
            checkpoint="none-test-only",
            extra={"test_only": True},
        )

    def load(self) -> _MockRetriever:
        self._device = "cpu"
        self._loaded = True
        return self

    def embed_batch(self, images: Sequence[ImageInput]) -> np.ndarray:
        self.require_loaded()
        rows = []
        for value in images:
            if isinstance(value, Image.Image):
                rgb = value.convert("RGB")
            else:
                with Image.open(value) as opened:
                    rgb = opened.convert("RGB")
            rows.append(np.asarray(rgb, dtype=np.float32).mean(axis=(0, 1)))
        return l2_normalize(np.stack(rows))


class _NumpyExactFixture:
    def __init__(self) -> None:
        self.descriptors: np.ndarray | None = None
        self.ids: list[str] = []
        self.metadata: list[dict[str, Any]] = []

    def __enter__(self) -> _NumpyExactFixture:
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
        self.metadata = [dict(row) for row in reference_metadata]

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
    image = Image.new("RGB", (96, 64), color)
    image.save(path, format="JPEG", quality=95)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_manifest(tmp_path: Path) -> Path:
    image_dir = tmp_path / "images"
    image_dir.mkdir()
    rows = []
    values = [
        (1, "gallery", "a", 55.75, 37.61, (240, 20, 20)),
        (2, "gallery", "a", 55.7501, 37.6101, (220, 30, 20)),
        (3, "gallery", "b", 55.80, 37.70, (20, 20, 240)),
        (4, "gallery", "b", 55.8001, 37.7001, (30, 20, 220)),
        (5, "query", "a", 55.75, 37.61, (235, 25, 20)),
        (6, "query", "a", 55.7501, 37.6101, (225, 35, 20)),
        (7, "query", "b", 55.80, 37.70, (20, 25, 235)),
        (8, "query", "b", 55.8001, 37.7001, (20, 35, 225)),
    ]
    for page_id, split, landmark, lat, lon, color in values:
        path = image_dir / f"{page_id}.jpg"
        sha256 = _write_image(path, color)
        rows.append(
            {
                "record_id": f"commons:{page_id}",
                "split": split,
                "landmark_id": landmark,
                "landmark_name": landmark.upper(),
                "page_id": page_id,
                "title": f"File:{page_id}.jpg",
                "page_url": f"https://commons.wikimedia.org/wiki/File:{page_id}.jpg",
                "local_path": f"images/{page_id}.jpg",
                "lat": lat,
                "lon": lon,
                "author": f"author-{page_id}",
                "captured_at": f"2020-01-{page_id:02d}",
                "license_short_name": "CC BY 4.0",
                "license_url": "https://creativecommons.org/licenses/by/4.0",
                "commons_sha1": f"{page_id:040x}",
                "downloaded_sha256": sha256,
                "perceptual_hash": f"{page_id:016x}",
            }
        )
    manifest = {
        "schema_version": 1,
        "dataset_id": "fixture-commons-proxy",
        "purpose": "evaluation_only_tiny_landmark_biased_proxy",
        "source": {"name": "Wikimedia Commons"},
        "split_audit": {
            "gallery_count": 4,
            "query_count": 4,
        },
        "images": rows,
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def test_mock_retriever_benchmark_reports_primary_metrics_and_no_leakage(
    tmp_path: Path,
) -> None:
    manifest_path = _write_manifest(tmp_path)
    payload, json_path, markdown_path = run_commons_benchmark(
        manifest_path=manifest_path,
        retriever=_MockRetriever(),
        output_dir=tmp_path / "reports",
        report_stem="fixture",
        top_k=10,
        search_factory=_NumpyExactFixture,
    )
    assert payload["primary"]["retrieval"]["recall_at"] == {
        "1": 1.0,
        "5": 1.0,
        "10": 1.0,
    }
    assert payload["primary"]["localization"]["accuracy_within_m"]["100"] == 1.0
    assert payload["localization"]["estimator"] == "weighted_medoid"
    assert payload["runtime"]["descriptor_dimension"] == 3
    assert payload["runtime"]["descriptor_dtype"] == "float32"
    assert payload["runtime"]["gallery_descriptor_storage_bytes"] == 4 * 3 * 4
    assert payload["runtime"]["exact_faiss_vector_storage_bytes"] == 4 * 3 * 4
    assert payload["split_audit"]["exact_cross_split_overlap"] == {
        "page_id": [],
        "commons_sha1": [],
        "downloaded_sha256": [],
    }
    assert json_path.is_file() and markdown_path.is_file()
    assert "not representative of Moscow" in markdown_path.read_text(encoding="utf-8")
    assert json.loads(json_path.read_text(encoding="utf-8"))["robustness"] is None


def test_augmentation_results_are_separate_from_primary_denominators(tmp_path: Path) -> None:
    manifest_path = _write_manifest(tmp_path)
    payload, _, _ = run_commons_benchmark(
        manifest_path=manifest_path,
        retriever=_MockRetriever(),
        output_dir=tmp_path / "reports",
        report_stem="fixture-robustness",
        top_k=10,
        robustness=True,
        search_factory=_NumpyExactFixture,
    )
    assert payload["primary"]["retrieval"]["query_count"] == 4
    assert payload["robustness"]["source_query_count"] == 4
    assert payload["robustness"]["derived_query_count"] == 36
    assert payload["robustness"]["variants_per_query"] == 9
