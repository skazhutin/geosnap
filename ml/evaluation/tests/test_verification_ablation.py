from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np

from ml.evaluation import verification_ablation
from ml.evaluation.commons_benchmark import LoadedProxy
from ml.indexing.faiss_index import RetrievalResult
from ml.localization import LocalizerConfig, SpatialLocalizer
from ml.retrieval.base import BaseRetriever, ImageInput, RetrieverMetadata, l2_normalize
from ml.verification import RerankResult, VerificationCandidate, VerifiedCandidate
from ml.verification.reranker import VerificationConfig


class _FixtureRetriever(BaseRetriever):
    model_name = "fixture-retriever"
    descriptor_dim = 3

    @property
    def metadata(self) -> RetrieverMetadata:
        return RetrieverMetadata(
            model_name=self.model_name,
            descriptor_dim=self.descriptor_dim,
            device=self.device,
            preprocessing="fixture",
            checkpoint="fixture",
            extra={"test_only": True},
        )

    def load(self) -> _FixtureRetriever:
        self._loaded = True
        self._device = "cpu"
        return self

    def embed_batch(self, images: Sequence[ImageInput]) -> np.ndarray:
        self.require_loaded()
        return l2_normalize(np.ones((len(images), self.descriptor_dim), dtype=np.float32))


class _FixtureSearch:
    def __init__(self) -> None:
        self.ids: list[str] = []
        self.metadata: list[dict[str, Any]] = []

    def __enter__(self) -> _FixtureSearch:
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
        del descriptors, retriever_metadata
        self.ids = list(reference_ids)
        self.metadata = [dict(row) for row in reference_metadata]

    def search_one(self, query: np.ndarray, *, k: int) -> list[RetrievalResult]:
        del query
        return [
            RetrievalResult(
                reference_id=reference_id,
                score=0.95 - index * 0.02,
                rank=index + 1,
                metadata=self.metadata[index],
            )
            for index, reference_id in enumerate(self.ids[:k])
        ]


class _FixtureReranker:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config

    def rerank(
        self,
        query_image: Any,
        candidates: Sequence[VerificationCandidate],
    ) -> RerankResult:
        del query_image
        rows = []
        for candidate in candidates:
            verified = candidate.original_rank <= self.config.verify_top_k
            rows.append(
                VerifiedCandidate(
                    reference_id=candidate.reference_id,
                    retrieval_score=candidate.retrieval_score,
                    original_rank=candidate.original_rank,
                    final_rank=candidate.original_rank,
                    rerank_score=candidate.retrieval_score,
                    verification_score=0.5 if verified else None,
                    evidence=None,
                    metadata=candidate.metadata,
                )
            )
        return RerankResult(
            enabled=True,
            backend="opencv_sift",
            verified_count=min(self.config.verify_top_k, len(candidates)),
            total_latency_ms=1.0,
            candidates=tuple(rows),
        )


def _proxy(tmp_path: Path) -> LoadedProxy:
    rows: list[dict[str, Any]] = []
    page_id = 1
    for landmark_index in range(4):
        landmark_id = f"landmark-{landmark_index}"
        lat = 55.70 + landmark_index * 0.02
        lon = 37.60 + landmark_index * 0.02
        for split, count in (("gallery", 3), ("query", 2)):
            for offset in range(count):
                rows.append(
                    {
                        "record_id": f"commons:{page_id}",
                        "split": split,
                        "landmark_id": landmark_id,
                        "page_id": page_id,
                        "title": f"File:{page_id}.jpg",
                        "page_url": f"https://commons.wikimedia.org/wiki/File:{page_id}.jpg",
                        "local_path": f"images/{page_id}.jpg",
                        "lat": lat + offset * 0.00001,
                        "lon": lon + offset * 0.00001,
                        "author": f"author-{page_id}",
                        "captured_at": f"2020-01-{page_id:02d}",
                        "license_short_name": "CC BY 4.0",
                        "license_url": "https://creativecommons.org/licenses/by/4.0/",
                        "commons_sha1": f"{page_id:040x}",
                        "downloaded_sha256": f"{page_id:064x}",
                        "perceptual_hash": f"{page_id:016x}",
                    }
                )
                page_id += 1
    manifest_path = tmp_path / "manifest.json"
    manifest = {
        "dataset_id": "fixture-proxy",
        "purpose": "evaluation_only_tiny_landmark_biased_proxy",
        "canonical_dataset_sha256": "fixture-canonical-sha256",
        "images": rows,
    }
    return LoadedProxy(
        manifest_path=manifest_path,
        manifest_sha256="fixture-sha256",
        manifest=manifest,
        gallery=tuple(row for row in rows if row["split"] == "gallery"),
        queries=tuple(row for row in rows if row["split"] == "query"),
    )


def test_rerank_conversion_supplies_combined_score_exactly_once() -> None:
    reranked = RerankResult(
        enabled=True,
        backend="opencv_sift",
        verified_count=2,
        total_latency_ms=2.0,
        candidates=(
            VerifiedCandidate(
                reference_id="verified-winner",
                retrieval_score=0.80,
                original_rank=2,
                final_rank=1,
                rerank_score=0.92,
                verification_score=0.05,
                evidence=None,
                metadata={"lat": 55.75, "lon": 37.61},
            ),
            VerifiedCandidate(
                reference_id="retrieval-winner",
                retrieval_score=0.99,
                original_rank=1,
                final_rank=2,
                rerank_score=0.55,
                verification_score=1.0,
                evidence=None,
                metadata={"lat": 55.80, "lon": 37.70},
            ),
        ),
    )

    converted = verification_ablation.localization_results_from_rerank(reranked)

    assert converted[0].score == 0.80
    assert converted[0].rank == 1
    assert converted[0].metadata["localization_score"] == 0.92
    assert converted[0].metadata["verification_score"] == 0.05
    localized = SpatialLocalizer(
        LocalizerConfig(
            minimum_cluster_candidates=1,
            minimum_cluster_mass=0.0,
            minimum_cluster_mass_margin=0.0,
            confidence_threshold=0.0,
        )
    ).localize(converted)
    assert localized.lat == 55.75
    assert localized.lon == 37.61


def test_runner_reports_both_8_by_12_arms_and_bounded_latency(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    proxy = _proxy(tmp_path)
    monkeypatch.setattr(verification_ablation, "load_proxy_snapshot", lambda path: proxy)
    config = VerificationConfig(
        enabled=True,
        backend="opencv_sift",
        verify_top_k=10,
        geometric_weight=0.35,
    )
    output_path = tmp_path / "ablation.json"

    payload, written_path = verification_ablation.run_verification_ablation(
        manifest_path=tmp_path / "manifest.json",
        retriever=_FixtureRetriever(),
        output_path=output_path,
        verification_config=config,
        search_factory=_FixtureSearch,
        reranker=_FixtureReranker(config),
    )

    assert written_path == output_path
    assert payload["dataset"]["split_audit"]["gallery_count"] == 12
    assert payload["dataset"]["split_audit"]["query_count"] == 8
    assert payload["dataset"]["canonical_dataset_sha256"] == "fixture-canonical-sha256"
    assert set(payload["arms"]) == {
        "retrieval_only",
        "retrieval_plus_verification",
    }
    assert payload["arms"]["retrieval_only"]["retrieval"]["query_count"] == 8
    assert set(payload["arms"]["retrieval_only"]["retrieval"]["recall_at"]) == {
        "1",
        "5",
        "10",
    }
    assert set(payload["arms"]["retrieval_plus_verification"]["localization"]["accuracy_within_m"]) == {
        "25",
        "50",
        "100",
    }
    verification_latency = payload["arms"]["retrieval_plus_verification"]["latency"]["stages"]["opencv_sift_backend"]
    assert verification_latency["count"] == 8
    assert verification_latency["median_ms"] == 1.0
    assert all(row["retrieval_plus_verification"]["verified_count"] == 10 for row in payload["per_query"])
    assert json.loads(output_path.read_text(encoding="utf-8"))["production_decision"] == "keep_verification_default_off"
