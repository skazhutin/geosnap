from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import imagehash
import numpy as np
import pandas as pd
import pytest
from PIL import Image

from ml.evaluation.moscow_verification_ablation import (
    ABLATION_KIND,
    MoscowVerificationAblationError,
    _decision,
    _paired_accuracy_evidence,
    _paired_latency_delta,
    run_moscow_verification_ablation,
)
from ml.indexing.faiss_index import RetrievalResult
from ml.ingestion.schema import canonical_record, manifest_dataframe
from ml.localization import LocalizerConfig, SpatialLocalizer, haversine_m
from ml.retrieval.base import BaseRetriever, ImageInput, RetrieverMetadata, l2_normalize
from ml.verification import GeometricEvidence, RerankResult, VerificationCandidate, VerifiedCandidate
from ml.verification.reranker import VerificationConfig


class _TrackingRetriever(BaseRetriever):
    model_name = "fixture-real-moscow"
    descriptor_dim = 3

    def __init__(self) -> None:
        super().__init__()
        self.load_calls = 0
        self.embed_batch_sizes: list[int] = []

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

    def load(self) -> _TrackingRetriever:
        self.load_calls += 1
        self._loaded = True
        self._device = "cpu"
        return self

    def embed_batch(self, images: Sequence[ImageInput]) -> np.ndarray:
        self.require_loaded()
        self.embed_batch_sizes.append(len(images))
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


class _FixtureReranker:
    def __init__(self, config: VerificationConfig) -> None:
        self.config = config

    def rerank(
        self,
        query_image: Any,
        candidates: Sequence[VerificationCandidate],
    ) -> RerankResult:
        del query_image
        rows = tuple(
            VerifiedCandidate(
                reference_id=candidate.reference_id,
                retrieval_score=candidate.retrieval_score,
                original_rank=candidate.original_rank,
                final_rank=candidate.original_rank,
                rerank_score=candidate.retrieval_score,
                verification_score=(
                    0.5 if candidate.original_rank <= self.config.verify_top_k else None
                ),
                evidence=None,
                metadata=candidate.metadata,
            )
            for candidate in candidates
        )
        return RerankResult(
            enabled=True,
            backend=self.config.backend,
            verified_count=min(self.config.verify_top_k, len(rows)),
            total_latency_ms=0.75,
            candidates=rows,
        )


class _LoadableFixtureVerifier:
    backend_name = "opencv_sift"
    max_pairs = 10

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.load_calls = 0
        self.verify_calls = 0

    def load(self) -> _LoadableFixtureVerifier:
        assert self.events == ["search_enter"]
        self.events.append("verifier_load")
        self.load_calls += 1
        return self

    def verify(
        self,
        query_image: Any,
        reference_images: Sequence[Any],
    ) -> tuple[GeometricEvidence, ...]:
        del query_image
        assert self.load_calls == 1
        self.verify_calls += 1
        return tuple(
            GeometricEvidence(
                backend=self.backend_name,
                query_keypoint_count=100,
                reference_keypoint_count=100,
                raw_match_count=20,
                tentative_match_count=20,
                homography_inlier_count=10,
                fundamental_inlier_count=0,
                inlier_count=10,
                inlier_ratio=0.5,
                normalized_score=0.5,
                selected_model="homography",
                latency_ms=0.1,
            )
            for _ in reference_images
        )


def _write_pattern_image(
    path: Path,
    *,
    base: tuple[int, int, int],
    seed: int,
) -> tuple[str, str]:
    rng = np.random.default_rng(seed)
    noise = rng.integers(0, 45, size=(64, 96, 3), dtype=np.uint8)
    pixels = np.clip(
        np.asarray(base, dtype=np.int16)[None, None, :] + noise.astype(np.int16),
        0,
        255,
    ).astype(np.uint8)
    Image.fromarray(pixels, mode="RGB").save(path, format="PNG")
    with Image.open(path) as opened:
        perceptual_hash = str(imagehash.phash(opened.convert("RGB"), hash_size=8))
    return hashlib.sha256(path.read_bytes()).hexdigest(), perceptual_hash


def _record(
    *,
    reference_id: str,
    source_image_id: str,
    sequence_id: str,
    image_path: Path,
    file_sha256: str,
    perceptual_hash: str,
    lat: float,
    lon: float,
    area_id: str,
    split: str,
    nearest_gallery_distance_m: float | None,
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
        "evaluation_split": split,
        "file_sha256": file_sha256,
        "perceptual_hash": perceptual_hash,
        "nearest_gallery_distance_m": nearest_gallery_distance_m,
    }


def _write_fixture(tmp_path: Path) -> tuple[Path, Path]:
    images = tmp_path / "images"
    images.mkdir(parents=True)
    gallery_rows: list[dict[str, Any]] = []
    gallery_coordinates: list[tuple[float, float]] = []
    for index in range(10):
        red = index < 5
        image_path = images / f"gallery-{index}.png"
        file_sha256, perceptual_hash = _write_pattern_image(
            image_path,
            base=(180, 20, 20) if red else (20, 20, 180),
            seed=index,
        )
        lat = 55.7500 + (index % 5) * 0.00002 if red else 55.8000 + (index % 5) * 0.00002
        lon = 37.6100 + (index % 5) * 0.00002 if red else 37.7000 + (index % 5) * 0.00002
        gallery_coordinates.append((lat, lon))
        gallery_rows.append(
            _record(
                reference_id=f"gallery-{index}",
                source_image_id=f"gallery-source-{index}",
                sequence_id=f"gallery-sequence-{index}",
                image_path=image_path,
                file_sha256=file_sha256,
                perceptual_hash=perceptual_hash,
                lat=lat,
                lon=lon,
                area_id="red-area" if red else "blue-area",
                split="gallery",
                nearest_gallery_distance_m=None,
            )
        )

    query_values = (
        ((170, 30, 30), 55.75001, 37.61001, "red-area"),
        ((175, 35, 35), 55.75005, 37.61005, "red-area"),
        ((30, 30, 170), 55.80001, 37.70001, "blue-area"),
        ((35, 35, 175), 55.80005, 37.70005, "blue-area"),
    )
    query_rows: list[dict[str, Any]] = []
    for index, (base, lat, lon, area_id) in enumerate(query_values):
        image_path = images / f"query-{index}.png"
        file_sha256, perceptual_hash = _write_pattern_image(
            image_path,
            base=base,
            seed=100 + index,
        )
        nearest = min(
            haversine_m(lat, lon, gallery_lat, gallery_lon)
            for gallery_lat, gallery_lon in gallery_coordinates
        )
        query_rows.append(
            _record(
                reference_id=f"query-{index}",
                source_image_id=f"query-source-{index}",
                sequence_id=f"query-sequence-{index}",
                image_path=image_path,
                file_sha256=file_sha256,
                perceptual_hash=perceptual_hash,
                lat=lat,
                lon=lon,
                area_id=area_id,
                split="calibration",
                nearest_gallery_distance_m=nearest,
            )
        )

    gallery_path = tmp_path / "gallery.parquet"
    query_path = tmp_path / "test_queries.parquet"
    manifest_dataframe(gallery_rows).to_parquet(gallery_path, index=False)
    manifest_dataframe(query_rows).to_parquet(query_path, index=False)
    return gallery_path, query_path


def _localizer() -> SpatialLocalizer:
    return SpatialLocalizer(
        LocalizerConfig(
            confidence_threshold=0.0,
            minimum_cluster_candidates=1,
            minimum_cluster_mass=0.0,
            minimum_cluster_mass_margin=0.0,
        )
    )


def _config() -> VerificationConfig:
    return VerificationConfig(
        enabled=True,
        backend="opencv_sift",
        verify_top_k=10,
        geometric_weight=0.35,
    )


def test_real_moscow_ablation_reports_both_arms_and_reuses_embeddings(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    retriever = _TrackingRetriever()
    config = _config()
    payload, json_path, markdown_path = run_moscow_verification_ablation(
        gallery_manifest_path=gallery_path,
        query_manifest_path=query_path,
        retriever=retriever,
        output_dir=tmp_path / "reports",
        report_stem="fixture",
        confidence_threshold=0.0,
        verification_config=config,
        search_factory=_NumpyExactSearch,
        localizer=_localizer(),
        reranker=_FixtureReranker(config),
    )

    assert payload["benchmark_kind"] == ABLATION_KIND
    assert payload["dataset"]["gallery"]["count"] == 10
    assert payload["dataset"]["queries"]["count"] == 4
    assert payload["dataset"]["query_subset"]["selected_count"] == 4
    assert payload["leakage_audit"]["base"]["passed"] is True
    assert payload["leakage_audit"]["verification_ablation_preflight"]["passed"] is True
    assert set(payload["arms"]) == {"retrieval_only", "retrieval_plus_verification"}
    assert payload["arms"]["retrieval_only"]["retrieval"]["query_count"] == 4
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
    assert "false_confident_errors" in payload["arms"]["retrieval_only"]
    latency_delta = payload["comparison"]["retrieval_plus_verification_minus_retrieval_only"][
        "offline_query_pipeline_latency_delta_ms"
    ]
    assert latency_delta["median_ms"] is not None
    assert payload["decision"]["configuration_change_applied"] is False
    assert all(
        row["retrieval_plus_verification"]["verified_count"] == 10
        for row in payload["per_query"]
    )
    assert all(
        row["query_quality"]["method"] == "shared_production_query_quality_v1"
        and 0.0 <= row["query_quality"]["confidence_signal"] <= 1.0
        for row in payload["per_query"]
    )

    # One gallery embedding call and exactly one embedding call per selected
    # query. The retrieval-only and verification arms never re-embed.
    assert retriever.embed_batch_sizes == [10, 1, 1, 1, 1]
    written = json.loads(json_path.read_text(encoding="utf-8"))
    assert written["measurement_protocol"]["shared_work"].startswith("Gallery embedded once")
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Primary geographic metrics" in markdown
    assert "False-confident errors" in markdown
    assert "Verification overhead" in markdown


def test_query_bound_is_deterministic_and_area_balanced(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    retriever = _TrackingRetriever()
    config = _config()
    payload, _, _ = run_moscow_verification_ablation(
        gallery_manifest_path=gallery_path,
        query_manifest_path=query_path,
        retriever=retriever,
        output_dir=tmp_path / "reports",
        max_queries=2,
        confidence_threshold=0.0,
        verification_config=config,
        search_factory=_NumpyExactSearch,
        localizer=_localizer(),
        reranker=_FixtureReranker(config),
    )

    assert payload["dataset"]["query_subset"]["selected_count"] == 2
    assert payload["dataset"]["query_subset"]["area_counts"] == {
        "area_id:blue-area": 1,
        "area_id:red-area": 1,
    }
    assert [row["query_id"] for row in payload["per_query"]] == ["query-2", "query-0"]
    assert retriever.embed_batch_sizes == [10, 1, 1]


def test_phash_near_duplicate_fails_before_model_load(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    gallery = pd.read_parquet(gallery_path)
    queries = pd.read_parquet(query_path)
    gallery_image = Path(str(gallery.at[0, "image_path"]))
    query_image = Path(str(queries.at[0, "image_path"]))
    with Image.open(gallery_image) as opened:
        pixels = np.asarray(opened.convert("RGB")).copy()
    pixels[0, 0, 0] = (int(pixels[0, 0, 0]) + 1) % 256
    Image.fromarray(pixels, mode="RGB").save(query_image, format="PNG")
    queries.at[0, "file_sha256"] = hashlib.sha256(query_image.read_bytes()).hexdigest()
    with Image.open(query_image) as opened:
        queries.at[0, "perceptual_hash"] = str(
            imagehash.phash(opened.convert("RGB"), hash_size=8)
        )
    queries.to_parquet(query_path, index=False)

    retriever = _TrackingRetriever()
    with pytest.raises(MoscowVerificationAblationError, match="perceptual-hash leakage"):
        run_moscow_verification_ablation(
            gallery_manifest_path=gallery_path,
            query_manifest_path=query_path,
            retriever=retriever,
            output_dir=tmp_path / "reports",
            search_factory=_NumpyExactSearch,
        )
    assert retriever.load_calls == 0


def test_blank_sequence_fails_before_model_load(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    queries = pd.read_parquet(query_path)
    queries.at[0, "sequence_id"] = None
    queries.to_parquet(query_path, index=False)

    retriever = _TrackingRetriever()
    with pytest.raises(MoscowVerificationAblationError, match="blank sequence_id"):
        run_moscow_verification_ablation(
            gallery_manifest_path=gallery_path,
            query_manifest_path=query_path,
            retriever=retriever,
            output_dir=tmp_path / "reports",
            search_factory=_NumpyExactSearch,
        )
    assert retriever.load_calls == 0


def test_declared_nearest_positive_is_recomputed_before_model_load(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    queries = pd.read_parquet(query_path)
    queries.at[0, "nearest_gallery_distance_m"] = 99.0
    queries.to_parquet(query_path, index=False)

    retriever = _TrackingRetriever()
    with pytest.raises(MoscowVerificationAblationError, match="nearest_gallery_distance_m"):
        run_moscow_verification_ablation(
            gallery_manifest_path=gallery_path,
            query_manifest_path=query_path,
            retriever=retriever,
            output_dir=tmp_path / "reports",
            search_factory=_NumpyExactSearch,
        )
    assert retriever.load_calls == 0


def test_test_or_mixed_query_scope_fails_before_model_load(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    queries = pd.read_parquet(query_path)
    queries.at[0, "evaluation_split"] = "test"
    queries.to_parquet(query_path, index=False)

    retriever = _TrackingRetriever()
    with pytest.raises(MoscowVerificationAblationError, match="calibration-only"):
        run_moscow_verification_ablation(
            gallery_manifest_path=gallery_path,
            query_manifest_path=query_path,
            retriever=retriever,
            output_dir=tmp_path / "reports",
            search_factory=_NumpyExactSearch,
        )
    assert retriever.load_calls == 0


def test_latency_overhead_uses_paired_query_deltas() -> None:
    # Pairwise overheads are [1, 101, 3] ms, whose median is 3 ms.  A
    # difference of arm medians would incorrectly report 2 ms here.
    result = _paired_latency_delta(
        after_samples=[101.0, 102.0, 1003.0],
        before_samples=[100.0, 1.0, 1000.0],
    )
    assert result["paired"] is True
    assert result["count"] == 3
    assert result["median_ms"] == 3.0
    assert result["median_ms"] != 102.0 - 100.0

    permuted = _paired_latency_delta(
        after_samples=[1003.0, 101.0, 102.0],
        before_samples=[1000.0, 100.0, 1.0],
    )
    assert permuted == result


def test_paired_accuracy_evidence_does_not_call_one_change_clear_gain() -> None:
    rows = []
    for index in range(30):
        before_error = 150.0 if index == 0 else 20.0
        rows.append(
            {
                "source": "fixture",
                "sequence_id": f"sequence-{index}",
                "retrieval_only": {
                    "localization": {"status": "ok", "error_m": before_error}
                },
                "retrieval_plus_verification": {
                    "localization": {"status": "ok", "error_m": 20.0}
                },
            }
        )
    evidence = _paired_accuracy_evidence(rows, bootstrap_samples=2_000, seed=7)
    assert evidence["observed_query_weighted_gain"] == pytest.approx(1 / 30)
    assert evidence["provider_sequence_count"] == 30
    assert evidence["bootstrap"]["lower"] == 0.0


def test_paired_accuracy_bootstrap_resamples_provider_sequences_not_frames() -> None:
    rows = []
    for sequence in range(10):
        positive = sequence < 8
        for frame in range(3):
            rows.append(
                {
                    "source": "fixture",
                    "sequence_id": f"sequence-{sequence}",
                    "query_id": f"sequence-{sequence}-frame-{frame}",
                    "retrieval_only": {
                        "localization": {
                            "status": "ok",
                            "error_m": 150.0 if positive else 20.0,
                        }
                    },
                    "retrieval_plus_verification": {
                        "localization": {
                            "status": "ok",
                            "error_m": 20.0 if positive else 150.0,
                        }
                    },
                }
            )
    evidence = _paired_accuracy_evidence(rows, bootstrap_samples=10_000, seed=11)
    assert evidence["query_count"] == 30
    assert evidence["provider_sequence_count"] == 10
    assert evidence["observed_query_weighted_gain"] == pytest.approx(0.6)
    assert evidence["observed_sequence_weighted_gain"] == pytest.approx(0.6)
    assert evidence["bootstrap"]["sampling_unit"] == "source_and_sequence_id"
    assert evidence["bootstrap"]["lower"] <= 0.0


def test_decision_rejects_recall_regression_despite_accuracy_gain() -> None:
    baseline = {
        "retrieval": {"recall_at": {"1": 0.8, "5": 0.9, "10": 0.95}},
        "localization": {
            "accuracy_within_m": {"25": 0.5, "50": 0.6, "100": 0.7},
            "answer_rate": 0.8,
            "median_error_m": 30.0,
            "p90_error_m": 80.0,
        },
        "false_confident_errors": {"count": 0, "errors": []},
    }
    verified = {
        "retrieval": {"recall_at": {"1": 0.7, "5": 0.9, "10": 0.95}},
        "localization": {
            "accuracy_within_m": {"25": 0.5, "50": 0.6, "100": 0.75},
            "answer_rate": 0.8,
            "median_error_m": 30.0,
            "p90_error_m": 80.0,
        },
        "false_confident_errors": {"count": 0, "errors": []},
    }
    comparison = {
        "recall_at": {"1": -0.1, "5": 0.0, "10": 0.0},
        "accuracy_within_m": {"25": 0.0, "50": 0.0, "100": 0.05},
        "answer_rate": 0.0,
        "offline_query_pipeline_latency_delta_ms": {"median_ms": 10.0},
        "paired_accuracy_at_100m": {
            "bootstrap": {"lower": 0.01, "upper": 0.09}
        },
    }
    decision = _decision(
        retrieval_only=baseline,
        verified=verified,
        comparison=comparison,
        selection_profile={
            "selected_count": 100,
            "sequence_count": 30,
            "area_count": 8,
        },
        minimum_accuracy_gain=0.02,
        maximum_median_overhead_ms=250.0,
        minimum_queries_for_enablement=30,
        minimum_sequences_for_enablement=10,
        minimum_areas_for_enablement=4,
    )
    assert decision["criteria"]["recall_at_1_not_worse"] is False
    assert decision["recommendation"] == "keep_verification_default_off"


def test_runner_weight_zero_keeps_both_metric_arms_identical(tmp_path: Path) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    config = VerificationConfig(
        enabled=True,
        backend="opencv_sift",
        verify_top_k=10,
        geometric_weight=0.0,
    )
    payload, _, _ = run_moscow_verification_ablation(
        gallery_manifest_path=gallery_path,
        query_manifest_path=query_path,
        retriever=_TrackingRetriever(),
        output_dir=tmp_path / "reports",
        confidence_threshold=0.0,
        verification_config=config,
        search_factory=_NumpyExactSearch,
        localizer=_localizer(),
        reranker=_FixtureReranker(config),
    )
    baseline = payload["arms"]["retrieval_only"]
    verified = payload["arms"]["retrieval_plus_verification"]
    assert verified["retrieval"] == baseline["retrieval"]
    assert verified["localization"] == baseline["localization"]
    assert [
        row["retrieval_plus_verification"]["localization"]
        for row in payload["per_query"]
    ] == [row["retrieval_only"]["localization"] for row in payload["per_query"]]


def test_verifier_load_occurs_after_exact_search_process_starts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gallery_path, query_path = _write_fixture(tmp_path)
    events: list[str] = []
    verifier = _LoadableFixtureVerifier(events)

    class _TrackingSearch(_NumpyExactSearch):
        def __enter__(self) -> _TrackingSearch:
            events.append("search_enter")
            return self

    monkeypatch.setattr(
        "ml.evaluation.moscow_verification_ablation.build_verifier",
        lambda _config: verifier,
    )
    payload, _, _ = run_moscow_verification_ablation(
        gallery_manifest_path=gallery_path,
        query_manifest_path=query_path,
        retriever=_TrackingRetriever(),
        output_dir=tmp_path / "reports",
        confidence_threshold=0.0,
        verification_config=_config(),
        search_factory=_TrackingSearch,
        localizer=_localizer(),
    )
    assert events == ["search_enter", "verifier_load"]
    assert verifier.load_calls == 1
    assert verifier.verify_calls == 4
    assert payload["runtime"]["verifier_setup_ms"] >= 0.0
    assert payload["config"]["verification"]["runtime_verifier"]["injected"] is False
