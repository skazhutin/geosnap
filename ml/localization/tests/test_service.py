from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
from PIL import Image

from ml.indexing import FaissExactIndex
from ml.localization.service import (
    LocalizationService,
    LocalizationServiceError,
    create_localization_service,
)
from ml.retrieval.testing import DeterministicFixtureRetriever
from ml.runtime_config import RuntimeConfigError, sha256_file
from ml.verification import RerankResult, VerifiedCandidate


@dataclass
class Diagnostics:
    sharpness: float = 0.9
    exposure: float = 0.8


@dataclass
class Prepared:
    image: Image.Image
    diagnostics: Diagnostics


class FixtureReranker:
    class Config:
        verify_top_k = 1

    config = Config()

    def __init__(self) -> None:
        self.calls = 0

    def rerank(self, _query, candidates) -> RerankResult:
        self.calls += 1
        reranked = tuple(
            VerifiedCandidate(
                reference_id=candidate.reference_id,
                retrieval_score=candidate.retrieval_score,
                original_rank=candidate.original_rank,
                final_rank=index,
                rerank_score=candidate.retrieval_score,
                verification_score=0.8 if index == 1 else None,
                evidence=None,
                metadata=candidate.metadata,
            )
            for index, candidate in enumerate(candidates, start=1)
        )
        return RerankResult(
            enabled=True,
            backend="fixture",
            verified_count=1,
            total_latency_ms=12.5,
            candidates=reranked,
        )


class BrokenReranker:
    class Config:
        verify_top_k = 1

    config = Config()

    def rerank(self, _query, _candidates):
        raise RuntimeError("optional verifier crashed with sensitive details")


def test_long_lived_service_contract_and_safe_mapping(tmp_path: Path) -> None:
    retriever = DeterministicFixtureRetriever(allow_test_only=True).load()
    red = Image.new("RGB", (60, 40), (240, 10, 10))
    blue = Image.new("RGB", (60, 40), (10, 10, 240))
    red_thumbnail = tmp_path / "red.jpg"
    red.save(red_thumbnail)
    descriptors = retriever.embed_batch([red, blue])
    index = FaissExactIndex.build(
        descriptors,
        ["red-ref", "blue-ref"],
        reference_metadata=[
            {
                "lat": 55.75,
                "lon": 37.61,
                "source": "mapillary",
                "attribution": "Mapillary contributor",
                "image_path": "/secret/local/red.jpg",
                "thumb_path": str(red_thumbnail),
            },
            {
                "lat": 55.85,
                "lon": 37.75,
                "source": "kartaview",
                "attribution": "KartaView contributor",
                "image_path": "/secret/local/blue.jpg",
            },
        ],
        retriever_metadata=retriever.metadata.to_dict(),
        index_id="fixture",
        city_id="moscow",
    )
    index.save(tmp_path / "index")

    # A fresh retriever proves service.load owns startup lifecycle.
    reranker = FixtureReranker()
    service = LocalizationService(
        DeterministicFixtureRetriever(allow_test_only=True),
        tmp_path / "index",
        top_k=2,
        geometric_reranker=reranker,
    )
    assert service.readiness() == {
        "model_loaded": False,
        "index_loaded": False,
        "metadata_available": False,
    }
    service.load()
    assert all(service.readiness().values())
    result = service.localize(Prepared(red.copy(), Diagnostics()))
    assert result["status"] == "low_confidence"
    assert "winning_geographic_mode_has_insufficient_support" in result["diagnostics"]["warnings"]
    assert result["prediction"]["lat"] == 55.75
    assert result["matches"][0]["reference_id"] == "red-ref"
    assert result["matches"][0]["verification_score"] == 0.8
    assert result["diagnostics"]["verification_ms"] == 12.5
    assert reranker.calls == 1
    assert result["matches"][0]["thumbnail_available"] is True
    thumbnail = service.get_thumbnail("red-ref")
    assert thumbnail is not None
    assert thumbnail.mode == "RGB"
    assert service.get_thumbnail("missing-ref") is None
    assert "image_path" not in result["matches"][0]
    assert result["diagnostics"]["retriever"].startswith("TEST-ONLY")
    assert result["diagnostics"]["embedding_ms"] >= 0
    assert result["diagnostics"]["retrieval_ms"] >= 0
    service.close()
    assert service.readiness()["model_loaded"] is False


def test_optional_verifier_failure_falls_back_to_retrieval(tmp_path: Path) -> None:
    retriever = DeterministicFixtureRetriever(allow_test_only=True).load()
    query = Image.new("RGB", (60, 40), (240, 10, 10))
    thumbnail = tmp_path / "one.jpg"
    query.save(thumbnail)
    FaissExactIndex.build(
        retriever.embed_batch([query]),
        ["one"],
        reference_metadata=[
            {
                "lat": 55.75,
                "lon": 37.61,
                "source": "mapillary",
                "attribution": "Mapillary contributor",
                "image_path": str(thumbnail),
            }
        ],
        retriever_metadata=retriever.metadata.to_dict(),
        city_id="moscow",
        index_id="fixture",
    ).save(tmp_path / "index")
    service = LocalizationService(
        DeterministicFixtureRetriever(allow_test_only=True),
        tmp_path / "index",
        geometric_reranker=BrokenReranker(),
    ).load()

    result = service.localize(Prepared(query, Diagnostics()))

    assert result["matches"][0]["reference_id"] == "one"
    assert result["matches"][0]["verification_score"] is None
    assert "verification_failed_used_retrieval" in result["diagnostics"]["warnings"]
    assert result["diagnostics"]["verification_ms"] >= 0


def test_factory_parses_faiss_process_isolation_strictly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FAISS_PROCESS_ISOLATION", "auto")
    automatic = create_localization_service()
    assert automatic.process_isolate_faiss is (sys.platform == "darwin")

    monkeypatch.setenv("FAISS_PROCESS_ISOLATION", "false")
    disabled = create_localization_service()
    assert disabled.process_isolate_faiss is False

    monkeypatch.setenv("FAISS_PROCESS_ISOLATION", "sometimes")
    with pytest.raises(ValueError, match="FAISS_PROCESS_ISOLATION"):
        create_localization_service()


def test_factory_uses_frozen_benchmark_runtime_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # ``make`` exports its legacy service defaults. They are intentionally not
    # part of this fixture: the frozen contract below is the source of truth.
    monkeypatch.delenv("RETRIEVAL_TOP_K", raising=False)
    index_dir = tmp_path / "index"
    index_dir.mkdir()
    metadata = index_dir / "index_metadata.json"
    metadata.write_text('{"fixture": true}\n', encoding="utf-8")
    runtime = tmp_path / "frozen.json"
    runtime.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "frozen_before_final_test",
                "retriever": {"name": "megaloc"},
                "retrieval": {"top_k": 50, "query_aggregation": "five_crop_85"},
                "localization": {
                    "estimator": "weighted_medoid",
                    "confidence_threshold": 0.81,
                    "cluster_radius_m": 100.0,
                    "max_cluster_diameter_m": 150.0,
                    "out_of_coverage_similarity": 0.15,
                    "confident_similarity": 0.65,
                    "good_geographic_margin": 0.08,
                    "minimum_cluster_mass": 0.45,
                    "minimum_cluster_mass_margin": 0.10,
                    "minimum_cluster_candidates": 2,
                    "good_hypothesis_separation_m": 500.0,
                    "score_temperature": 0.08,
                },
                "verification": {"enabled": False},
                "dataset": {"gallery_sha256": "unused-by-service"},
                "index": {
                    "directory": str(index_dir),
                    "city_id": "moscow",
                    "index_id": "moscow-real-v2",
                    "index_metadata_sha256": sha256_file(metadata),
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("GEOSNAP_RUNTIME_CONFIG", str(runtime))

    service = create_localization_service()

    assert service.top_k == 50
    assert service.query_aggregation == "five_crop_85"
    assert service.expected_city_id == "moscow"
    assert service.expected_index_id == "moscow-real-v2"
    assert service.localizer.config.confidence_threshold == 0.81
    assert service.localizer.config.estimator.value == "weighted_medoid"
    assert service.localizer.config.score_temperature == 0.08

    monkeypatch.setenv("CONFIDENCE_THRESHOLD", "0.810")
    assert create_localization_service().localizer.config.confidence_threshold == 0.81

    monkeypatch.setenv("RETRIEVAL_TOP_K", "20")
    with pytest.raises(RuntimeConfigError, match="conflicts with frozen"):
        create_localization_service()


def test_service_rejects_wrong_city_or_index_artifact(tmp_path: Path) -> None:
    retriever = DeterministicFixtureRetriever(allow_test_only=True).load()
    image = Image.new("RGB", (60, 40), (20, 30, 40))
    index = FaissExactIndex.build(
        retriever.embed_batch([image]),
        ["one"],
        reference_metadata=[
            {
                "lat": 55.75,
                "lon": 37.61,
                "source": "kartaview",
                "attribution": "KartaView contributor",
            }
        ],
        retriever_metadata=retriever.metadata.to_dict(),
        city_id="moscow",
        index_id="fixture",
    )
    index.save(tmp_path / "index")
    service = LocalizationService(
        DeterministicFixtureRetriever(allow_test_only=True),
        tmp_path / "index",
        expected_city_id="saint-petersburg",
        expected_index_id="fixture",
    )
    with pytest.raises(LocalizationServiceError, match="index city"):
        service.load()


def test_service_rejects_stale_retriever_revision(tmp_path: Path) -> None:
    retriever = DeterministicFixtureRetriever(allow_test_only=True).load()
    image = Image.new("RGB", (60, 40), (20, 30, 40))
    index_dir = tmp_path / "index"
    FaissExactIndex.build(
        retriever.embed_batch([image]),
        ["one"],
        reference_metadata=[
            {
                "lat": 55.75,
                "lon": 37.61,
                "source": "kartaview",
                "attribution": "KartaView contributor",
            }
        ],
        retriever_metadata=retriever.metadata.to_dict(),
        city_id="moscow",
        index_id="fixture",
    ).save(index_dir)
    metadata_path = index_dir / "index_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["retriever"]["revision"] = "stale-revision"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    service = LocalizationService(
        DeterministicFixtureRetriever(allow_test_only=True),
        index_dir,
    )
    with pytest.raises(LocalizationServiceError, match="identity.*revision"):
        service.load()
