from __future__ import annotations

import importlib
from collections.abc import Sequence

import cv2
import numpy as np
import pytest

from ml.verification import (
    MAX_VERIFY_TOP_K,
    BaseGeometricVerifier,
    GeometricEvidence,
    GeometricReranker,
    LightGlueSiftVerifier,
    OpenCvSiftVerifier,
    OptionalVerificationDependencyError,
    VerificationCandidate,
    VerificationConfig,
    VerificationError,
)
from ml.verification.geometry import normalize_geometric_score
from ml.verification.models import ImageInput


def _evidence(score: float, *, backend: str = "fixture") -> GeometricEvidence:
    tentative = 20
    inliers = round(score * tentative)
    return GeometricEvidence(
        backend=backend,
        query_keypoint_count=100,
        reference_keypoint_count=100,
        raw_match_count=80,
        tentative_match_count=tentative,
        homography_inlier_count=inliers,
        fundamental_inlier_count=0,
        inlier_count=inliers,
        inlier_ratio=inliers / tentative,
        normalized_score=score,
        selected_model="homography" if inliers else None,
        latency_ms=1.0,
        failure_reason=None if inliers else "no_support",
    )


class FixtureVerifier(BaseGeometricVerifier):
    backend_name = "fixture"

    def __init__(self, scores: Sequence[float], *, max_pairs: int = 20) -> None:
        super().__init__(max_pairs=max_pairs)
        self.scores = list(scores)
        self.calls: list[tuple[ImageInput, tuple[ImageInput, ...]]] = []

    def verify(
        self,
        query_image: ImageInput,
        reference_images: Sequence[ImageInput],
    ) -> tuple[GeometricEvidence, ...]:
        self._validate_pair_count(reference_images)
        self.calls.append((query_image, tuple(reference_images)))
        return tuple(_evidence(value) for value in self.scores[: len(reference_images)])


def _structured_pair() -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    rng = np.random.default_rng(42)
    reference = np.full((480, 640, 3), 240, dtype=np.uint8)
    for _ in range(60):
        x, y = rng.integers(20, 620), rng.integers(20, 460)
        color = tuple(int(value) for value in rng.integers(0, 200, 3))
        cv2.circle(
            reference,
            (int(x), int(y)),
            int(rng.integers(3, 14)),
            color,
            -1,
        )
    cv2.putText(
        reference,
        "GEOSNAP MOSCOW",
        (70, 240),
        cv2.FONT_HERSHEY_SIMPLEX,
        1.2,
        (10, 10, 10),
        3,
        cv2.LINE_AA,
    )
    source = np.float32([[0, 0], [639, 0], [639, 479], [0, 479]])
    target = np.float32([[35, 20], [610, 5], [630, 450], [10, 470]])
    query = cv2.warpPerspective(
        reference,
        cv2.getPerspectiveTransform(source, target),
        (640, 480),
    )
    distractor = np.full_like(reference, 128)
    cv2.putText(
        distractor,
        "UNRELATED",
        (100, 250),
        cv2.FONT_HERSHEY_SIMPLEX,
        2,
        (240, 240, 240),
        4,
        cv2.LINE_AA,
    )
    return query, reference, distractor


def test_default_configuration_is_off_and_does_not_touch_images() -> None:
    candidates = [
        VerificationCandidate("a", "/definitely/missing-a.jpg", 0.9, 1),
        VerificationCandidate("b", "/definitely/missing-b.jpg", 0.8, 2),
    ]
    result = GeometricReranker().rerank("/missing-query.jpg", candidates)
    assert result.enabled is False
    assert result.backend is None
    assert result.verified_count == 0
    assert [item.reference_id for item in result.candidates] == ["a", "b"]
    assert all(item.evidence is None for item in result.candidates)


def test_environment_configuration_is_explicit_and_top_k_is_hard_bounded() -> None:
    settings = VerificationConfig.from_env(
        {
            "VERIFICATION_ENABLED": "true",
            "VERIFICATION_BACKEND": "opencv_sift",
            "VERIFY_TOP_K": "7",
            "VERIFICATION_GEOMETRIC_WEIGHT": "0.4",
        }
    )
    assert settings.enabled is True
    assert settings.verify_top_k == 7
    assert settings.geometric_weight == 0.4
    with pytest.raises(ValueError, match="verify_top_k"):
        VerificationConfig(verify_top_k=MAX_VERIFY_TOP_K + 1)


def test_reranker_verifies_only_top_k_and_never_reorders_the_tail() -> None:
    verifier = FixtureVerifier([0.0, 1.0], max_pairs=2)
    config = VerificationConfig(enabled=True, verify_top_k=2, geometric_weight=0.5)
    candidates = [
        VerificationCandidate("first", "first.jpg", 0.90, 1),
        VerificationCandidate("second", "second.jpg", 0.85, 2),
        VerificationCandidate("third", "third.jpg", 0.80, 3),
        VerificationCandidate("fourth", "fourth.jpg", 0.70, 4),
    ]
    result = GeometricReranker(config, verifier=verifier).rerank("query.jpg", candidates)
    assert result.enabled is True
    assert result.verified_count == 2
    assert len(verifier.calls) == 1
    assert verifier.calls[0][1] == ("first.jpg", "second.jpg")
    assert [item.reference_id for item in result.candidates] == [
        "second",
        "first",
        "third",
        "fourth",
    ]
    assert result.candidates[2].verification_score is None
    assert result.candidates[3].verification_score is None


def test_opencv_sift_prefers_a_perspective_transform_over_distractor() -> None:
    query, reference, distractor = _structured_pair()
    related, unrelated = OpenCvSiftVerifier().verify(query, [reference, distractor])
    assert related.raw_match_count > 100
    assert related.tentative_match_count > 50
    assert related.inlier_count > 40
    assert related.inlier_ratio > 0.75
    assert related.normalized_score > 0.7
    assert related.selected_model in {"homography", "fundamental"}
    assert related.homography_inlier_count > 0
    assert related.fundamental_inlier_count > 0
    assert unrelated.normalized_score < related.normalized_score


def test_opencv_sift_reports_blank_image_without_fabricating_matches() -> None:
    blank = np.zeros((320, 320, 3), dtype=np.uint8)
    evidence = OpenCvSiftVerifier().verify(blank, [blank])[0]
    assert evidence.query_keypoint_count == 0
    assert evidence.raw_match_count == 0
    assert evidence.inlier_count == 0
    assert evidence.normalized_score == 0.0
    assert evidence.failure_reason == "query_has_no_sift_features"


def test_minimally_determined_ransac_fit_does_not_receive_a_positive_score() -> None:
    # Eight points can exactly determine a fundamental matrix. Reporting the
    # inliers is useful, but this minimum alone is not strong reranking evidence.
    assert normalize_geometric_score(8, 8) == 0.0
    assert normalize_geometric_score(30, 30) == 1.0


def test_real_opencv_evidence_can_reverse_a_wrong_retrieval_order() -> None:
    query, reference, distractor = _structured_pair()
    config = VerificationConfig(enabled=True, verify_top_k=2, geometric_weight=0.4)
    candidates = [
        VerificationCandidate("wrong", distractor, 0.90, 1),
        VerificationCandidate("same-scene", reference, 0.86, 2),
    ]
    result = GeometricReranker(config).rerank(query, candidates)
    assert [item.reference_id for item in result.candidates] == ["same-scene", "wrong"]
    assert result.candidates[0].evidence is not None
    assert result.candidates[0].evidence.inlier_count > 40


def test_lightglue_missing_dependency_is_an_explicit_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing_import(_name: str) -> object:
        raise ModuleNotFoundError("fixture: unavailable")

    monkeypatch.setattr(importlib, "import_module", missing_import)
    with pytest.raises(OptionalVerificationDependencyError, match="official cvg/LightGlue"):
        LightGlueSiftVerifier().load()


def test_backend_rejects_unbounded_direct_batch() -> None:
    verifier = FixtureVerifier([0.0], max_pairs=1)
    with pytest.raises(ValueError, match="bounded"):
        verifier.verify("query", ["one", "two"])


def test_reranker_normalizes_backend_cardinality_failure() -> None:
    verifier = FixtureVerifier([], max_pairs=1)
    reranker = GeometricReranker(
        VerificationConfig(enabled=True, verify_top_k=1),
        verifier=verifier,
    )
    with pytest.raises(VerificationError, match="returned 0 records"):
        reranker.rerank(
            "query",
            [VerificationCandidate("one", "one.jpg", 0.9, 1)],
        )


def test_reranker_normalizes_unexpected_backend_exception() -> None:
    class BrokenVerifier(FixtureVerifier):
        def verify(self, query_image, reference_images):
            raise RuntimeError("backend implementation bug")

    reranker = GeometricReranker(
        VerificationConfig(enabled=True, verify_top_k=1),
        verifier=BrokenVerifier([], max_pairs=1),
    )
    with pytest.raises(VerificationError, match="RuntimeError"):
        reranker.rerank(
            "query",
            [VerificationCandidate("one", "one.jpg", 0.9, 1)],
        )
