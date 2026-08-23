"""Bounded, opt-in geometric reranking of global-retrieval candidates."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .base import BaseGeometricVerifier
from .lightglue_sift import LightGlueSiftVerifier
from .models import (
    GeometricEvidence,
    ImageInput,
    RerankResult,
    VerificationCandidate,
    VerificationError,
    VerifiedCandidate,
)
from .opencv_sift import OpenCvSiftVerifier

MAX_VERIFY_TOP_K = 20
SUPPORTED_BACKENDS = frozenset({"opencv_sift", "lightglue_sift"})


def _parse_bool(value: str, *, key: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{key} must be a boolean, got {value!r}")


@dataclass(frozen=True, slots=True)
class VerificationConfig:
    """Safe defaults: local verification is disabled until explicitly enabled."""

    enabled: bool = False
    backend: str = "opencv_sift"
    verify_top_k: int = 10
    geometric_weight: float = 0.35
    retrieval_score_min: float = -1.0
    retrieval_score_max: float = 1.0

    def __post_init__(self) -> None:
        if self.backend not in SUPPORTED_BACKENDS:
            raise ValueError(
                f"unsupported verification backend {self.backend!r}; "
                f"choose one of {sorted(SUPPORTED_BACKENDS)}"
            )
        if not 1 <= self.verify_top_k <= MAX_VERIFY_TOP_K:
            raise ValueError(f"verify_top_k must be in [1, {MAX_VERIFY_TOP_K}]")
        if not 0 <= self.geometric_weight <= 1:
            raise ValueError("geometric_weight must be in [0, 1]")
        if self.retrieval_score_min >= self.retrieval_score_max:
            raise ValueError("retrieval_score_min must be smaller than retrieval_score_max")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        prefix: str = "",
    ) -> VerificationConfig:
        values = os.environ if environ is None else environ
        enabled_key = f"{prefix}VERIFICATION_ENABLED"
        top_k_key = f"{prefix}VERIFY_TOP_K"
        backend_key = f"{prefix}VERIFICATION_BACKEND"
        weight_key = f"{prefix}VERIFICATION_GEOMETRIC_WEIGHT"
        return cls(
            enabled=_parse_bool(values.get(enabled_key, "false"), key=enabled_key),
            backend=values.get(backend_key, "opencv_sift").strip().lower(),
            verify_top_k=int(values.get(top_k_key, "10")),
            geometric_weight=float(values.get(weight_key, "0.35")),
        )


def build_verifier(config: VerificationConfig) -> BaseGeometricVerifier:
    """Build exactly the requested backend; never silently substitute one."""

    if config.backend == "opencv_sift":
        return OpenCvSiftVerifier(max_pairs=config.verify_top_k)
    if config.backend == "lightglue_sift":
        return LightGlueSiftVerifier(max_pairs=config.verify_top_k)
    raise AssertionError(f"validated backend unexpectedly unsupported: {config.backend}")


class GeometricReranker:
    """Rerank only ``VERIFY_TOP_K`` references and append the untouched tail."""

    def __init__(
        self,
        config: VerificationConfig | None = None,
        *,
        verifier: BaseGeometricVerifier | None = None,
    ) -> None:
        self.config = config or VerificationConfig()
        self._verifier = verifier

    def _retrieval_unit_score(self, value: float) -> float:
        low = self.config.retrieval_score_min
        high = self.config.retrieval_score_max
        return max(0.0, min(1.0, (value - low) / (high - low)))

    @staticmethod
    def _validate_candidates(candidates: Sequence[VerificationCandidate]) -> None:
        ids = [candidate.reference_id for candidate in candidates]
        ranks = [candidate.original_rank for candidate in candidates]
        if len(ids) != len(set(ids)):
            raise ValueError("verification candidates contain duplicate reference IDs")
        if len(ranks) != len(set(ranks)):
            raise ValueError("verification candidates contain duplicate original ranks")

    def _unverified_candidate(
        self,
        candidate: VerificationCandidate,
        *,
        final_rank: int,
    ) -> VerifiedCandidate:
        return VerifiedCandidate(
            reference_id=candidate.reference_id,
            retrieval_score=candidate.retrieval_score,
            original_rank=candidate.original_rank,
            final_rank=final_rank,
            rerank_score=self._retrieval_unit_score(candidate.retrieval_score),
            verification_score=None,
            evidence=None,
            metadata=candidate.metadata,
        )

    def rerank(
        self,
        query_image: ImageInput,
        candidates: Sequence[VerificationCandidate],
    ) -> RerankResult:
        """Return reranked candidates without ever matching the full gallery."""

        self._validate_candidates(candidates)
        ordered = sorted(candidates, key=lambda item: item.original_rank)
        if not self.config.enabled or not ordered:
            unchanged = tuple(
                self._unverified_candidate(candidate, final_rank=rank)
                for rank, candidate in enumerate(ordered, start=1)
            )
            return RerankResult(
                enabled=False,
                backend=None,
                verified_count=0,
                total_latency_ms=0.0,
                candidates=unchanged,
            )

        try:
            if self._verifier is None:
                self._verifier = build_verifier(self.config)
            verifier = self._verifier
            verify_count = min(self.config.verify_top_k, len(ordered))
            head = ordered[:verify_count]
            started = time.perf_counter()
            evidence = verifier.verify(query_image, [candidate.image for candidate in head])
        except VerificationError:
            raise
        except Exception as exc:
            raise VerificationError(
                f"geometric verification backend failed: {type(exc).__name__}"
            ) from exc
        elapsed_ms = (time.perf_counter() - started) * 1_000
        if len(evidence) != verify_count:
            raise VerificationError(
                f"verification backend returned {len(evidence)} records for {verify_count} inputs"
            )

        weighted: list[tuple[float, VerificationCandidate, GeometricEvidence]] = []
        for candidate, pair_evidence in zip(head, evidence, strict=True):
            retrieval_score = self._retrieval_unit_score(candidate.retrieval_score)
            combined = (
                (1.0 - self.config.geometric_weight) * retrieval_score
                + self.config.geometric_weight * pair_evidence.normalized_score
            )
            weighted.append((combined, candidate, pair_evidence))
        weighted.sort(key=lambda item: (-item[0], item[1].original_rank))

        reranked: list[VerifiedCandidate] = []
        for final_rank, (combined, candidate, pair_evidence) in enumerate(weighted, start=1):
            reranked.append(
                VerifiedCandidate(
                    reference_id=candidate.reference_id,
                    retrieval_score=candidate.retrieval_score,
                    original_rank=candidate.original_rank,
                    final_rank=final_rank,
                    rerank_score=combined,
                    verification_score=pair_evidence.normalized_score,
                    evidence=pair_evidence,
                    metadata=candidate.metadata,
                )
            )
        for candidate in ordered[verify_count:]:
            reranked.append(
                self._unverified_candidate(candidate, final_rank=len(reranked) + 1)
            )
        return RerankResult(
            enabled=True,
            backend=verifier.backend_name,
            verified_count=verify_count,
            total_latency_ms=elapsed_ms,
            candidates=tuple(reranked),
        )
