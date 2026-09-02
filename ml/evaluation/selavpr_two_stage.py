"""Exact SelaVPR++ binary retrieval followed by float-descriptor reranking."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

from ml.indexing.faiss_index import RetrievalResult
from ml.retrieval.selavprplusplus import SelaVPRPlusPlusRerankRetriever


class SelaVPRPlusPlusTwoStageSearch:
    """In-memory exact evaluator for the official two-branch checkpoint.

    The binary branch retrieves a fixed candidate pool. The float branch only
    reranks that pool. This mirrors the authors' intended two-stage contract
    and prevents any use of the concatenated storage vector as one descriptor.
    """

    backend_name = "SelaVPR++ exact binary top-100 -> exact float rerank"

    def __init__(self, *, candidate_pool: int = 100) -> None:
        if candidate_pool < 50:
            raise ValueError("candidate_pool must be >= 50 for the v3 K grid")
        self.candidate_pool = int(candidate_pool)
        self._binary: np.ndarray | None = None
        self._floating: np.ndarray | None = None
        self._ids: list[str] = []
        self._metadata: list[dict[str, Any]] = []
        self._cached_query: bytes | None = None
        self._cached_order: np.ndarray | None = None
        self._cached_scores: np.ndarray | None = None

    def __enter__(self) -> SelaVPRPlusPlusTwoStageSearch:
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def close(self) -> None:
        self._binary = None
        self._floating = None
        self._cached_query = None
        self._cached_order = None
        self._cached_scores = None

    @property
    def size(self) -> int:
        return len(self._ids)

    def build(
        self,
        descriptors: np.ndarray,
        reference_ids: Sequence[str],
        reference_metadata: Sequence[Mapping[str, Any]],
        retriever_metadata: Mapping[str, Any],
    ) -> None:
        if str(retriever_metadata.get("model_name")) != "selavprplusplus-base-rerank":
            raise ValueError("two-stage search requires the official SelaVPR++ rerank model")
        matrix = np.asarray(descriptors, dtype=np.float32)
        if matrix.shape != (len(reference_ids), SelaVPRPlusPlusRerankRetriever.descriptor_dim):
            raise ValueError("two-stage gallery descriptor shape mismatch")
        if len(set(reference_ids)) != len(reference_ids):
            raise ValueError("two-stage gallery reference IDs must be unique")
        binary, floating = SelaVPRPlusPlusRerankRetriever.split_descriptor(matrix)
        self._binary = np.ascontiguousarray(binary)
        self._floating = np.ascontiguousarray(floating)
        self._ids = [str(value) for value in reference_ids]
        self._metadata = [dict(value) for value in reference_metadata]

    def _rank(self, query: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        if self._binary is None or self._floating is None:
            raise RuntimeError("two-stage search has not been built")
        vector = np.asarray(query, dtype=np.float32).reshape(1, -1)
        key = vector.tobytes()
        if key == self._cached_query and self._cached_order is not None and self._cached_scores is not None:
            return self._cached_order, self._cached_scores
        binary, floating = SelaVPRPlusPlusRerankRetriever.split_descriptor(vector)
        initial_scores = self._binary @ binary[0]
        initial_order = np.lexsort((np.arange(self.size), -initial_scores))
        pool_size = min(self.candidate_pool, self.size)
        pool = initial_order[:pool_size]
        rerank_scores = self._floating[pool] @ floating[0]
        pool_order = pool[np.lexsort((pool, -rerank_scores))]
        order = np.concatenate((pool_order, initial_order[pool_size:]))

        # Top-pool scores are the actual reranker similarities consumed by
        # localization. Remaining values are shifted initial scores used only
        # for honest full-gallery rank diagnostics.
        scores = np.empty(self.size, dtype=np.float32)
        scores[:pool_size] = self._floating[pool_order] @ floating[0]
        scores[pool_size:] = initial_scores[initial_order[pool_size:]] - 2.0
        self._cached_query = key
        self._cached_order = order
        self._cached_scores = scores
        return order, scores

    def search_one(self, query: np.ndarray, *, k: int) -> list[RetrievalResult]:
        if k < 1 or k > min(self.candidate_pool, self.size):
            raise ValueError("two-stage k must fit inside the reranked candidate pool")
        order, scores = self._rank(query)
        return [
            RetrievalResult(
                reference_id=self._ids[int(row)],
                score=float(scores[rank]),
                rank=rank + 1,
                metadata=self._metadata[int(row)],
            )
            for rank, row in enumerate(order[:k])
        ]

    def diagnose_one(
        self,
        query: np.ndarray,
        *,
        true_lat: float,
        true_lon: float,
        distance_thresholds_m: Sequence[float] = (25.0, 50.0, 100.0),
    ) -> dict[str, Any]:
        order, scores = self._rank(query)
        try:
            gallery_lat = np.asarray([float(row["lat"]) for row in self._metadata])
            gallery_lon = np.asarray([float(row["lon"]) for row in self._metadata])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("two-stage diagnostics require finite gallery coordinates") from exc
        query_lat = np.radians(float(true_lat))
        query_lon = np.radians(float(true_lon))
        latitudes = np.radians(gallery_lat)
        delta_lat = latitudes - query_lat
        delta_lon = np.radians(gallery_lon) - query_lon
        value = np.sin(delta_lat / 2.0) ** 2 + np.cos(query_lat) * np.cos(latitudes) * (
            np.sin(delta_lon / 2.0) ** 2
        )
        distances = 2.0 * 6_371_008.8 * np.arcsin(np.sqrt(np.clip(value, 0.0, 1.0)))
        ranked_distances = distances[order]
        diagnostics: dict[str, Any] = {}
        for threshold_value in distance_thresholds_m:
            threshold = float(threshold_value)
            positive_positions = np.flatnonzero(ranked_distances <= threshold)
            negative_positions = np.flatnonzero(ranked_distances > threshold)
            key = str(int(threshold) if threshold.is_integer() else threshold)
            incorrect_score = None if not len(negative_positions) else float(scores[negative_positions[0]])
            if not len(positive_positions):
                diagnostics[key] = {
                    "positive_rank": None,
                    "positive_score": None,
                    "best_incorrect_score": incorrect_score,
                    "positive_minus_best_incorrect_margin": None,
                    "positive_reference": None,
                }
                continue
            position = int(positive_positions[0])
            row = int(order[position])
            positive_score = float(scores[position])
            diagnostics[key] = {
                "positive_rank": position + 1,
                "positive_score": positive_score,
                "best_incorrect_score": incorrect_score,
                "positive_minus_best_incorrect_margin": (
                    None if incorrect_score is None else positive_score - incorrect_score
                ),
                "positive_reference": {
                    "reference_id": self._ids[row],
                    "distance_m": float(ranked_distances[position]),
                    "metadata": self._metadata[row],
                },
            }
        return {
            "gallery_size": self.size,
            "rank_scope": "complete_two_stage_gallery_binary_top100_then_float",
            "by_positive_distance_m": diagnostics,
        }
