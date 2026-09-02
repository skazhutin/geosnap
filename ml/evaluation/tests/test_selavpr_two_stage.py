from __future__ import annotations

import numpy as np

from ml.evaluation.selavpr_two_stage import SelaVPRPlusPlusTwoStageSearch
from ml.retrieval.base import l2_normalize


def _storage(binary: np.ndarray, floating: np.ndarray) -> np.ndarray:
    return l2_normalize(np.concatenate((l2_normalize(binary), l2_normalize(floating)), axis=1))


def test_two_stage_search_reranks_binary_candidates_deterministically() -> None:
    binary = np.zeros((3, 512), dtype=np.float32)
    floating = np.zeros((3, 2048), dtype=np.float32)
    binary[:, 0] = [1.0, 0.9, 0.8]
    binary[:, 1] = [0.0, 0.1, 0.2]
    floating[0, :2] = [0.0, 1.0]
    floating[1, :2] = [1.0, 0.0]
    floating[2, :2] = [0.5, 0.5]
    descriptors = _storage(binary, floating)
    query_binary = np.zeros((1, 512), dtype=np.float32)
    query_float = np.zeros((1, 2048), dtype=np.float32)
    query_binary[0, 0] = 1.0
    query_float[0, 0] = 1.0
    query = _storage(query_binary, query_float)[0]
    metadata = [
        {"lat": 55.0, "lon": 37.0},
        {"lat": 55.0001, "lon": 37.0001},
        {"lat": 56.0, "lon": 38.0},
    ]
    search = SelaVPRPlusPlusTwoStageSearch(candidate_pool=50)
    search.build(
        descriptors,
        ["binary-first", "float-first", "third"],
        metadata,
        {"model_name": "selavprplusplus-base-rerank"},
    )
    first = search.search_one(query, k=2)
    second = search.search_one(query, k=2)
    assert [row.reference_id for row in first] == ["float-first", "third"]
    assert first == second
    diagnostics = search.diagnose_one(query, true_lat=55.0001, true_lon=37.0001)
    assert diagnostics["rank_scope"].startswith("complete_two_stage")
    assert diagnostics["by_positive_distance_m"]["100"]["positive_rank"] == 1
