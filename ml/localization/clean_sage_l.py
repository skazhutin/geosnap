"""Versioned cleaned-gallery SAGE-L candidate; old frozen runtime is unchanged."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from time import perf_counter
from typing import Any

import numpy as np

from ml.retrieval.sage import SageVitLRetriever

from .service import LocalizationService, LocalizationServiceError

CONTEXT_SHA256 = "b3b49e7aa0e7c848c4c57b2d79660c3654a9946fb9f423f6f0e91681afc414b2"
GALLERY_SHA256 = "fa24d16e387ea939536fe2760fe1615327797ad665602421d54e61b473ace39c"
INDEX_ID = "moscow_clean_v5_sage_l_111032"


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(4 * 1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


class CleanSageLContextService(LocalizationService):
    """Dual-resolution exact retrieval, frozen context30, top-reference output.

    The output is always tentative because no confidence model was calibrated
    for this 111,032-reference index. Query GPS never enters inference.
    """

    def __init__(self, *, index_dir: Path, context_checkpoint: Path, config_sha256: str) -> None:
        super().__init__(
            SageVitLRetriever(device=os.environ.get("TORCH_DEVICE", "auto"), batch_size=1),
            index_dir,
            top_k=100,
            expected_city_id="moscow",
            expected_index_id=INDEX_ID,
            faiss_worker_timeout_seconds=600.0,
        )
        self.context_checkpoint = context_checkpoint
        self.production_config_sha256 = config_sha256
        self._context: Any | None = None
        self._rows_by_id: dict[str, int] = {}

    def load(self) -> CleanSageLContextService:
        super().load()
        try:
            assert self.index is not None
            if self.index.size != 111_032 or self.index.build_metadata.get("source_gallery_sha256") != GALLERY_SHA256:
                raise LocalizationServiceError("cleaned gallery identity/count mismatch")
            if _digest(self.context_checkpoint) != CONTEXT_SHA256:
                raise LocalizationServiceError("SAGE context checkpoint checksum mismatch")
            import torch

            encoder = torch.nn.TransformerEncoder(
                torch.nn.TransformerEncoderLayer(
                    d_model=768, nhead=16, dim_feedforward=1024,
                    activation="gelu", dropout=0.1, batch_first=False,
                ), 2,
            )
            encoder.load_state_dict(
                torch.load(self.context_checkpoint, map_location="cpu", weights_only=True),
                strict=True,
            )
            self._context = encoder.eval()
            self._rows_by_id = {value: row for row, value in enumerate(self.index.reference_ids)}
            return self
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        self._context = None
        self._rows_by_id = {}
        super().close()

    def runtime_identity(self) -> dict[str, Any]:
        assert self.index is not None
        return {
            "retriever": self.retriever.model_name,
            "source_revision": self.retriever.revision,
            "checkpoint_revision": self.retriever.checkpoint_revision,
            "checkpoint_sha256": self.retriever.checkpoint_sha256,
            "index_id": INDEX_ID,
            "faiss_index_type": "IndexFlatIP",
            "gallery_count": self.index.size,
            "descriptor_dimension": self.index.descriptor_dim,
            "top_k": self.top_k,
            "query_aggregation": "dual_resolution_mean",
            "geographic_aggregation": "top_reference_after_context",
            "coordinate_estimator": "reference_top1",
            "confidence_feature_count": 0,
            "confidence_threshold": None,
            "reranking_enabled": True,
            "approximate_tier_enabled": False,
        }

    def localize(self, query: Any) -> dict[str, Any]:
        if self._context is None or self.index is None or not self.index.is_ready or not self.retriever.is_loaded:
            raise LocalizationServiceError("cleaned-gallery candidate is not ready")
        import torch

        started = perf_counter()
        q322, q504 = self.retriever.embed_dual_resolution(self._query_image(query))
        embedding_ms = (perf_counter() - started) * 1000.0
        retrieved_at = perf_counter()
        mean = np.ascontiguousarray((q322 + q504) * 0.5, dtype=np.float32)
        candidates = self.index.search_one(mean, k=100)
        retrieval_ms = (perf_counter() - retrieved_at) * 1000.0
        rerank_at = perf_counter()
        head = candidates[:30]
        references = self.index.reconstruct_rows([self._rows_by_id[item.reference_id] for item in head])
        mean /= np.linalg.norm(mean)
        features = np.concatenate([mean[None, :], references], axis=0)
        with torch.inference_mode():
            encoded = self._context(torch.from_numpy(features).view(len(features), 11, 768))
            encoded = torch.nn.functional.normalize(encoded.flatten(1), dim=1)
            contextual = (encoded[0] @ encoded[1:].T).numpy()
        original = 0.5 * (references @ q322 + references @ q504)
        aligned = (
            (contextual - contextual.mean()) / max(float(contextual.std()), 1e-6)
            * max(float(original.std()), 1e-6) + original.mean()
        )
        blended = 0.5 * original + 0.5 * aligned
        order = np.argsort(-blended, kind="stable")
        ranked = [head[int(i)] for i in order] + candidates[30:]
        rerank_ms = (perf_counter() - rerank_at) * 1000.0

        first = ranked[0]
        lat, lon = float(first.metadata["lat"]), float(first.metadata["lon"])
        hypotheses = []
        # Distinct coordinates, bounded to ten for the secondary multi-photo
        # candidate union. Scores are raw cosine evidence, not probabilities.
        for candidate in ranked:
            point = (float(candidate.metadata["lat"]), float(candidate.metadata["lon"]))
            if point not in {(item["lat"], item["lon"]) for item in hypotheses}:
                hypotheses.append({
                    "lat": point[0], "lon": point[1],
                    "score": float(np.clip(candidate.score, 0.0, 1.0)),
                })
            if len(hypotheses) >= 10:
                break
        safe_matches = []
        for candidate in ranked[:30]:
            metadata = candidate.metadata
            license_name = str(metadata["license"])
            safe_matches.append({
                "reference_id": candidate.reference_id,
                "source": str(metadata["source"]),
                "lat": float(metadata["lat"]), "lon": float(metadata["lon"]),
                "retrieval_score": float(candidate.score),
                "verification_score": None,
                "attribution": str(metadata["attribution"]),
                "license": license_name,
                "license_url": (
                    "https://creativecommons.org/licenses/by-nc-sa/4.0/"
                    if "CC BY-NC-SA 4.0" in license_name else self._license_url(license_name)
                ),
                "source_url": str(metadata["source_url"]),
                "contributor_url": None,
                "thumbnail_available": False,
            })
        return {
            "status": "low_confidence",
            "prediction": {"lat": lat, "lon": lon, "confidence": 0.0},
            "hypotheses": hypotheses,
            "matches": safe_matches,
            "diagnostics": {
                "retriever": "sage-vitl-322+504-context30-clean-v5",
                "embedding_ms": embedding_ms,
                "retrieval_ms": retrieval_ms,
                "verification_ms": rerank_ms,
                "query_ms": (perf_counter() - started) * 1000.0,
                "warnings": ["research_gallery_msls_included", "confidence_not_calibrated"],
            },
            "message": "Tentative location from the cleaned research gallery; confidence is not calibrated.",
        }


def create_clean_sage_l_service() -> CleanSageLContextService:
    path = Path(os.environ["GEOSNAP_CLEAN_RUNTIME_CONFIG"])
    config = json.loads(path.read_text())
    if config.get("index_id") != INDEX_ID or config.get("gallery_sha256") != GALLERY_SHA256:
        raise LocalizationServiceError("clean SAGE-L runtime config identity mismatch")
    return CleanSageLContextService(
        index_dir=Path(os.environ.get("GEOSNAP_CLEAN_INDEX_DIR", config["index_dir"])),
        context_checkpoint=Path(os.environ.get("GEOSNAP_SAGE_CONTEXT_CHECKPOINT", config["context_checkpoint"])),
        config_sha256=_digest(path),
    )
