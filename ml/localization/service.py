"""Long-lived ML composition adapter for FastAPI or other callers.

This module deliberately does not import HTTP/FastAPI types.  ``localize``
accepts either a PIL image or any prepared-image object exposing ``.image``.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import quote

from PIL import Image, ImageOps, UnidentifiedImageError

from ml.indexing import FaissExactIndex, FaissIndexWorker, RetrievalResult
from ml.retrieval import BaseRetriever, create_retriever

from .estimators import CoordinateEstimator
from .pipeline import LocalizerConfig, SpatialLocalizer

logger = logging.getLogger(__name__)


class LocalizationServiceError(RuntimeError):
    """The composed production pipeline is unavailable or inconsistent."""


def _elapsed_ms(started: float) -> float:
    return (perf_counter() - started) * 1000.0


def _optional_env_bool(name: str) -> bool | None:
    value = os.environ.get(name)
    if value is None or value.strip().lower() in {"", "auto"}:
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be auto, true, or false")


class LocalizationService:
    """Own one model and one exact gallery index for the process lifetime."""

    def __init__(
        self,
        retriever: BaseRetriever,
        index_dir: str | Path,
        *,
        localizer: SpatialLocalizer | None = None,
        top_k: int = 20,
        process_isolate_faiss: bool | None = None,
        expected_city_id: str | None = None,
        expected_index_id: str | None = None,
        geometric_reranker: Any | None = None,
    ) -> None:
        if top_k < 1:
            raise ValueError("top_k must be >= 1")
        self.retriever = retriever
        self.index_dir = Path(index_dir)
        self.localizer = localizer or SpatialLocalizer()
        self.top_k = top_k
        self.expected_city_id = expected_city_id
        self.expected_index_id = expected_index_id
        self.geometric_reranker = geometric_reranker
        self.process_isolate_faiss = (
            sys.platform == "darwin" if process_isolate_faiss is None else process_isolate_faiss
        )
        self.index: FaissExactIndex | FaissIndexWorker | None = None
        self._metadata_by_reference_id: dict[str, Mapping[str, Any]] = {}

    def load(self) -> LocalizationService:
        """Load the official checkpoint and persisted index once."""

        index: FaissExactIndex | FaissIndexWorker
        if self.process_isolate_faiss:
            index = FaissIndexWorker(self.index_dir).start()
        else:
            index = FaissExactIndex.load(self.index_dir)
        try:
            indexed_city = index.build_metadata.get("city_id")
            if self.expected_city_id and indexed_city != self.expected_city_id:
                raise LocalizationServiceError(
                    f"index city {indexed_city!r} does not match configured city {self.expected_city_id!r}"
                )
            indexed_id = index.build_metadata.get("index_id")
            if self.expected_index_id and indexed_id != self.expected_index_id:
                raise LocalizationServiceError(
                    f"index ID {indexed_id!r} does not match configured index {self.expected_index_id!r}"
                )
            if index.descriptor_dim != self.retriever.descriptor_dim:
                raise LocalizationServiceError(
                    f"index descriptor dimension {index.descriptor_dim} does not match "
                    f"{self.retriever.model_name} ({self.retriever.descriptor_dim})"
                )
            indexed_retriever = index.build_metadata.get("retriever", {}).get("model_name")
            if indexed_retriever and indexed_retriever != self.retriever.model_name:
                raise LocalizationServiceError(
                    f"index was built with {indexed_retriever!r}, configured retriever is {self.retriever.model_name!r}"
                )
            indexed_metadata = index.build_metadata.get("retriever", {})
            current_metadata = self.retriever.metadata.to_dict()
            identity_fields = (
                "checkpoint",
                "repository",
                "revision",
                "preprocessing",
                "normalized",
                "extra",
            )
            mismatched = [
                field
                for field in identity_fields
                if indexed_metadata.get(field) != current_metadata.get(field)
            ]
            if mismatched:
                raise LocalizationServiceError(
                    "index retriever identity is incompatible with the configured adapter: "
                    + ", ".join(mismatched)
                )
            self.retriever.load()
        except Exception:
            if isinstance(index, FaissIndexWorker):
                index.close()
            raise
        self.index = index
        self._metadata_by_reference_id = {
            reference_id: metadata
            for reference_id, metadata in zip(
                index.reference_ids,
                index.reference_metadata,
                strict=True,
            )
        }
        return self

    def close(self) -> None:
        """Release process references; framework shutdown may then reclaim device memory."""

        index = self.index
        self.index = None
        self._metadata_by_reference_id = {}
        if isinstance(index, FaissIndexWorker):
            index.close()
        self.retriever.close()

    @staticmethod
    def _thumbnail_path(metadata: Mapping[str, Any]) -> Path | None:
        for key in ("thumbnail_path", "thumb_path", "image_path"):
            raw_path = metadata.get(key)
            if raw_path is None or not str(raw_path).strip():
                continue
            path = Path(str(raw_path))
            if path.is_file():
                return path
        return None

    def get_thumbnail(
        self,
        reference_id: str,
        *,
        max_size: tuple[int, int] = (720, 480),
    ) -> Image.Image | None:
        """Return a bounded RGB copy for an indexed ID, never an arbitrary path.

        The caller supplies only a stable reference ID. Paths are resolved from
        the already-loaded, trusted index sidecar and never returned publicly.
        """

        if max_size[0] <= 0 or max_size[1] <= 0:
            raise ValueError("thumbnail max_size values must be positive")
        metadata = self._metadata_by_reference_id.get(reference_id)
        if metadata is None:
            return None
        path = self._thumbnail_path(metadata)
        if path is None:
            return None
        try:
            with Image.open(path) as opened:
                opened.load()
                image = ImageOps.exif_transpose(opened).convert("RGB")
            image.thumbnail(max_size, Image.Resampling.LANCZOS)
            return image
        except (OSError, SyntaxError, ValueError, UnidentifiedImageError):
            return None

    def readiness(self) -> dict[str, bool]:
        index_loaded = self.index is not None and self.index.is_ready
        metadata_available = bool(
            index_loaded
            and self.index is not None
            and len(self.index.reference_metadata) == self.index.size
            and all(
                metadata.get("lat") is not None
                and metadata.get("lon") is not None
                and metadata.get("source")
                and metadata.get("attribution")
                for metadata in self.index.reference_metadata
            )
        )
        return {
            "model_loaded": self.retriever.is_loaded,
            "index_loaded": index_loaded,
            "metadata_available": metadata_available,
        }

    @staticmethod
    def _query_image(value: Any) -> Any:
        return getattr(value, "image", value)

    @staticmethod
    def _query_quality(value: Any) -> float | None:
        diagnostics = getattr(value, "diagnostics", None)
        if diagnostics is None:
            return None
        scores = [
            float(score)
            for score in (
                getattr(diagnostics, "sharpness", None),
                getattr(diagnostics, "exposure", None),
            )
            if score is not None
        ]
        return sum(scores) / len(scores) if scores else None

    @staticmethod
    def _contributor_url(metadata: Mapping[str, Any], source: str) -> str | None:
        if source != "mapillary":
            return None
        raw_metadata = metadata.get("metadata_json")
        if not isinstance(raw_metadata, str):
            return None
        try:
            payload = json.loads(raw_metadata)
        except (TypeError, json.JSONDecodeError):
            return None
        creator = payload.get("creator") if isinstance(payload, dict) else None
        username = creator.get("username") if isinstance(creator, dict) else None
        if not isinstance(username, str) or not username.strip():
            return None
        return "https://www.mapillary.com/app/user/" + quote(username.strip(), safe="")

    @staticmethod
    def _license_url(license_name: str) -> str | None:
        normalized = license_name.strip().lower().replace("-", " ")
        if "cc by sa" in normalized and "4.0" in normalized:
            return "https://creativecommons.org/licenses/by-sa/4.0/"
        return None

    def localize(self, query: Any) -> Mapping[str, Any]:
        """Embed, retrieve, and localize one prepared image with stage timings."""

        readiness = self.readiness()
        if not readiness["model_loaded"]:
            raise LocalizationServiceError("model is not loaded")
        if not readiness["index_loaded"]:
            raise LocalizationServiceError("index is not loaded")
        if not readiness["metadata_available"]:
            raise LocalizationServiceError("reference metadata is incomplete")
        assert self.index is not None

        query_started = perf_counter()
        embedding_started = perf_counter()
        descriptor = self.retriever.embed_query(self._query_image(query))
        embedding_ms = _elapsed_ms(embedding_started)

        retrieval_started = perf_counter()
        matches = self.index.search_one(descriptor, k=self.top_k)
        retrieval_ms = _elapsed_ms(retrieval_started)

        localization_matches: list[Any] = list(matches)
        verification_ms: float | None = None
        verification_warning: str | None = None
        if self.geometric_reranker is not None and matches:
            from ml.verification import VerificationCandidate

            verify_count = min(
                self.geometric_reranker.config.verify_top_k,
                len(matches),
            )
            head_paths = [self._thumbnail_path(dict(match.metadata)) for match in matches[:verify_count]]
            if any(path is None for path in head_paths):
                verification_warning = "verification_skipped_missing_reference_images"
            else:
                query_image = self._query_image(query)
                verification_candidates = [
                    VerificationCandidate(
                        reference_id=match.reference_id,
                        image=(
                            head_paths[index]
                            if index < verify_count
                            else query_image  # tail is never decoded by the bounded reranker
                        ),
                        retrieval_score=match.score,
                        original_rank=match.rank,
                        metadata=match.metadata,
                    )
                    for index, match in enumerate(matches)
                ]
                verification_started = perf_counter()
                try:
                    reranked = self.geometric_reranker.rerank(
                        query_image,
                        verification_candidates,
                    )
                except Exception as exc:  # optional stage must fail open
                    verification_ms = _elapsed_ms(verification_started)
                    verification_warning = "verification_failed_used_retrieval"
                    logger.warning(
                        "geometric verification failed; using global retrieval order: %s",
                        type(exc).__name__,
                    )
                else:
                    verification_ms = reranked.total_latency_ms
                    localization_matches = [
                        RetrievalResult(
                            reference_id=candidate.reference_id,
                            score=candidate.retrieval_score,
                            rank=candidate.final_rank,
                            metadata=dict(candidate.metadata)
                            | {
                                "verification_score": candidate.verification_score,
                                "localization_score": candidate.rerank_score,
                            },
                        )
                        for candidate in reranked.candidates
                    ]

        result = self.localizer.localize(
            localization_matches,
            query_quality=self._query_quality(query),
        )
        prediction = None
        if result.lat is not None and result.lon is not None:
            prediction = {
                "lat": result.lat,
                "lon": result.lon,
                "confidence": result.confidence,
                "uncertainty_radius_m": result.uncertainty_radius_m,
            }

        safe_matches: list[dict[str, Any]] = []
        for candidate in result.matches:
            metadata = dict(candidate.metadata)
            source = str(metadata.get("source") or candidate.source or "unknown")
            attribution = str(metadata.get("attribution") or source)
            license_name = str(metadata.get("license") or "")
            safe_matches.append(
                {
                    "reference_id": candidate.reference_id,
                    "source": source,
                    "lat": candidate.lat,
                    "lon": candidate.lon,
                    "retrieval_score": candidate.retrieval_score,
                    "verification_score": candidate.verification_score,
                    "attribution": attribution,
                    "license": license_name,
                    "license_url": self._license_url(license_name),
                    "source_url": str(metadata.get("source_url") or ""),
                    "contributor_url": self._contributor_url(metadata, source),
                    "thumbnail_available": self._thumbnail_path(metadata) is not None,
                }
            )

        warnings = list(result.reasons)
        if verification_warning is not None:
            warnings.append(verification_warning)
        if result.uncertainty_radius_m is None:
            warnings.append("uncertainty_not_calibrated")
        diagnostics = dict(result.diagnostics)
        diagnostics.update(
            {
                "retriever": self.retriever.model_name,
                "embedding_ms": embedding_ms,
                "retrieval_ms": retrieval_ms,
                "verification_ms": verification_ms,
                "query_ms": _elapsed_ms(query_started),
                "warnings": warnings,
            }
        )
        return {
            "status": result.status.value,
            "prediction": prediction,
            "hypotheses": [
                {
                    "lat": hypothesis.lat,
                    "lon": hypothesis.lon,
                    # Cluster mass fraction is bounded and directly interpretable.
                    "score": hypothesis.mass_fraction,
                    "uncertainty_radius_m": None,
                }
                for hypothesis in result.hypotheses
            ],
            "matches": safe_matches,
            "diagnostics": diagnostics,
            "message": None,
        }


def create_localization_service() -> LocalizationService:
    """No-argument factory used by the backend lifespan.

    Configuration is environment-only so the ML package remains independent of
    backend settings and HTTP objects.
    """

    retriever_name = os.environ.get("RETRIEVER", "megaloc")
    model_cache = os.environ.get("MODEL_CACHE") or os.environ.get("GEOSNAP_MODEL_CACHE")
    device = os.environ.get("TORCH_DEVICE", "auto")
    batch_size = int(os.environ.get("EMBEDDING_BATCH_SIZE", "8"))
    top_k = int(os.environ.get("RETRIEVAL_TOP_K", "20"))
    index_path = Path(os.environ.get("FAISS_INDEX_PATH", "data/indexes/moscow/index.faiss"))
    index_dir = Path(os.environ.get("GEOSNAP_INDEX_DIR", str(index_path.parent)))
    confidence_threshold = float(
        os.environ.get("CONFIDENCE_THRESHOLD", str(LocalizerConfig().confidence_threshold))
    )
    cluster_radius_m = float(os.environ.get("LOCALIZATION_CLUSTER_RADIUS_M", "100"))
    max_cluster_diameter_m = float(os.environ.get("LOCALIZATION_MAX_CLUSTER_DIAMETER_M", "150"))
    out_of_coverage_similarity = float(os.environ.get("OOC_SIMILARITY_THRESHOLD", "0.15"))
    confident_similarity = float(os.environ.get("CONFIDENT_SIMILARITY_THRESHOLD", "0.65"))
    geographic_margin = float(os.environ.get("GOOD_GEOGRAPHIC_MARGIN", "0.08"))
    minimum_cluster_mass = float(os.environ.get("MINIMUM_CLUSTER_MASS", "0.45"))
    minimum_cluster_mass_margin = float(os.environ.get("MINIMUM_CLUSTER_MASS_MARGIN", "0.10"))
    minimum_cluster_candidates = int(
        os.environ.get("MINIMUM_CLUSTER_CANDIDATES", "2")
    )
    hypothesis_separation_m = float(os.environ.get("GOOD_HYPOTHESIS_SEPARATION_M", "500"))
    estimator = CoordinateEstimator(os.environ.get("COORDINATE_ESTIMATOR", CoordinateEstimator.WEIGHTED_MEDOID.value))
    city_id = os.environ.get("CITY_ID", "moscow")
    index_id = os.environ.get("INDEX_ID", "moscow")
    process_isolation = _optional_env_bool("FAISS_PROCESS_ISOLATION")
    from ml.verification import GeometricReranker, VerificationConfig, build_verifier

    verification_config = VerificationConfig.from_env()
    geometric_reranker = None
    if verification_config.enabled:
        verifier = build_verifier(verification_config)
        loader = getattr(verifier, "load", None)
        if loader is not None:
            loader()
        geometric_reranker = GeometricReranker(
            verification_config,
            verifier=verifier,
        )
    retriever = create_retriever(
        retriever_name,
        device=device,
        batch_size=batch_size,
        cache_dir=model_cache,
    )
    localizer = SpatialLocalizer(
        LocalizerConfig(
            cluster_radius_m=cluster_radius_m,
            max_cluster_diameter_m=max_cluster_diameter_m,
            estimator=estimator,
            confidence_threshold=confidence_threshold,
            out_of_coverage_similarity=out_of_coverage_similarity,
            confident_similarity=confident_similarity,
            good_geographic_margin=geographic_margin,
            minimum_cluster_mass=minimum_cluster_mass,
            minimum_cluster_mass_margin=minimum_cluster_mass_margin,
            minimum_cluster_candidates=minimum_cluster_candidates,
            good_hypothesis_separation_m=hypothesis_separation_m,
        )
    )
    return LocalizationService(
        retriever,
        index_dir,
        localizer=localizer,
        top_k=top_k,
        process_isolate_faiss=process_isolation,
        expected_city_id=city_id,
        expected_index_id=index_id,
        geometric_reranker=geometric_reranker,
    )
