"""Optional, bounded local geometric verification for GeoSnap."""

from .base import BaseGeometricVerifier
from .geometry import RansacConfig, estimate_geometry
from .lightglue_sift import LightGlueSiftVerifier, lightglue_sift_available
from .models import (
    GeometricEvidence,
    OptionalVerificationDependencyError,
    RerankResult,
    VerificationCandidate,
    VerificationError,
    VerificationInputError,
    VerifiedCandidate,
)
from .opencv_sift import OpenCvSiftConfig, OpenCvSiftVerifier
from .reranker import (
    MAX_VERIFY_TOP_K,
    GeometricReranker,
    VerificationConfig,
    build_verifier,
)

__all__ = [
    "MAX_VERIFY_TOP_K",
    "BaseGeometricVerifier",
    "GeometricEvidence",
    "GeometricReranker",
    "LightGlueSiftVerifier",
    "OpenCvSiftConfig",
    "OpenCvSiftVerifier",
    "OptionalVerificationDependencyError",
    "RansacConfig",
    "RerankResult",
    "VerificationCandidate",
    "VerificationConfig",
    "VerificationError",
    "VerificationInputError",
    "VerifiedCandidate",
    "build_verifier",
    "estimate_geometry",
    "lightglue_sift_available",
]
