"""Model-agnostic place consensus for future independent multi-photo captures."""
from __future__ import annotations

import numpy as np
from scipy.spatial import cKDTree

from ml.research.night_place import xyz


def combine_scores(image_gallery_scores: np.ndarray, reference_gps: np.ndarray, *,
                   shortlist_depth: int = 1000, support_radius_m: float = 50.,
                   output_depth: int = 100) -> dict:
    """Combine separately encoded user views without using query ground truth.

    Each view supplies its strongest nearby reference score for a proposed place.
    A reference sequence with many frames cannot add repeated votes within a view.
    Input scores can come from the selected frozen single-image research pipeline.
    """
    scores = np.asarray(image_gallery_scores, dtype=np.float32)
    gps = np.asarray(reference_gps, dtype=np.float64)
    if scores.ndim != 2 or scores.shape[0] < 2 or scores.shape[1] != len(gps):
        raise ValueError("Two or more full-gallery score rows are required")
    if gps.shape != (scores.shape[1], 2) or not np.isfinite(scores).all() or not np.isfinite(gps).all():
        raise ValueError("Scores and reference GPS must be finite and aligned")
    if shortlist_depth < 1 or output_depth < 1 or support_radius_m <= 0:
        raise ValueError("Invalid candidate depth or geographic support radius")
    depth = min(shortlist_depth, scores.shape[1])
    shortlisted = np.unique(np.argsort(-scores, axis=1, kind="stable")[:, :depth])
    positions = xyz(gps[:, 0], gps[:, 1])
    tree = cKDTree(positions)
    neighborhoods = tree.query_ball_point(positions[shortlisted], support_radius_m)
    evidence = np.stack([scores[:, members].max(axis=1) for members in neighborhoods], axis=1)
    consensus = evidence.mean(axis=0)
    # First view score and gallery row are deterministic tie breakers only.
    order = np.lexsort((shortlisted, -scores[0, shortlisted], -consensus))[:output_depth]
    return {"gallery_rows": shortlisted[order], "consensus_scores": consensus[order],
            "per_image_place_scores": evidence[:, order],
            "unique_shortlisted_places": len(shortlisted),
            "user_image_count": scores.shape[0]}
