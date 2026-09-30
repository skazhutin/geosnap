"""Independent-photo score and geographic consensus, ready for real phone sessions.

No query coordinates or outcomes enter these inference functions. The quality
weights must come from a separately validated query-image quality model.
"""
from __future__ import annotations

import numpy as np
from scipy.spatial import cKDTree

from ml.research.night_place import xyz


def combine(image_scores, reference_gps, quality, *, shortlist_depth=1000,
            support_radius_m=50., output_depth=100):
    scores = np.asarray(image_scores, np.float32)
    gps = np.asarray(reference_gps, np.float64)
    q = np.asarray(quality, np.float64)
    if scores.ndim != 2 or scores.shape[0] < 2 or scores.shape[1] < 2:
        raise ValueError("At least two independently encoded photos and two references are required")
    if gps.shape != (scores.shape[1], 2) or q.shape != (scores.shape[0],):
        raise ValueError("Image scores, reference GPS, and quality are misaligned")
    if not np.isfinite(scores).all() or not np.isfinite(gps).all() or not np.isfinite(q).all():
        raise ValueError("All inference inputs must be finite")
    if (q < 0).any() or (q > 1).any() or shortlist_depth < 1 or output_depth < 1 or support_radius_m <= 0:
        raise ValueError("Quality weights and geographic search parameters are invalid")
    if q.sum() <= 0:
        raise ValueError("Every photo was rejected by the supplied quality model; request another view")
    weight = q/q.sum()
    depth = min(shortlist_depth, scores.shape[1])
    out = {}
    for name, values in {
        "best_photo": scores[int(np.argmax(q))],
        "equal_score_mean": scores.mean(axis=0),
        "quality_weighted_score_mean": weight@scores,
    }.items():
        ranking = np.argsort(-values, kind="stable")[:output_depth]
        out[name] = {"gallery_rows": ranking, "scores": values[ranking]}
    shortlist = np.unique(np.argsort(-scores, axis=1, kind="stable")[:, :depth])
    locations = xyz(gps[:, 0], gps[:, 1])
    tree = cKDTree(locations)
    neighborhoods = tree.query_ball_point(locations[shortlist], support_radius_m)
    evidence = np.stack([scores[:, members].max(axis=1) for members in neighborhoods], axis=1)
    for name, values in {
        "equal_geographic_consensus": evidence.mean(axis=0),
        "quality_weighted_geographic_consensus": weight@evidence,
    }.items():
        order = np.lexsort((shortlist, -scores[0, shortlist], -values))[:output_depth]
        out[name] = {"gallery_rows": shortlist[order], "scores": values[order],
                     "per_photo_place_evidence": evidence[:, order]}
    return {"methods": out, "normalized_quality_weights": weight,
            "photo_count": len(q), "unique_shortlisted_reference_count": len(shortlist),
            "reference_coordinates_only": True, "query_gt_used": False}


def capture_decision(quality_probability, retake_action, *, poor_threshold,
                     photos_taken, max_photos):
    """Policy shell; caller must supply a threshold frozen on independent data."""
    if not 0 <= quality_probability <= 1 or not 0 <= poor_threshold <= 1:
        raise ValueError("Quality score and frozen threshold must be in [0, 1]")
    if photos_taken < 1 or max_photos < 1 or photos_taken > max_photos:
        raise ValueError("Invalid photo budget")
    if quality_probability < poor_threshold and photos_taken < max_photos:
        if not retake_action:
            raise ValueError("A poor-photo retake requires actionable guidance")
        return {"decision": "request_targeted_retake", "action": retake_action}
    if quality_probability < poor_threshold:
        return {"decision": "insufficient_input_at_photo_limit", "action": None}
    return {"decision": "localize", "action": None}
