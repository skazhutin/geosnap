"""Conservative, uncalibrated consensus across independently localized views.

One photo contributes at most one hypothesis score to each 100 m neighborhood.
Repeated reference images cannot inflate its vote. We choose an existing
hypothesis coordinate, never an average of geographically distant locations.
"""
from __future__ import annotations

import math

from app.schemas.api import (
    ApiStatus,
    Diagnostics,
    Hypothesis,
    LocalizeResponse,
    MultiPhotoEvidence,
    MultiPhotoResponse,
    Prediction,
)

SUPPORT_RADIUS_M = 100.0


def distance_m(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    value = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 6_371_000 * 2 * math.asin(math.sqrt(min(1.0, max(0.0, value))))


def combine_views(results: list[LocalizeResponse], *, submitted: int, request_id: str) -> MultiPhotoResponse:
    if not 1 <= len(results) <= submitted <= 10:
        raise ValueError("Expected one to ten unique photo results")
    metadata = {
        "submitted_images": submitted,
        "unique_images": len(results),
        "duplicate_images": submitted - len(results),
        "support_radius_m": SUPPORT_RADIUS_M,
    }
    if len(results) == 1:
        result = results[0]
        return MultiPhotoResponse(
            **(result.model_dump() | {"request_id": request_id}),
            multi_photo=MultiPhotoEvidence(
                **metadata, method="single_unique_photo", supporting_images=int(result.prediction is not None), agreement="single",
            ),
        )

    # A photo's selected prediction is stronger evidence than a lower-ranked
    # candidate. Resolve an unambiguous top-1 agreement before considering
    # candidates that may be shared by many unrelated street scenes.
    top_predictions = [
        (result.prediction.lat, result.prediction.lon, result.prediction.confidence)
        for result in results
        if result.status is not ApiStatus.OUT_OF_COVERAGE and result.prediction is not None
    ]
    top_modes = []
    for seed in sorted({(lat, lon) for lat, lon, _ in top_predictions}):
        supporters = [score for lat, lon, score in top_predictions if distance_m(seed, (lat, lon)) <= SUPPORT_RADIUS_M]
        top_modes.append((len(supporters), sum(supporters) / len(supporters), seed))
    top_modes.sort(key=lambda item: (-item[0], -item[1], item[2]))
    distinct_top_modes = []
    for mode in top_modes:
        if all(distance_m(mode[2], other[2]) > 2 * SUPPORT_RADIUS_M for other in distinct_top_modes):
            distinct_top_modes.append(mode)
    top1_agreed = bool(
        distinct_top_modes and distinct_top_modes[0][0] >= 2
        and (len(distinct_top_modes) == 1 or distinct_top_modes[0][0] > distinct_top_modes[1][0])
    )

    views: list[list[Hypothesis]] = []
    for result in results:
        hypotheses = [] if result.status is ApiStatus.OUT_OF_COVERAGE else list(result.hypotheses)
        if not hypotheses and result.status is not ApiStatus.OUT_OF_COVERAGE and result.prediction:
            p = result.prediction
            hypotheses = [Hypothesis(lat=p.lat, lon=p.lon, score=p.confidence)]
        views.append([h for h in hypotheses if h.score > 0])
    seeds = sorted({(h.lat, h.lon) for hypotheses in views for h in hypotheses})
    neighborhoods = []
    for seed in seeds:
        support = [max((h.score for h in hypotheses if distance_m(seed, (h.lat, h.lon)) <= SUPPORT_RADIUS_M), default=0.0)
                   for hypotheses in views]
        neighborhoods.append((sum(x > 0 for x in support), sum(support) / len(views), seed))
    neighborhoods.sort(key=lambda value: (-value[0], -value[1], value[2]))
    modes = []
    for item in neighborhoods:
        if all(distance_m(item[2], other[2]) > 2 * SUPPORT_RADIUS_M for other in modes):
            modes.append(item)
        if len(modes) == 3:
            break
    top_support = modes[0][0] if modes else 0
    # Equal support for separate places is unresolved, regardless of a tiny score
    # margin. Single-photo confidence has not been calibrated for this fusion.
    agreed = bool(modes and top_support >= 2 and (len(modes) == 1 or top_support > modes[1][0]))
    if top1_agreed:
        # Keep the predicted coordinate from an actual reference. Candidate
        # scores cannot override agreement between independent top choices.
        modes = [distinct_top_modes[0]] + [
            mode for mode in modes
            if distance_m(mode[2], distinct_top_modes[0][2]) > 2 * SUPPORT_RADIUS_M
        ][:2]
        top_support = distinct_top_modes[0][0]
        agreed = True
    warning = "multi_photo_uncalibrated"
    prediction = None
    matches = []
    if agreed:
        _, score, seed = modes[0]
        prediction = Prediction(lat=seed[0], lon=seed[1], confidence=score)
        seen = set()
        for result in results:
            if result.status is ApiStatus.OUT_OF_COVERAGE:
                continue
            for match in result.matches:
                if match.reference_id not in seen and distance_m(seed, (match.lat, match.lon)) <= SUPPORT_RADIUS_M:
                    matches.append(match)
                    seen.add(match.reference_id)
    warnings = [warning]
    if submitted != len(results):
        warnings.append("duplicate_photos_ignored")
    agreement = "consensus" if agreed else "ambiguous" if modes else "no_candidates"
    return MultiPhotoResponse(
        status=ApiStatus.OUT_OF_COVERAGE if not modes else ApiStatus.LOW_CONFIDENCE,
        prediction=prediction,
        hypotheses=[Hypothesis(lat=seed[0], lon=seed[1], score=score) for _, score, seed in modes],
        matches=matches[:100],
        diagnostics=Diagnostics(warnings=warnings),
        message=("Multiple views support a tentative location; multi-photo accuracy is not calibrated."
                 if agreed else "The photos do not establish one shared location. Try distinct views from the same place."),
        request_id=request_id,
        multi_photo=MultiPhotoEvidence(**metadata, method="top1_then_geographic_consensus_v2", supporting_images=top_support, agreement=agreement),
    )
