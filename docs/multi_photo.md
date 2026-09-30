# Multiple photos from the same place

The Telegram bot accepts up to ten photos as an album, or as separate messages after `/photos`. Submission is explicit: press **Locate / Определить** or send `/done` once every image appears in the counter. `/cancel` discards a set. Photograph different directions from the same position; travelling between frames defeats the shared-location assumption.

This adds a product capability, not a measured new accuracy result. There is no independent smartphone-series benchmark yet. The default frozen SAGE-B release remains available; the opt-in cleaned SAGE-L local deployment uses a separate index and tentative single-photo outputs.

## API

```bash
curl --fail-with-body http://localhost:8000/localize/multi \
  -F 'images=@north.jpg' \
  -F 'images=@east.jpg' \
  -F 'images=@south.jpg'
```

Through the public proxy, use `/api/localize/multi`. Every part must be named `images`. JPEG, PNG and single-frame WebP are accepted with matching MIME/filename/signature. GPS EXIF does not enter localization. Default deployment limits: 1–10 images, 10 MiB per image, 40 MiB of image bytes in total, 25 million decoded pixels per image, 180 seconds of processing time. Images are decoded and localized sequentially within one backend capacity lease.

Normal responses retain `status`, `prediction`, `hypotheses`, `matches`, `diagnostics`, `message`, `request_id`, and add:

```json
{
  "multi_photo": {
    "method": "top1_then_geographic_consensus_v2",
    "submitted_images": 4,
    "unique_images": 3,
    "duplicate_images": 1,
    "supporting_images": 3,
    "agreement": "consensus",
    "support_radius_m": 100.0
  }
}
```

Counts above illustrate the schema. `agreement` is `single`, `consensus`, `ambiguous`, or `no_candidates`. An invalid image rejects the request rather than silently reducing the set. Invalid input uses 422/415, oversized uploads 413, rate limiting 429, unavailable/busy service 503, and inference timeout 504. Errors use the existing error response envelope and do not require `multi_photo`.

## Current consensus version 2

After independent per-photo localization, test whether at least two selected top-1 predictions agree within 100 m. Use that mode only if its support count strictly exceeds every geographically separate top-1 mode. This protects direct agreement between several photos from a large collection of lower-ranked candidates shared by unrelated scenes. When no unambiguous top-1 mode exists, use the geographic candidate consensus below. The returned coordinate is always an existing reference, never an average of distant places. The six supplied panorama views yielded three top-1 votes for a reference 21.8 m from the user-provided coordinate; this single example does not establish general accuracy.

## Historical consensus version 1 fallback

1. Apply the same EXIF orientation and image preparation as single-photo inference. Hash the decoded RGB pixels and dimensions; evaluate each exact duplicate once. This does not identify every near-duplicate or recompressed photograph.
2. Run the configured single-photo localizer independently for each unique image. No collages, training or query coordinates are used.
3. Use the returned geographic hypotheses. For every candidate location, collect support within 100 m. One photo contributes at most its maximum hypothesis score in this neighborhood. An out-of-coverage result supplies no vote.
4. Order neighborhoods by number of supporting photos, then mean supporting score across all unique photos. Select up to three separated modes, suppressing centers within 200 m of a stronger center. The coordinate is an existing candidate, never an average between distant places.
5. Return a tentative location only if at least two photos support it and its support count strictly exceeds that of the next separate mode. A tie remains ambiguous even when scores differ. A single non-overlapping photograph cannot establish consensus.

The radii and decision rule are explicit engineering defaults; they were not optimized on development correctness. The score is uncalibrated evidence, not a probability. Every genuine multi-photo prediction has `status=low_confidence` and `multi_photo_uncalibrated` in diagnostics. No candidate means `out_of_coverage`; conflicting candidates mean `low_confidence` without a prediction. If deduplication leaves one unique image, `method=single_unique_photo` preserves the selected runtime's single-photo result.

## Limits and validation

Shared candidate biases can still produce a wrong consensus. Supporting photos are distinct inputs, not statistically independent evidence. Photos from different positions, repeated views, season changes and weak reference coverage can all defeat the method. The website currently accepts one photo; multi-photo is available through Telegram and the API.

Tests exercise candidate consensus, distant disagreement, per-photo voting, duplicate removal, bounds, malformed uploads, timeout capacity, album completion, session ownership, cancellation and cooldown. They establish implementation behavior, not localization accuracy. Next accuracy evaluation requires independently collected phone sessions and a frozen policy before opening their outcomes.
