# Frontend phase handoff

This document is the production contract for the later GeoSnap frontend redesign. The current frontend was changed in Part 3 only where production routing, status handling, maps, privacy text, or deployment required it. It is functional, but it is not the final visual product.

## Stable API contract

The browser uses the same FastAPI localization authority as the Telegram client. In production the public API base is the same-origin `/api`; never compile a localhost backend URL into a production bundle.

`POST /api/localize` accepts exactly one multipart field named `image`. Supported decoded formats are JPEG, PNG, and single-frame WebP. The production defaults limit the compressed image to 10 MiB, either decoded dimension to 10,000 pixels, and the decoded image to 25 million pixels. A successful product response is HTTP 200 with this shape:

```json
{
  "status": "ok",
  "prediction": {
    "lat": 55.751244,
    "lon": 37.618423,
    "confidence": 0.95,
    "uncertainty_radius_m": null
  },
  "hypotheses": [],
  "matches": [
    {
      "reference_id": "opaque-id",
      "source": "mapillary",
      "lat": 55.75,
      "lon": 37.61,
      "retrieval_score": 0.8,
      "verification_score": null,
      "thumbnail_url": "/thumbnails/opaque-id",
      "attribution": "provider attribution",
      "license": "provider license",
      "source_url": "https://provider.example/reference",
      "license_url": null,
      "contributor_url": null
    }
  ],
  "diagnostics": {},
  "message": null,
  "request_id": "bounded-id"
}
```

The three product statuses are:

- `ok`: a location passed the frozen production policy; coordinates and matches are present.
- `low_confidence`: candidate matches exist but evidence is insufficient; do not present an authoritative pin.
- `out_of_coverage`: the scene is not sufficiently represented by current Moscow references; do not present coordinates.

Transport and service failures use structured statuses including `invalid_image`, `unsupported_format`, `image_too_large`, `rate_limited`, `service_overloaded`, `gateway_timeout`, `model_not_ready`, `index_not_ready`, and `internal_error`, with an appropriate 4xx/5xx response. Preserve the `X-Request-ID` response header for support diagnostics. Do not display server implementation details.

`prediction.confidence` is a policy/evidence ranking score, not a calibrated probability. It must not be formatted as “probability correct.” The published benchmark is an aggregate result with 8.47% answer rate and 96.85% conditional <=100 m accuracy among accepted answers; it is not a per-photo guarantee.

## Map and references

Production builds require `VITE_MAP_TILE_URL` and `VITE_MAP_ATTRIBUTION`; `VITE_MAP_PUBLIC_PARAMETER` is an optional public browser parameter. The URL may contain `{z}`, `{x}`, `{y}`, and `{token}`. The deployment CSP also requires the tile origin in `MAP_TILE_ORIGIN`. `VITE_*` values are public and must never contain a server-side secret.

Keep the tile provider's required attribution, OpenStreetMap data attribution where applicable, and each reference match's Mapillary or KartaView attribution/license/source link. The standard public OpenStreetMap tile service is development-only and must not be treated as an unlimited production provider.

`GET /api/thumbnails/{reference_id}` serves only an opaque, known production reference ID. Use the returned `thumbnail_url`; never construct a storage path or fetch a provider URL from the browser. Unknown IDs return 404. Thumbnail responses are cacheable for one day.

## Required product language and privacy

The UI must say that GeoSnap attempts to localize supported Moscow street scenes and may abstain when evidence is insufficient. Do not claim complete Moscow coverage or “96.85% accuracy in Moscow.” Communicate that estimates may be wrong.

Uploads are processed in memory and are not permanently retained by default. GPS metadata is not required or used as localization evidence. Avoid adding analytics or persistence that captures images, full EXIF, exact predictions, or provider/user identifiers without a separate privacy decision.

## Frozen backend behavior

The frontend phase must not change the SAGE ViT-B revision/checkpoint, 8,448-dimensional normalized descriptors, exact `IndexFlatIP`, top-K 30, gallery, density-aware geographic-mode aggregation, rank decay, sequence-deduplicated support, weighted medoid, 14-feature confidence model, threshold `0.9349250249145314`, disabled reranking, or production statuses. Any ML change requires a separately versioned research and evaluation cycle.

## Known UI work intentionally deferred

The next phase should evaluate visual identity, content hierarchy, upload affordance, result and map comprehension, abstention presentation, loading/timeout states, mobile/responsive behavior, keyboard and screen-reader experience, color contrast, motion, branding, and cross-browser visual QA. These are observations about scope, not a prescribed design.
