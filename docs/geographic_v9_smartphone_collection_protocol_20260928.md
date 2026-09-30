# GeoSnap v9: prospective smartphone and retake study

No independent smartphone result exists yet. The 1,184 provider images are a reused development set, and the 43-parent panorama exercise is a synthetic auxiliary study. Neither can establish real phone retake performance.

## Freeze before collection

Seal separate, immutable configurations for (1) the one-photo coordinate predictor, (2) the quality/geolocatability model and retake action mapping, (3) KEEP/SWITCH if it improves held-out results, and (4) multi-photo evidence fusion and stopping thresholds. Each must contain source commit and file hashes, model/checkpoint revisions, gallery manifest SHA-256, preprocessing, and numerical parameters. Keep the historical production config untouched. Do not open the final capture manifest or inspect its outcomes while selecting these parameters.

## Location and session sampling

Before taking photographs, generate a Moscow location list from a prespecified road/area grid independent of all current model errors. Stratify the list by central/outer area, residential/commercial/green roads, and frozen-gallery coverage/no-coverage; coverage is calculated later from the reference gallery and must not control inference. Target at least 150 independent locations across multiple geographic blocks and new capture sessions; record the actual attainable sample and precision intervals. No person/session should contribute a large fraction of the sample. Include daylight, low light, repetitive roads and vegetation where practical. Weather and season must be reported, not assumed balanced.

At each location, use a normal initial phone photograph designated **before** looking at a model result. Then capture two or three independent directions with a wider street view, buildings, intersections or signage when available. Where safe and appropriate, also capture one intentionally weak view (wall/obstruction, ground/sky, motion blur). Mark it as a separate quality challenge, never substitute it for the natural initial photo or choose whichever image gives the best result after scoring. A retake simulation may use only the prespecified capture order and the images that would have been available by each step. All photographs at one location share a `location_id`; keep person, device and capture session together in resampling.

Store original JPEG/HEIC bytes and SHA-256, image dimensions, capture timestamp and order, camera/device, photographer ID, session ID, route/block ID, whether the image was intentionally weak, and capture-condition tags. Determine the location coordinate independently of the image model, with acquisition method and estimated uncertainty. Exclude only locations whose ground truth cannot support the 100 m threshold, using an exclusion rule registered before inference. Preserve all accepted initial photos in the single-shot denominator, including poor and uncovered images. Reject exact/perceptual duplicates and any overlap with existing development/reference sequences before opening model predictions.

## Blinded human assessment

Use the v9 taxonomy and 0–4 geolocatability anchors in `geographic_v9_annotation_protocol_20260928.md`. At least two raters should label a random subset from all initial/retake/intentional-weak categories, with opaque image tokens and randomized order. Hide location, capture role, prediction, error, confidence and failure bucket. Retain individual labels, disagreement, ordinal weighted kappa, retake agreement and action agreement. Do not train on the final held-out ratings.

## Write-once evaluation

The location-level manifest has one row per location with: `location_id`, `initial_image_id`, `lat`, `lon`, `gt_uncertainty_m`, `session_id`, `photographer_id`, `route_block`, `selection_stratum`, and `captured_at`. The photo manifest has one row per photograph with: `image_id`, `location_id`, `image_path`, `file_sha256`, `capture_order`, `is_initial`, `intentionally_weak`, `captured_at`, and capture-condition tags. A prediction artifact must preserve per-photo quality output, retake decision/action, per-photo candidate scores and references, photos used, single-shot coordinate, final adaptive coordinate or abstention, and multi-photo coordinate. Inference receives image pixels and frozen reference metadata only; location GT stays in the evaluator.

Run the frozen pipeline once. Report these separately:

1. Single-shot RAW25/50/100/500, median, p90 and >500 m on **all** initial images.
2. Quality detection versus blinded humans: per-reason AUROC/AUPRC, calibration where meaningful, false rejection of good images and missed poor images.
3. Adaptive capture: immediate-accept fraction, retake fraction, average/median photos used, answer rate, conditional <=100 m accuracy, >500 m risk, and full-denominator success with unanswered locations counted as incorrect.
4. Multi-photo coordinate accuracy at a fixed photo budget, against best-photo, equal score fusion, quality-weighted score fusion, equal geographic consensus and quality-weighted geographic consensus.
5. Covered/uncovered, session, season/light and intentional-weak strata, without removing any from their prespecified denominators.

Use session and geographic-block bootstrap intervals; compare methods on paired locations. Publish all per-location transitions and artifact hashes. If the first prospective set becomes a tuning set, collect a **new** post-freeze final set rather than relabeling it independent.

The write-once structural and localization evaluator is `ml/research/geographic_v9/smartphone_validation.py`. After the research candidate and predictions are frozen, run:

```sh
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.smartphone_validation \
  --candidate candidate_frozen_v9.json \
  --locations smartphone_locations.parquet \
  --photos smartphone_photos.parquet \
  --predictions smartphone_predictions.npz \
  --receipt smartphone_prediction_receipt.json \
  --output smartphone_independent_result.json
```

The evaluator checks the freeze, population, image hashes, capture order and one prediction per location before scoring. These filenames are schema examples; they do not imply that the data or a new frozen candidate already exists.
