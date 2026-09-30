# GeoSnap v9 blinded geolocatability annotation

This is a research annotation, not a request to guess the GPS position. The 200-image master set was selected with a fixed seed: 50 uniformly sampled images from each of four **hidden** frozen v8 outcome groups, followed by a global shuffle. A 20-image pilot requested by the user was drawn independently from that sealed master set: five random images per hidden group, globally shuffled. The form exposes neither group, query ID, filename, GPS, model prediction nor confidence. Private keys are `data/evaluation/geographic_v9_20260928/annotations/private_sample.json` and `data/evaluation/geographic_v9_20260928/annotations/quick20_private_sample.json`; coordinators must not show them to raters. The image route uses an opaque token and the server binds only to localhost.

Each rater should work independently, without seeing another rater's labels or discussing individual images until both have finished the agreed subset. Start separate local servers with distinct rater IDs and ports. For the current 20-image pilot:

```sh
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.annotation_server --rater reviewer1 --port 8769 --sample quick20
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geographic_v9.annotation_server --rater reviewer2 --port 8770 --sample quick20
```

Open `http://127.0.0.1:8769/` for the first rater and `http://127.0.0.1:8770/` for the second. The 20 images appear in different randomized orders. Saved labels are append-only under `annotations/quick20_rater_<ID>.jsonl`; a rater cannot overwrite an answer through the form. Do not reuse a rater ID for another person. The pilot is for workflow validation and has wide statistical uncertainty. For the full 200-image study, rerun these commands with `--sample full`; its labels use `annotations/rater_<ID>.jsonl`. If only a subset of the full study is feasible, define an identical token subset for both raters *before* either starts; each rater's first 100 images are different because their orders are randomized.

Judge two independent properties. **Technical quality** covers motion blur, focus, exposure, compression/corruption and obstruction. **Geolocation utility** asks whether the visible scene contains stable cues that distinguish this place from other streets. A sharp wall may score 0; a slightly blurred intersection with buildings and signs may score 3. Mark all applicable negative reasons and positive evidence separately. Do not infer a defect from the model's likely performance.

Use the ordinal scale consistently:

| Score | Decision anchor |
|---:|---|
| 0 | Effectively unusable: no stable view of the surrounding place. |
| 1 | Very weak: a few generic cues, unlikely to discriminate among locations. |
| 2 | Potentially usable: some street or landmark context, but ambiguity remains. |
| 3 | Good: multiple visible, stable cues support recognition. |
| 4 | Very informative: distinctive, independent cues strongly constrain the place. |

For `would_request_another_photo`, answer **yes** only when a new view could plausibly add useful evidence; select one specific action. For an ambiguous but technically good image, `try_another_direction` is appropriate. For motion blur, choose `hold_camera_still`; for a wall or close object, `step_away_from_close_surface`; for ground/sky, `avoid_ground_or_sky`. Write a short note only when labels do not capture the reason. Do not look at maps, EXIF GPS, filenames, candidate images or outputs while labeling.

Analysis starts only after independent labels exist. Preserve both raw raters, calculate quadratic-weighted Cohen kappa for the 0–4 score, ordinary kappa for retake yes/no, per-reason agreement, and disagreement tables. Prevalence and conditional localization relationships must reweight the balanced strata to their true counts (411/202/309/262), with uncertainty intervals clustered by geographic group where possible. No image is removed from the original 1,184-query RAW denominator. Human labels are exploratory development data, not independent final validation. Run `python -m ml.research.geographic_v9.analyze_annotations --sample quick20 --raters reviewer1 reviewer2` after both raters have completed the pilot. Until then the command reports progress and no prevalence estimate.
