# Research gallery quality filter v2 (2026-09-29)

This is an **opt-in derived research gallery**, not a change to the fixed 112,163-reference benchmark, production gallery, or source photographs. It removes the union of two image-only signals:

1. SigLIP2 semantic geolocatability teacher estimate **< 0.10** on the frozen full-gallery score file (1,038 references);
2. a two-pass Qwen consensus to exclude in the 448-image extreme technical-defect shortlist (408 references), plus a two-pass severe-defect consensus in the independent 100-image stratified SigLIP2 audit (3 references, two already in the 448-image shortlist).

SigLIP2 was run on **all 112,163** images. Qwen was run on **448 pixel-shortlisted images** using the extreme-reject prompts and on **100 separate stratified audit images** using shorter technical/semantic prompts. Qwen did **not** review the full gallery. All Qwen responses and the SigLIP2 scores were frozen before this filter was built; neither annotator received query outcomes or gallery retrieval results.

The `<0.10` threshold was chosen from image-only evidence before looking at localization impact. In the 100-image stratified audit, 6 of 7 sampled images below 0.10 received Qwen semantic scores <=0.25; from 0.10 to 0.20 this was 5 of 13. This is a small, stratified pseudo-label audit, **not** a validated defect-rate estimate. A low SigLIP2 score expresses weak semantic geolocatability, which can include intact but generic streets and paths. The Qwen extreme-review decision can also contain mistakes; neither model is human ground truth.

A separate deterministic random visual check of 12 SigLIP2-only exclusions found mostly blurred, dark, obstructed, or generic views, **but also some visually intact forest paths**. This reinforces that the 718 SigLIP2-only exclusions are a semantic-utility filter, not a list of proven damaged files.

## Result

| Source | Original | Excluded | Retained |
|---|---:|---:|---:|
| Mapillary | 34,087 | 981 | 33,106 |
| KartaView | 10,908 | 78 | 10,830 |
| MSLS, research-only | 67,168 | 68 | 67,100 |
| **Total** | **112,163** | **1,127** | **111,036** |

The two principal criteria overlap on 320 images. The extra Qwen audit contributes one consensus-severe image outside both principal sets. The release-compatible Mapillary + KartaView subset would contain 43,936 references after this filter; that is a **new** subset, not the frozen 44,995-reference comparison.

The versioned filtered gallery (`data/evaluation/gallery_quality_filter_v2_20260929/gallery_filtered.parquet`; local artifact), excluded-image audit (`data/evaluation/gallery_quality_filter_v2_20260929/excluded.jsonl`; local artifact), retained original row indices (`data/evaluation/gallery_quality_filter_v2_20260929/retained_original_rows.npy`; local artifact), and filter receipt (`data/evaluation/gallery_quality_filter_v2_20260929/filter_receipt.json`; local artifact) form the frozen output. They preserve identity, source, image SHA-256, model score, and the exact criterion for each exclusion. The original files and frozen gallery are unchanged. All 85 protected production files passed the before/after hash guard.

## Post-freeze localization impact check

After the filter was fixed and hashed, a separate read-only analysis replayed **cached SAGE exact scores**. It did not train, rerun inference, or evaluate the SAGE context reranker on the filtered gallery.

| Measure on 1,184 development queries | Frozen gallery | Filtered gallery |
|---|---:|---:|
| <=100 m reference coverage | 922 | 913 |
| <=100 m positive in exact-score top100 | 613 | 608 |

Eight previously retrievable queries lost a <=100 m top100 candidate, while three gained one: **net -5**. For 41 queries, the original SAGE top1 reference is excluded; three of those originally belonged to the 411 correct cases. This does **not** measure the new final RAW <=100 m result, because the context reranker was not replayed. It establishes that filtering has a measurable retrieval/coverage cost and must not be described as an accuracy improvement. These development outcomes were not used to revise the threshold.

## Decision and limitations

The requested combined exclusion is available as an opt-in research artifact. It is **not activated in production or in the fixed-gallery primary benchmark**. The existing 400-image stricter filter v1 (`data/evaluation/gallery_quality_filter_v1_20260929/filter_receipt.json`; local artifact) remains intact; v2 does not overwrite it. Physical image deletion would make the frozen gallery and earlier experiments irreproducible, so no source files were deleted.

For any decision to promote a filtered gallery, first obtain blinded human review of random exclusions and near-threshold retained images, then test a frozen filter on independent smartphone data. The present Qwen100 audit is too small to estimate the false-removal rate for the 718 SigLIP2-only exclusions. The fixed 1,184-query development set has been reused heavily, so post-freeze impact is diagnostic rather than an independent final claim.

Reproduce and verify with:

```bash
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.build_filtered_gallery_v2
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.analyze_filtered_gallery_v2
```

The post-freeze impact receipt (`data/evaluation/gallery_quality_filter_v2_20260929/postfreeze_impact.json`; local artifact) records source hashes and the exact cached-score analysis.
