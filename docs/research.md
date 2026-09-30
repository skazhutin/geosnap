# Research status

GeoSnap contains a frozen deployable SAGE ViT-B pipeline and a separate SAGE-L research program. The website and Telegram bot use the frozen deployment artifacts. Research has not silently replaced them.

For a map of current documents versus historical reports, see the [documentation index](README.md). The cleaned gallery is **111,032 images** (33,102 Mapillary, 10,830 KartaView, 67,100 MSLS); the 43,932 Mapillary/KartaView images are only its source-restricted subset. A separate SAGE-L exact index for all 111,032 images was verified in the opt-in local site/bot deployment on 30 September; Docker is now stopped. The default frozen release still uses 20,487 references and SAGE ViT-B. Cached SAGE-L descriptors cannot be substituted into that old index or its confidence policy; the new local runtime has a separate index and no calibrated confidence model.

## Results as of 29 September 2026

| Population and gallery | Selected within 100 m | Gallery oracle within 100 m | Interpretation |
|---|---:|---:|---|
| Original 1,184 development queries, 112,163 research references | 411/1,184 (34.71%) | 922/1,184 (77.87%) | Best original SAGE-L result |
| Original 1,184 queries, 111,032 cleaned references | 409/1,184 (34.54%) | 913/1,184 (77.11%) | Comparable denominator after gallery curation |
| Qwen-curated 1,071 queries, 111,032 cleaned references | 400/1,071 (37.35%) | 844/1,071 (78.80%) | Different, filtered population |
| Original 1,184 queries, Mapillary + KartaView only (44,995 references) | 380/1,184 (32.09%) | See source report | No research-only MSLS |

The final row uses the **original, unfiltered** Mapillary/KartaView gallery. It is not a measured selected-score result for the cleaned 43,932-image subset; only that subset's geometric coverage was measured on the 1,071-query filtered population. Do not transfer the 32.09% result to the cleaned subset.

The oracle assumes perfect selection of the geographically nearest reference. It measures coverage around these query locations, not achieved localization or complete Moscow coverage. The combined candidate union reached 652/1,184 (55.07%) before cleanup; it is also an oracle, not a selected predictor.

Development queries have been repeatedly examined. Qwen labels are automatic pseudo-labels; semantic curation changes the population and cannot prove an accuracy gain. Multi-photo support added to the bot has implementation tests but no independent smartphone accuracy number.

The historical production report's 96.85% within 100 m applies only to 127 accepted answers out of 1,499 queries (8.47% answer rate). It must not be compared directly with research RAW scores across all queries.

## Evidence and reproduction

- [Final research comparison, distance thresholds and coverage](geosnap_project_final_report_20260929.md)
- [v8 model research](geographic_v8_research_report_20260928.md)
- [v9 selection and acquisition research](geographic_v9_research_report_20260928.md)
- [Automatic geolocatability annotation](geolocatability_v1_report.md)
- [SigLIP2 gallery pass](siglip2_gallery_full_20260929.md)
- [Independent phone collection protocol](geographic_v9_smartphone_collection_protocol_20260928.md)
- [Historical experiment registry](research_experiment_registry_20260910.md)

Reports preserve the paths, model revisions, commands and hashes used during research. Most generated JSONL/Parquet/NPZ artifacts and isolated runtimes are **not in the Git repository**. Links to those local artifacts require the original archive; a fresh clone cannot reproduce all research numbers from source alone. Do not reconstruct missing labels or predictions from report summaries. Production distribution is separate and uses the public versioned artifact manifest.

MSLS is research-only. Dataset, checkpoint and source-code licenses are separate; repository MIT licensing does not apply automatically to any of them. The default public release gallery excludes MSLS; the opt-in local candidate includes it and must not be published as an unrestricted dataset or index.
