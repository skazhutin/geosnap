# GeoSnap documentation map

Updated 2026-09-30. Start with the root [README](../README.md) for setup and product behavior. This page separates the deployable system, current research, and historical evidence; a newer file date alone does not make two results comparable. The [final acceptance snapshot](final_acceptance_20260930.md) records the verified product state and its remaining limits. GeoSnap services on the original Mac are stopped.

For a chronological account of the project, including decisions that were later superseded, read the [research and product history](history.md).

## Product and deployment

| Document | Use it for |
| --- | --- |
| [Architecture](architecture.md) | Components and the frozen runtime boundary |
| [Final acceptance and product freeze](final_acceptance_20260930.md) | Final checks, artifact identities, scope of the source freeze, and known limitations |
| [Deployment](deployment.md) | Provisioning, Compose, HTTPS, and operational checks |
| [Cleaned SAGE-L local rollout](cleaned_sage_l_local_rollout_20260930.md) | Opt-in 111,032-image candidate, exact identity, six-photo HTTP check, latency, and rollback |
| [Operations](operations.md) | Running and troubleshooting the service |
| [Telegram bot](telegram_bot.md) and [multi-photo API](multi_photo.md) | Current client flow and its uncalibrated multi-photo behavior |
| [Frontend](frontend.md) | Current website behavior |
| [Security](security.md) and [licenses](licenses.md) | Data handling, attribution, and source restrictions |
| [Frozen localization core](final_localization_core.md) | The September 3 production selection and sealed historical evaluation |

The default frozen release is **SAGE ViT-B with 20,487 Mapillary/KartaView references**. Its historical sealed test answered 127/1,499 queries; 123 of those answers were within 100 m. The 96.85% figure is conditional on answering 8.47% of queries. The production config is `configs/moscow_production_frozen.json`; its hash is recorded in that file's `.sha256` companion. On the original local machine, an opt-in SAGE ViT-L deployment with the cleaned 111,032-image gallery was verified and then stopped with Docker; see the rollout record above.

## Current research and data curation

| Document | Use it for |
| --- | --- |
| [Research status](research.md) | First stop for comparable populations and scores |
| [September 29 synthesis](geosnap_project_final_report_20260929.md) | Original versus cleaned gallery/query metrics and coverage ceilings |
| [Gallery filter v3](gallery_quality_filter_v3_20260929.md) | Current outcome-blind reference exclusion policy; supersedes [v2](gallery_quality_filter_v2_20260929.md) |
| [v8 model study](geographic_v8_research_report_20260928.md) | SAGE-L, alternative models, and candidate union |
| [v9 selection study](geographic_v9_research_report_20260928.md) | Query utility and KEEP/SWITCH experiments |
| [VLM query annotations](geolocatability_v1_report.md) | Automatic pseudo-labels and their limitations |
| [SigLIP2 gallery pass](siglip2_gallery_full_20260929.md) | Fast full-gallery quality scores, not human labels |
| [Smartphone collection protocol](geographic_v9_smartphone_collection_protocol_20260928.md) | Prospective independent evaluation that has not yet happened |
| [Research progress dashboard](research_progress_dashboard.md) | How to inspect local long-running jobs; live state depends on the original workspace |

The original research gallery has **112,163** images. The version-3 cleaned research gallery retains **111,032: 33,102 Mapillary, 10,830 KartaView, and 67,100 MSLS**. The 43,932 Mapillary/KartaView images are only its source-restricted subset, not the entire cleaned gallery. Cleaning did not alter the frozen 20,487-image index. A new, separate exact index is available for the opt-in local deployment. MSLS remains research-only in the source audit; the new runtime has no calibrated confidence policy.

On all 1,184 reused development queries, the best original SAGE-L predictor scored 411/1,184 (34.71%) RAW within 100 m. Replaying it against the cleaned research gallery scored 409/1,184 (34.54%). The 400/1,071 (37.35%) score uses a filtered query population and cannot be compared as a model gain. These are exploratory development results, not a new independent product claim.

## Historical evidence

The [September 10 experiment registry](research_experiment_registry_20260910.md), [research summary](research_summary_20260910.md), [gallery-scale v6 results](gallery_scale_v6_results.md), [night v7 report](night_v7_20260909.md), [research v5](research_v5.md), [earlier phase reports](phase2_quality_improvement.md), and [production evaluation history](evaluation_report.md) retain negative results and the conditions under which old numbers were obtained. The [product readiness snapshot](product_readiness_20260929.md) records tests run on September 29; it is not a live deployment status page.

Additional historical handoffs and protocols: [Part 3 handoff](part3_handoff.md), [v6 study design](gallery_scale_v6.md), [v8 resumption handoff](geographic_v8_20260928_handoff.md), [v8 independent-validation protocol](geographic_v8_independent_validation_protocol.md), and [v9 blind annotation protocol](geographic_v9_annotation_protocol_20260928.md). The [threshold audit](gallery_quality_threshold_audit_20260929.md) explains why the SigLIP2 cutoff was not raised. The [Qwen 100-image review](siglip2_qwen100_stratified_20260929.md) is a teacher comparison, not a full-gallery human validation.

`project_sot.md` and `completion_directive.md` are **archived August task briefs**, not current instructions or product documentation. Their companion SHA-256 files are preserved. Do not follow them as a replacement for the current README, runtime config, or research reports.

Generated manifests, model weights, image files, cached descriptors, per-query outputs, and most JSON receipts live under ignored `data/` or external storage. A GitHub clone contains source and reports, not all underlying research data. Local artifact paths in historical reports identify evidence on the original workspace; they are not public download links. Do not reconstruct missing results from aggregate tables.
