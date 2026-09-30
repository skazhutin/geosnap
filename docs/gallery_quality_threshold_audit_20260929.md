# Should the SigLIP2 gallery cutoff rise to 0.15 or 0.20?

**Decision: keep the current `<0.10` research-filter cutoff; do not raise it.** SigLIP2 scores are in `[0,1]`, so the proposed 1.5 and 2.0 cutoffs were interpreted as **0.15** and **0.20**. This is a threshold audit, not a new production gallery or a new localization experiment. The fixed 112,163-reference gallery and v2 derived filter (`data/evaluation/gallery_quality_filter_v2_20260929/filter_receipt.json`; local artifact) remain unchanged.

## Manual image review

I visually reviewed a deterministic random sample of **20 original photos**: ten in `0.10,0.15)` and ten in `[0.15,0.20)`. Both samples excluded images that already had a two-pass Qwen severe-defect consensus, so they represent the **additional** images that raising the SigLIP2 cutoff would remove. The sample seeds were `20260944` and `20260949`; original file SHA-256 values were checked against the frozen gallery. This review used images and SigLIP2 bands, **not** query locations, SAGE predictions, retrieval scores, or outcome buckets. The [20 per-image judgments (`data/evaluation/gallery_quality_threshold_audit_v1_20260929/manual_review_20.jsonl`; local artifact) and first contact sheet (`data/evaluation/gallery_quality_threshold_audit_v1_20260929/manual_0_10_to_0_15.jpg`; local artifact) / second contact sheet (`data/evaluation/gallery_quality_threshold_audit_v1_20260929/manual_0_15_to_0_20.jpg`; local artifact) preserve what was inspected.

| Band | Non-street unusable | Inverted but fixable | Weak scene still visible | Usable scene |
|---|---:|---:|---:|---:|
| 0.10–0.15 | 1 | 1 | 7 | 1 |
| 0.15–0.20 | 1 | 1 | 4 | 4 |
| **Total** | **2** | **2** | **11** | **5** |

Examples of scenes that a higher cutoff would remove include tunnels with visible lane and wall geometry, a wide snowy intersection with buildings, wet nighttime streets with road layout, and intact forest trails. Some images are weak, yet a blanket cutoff cannot distinguish them from the two laptop-keyboard images or the two inverted frames. Inversion is a repairable orientation problem, not necessarily a reason to discard the reference. These are one engineer's visual judgments on a small sample, not human-label ground truth or an estimate of full-gallery defect prevalence.

The separate 100-image Qwen audit also becomes less convincing as the cutoff rises: among its randomly chosen images, Qwen semantic score was <=0.25 for 6/7 below 0.10, 2/3 in `0.10,0.15)`, and 3/10 in `[0.15,0.20)`. These sample sizes are too small for a precise error rate, and Qwen is another pseudo-labeler.

## Post-review cached SAGE impact

After the manual judgments were saved, the prespecified cutoff variants were evaluated against the heavily reused 1,184-query development set. The Qwen two-pass consensus set is held constant at **409 unique images**. All figures below use the original 112,163-reference gallery as the starting population and cached exact SAGE scores; the SAGE context reranker and final RAW accuracy were **not** recomputed.

| Image-only exclusion policy | Excluded | Retained | <=100 m coverage | Correct candidate in top100 | Original correct top1 reference removed |
|---|---:|---:|---:|---:|---:|
| Fixed gallery, no filter | 0 | 112,163 | 922 | 613 | 0 |
| Qwen consensus only | 409 | 111,754 | 922 | 613 | 2 |
| Qwen + SigLIP2 `<0.10` (current research v2) | 1,127 | 111,036 | 913 | 608 | 3 |
| Qwen + SigLIP2 `<0.15` | 1,835 | 110,328 | 908 | 605 | 3 |
| Qwen + SigLIP2 `<0.20` | 3,116 | 109,047 | 897 | 597 | 6 |

Compared with the current `<0.10` filter, raising to `<0.15` would remove **708 more** references, lose **5** more covered queries, and reduce top100 oracle by **3**. Raising to `<0.20` would remove **1,989 more** references, lose **16** more covered queries, and reduce top100 oracle by **11**. The individual query changes include both losses and occasional gains; the [impact artifact (`data/evaluation/gallery_quality_threshold_audit_v1_20260929/threshold_impact.json`; local artifact) records both directions. The development outcomes were used only for this post-review diagnostic; the sample judgments were not revised afterward.

## Recommendation

**Do not raise the global SigLIP2 cutoff to 0.15 or 0.20.** The manual sample contains many intact, potentially useful references, and the cached-retrieval comparison shows a growing coverage cost. Even the current 0.10 filter is an opt-in research artifact, not an established localization improvement. For more aggressive cleanup, review specific suspected defects or repair orientation rather than discard every low-scoring photo. No source photograph, production file, or frozen gallery artifact was modified; all 85 production hashes remain valid.

Reproduce the cached-score comparison:

```bash
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.audit_gallery_quality_thresholds
```
