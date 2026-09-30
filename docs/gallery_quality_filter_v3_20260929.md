# Corrected research gallery quality policy (2026-09-29)

**Inverted frames count as bad inputs for this gallery filter.** The earlier phrase “fixable by rotation” described a possible remediation, but incorrectly treated these frames as acceptable under the user's requested **current-image quality** rule. This report supersedes that interpretation in the [threshold audit](gallery_quality_threshold_audit_20260929.md). The original outcome-blind visual observations remain preserved; a separate policy adjudication (`data/evaluation/gallery_quality_filter_v3_20260929/manual_policy_adjudication_20.jsonl`; local artifact) marks both inverted frames and both non-street laptop images as bad.

Of the 20 randomly reviewed images in the 0.10–0.20 SigLIP2 range, **4/20 are bad by this policy**: two inverted outdoor frames and two laptop close-ups. Eleven more contain weak but visible outdoor scenes; five have visibly usable street/scene structure. This is a small manual review, not an estimated gallery-wide defect fraction. It does not justify discarding every image with a SigLIP2 score below 0.15 or 0.20: doing so would remove many nighttime roads, tunnels, intersections, and trails whose scene content remains visible.

The corrected version-3 research gallery (`data/evaluation/gallery_quality_filter_v3_20260929/gallery_filtered.parquet`; local artifact) keeps the version-2 `<0.10` SigLIP2 cutoff and the frozen two-pass Qwen exclusions, and additionally removes all four manually confirmed bad images. It excludes **1,131 of 112,163** references and retains **111,032**. The exclusion audit (`data/evaluation/gallery_quality_filter_v3_20260929/excluded.jsonl`; local artifact) records the exact basis for each removal. The four new exclusions are Mapillary references. No source photos, frozen gallery artifacts, or production files were changed.

After freezing the corrected filter, cached SAGE exact-score evaluation on the heavily reused 1,184-query development set found the same aggregate result as version 2: **913** queries retain a <=100 m reference, and **608** have one in the exact-score top100. The four corrections changed neither aggregate. This is post-freeze diagnostic evidence (`data/evaluation/gallery_quality_filter_v3_20260929/postfreeze_impact.json`; local artifact), not a full context-reranked RAW accuracy result. Compared with the fixed gallery, both figures remain lower (922 coverage and 613 top100 oracle before filtering).

The threshold comparison still argues against a blanket increase: at `<0.15`, 1,835 references would be excluded, coverage would be 908, and top100 oracle 605; at `<0.20`, 3,116 would be excluded, coverage 897, and top100 oracle 597. Those exploratory figures were computed before the four manual exclusions, but they do not become a reason to raise the cutoff after relabeling the inverted frames. A targeted orientation/non-street detector or more human review is a better way to find additional bad images than globally increasing the semantic-utility threshold.

Reproduce and verify:

```bash
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.build_filtered_gallery_v3
data/evaluation/geographic_v8_20260928/runtime/bin/python -m ml.research.geolocatability_v1.analyze_filtered_gallery_v3
```

The v3 filter receipt (`data/evaluation/gallery_quality_filter_v3_20260929/filter_receipt.json`; local artifact) hashes its source gallery, prior filter, manual review, and outputs. All 85 protected production files passed verification.
