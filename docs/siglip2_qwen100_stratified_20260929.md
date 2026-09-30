# Blind Qwen check of 100 SigLIP2 gallery scores (2026-09-29)

## Design and integrity

From the frozen 112,163-image gallery score file, a fixed seed (`20260929`) selected **20 images at random within each prespecified SigLIP2 band**: [0,.2), [.2,.4), [.4,.6), [.6,.8), [.8,1]. The selected IDs were shuffled. The blind input manifest (`data/evaluation/geolocatability_siglip2_gallery_qwen100_v1_20260929/blind_input_manifest.jsonl`; local artifact) contains only image ID, source path, and source SHA-256. It contains no SigLIP2 score, band, GeoSnap outcome, GPS, or candidate result.

Qwen3.5-4B-MLX-4bit (`mlx-community/Qwen3.5-4B-MLX-4bit`, revision `32f3e8ecf65426fc3306969496342d504bfa13f3`, checkpoint SHA-256 `5fb9acd0246866381cf8c5c354c6db1019f6498eec4ccb4f5edcc71ffeacb2db`) reviewed each image in **two independent deterministic passes**. Both prompts asked for separate technical-quality and semantic-geolocatability judgments using only visible pixels. These are shorter prompts than the original three-pass development-query Qwen annotation; their numeric scores should not be treated as exactly interchangeable with that annotation. The prompt and model contract (`data/evaluation/geolocatability_siglip2_gallery_qwen100_v1_20260929/qwen_contract.json`; local artifact) records the settings and prompt hashes.

The source disk was briefly unavailable before inference. This produced 100 explicit `source_failure.json` records, **no Qwen scores**. After reconnection, all 100 source files were found and checked against their manifest hashes; 200 valid raw Qwen passes were then saved. The Qwen-only annotations (`data/evaluation/geolocatability_siglip2_gallery_qwen100_v1_20260929/qwen_blind_annotations.jsonl`; local artifact) were frozen and hashed **before** joining the SigLIP2 scores. The comparison (`data/evaluation/geolocatability_siglip2_gallery_qwen100_v1_20260929/comparison.jsonl`; local artifact) has one row per image, including both scores and the two short Qwen explanations. No localization outcomes were accessed. All 85 guarded production files passed verification.

The photo-by-photo review index (`data/evaluation/geolocatability_siglip2_gallery_qwen100_v1_20260929/review_index.md`; local artifact) links all 100 original images alongside the paired scores and Qwen's visible-content reason.

## Results

| SigLIP2 band | Images | SigLIP2 median | Qwen median | Mean absolute difference | Qwen severe-defect votes on any pass |
|---|---:|---:|---:|---:|---:|
| 0–0.2 | 20 | 0.146 | 0.250 | 0.198 | 6 |
| 0.2–0.4 | 20 | 0.345 | 0.500 | 0.163 | 0 |
| 0.4–0.6 | 20 | 0.540 | 0.500 | 0.065 | 0 |
| 0.6–0.8 | 20 | 0.708 | 0.650 | 0.081 | 0 |
| 0.8–1.0 | 20 | 0.822 | 0.675 | 0.134 | 0 |

All 100 images received two valid Qwen answers. Overall rank agreement is Spearman **0.836** and mean absolute difference **0.128**. The rank correlation remains positive within providers: Mapillary **0.836** (n=33), MSLS **0.800** (n=58), KartaView **0.843** (n=9; highly uncertain). Two Qwen passes gave exactly the same geolocatability score for 64 images; their median absolute disagreement was zero. Eighteen images differ from SigLIP2 by at least 0.2, including six by at least 0.3. Of the six images Qwen marked as severe capture defects on at least one pass, only three received that label in both passes. All six fall in the lowest SigLIP2 band.

Qwen medians rise across SigLIP2 bands, so the small model **does broadly order scenes in the same direction as Qwen**. The low SigLIP2 band is not equivalent to technical rejection: even there, Qwen's median technical-quality score is 0.55, and only 3/20 images received two severe-defect votes. The top SigLIP2 band also should not be interpreted as near-certain geolocatability: Qwen's median there is 0.675, and its two-pass scores never reach 0.8 in this sample.

## Largest disagreements inspected

- Vegetation-heavy path with buildings visible in the distance (`gallery_expansion/mapillary/42b715df-a41d-5f71-9014-7ac1cc29896b_514812976359672.jpg`; local source image): SigLIP2 0.089; Qwen 0.50. Qwen sees some distant context, but the path and vegetation dominate; the picture is not clearly easy to localize.
- Night view through a dirty windshield (`mapillary/35ac6567-eafe-5b8b-8cdf-42714b528fc3_2185925574882287.jpg`; local source image): SigLIP2 0.386; Qwen 0.75. Street lights and road layout are visible, but rain/glare, a car ahead, and dashboard objects obscure much of the scene. Qwen's score appears optimistic.
- Blurred forest path (`msls/query/R8HNvLGCN3Q45FzDgvh8fg.jpg`; local source image): SigLIP2 0.138; Qwen 0.50. There is a visible path, but substantial motion blur and generic woodland; Qwen's moderate score should not be taken as proof the image is useful.
- Overexposed woodland path (`gallery_expansion/mapillary/bd44bf34-655e-5bb3-9ed2-368d7fcd2e24_479841883073441.jpg`; local source image): SigLIP2 0.167; Qwen 0.50. Qwen notes the weak distinctiveness but still chooses a midpoint score.

These four visual checks are interpretation of discrepancies, not relabeling of the blinded sample.

## Limits and decision

This is a **stratified comparison**, not a random sample of the whole gallery: 20 images were forced into each SigLIP2 band. The band mix and provider mix differ from the gallery, so the overall Spearman and defect prevalence cannot be extrapolated directly. Qwen is an automatic pseudo-labeler and the original target model family; agreement is not independent human accuracy. In particular, a low score may mean generic/weak visual evidence rather than broken pixels.

The check supports using SigLIP2 to **prioritize human review**, especially at the low end. It does **not** justify deleting images or setting an automatic rejection threshold. Any such threshold requires blinded human labels for both technical defects and semantic usefulness, plus a separate localization impact check.

Reproduce: `data/evaluation/geolocatability_v1_20260928/runtime/bin/python -m ml.research.geolocatability_v1.review_siglip2_qwen100 prepare`, then `run`, then `analyze`. The stages resume from immutable raw results and reject changed manifests, model weights, and prompt contracts.
