# Fast semantic image models for GeoSnap research (2026-09-29)

This is a frozen, outcome-blind **model-selection probe**, not a gallery-cleaning
decision or a localization experiment. Production and the fixed gallery were not
changed. Git revision: `da059324644cd47d6318725260ba8cd98f047bd7`.

## Question and protocol

The fixed gallery contains 112,163 photographs. A six-hour single-pass budget
requires at least 5.19 images/s end to end. The previous DINOv2-S gallery probe
measured 10.94 images/s on 2,048 evenly spaced original files, and its frozen
ridge head reached 0.09496 MAE against Qwen's three-pass median
geolocatability **pseudo-label** on a 220-image geographically grouped holdout.

We tested three small, instruction-following vision-language models on the same
32 outcome-blind query images. Each received one fixed short 0–4 prompt, generated
at most eight tokens with temperature zero, and had its raw answer preserved.
The 32 images are sufficient for a speed and format screen, not a reliable
quality ranking. Qwen labels were joined only after these raw answers were saved.

We then extracted frozen image features from MobileCLIP2-S0 and SigLIP2-Base
for all 1,184 development images. A ridge head was trained on 828 images and
evaluated on the same 220-image geographically grouped holdout as DINO; the
remaining images include the human-pilot geographic groups and are excluded
from this comparison. Ridge strength was chosen by inner grouped CV on the
training partition. Six fixed positive/negative text descriptions were also
scored without fitting any parameters. No GeoSnap outcomes, GPS, retrieval
scores, or failure buckets were used.

## Speed and teacher agreement

| Model / mode | Probe | Images/s | Full-gallery projection | Qwen agreement | Decision |
|---|---:|---:|---:|---|---|
| DINOv2-S + ridge | 2,048 original gallery files | 10.94 | 2.85 h | MAE 0.09496; Spearman 0.798 on grouped 220 | Previous baseline |
| SmolVLM2 256M, 0–4 response | 32 prepared queries | 0.864 | 36.1 h | MAE 0.364 on 32; 31/32 answers were 0 | Stop |
| SmolVLM2 500M, 0–4 response | 32 prepared queries | 0.779 | 40.0 h | MAE 0.233 on 32; 23/32 answers were 1 | Stop |
| LFM2.5-VL 450M 4-bit, 0–4 response | 32 prepared queries | 1.677 | 18.6 h | MAE 0.195 on 32; 31/32 answers were 2 | Stop |
| MobileCLIP2-S0, image features + ridge | 256 original gallery files | 17.98 | 1.73 h | MAE 0.17685; Spearman 0.170 on grouped 220 | Fast but weak on this target |
| SigLIP2-Base, image features + ridge | 2,048 original gallery files | **10.21** | **3.05 h** | **MAE 0.07938; Spearman 0.851** on grouped 220 | Best follow-up candidate |
| DINOv2-S + SigLIP2 features + ridge | Not timed as a combined pipeline | — | — | MAE 0.07778; Spearman 0.859 on grouped 220 | Small incremental teacher gain; requires both encoders |

The SigLIP2 MAE difference against DINO is **−0.01559**, with a paired
geographic-group bootstrap 95% interval of **[−0.02262, −0.00930]**. For the
combined DINO+SigLIP2 features the difference is −0.01719, interval
[−0.02350, −0.01116]. The teacher-low (Qwen score ≤0.4) AUROC is 0.917 for
SigLIP2 and 0.896 for DINO. These figures measure agreement with a particular
teacher and do **not** establish human defect-detection accuracy or actual
improvement in GeoSnap localization.

With **no fitted head**, SigLIP2's fixed positive-minus-negative text similarity
achieved Spearman 0.670 with Qwen geolocatability and AUROC 0.830 for the
teacher-low condition on the grouped 220. MobileCLIP2 achieved Spearman −0.031
and AUROC 0.545 with the same descriptions. The text formulation is one
preregistered probe, not a broad prompt search.

The two 2,048-file gallery timings include image reading, official preprocessing,
and MPS inference for each batch; they exclude initial model loading and the
one-time check that all sampled paths exist. MobileCLIP2's 256-file estimate is
shorter and less comparable. All full-gallery times are projections, not completed
112,163-image runs. Any full run must be resumable and measured end to end.

## Models and provenance

| Model | Verified checkpoint revision | License / role |
|---|---|---|
| [SmolVLM2 256M](https://huggingface.co/HuggingFaceTB/SmolVLM2-256M-Video-Instruct) / [MLX conversion](https://huggingface.co/mlx-community/SmolVLM2-256M-Video-Instruct-mlx) | MLX `79901655c1a3d7ed6646c91325e3fde47e40e611` | Apache-2.0; prompt-generating VLM |
| [SmolVLM2 500M](https://huggingface.co/HuggingFaceTB/SmolVLM2-500M-Video-Instruct) / [MLX conversion](https://huggingface.co/mlx-community/SmolVLM2-500M-Video-Instruct-mlx) | MLX `fa57db46815177fbdfd65cc85a2b3416a8332268` | Apache-2.0; prompt-generating VLM |
| [LFM2.5-VL 450M MLX](https://huggingface.co/LiquidAI/LFM2.5-VL-450M-MLX-4bit) | `f19926f17a25164d4cbcdc16d9eaf4714b807cfb` | LFM Open License v1.0; prompt-generating VLM |
| [MobileCLIP2-S0 OpenCLIP](https://huggingface.co/timm/MobileCLIP2-S0-OpenCLIP) / [Apple original](https://huggingface.co/apple/MobileCLIP2-S0) | `095906d28bf54d7584dc411e8ffe448f34289e05` | Apple research model license; research-only here |
| [SigLIP2-Base](https://huggingface.co/google/siglip2-base-patch16-224) | `75de2d55ec2d0b4efc50b3e9ad70dba96a7b2fa2` | Apache-2.0; image-text encoder; model card names WebLI training data |

Checkpoint SHA-256 values and exact inputs are in
`data/evaluation/geolocatability_fast_models_v1_20260929/model_provenance.json`,
`input_receipt.json`, and the per-model run receipts. Research use of a
research-restricted checkpoint does not imply permission to ship it in a product.

## Recommendation and limits

SigLIP2-Base is the one tested semantic alternative that is both fast enough on
original gallery files and better at imitating Qwen's semantic score on the
predeclared geographic holdout. The separate two-pass VLM review of **448
preselected extreme-defect candidates** is complete: 408 are proposed for
exclusion, 26 need disagreement review, 11 were kept, and three had model
failures. These are proposals, not gallery edits. Comparing SigLIP2 against
that narrow set and inspecting human disagreements would be necessary before
considering a gallery-wide scan.
Do not translate the 0–1 score directly into a deletion threshold: high
geolocatability, technical defect, and the usefulness of another photograph
are distinct properties. MobileCLIP2 and the three tested tiny generative VLMs
do not warrant a full-gallery pass on the present evidence.

Reproduction: `python -m ml.research.geolocatability_v1.benchmark_prompt_vlm`,
`extract_semantic_probe`, `evaluate_semantic_probe`, `score_semantic_prompts`,
`evaluate_semantic_prompts`, and `benchmark_semantic_gallery` with the model
arguments recorded in their receipts. The isolated Python runtimes and OpenCLIP
dependency tree are under `data/evaluation/`; no production dependency changed.
