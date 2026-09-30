# Full SigLIP2 gallery pass (2026-09-29)

The fixed gallery has **112,163** unique references. SigLIP2 returned scores for **112,163**; **0** have explicit failure records. Every source file used for a valid score was SHA-256 checked against the frozen gallery manifest. No photograph was removed or changed.

The fitted score imitates Qwen's three-pass **semantic geolocatability pseudo-label**, not human ground truth and not technical defect probability. A second, unfitted score compares each SigLIP2 image feature with six frozen positive/negative scene descriptions. Neither uses GeoSnap localization outcomes.

## Full-gallery score distribution

| SigLIP2 teacher-estimate band | Images |
|---|---:|
| 0.0–0.2 | 3,108 |
| 0.2–0.4 | 15,373 |
| 0.4–0.6 | 43,006 |
| 0.6–0.8 | 48,912 |
| 0.8–1.0 | 1,764 |

These fixed bands describe automatic scores only; they are not deletion thresholds and do not change any localization denominator.

## Comparison within the separately selected extreme-defect shortlist

| Two-pass Qwen decision | Images | SigLIP2 median | Middle 50% |
|---|---:|---:|---:|
| proposed_extreme_exclusion | 408 | 0.047 | 0.003–0.090 |
| disagreement_review | 26 | 0.072 | 0.022–0.143 |
| keep | 11 | 0.334 | 0.183–0.421 |
| model_failure_review | 3 | 0.308 | 0.305–0.429 |

For the 419 valid images with a two-pass Qwen proposal or two-pass keep result, lower SigLIP2 scores identify proposed exclusions with AUROC **0.956**. The unfitted text contrast gives **0.830**. This is agreement with a second automatic review on a preselected shortlist, not independent defect detection accuracy. The keep group is small, so this AUROC is unstable; disagreement cases are shown separately and excluded from it.

## Integrity and next decision

Canonical JSONL SHA-256: `90796ebf8c995686d093a131fe3a036e325b51971906d17fedaffdd5868be0e9`. Parquet SHA-256: `1ce0b6e9048905dd03c8475b5c8b1c3368ee57e7ceaca7e9ebdf6c8965d5663b`. Gallery manifest SHA-256: `8d92a0bdf3492c4a73ce4032ab0966b3380794ab2860d2d68c8cfd5d7b9d094a`. All 85 guarded production files passed final hash verification.

Do not automatically exclude references based on these scores. Review a blind human sample of clear defects and high-quality street views, then validate any fixed threshold separately. Technical blur and semantic usefulness remain different targets.
