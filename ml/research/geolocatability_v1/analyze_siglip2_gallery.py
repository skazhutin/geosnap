"""Join frozen gallery scores to the separate Qwen extreme-defect review."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

from .common import json_once, sha256, verify_production

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data/evaluation/geolocatability_siglip2_gallery_v1_20260929"
DECISIONS = ROOT / "data/evaluation/gallery_extreme_quality_v1_20260929/candidate_review_decisions.jsonl"
REPORT = ROOT / "docs/siglip2_gallery_full_20260929.md"


def summarize(series: pd.Series) -> dict:
    values = series.dropna().to_numpy(dtype=float)
    if not len(values):
        return {"n": 0}
    return {"n": len(values), "median": round(float(np.median(values)), 5),
            "p25": round(float(np.quantile(values, 0.25)), 5),
            "p75": round(float(np.quantile(values, 0.75)), 5)}


def main() -> None:
    frozen = json.loads((OUT / "annotation_freeze.json").read_text())
    scores_path = OUT / "gallery_scores.parquet"
    if sha256(scores_path) != frozen["gallery_scores_parquet_sha256"]:
        raise RuntimeError("Frozen SigLIP2 annotation table changed")
    scores = pd.read_parquet(scores_path)
    assert len(scores) == scores.id.nunique() == 112163
    decisions = pd.DataFrame(json.loads(line) for line in DECISIONS.read_text().splitlines())
    assert len(decisions) == decisions.id.nunique() == 448
    joined = decisions[["id", "decision"]].merge(scores, on="id", validate="one_to_one")
    assert len(joined) == 448
    fields = ("geolocatability_teacher_estimate", "semantic_prompt_contrast")
    by_decision = {}
    for category, part in joined.groupby("decision"):
        by_decision[category] = {"count": len(part),
                                 **{field: summarize(part[field]) for field in fields}}
    contrast = joined[joined.decision.isin(
        ["proposed_extreme_exclusion", "keep"]) &
        joined.status.eq("ok")].copy()
    contrast["qwen_proposed"] = contrast.decision.eq("proposed_extreme_exclusion")
    assert contrast.qwen_proposed.nunique() == 2
    auc = {field: round(float(roc_auc_score(contrast.qwen_proposed,
                                           -contrast[field].to_numpy())), 5)
           for field in fields}
    comparison = {"gallery_scores_sha256": frozen["gallery_scores_parquet_sha256"],
                  "qwen_decisions_sha256": sha256(DECISIONS), "candidate_n": len(joined),
                  "qwen_candidate_comparison_n": len(contrast),
                  "by_qwen_decision": by_decision,
                  "auroc_predicting_qwen_proposal_with_lower_score": auc,
                  "Qwen_review_is_not_human_ground_truth": True,
                  "candidate_set_is_technical_preselection_not_representative_gallery": True,
                  "no_images_removed": True, "localization_outcomes_accessed": False,
                  "production": verify_production()}
    json_once(OUT / "candidate_comparison.json", comparison)
    bands = frozen["geolocatability_bands_0_0.2_0.4_0.6_0.8_1"]
    lines = [
        "# Full SigLIP2 gallery pass (2026-09-29)", "",
        f"The fixed gallery has **{frozen['gallery_count']:,}** unique references. SigLIP2 returned "
        f"scores for **{frozen['valid_count']:,}**; **{frozen['failure_count']}** have explicit "
        "failure records. Every source file used for a valid score was SHA-256 checked "
        "against the frozen gallery manifest. No photograph was removed or changed.", "",
        "The fitted score imitates Qwen's three-pass **semantic geolocatability "
        "pseudo-label**, not human ground truth and not technical defect probability. "
        "A second, unfitted score compares each SigLIP2 image feature with six frozen "
        "positive/negative scene descriptions. Neither uses GeoSnap localization outcomes.", "",
        "## Full-gallery score distribution", "",
        "| SigLIP2 teacher-estimate band | Images |", "|---|---:|",
        *[f"| {i/5:.1f}–{(i+1)/5:.1f} | {bands[str(i)]:,} |" for i in range(5)],
        "", "These fixed bands describe automatic scores only; they are not deletion "
        "thresholds and do not change any localization denominator.", "",
        "## Comparison within the separately selected extreme-defect shortlist", "",
        "| Two-pass Qwen decision | Images | SigLIP2 median | Middle 50% |", "|---|---:|---:|---:|",
    ]
    for category in ("proposed_extreme_exclusion", "disagreement_review", "keep",
                     "model_failure_review"):
        item = by_decision.get(category, {"count": 0, fields[0]: {"n": 0}})
        score = item[fields[0]]
        middle = (f"{score['p25']:.3f}–{score['p75']:.3f}" if score["n"] else "—")
        median = f"{score['median']:.3f}" if score["n"] else "—"
        lines.append(f"| {category} | {item['count']} | {median} | {middle} |")
    lines += ["", f"For the {len(contrast)} valid images with a two-pass Qwen proposal "
              "or two-pass keep result, lower SigLIP2 scores identify proposed "
              f"exclusions with AUROC **{auc[fields[0]]:.3f}**. The unfitted text "
              f"contrast gives **{auc[fields[1]]:.3f}**. This is agreement with a "
              "second automatic review on a preselected shortlist, not independent "
              "defect detection accuracy. The keep group is small, so this AUROC is "
              "unstable; disagreement cases are shown separately and excluded from it.", "",
              "## Integrity and next decision", "",
              f"Canonical JSONL SHA-256: `{frozen['gallery_scores_jsonl_sha256']}`. "
              f"Parquet SHA-256: `{frozen['gallery_scores_parquet_sha256']}`. "
              f"Gallery manifest SHA-256: `{frozen['gallery_manifest_sha256']}`. "
              "All 85 guarded production files passed final hash verification.", "",
              "Do not automatically exclude references based on these scores. "
              "Review a blind human sample of clear defects and high-quality street "
              "views, then validate any fixed threshold separately. Technical blur and "
              "semantic usefulness remain different targets.", ""]
    if REPORT.exists():
        raise FileExistsError(REPORT)
    REPORT.write_text("\n".join(lines))
    print(json.dumps({"candidate_n": len(joined), "auroc": auc,
                      "report": str(REPORT)}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
