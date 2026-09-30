"""Continue independent development experiments after current extraction jobs.

This one-off local runner never opens calibration or final. Every subprocess
is fail-fast, and all large embedding stages retain existing checkpoints.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from ml.research.prepare_queries import ROOT
from ml.research.seal import sha256


def run(module, *args):
    label = module.rsplit(".", 1)[-1] + "_" + str(int(time.time()))
    log = ROOT / "development/stage_logs" / f"{label}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    print("starting", module, *args, flush=True)
    with log.open("w") as stream:
        subprocess.run(
            [sys.executable, "-m", module, *map(str, args)], stdout=stream, stderr=subprocess.STDOUT, check=True
        )
    print("completed", module, "log", log, flush=True)


def await_embeddings(path):
    # Final metadata is the embedding job's atomic commit record.
    while not (path / "build_metadata.json").exists():
        time.sleep(15)
    metadata = json.loads((path / "build_metadata.json").read_text())
    if metadata["failure_count"]:
        raise RuntimeError("incomplete reference extraction; do not compare filtered galleries")


def localize_gallery(model, split):
    queries = (
        Path("data/evaluation/moscow_real_v4/development_queries.parquet")
        if split == "historical"
        else ROOT / "prospective/development.parquet"
    )
    for arm, filename in [
        ("baseline", "data/evaluation/moscow_real_v4/gallery.parquet"),
        ("random", ROOT / "gallery_expansion/gallery_random.parquet"),
        ("diversity", ROOT / "gallery_expansion/gallery_diversity.parquet"),
        ("union_larger_budget", ROOT / "gallery_expansion/gallery_union.parquet"),
    ]:
        run(
            "ml.research.localize_stream",
            "--stream",
            ROOT / f"development/gallery_{model}_{split}/retrieval.json",
            "--queries",
            queries,
            "--gallery",
            filename,
            "--method",
            arm,
            "--output",
            ROOT / f"development/gallery_localized_{model}_{split}/{arm}",
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=["gallery-b", "gallery-l", "boq", "full", "fusion"],
        default=["gallery-b", "gallery-l", "boq", "full"],
    )
    parser.add_argument("--query-device", choices=["cpu", "mps"], default="mps")
    args = parser.parse_args()
    external = Path(json.loads((ROOT / "storage.json").read_text())["external_root"])
    if not external.is_dir():
        raise RuntimeError("external research volume is unavailable")
    if "gallery-b" in args.stages:
        await_embeddings(Path("data/embeddings/moscow_research_v5/gallery_union/sage-vitb"))
    for split in ("historical", "new") if "gallery-b" in args.stages else ():
        run("ml.research.gallery_experiment", "--model", "sage-vitb", "--split", split)
        queries = (
            Path("data/evaluation/moscow_real_v4/development_queries.parquet")
            if split == "historical"
            else ROOT / "prospective/development.parquet"
        )
        for arm, filename in [
            ("baseline", "data/evaluation/moscow_real_v4/gallery.parquet"),
            ("random", ROOT / "gallery_expansion/gallery_random.parquet"),
            ("diversity", ROOT / "gallery_expansion/gallery_diversity.parquet"),
            ("union_larger_budget", ROOT / "gallery_expansion/gallery_union.parquet"),
        ]:
            run(
                "ml.research.localize_stream",
                "--stream",
                ROOT / f"development/gallery_sage-vitb_{split}/retrieval.json",
                "--queries",
                queries,
                "--gallery",
                filename,
                "--method",
                arm,
                "--output",
                ROOT / f"development/gallery_localized_sage-vitb_{split}/{arm}",
            )
    # Use the freed accelerator slot to test the data/model interaction.
    if "gallery-l" in args.stages:
        run(
            "ml.research.embed_global",
            "--model",
            "sage-vitl",
            "--manifest",
            ROOT / "gallery_expansion/audited_union.parquet",
            "--output",
            "data/embeddings/moscow_research_v5/gallery_union/sage-vitl",
            "--batch-size",
            "4",
        )
    for split in ("historical", "new") if "gallery-l" in args.stages else ():
        run("ml.research.gallery_experiment", "--model", "sage-vitl", "--split", split)
        localize_gallery("sage-vitl", split)
    # The independent BoQ job was started separately. Do not start a duplicate.
    if "boq" in args.stages:
        await_embeddings(Path("data/embeddings/moscow_research_v5/boq"))
    for split in ("historical", "new") if "boq" in args.stages else ():
        run("ml.research.global_experiment", "--model", "boq", "--split", split, "--device", args.query_device)
        queries = (
            Path("data/evaluation/moscow_real_v4/development_queries.parquet")
            if split == "historical"
            else ROOT / "prospective/development.parquet"
        )
        run(
            "ml.research.localize_stream",
            "--stream",
            ROOT / f"development/global_boq_{split}/retrieval.json",
            "--queries",
            queries,
            "--output",
            ROOT / f"development/localized_boq_{split}",
        )
    link = Path("data/embeddings/moscow_research_v5/sage-full-features")
    if "full" in args.stages:
        run("ml.research.embed_global", "--model", "sage-full-features", "--output", link, "--batch-size", "4")
    for split in ("historical", "new") if "full" in args.stages else ():
        run(
            "ml.research.global_experiment",
            "--model",
            "sage-full-features",
            "--split",
            split,
            "--device",
            args.query_device,
        )
        run("ml.research.context_rerank", "--split", split)
    if "fusion" in args.stages:
        await_embeddings(Path("data/embeddings/moscow_research_v5/gallery_union/sage-vitl"))
        for split in ("historical", "new"):
            run("ml.research.gallery_fusion", "--split", split)
            queries = (
                Path("data/evaluation/moscow_real_v4/development_queries.parquet")
                if split == "historical"
                else ROOT / "prospective/development.parquet"
            )
            for arm, filename in [
                ("baseline", Path("data/evaluation/moscow_real_v4/gallery.parquet")),
                ("random", ROOT / "gallery_expansion/gallery_random.parquet"),
                ("diversity", ROOT / "gallery_expansion/gallery_diversity.parquet"),
                ("union_larger_budget", ROOT / "gallery_expansion/gallery_union.parquet"),
            ]:
                run(
                    "ml.research.localize_stream",
                    "--stream",
                    ROOT / f"development/gallery_fusion_{split}/{arm}/retrieval.json",
                    "--queries",
                    queries,
                    "--gallery",
                    filename,
                    "--output",
                    ROOT / f"development/gallery_fusion_localized_{split}/{arm}",
                )
    (ROOT / f"development/next_stage_{'_'.join(args.stages)}_completed.json").write_text(
        json.dumps(
            {
                "status": "development stages completed",
                "runner_sha256": sha256(Path(__file__)),
                "completed_stages": args.stages,
                "calibration_opened": False,
                "final_opened": False,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
