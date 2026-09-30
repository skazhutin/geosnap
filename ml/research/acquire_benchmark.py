"""Resume assignment and bounded image downloads after metadata discovery completes."""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

from ml.ingestion.download_images import run as download
from ml.research.gallery_plan import run as gallery_plan
from ml.research.prepare_queries import ROOT
from ml.research.prepare_queries import run as prepare
from ml.research.seal import sha256


def run():
    discovery = ROOT / "acquisition/mapillary_discovery.json"
    # The independent metadata worker writes this atomically only on completion.
    while not discovery.is_file():
        time.sleep(15)
    prospective = ROOT / "prospective"
    if not (prospective / "assignment.json").exists():
        prepare(discovery)
    if not (ROOT / "gallery_expansion/plan.json").exists():
        gallery_plan(discovery, 6000)
    assignment = json.loads((prospective / "assignment.json").read_text())
    stages = [(stage, Path(entry["path"]), entry["sha256"]) for stage, entry in assignment["private_manifests"].items()]
    plan = json.loads((ROOT / "gallery_expansion/plan.json").read_text())
    path = ROOT / "gallery_expansion/download_union.parquet"
    stages.append(("gallery_union", path, plan["files"]["download_union.parquet"]))
    for stage, path, digest in stages:
        if sha256(path) != digest:
            raise ValueError("assigned download manifest changed")
        if shutil.disk_usage(ROOT).free < 5 * 1024**3:
            raise RuntimeError(
                "less than 5 GiB free; download stopped before exhausting disk; resume after space is available"
            )
        print("starting bounded downloads:", stage, flush=True)
        stats = download(
            path,
            ROOT / f"acquisition/{stage}.errors.json",
            retries=2,
            min_valid_size_bytes=1000,
            workers=8,
            timeout_sec=20,
            min_width=64,
            min_height=64,
            stats_path=ROOT / f"acquisition/{stage}.download.json",
        )
        print("completed", stage, json.dumps(stats), flush=True)
    print(
        "Downloads complete. Duplicate/provenance audit and sealing are separate; no inference has run on new queries.",
        flush=True,
    )


if __name__ == "__main__":
    run()
