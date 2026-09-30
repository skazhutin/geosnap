"""Build a separately sealed recent-photography stress cohort without inference.

The original prospective seal remains unchanged. Selection uses metadata,
public development locations for a cross-split embargo, and known author aliases;
never model scores, gallery coverage or primary held-out coordinates.
"""

from __future__ import annotations

import json
import re
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import h3
import pandas as pd
import requests

from ml.evaluation.moscow_split import _PerceptualHashIndex
from ml.research.commons_discovery import OUT
from ml.research.commons_provenance import normalized
from ml.research.prepare_queries import ROOT, distant_mask, score
from ml.research.seal import sha256, write_once
from ml.research.seal_queries import image_identity

BUNDLE = ROOT / "recent_commons"
OUTDOOR = re.compile(
    r"street|building|house|facade|façade|road|square|courtyard|cityscape|embankment|bridge|exterior|улиц|здани|площад|набереж|фасад|двор",
    re.I,
)
NONSTREET = re.compile(
    r"interior|indoors|aerial|drone|airplane|painting|drawing|\bmaps? of\b|интерьер|аэрофото|картина|рисунок", re.I
)


def plan():
    existing = BUNDLE / "plan.json"
    if existing.exists():
        return json.loads(existing.read_text())
    eligible_path = OUT / "metadata_eligible_extended.json"
    rows = json.loads(eligible_path.read_text())
    history_paths = sorted(Path("data/evaluation").glob("moscow_real*/*.parquet"))
    exposed = set()
    for path in [
        *history_paths,
        ROOT / "prospective/development.parquet",
        ROOT / "gallery_expansion/gallery_union.parquet",
    ]:
        frame = pd.read_parquet(path, columns=["attribution"])
        for value in frame.attribution.dropna():
            if str(value).startswith("Mapillary image by "):
                exposed.add(normalized(str(value).removeprefix("Mapillary image by ")))
    counts = Counter()
    valid = []
    for row in rows:
        text = " ".join(str(row.get(k) or "") for k in ["title", "categories", "description"])
        if NONSTREET.search(text) or not OUTDOOR.search(text):
            counts["not_metadata_street_exterior_population"] += 1
            continue
        if any(a.split(":", 1)[1] in exposed for a in row["authors"]):
            counts["known_matching_provider_handle"] += 1
            continue
        url = urlparse(row["url"])
        if url.scheme != "https" or url.hostname != "upload.wikimedia.org":
            counts["unregistered_image_host"] += 1
            continue
        row = dict(
            row, author_group="|".join(row["authors"]), selection_hash=score("recent-commons-final", row["pageid"])
        )
        valid.append(row)
    frame = pd.DataFrame(valid).sort_values("selection_hash")
    public_dev = pd.concat(
        [
            pd.read_parquet("data/evaluation/moscow_real_v4/development_queries.parquet"),
            pd.read_parquet(ROOT / "prospective/development.parquet"),
        ],
        ignore_index=True,
    )
    mask = distant_mask(frame, public_dev, 250)
    counts["within_250m_of_public_development"] = int((~mask).sum())
    frame = frame[mask].copy()
    frame["author_rank"] = frame.groupby("author_group").cumcount()
    frame = frame.sort_values(["author_rank", "selection_hash"])
    kept, authors, sessions = [], Counter(), Counter()
    for row in frame.to_dict("records"):
        author, day = row["author_group"], row["captured_at"][:10]
        if authors[author] >= 50 or sessions[(author, day)] >= 5:
            counts["author_or_capture_day_cap"] += 1
            continue
        if kept and not distant_mask(pd.DataFrame([row]), pd.DataFrame(kept), 30)[0]:
            counts["within_30m_of_another_selected_query"] += 1
            continue
        kept.append(row)
        authors[author] += 1
        sessions[(author, day)] += 1
        if len(kept) >= 300:
            break
    if len(kept) < 100:
        raise ValueError(f"only {len(kept)} metadata candidates; do not relax independence to fill a quota")
    BUNDLE.mkdir(parents=True, exist_ok=True)
    assigned = BUNDLE / "private/assigned.json"
    write_once(assigned, {"queries": kept})
    report = {
        "status": "fixed_before_image_download_and_inference",
        "purpose": "separate recent-camera street-exterior stress cohort",
        "candidate_population_sha256": sha256(eligible_path),
        "source_population_rows": len(rows),
        "assigned_sha256": sha256(assigned),
        "assigned_count": len(kept),
        "target": 300,
        "metadata_exclusions": dict(counts),
        "author_groups": len(authors),
        "capture_period": "2024-01-01 through 2026-09-06; original EXIF datetime required",
        "author_cap": 50,
        "author_day_cap": 5,
        "spacing_m": 30,
        "public_development_embargo_m": 250,
        "query_model_scores_used": False,
        "nearby_gallery_positive_required": False,
        "primary_heldout_manifest_or_images_read": False,
        "original_prospective_seal_sha256": sha256(ROOT / "prospective/seal.json"),
        "evaluation": "same single frozen baseline/candidate transaction; threshold transfers from primary calibration; report separately, no pooled denominator or retuning",
        "limitations": [
            "metadata-screened street content, not independently visually certified",
            "camera EXIF and template agree but are not independent ground-truth measurements",
            "known author aliases excluded; anonymous provider identities and unknown aliases remain unresolved",
            "no use of primary private query fingerprints; cross-cohort duplicates, if any, must be disclosed after the joint final transaction",
        ],
    }
    write_once(existing, report)
    return report


def download(session, url, path):
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".part")
    for attempt in range(5):
        with session.get(url, stream=True, timeout=(10, 45)) as response:
            if response.status_code in {429, 502, 503, 504}:
                value = response.headers.get("Retry-After", "")
                delay = int(value) if value.isdigit() else 15 * (attempt + 1)
                print("Commons image transient response", response.status_code, "retry after", delay, flush=True)
                while delay > 0:
                    time.sleep(min(delay, 30))
                    delay -= 30
                continue
            response.raise_for_status()
            size = 0
            with temporary.open("wb") as stream:
                for chunk in response.iter_content(1024 * 1024):
                    size += len(chunk)
                    if size > 20 * 1024**2:
                        raise ValueError("image exceeds fixed 20 MiB acquisition limit")
                    stream.write(chunk)
            temporary.replace(path)
            time.sleep(1)
            return
    raise RuntimeError("image remains unavailable after bounded retries")


def run():
    if (BUNDLE / "seal.json").exists():
        raise ValueError("supplemental cohort already sealed; never rebuild it")
    fixed = plan()
    assigned = BUNDLE / "private/assigned.json"
    if sha256(assigned) != fixed["assigned_sha256"]:
        raise ValueError("fixed assignment changed")
    history_paths = sorted(Path("data/evaluation").glob("moscow_real*/*.parquet"))
    audit_paths = [
        *history_paths,
        ROOT / "prospective/development.parquet",
        ROOT / "gallery_expansion/gallery_union.parquet",
    ]
    reference = pd.concat(
        [pd.read_parquet(p, columns=["file_sha256", "perceptual_hash"]) for p in audit_paths], ignore_index=True
    )
    exact = set(reference.file_sha256.dropna().astype(str))
    index, count = _PerceptualHashIndex(4), 0
    for value in reference.perceptual_hash.dropna().astype(str).unique():
        index.add(count, int(value, 16))
        count += 1
    session = requests.Session()
    session.headers["User-Agent"] = "GeoSnapResearch/0.1 (bounded public-photo benchmark acquisition)"
    kept, exclusions = [], []
    for i, row in enumerate(json.loads(assigned.read_text())["queries"]):
        path = BUNDLE / "private/images" / (str(row["pageid"]) + ".jpg")
        try:
            download(session, row["url"], path)
            identity, hashes = image_identity(path)
        except (OSError, ValueError, RuntimeError, requests.RequestException) as exc:
            exclusions.append(
                {"pageid": row["pageid"], "reason": "unavailable_or_undecodable", "error_type": type(exc).__name__}
            )
            continue
        if identity["file_sha256"] in exact or any(index.matches(v) for v in hashes):
            exclusions.append({"pageid": row["pageid"], "reason": "exact_or_near_duplicate"})
            continue
        exact.add(identity["file_sha256"])
        for value in hashes:
            index.add(count, value)
            count += 1
        kept.append(
            dict(
                row,
                **identity,
                id=f"commons:{row['pageid']}",
                source="wikimedia_commons",
                image_path=str(path),
                sequence_id=row["author_group"] + "::" + row["captured_at"][:10],
                h3_coarse=h3.latlng_to_cell(row["lat"], row["lon"], 6),
                evaluation_split="final",
                attribution=row["artist_html"],
            )
        )
        if (i + 1) % 20 == 0:
            print("audited recent photographs", i + 1, "/", fixed["assigned_count"], "retained", len(kept), flush=True)
    if len(kept) < 100:
        (BUNDLE / "acquisition_failure.json").write_text(
            json.dumps({"retained": len(kept), "exclusions": exclusions}, indent=2) + "\n"
        )
        raise ValueError("fewer than 100 valid assigned photographs; no replacement sampling")
    manifest = BUNDLE / "private/final_queries.parquet"
    pd.DataFrame(kept).to_parquet(manifest, index=False)
    write_once(
        BUNDLE / "seal.json",
        {
            "status": "sealed_before_inference",
            "kind": "supplementary_recent_camera_cohort",
            "plan_sha256": sha256(BUNDLE / "plan.json"),
            "splits": {
                "final": {"path": "private/final_queries.parquet", "sha256": sha256(manifest), "count": len(kept)}
            },
            "acquisition_exclusions": exclusions,
            "reference_and_public_development_audit_sha256": {str(p): sha256(p) for p in audit_paths},
            "model_inference_performed": False,
            "original_prospective_seal_sha256": fixed["original_prospective_seal_sha256"],
        },
    )
    print("Recent-photography supplementary cohort sealed", len(kept), "queries; no inference", flush=True)


if __name__ == "__main__":
    run()
