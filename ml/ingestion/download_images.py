"""Download images from unified manifest."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
import requests

from ml.ingestion.common import download_file, ensure_parent

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def run(manifest_path: Path, errors_log: Path, retries: int, min_valid_size_bytes: int) -> None:
    df = pd.read_parquet(manifest_path)
    ensure_parent(errors_log)

    ok = 0
    failed = 0
    with requests.Session() as session:
        with errors_log.open("a", encoding="utf-8") as errors_fp:
            for _, row in df.iterrows():
                image_path = Path(str(row["image_path"]))
                url = row.get("download_url")
                if not url or str(url).lower() == "nan":
                    failed += 1
                    errors_fp.write(f"missing_url\t{row.get('id')}\n")
                    continue

                success = download_file(
                    str(url),
                    image_path,
                    retries=retries,
                    session=session,
                    min_valid_size_bytes=min_valid_size_bytes,
                )
                if success:
                    ok += 1
                else:
                    failed += 1
                    errors_fp.write(f"download_failed\t{row.get('id')}\t{url}\n")

                if (ok + failed) % 1000 == 0:
                    logger.info("download progress total=%s ok=%s failed=%s", ok + failed, ok, failed)

    logger.info("download done: ok=%s failed=%s", ok, failed)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download images listed in manifest")
    parser.add_argument("--manifest", default="data/raw/manifest.parquet")
    parser.add_argument("--errors-log", default="data/raw/download_errors.log")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--min-valid-size-bytes", type=int, default=10_000)
    args = parser.parse_args()

    run(
        manifest_path=Path(args.manifest),
        errors_log=Path(args.errors_log),
        retries=args.retries,
        min_valid_size_bytes=args.min_valid_size_bytes,
    )


if __name__ == "__main__":
    main()
