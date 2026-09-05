#!/usr/bin/env python3
"""Measure warm HTTP localization latency and bounded concurrency behavior."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _request(url: str, image: bytes) -> dict[str, Any]:
    boundary = "geosnap-benchmark-" + uuid.uuid4().hex
    body = (
        (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="image"; filename="smoke.jpg"\r\n'
            "Content-Type: image/jpeg\r\n\r\n"
        ).encode()
        + image
        + f"\r\n--{boundary}--\r\n".encode()
    )
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            payload = json.load(response)
            status_code = response.status
    except urllib.error.HTTPError as exc:
        status_code = exc.code
        try:
            payload = json.load(exc)
        except json.JSONDecodeError:
            payload = {"status": "non_json_error"}
    return {
        "http_status": status_code,
        "status": payload.get("status"),
        "latency_ms": (time.perf_counter() - started) * 1000.0,
        "diagnostics": payload.get("diagnostics", {}),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8080/api/localize")
    parser.add_argument("--image", type=Path, required=True)
    parser.add_argument("--warm-runs", type=int, default=10)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 2, 4])
    args = parser.parse_args()
    image = args.image.read_bytes()

    # One unreported request removes first-request allocations from warm timing.
    warmup = _request(args.url, image)
    if warmup["http_status"] != 200:
        raise SystemExit(f"warmup failed: {warmup}")
    warm = [_request(args.url, image) for _ in range(args.warm_runs)]
    latencies = [float(item["latency_ms"]) for item in warm]
    concurrency: dict[str, Any] = {}
    for value in args.concurrency:
        with ThreadPoolExecutor(max_workers=value) as executor:
            started = time.perf_counter()
            results = list(executor.map(lambda _: _request(args.url, image), range(value)))
            wall_ms = (time.perf_counter() - started) * 1000.0
        concurrency[str(value)] = {
            "wall_ms": wall_ms,
            "requests": results,
            "successes": sum(item["http_status"] == 200 for item in results),
        }
    diagnostics = [item["diagnostics"] for item in warm]
    print(
        json.dumps(
            {
                "warm_requests": len(warm),
                "warm_latency_ms": {
                    "min": min(latencies),
                    "mean": statistics.fmean(latencies),
                    "p50": _percentile(latencies, 0.50),
                    "p90": _percentile(latencies, 0.90),
                    "p95": _percentile(latencies, 0.95),
                    "max": max(latencies),
                },
                "stage_mean_ms": {
                    key: statistics.fmean(
                        float(item[key]) for item in diagnostics if item.get(key) is not None
                    )
                    for key in ("embedding_ms", "retrieval_ms", "policy_ms", "total_ms")
                },
                "concurrency": concurrency,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
