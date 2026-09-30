"""Read-only local progress view for the blind annotation run."""
from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .common import OUT

HTML = Path(__file__).with_name("progress_dashboard.html")
RAW = OUT / "annotations/raw_qwen35_full1024"
MANIFEST = OUT / "annotation_input_manifest.jsonl"
FINAL_STAGES = (
    ("annotation_freeze.json", "Фиксация аннотаций"),
    ("analysis_receipt.json", "Анализ результатов"),
    ("final_integrity.json", "Проверка целостности"),
)


def status() -> dict:
    ids = [json.loads(line)["query_id"] for line in MANIFEST.read_text().splitlines()]
    total = len(ids)
    valid = failed = partial = 0
    passes = [0, 0, 0]
    terminal_times: list[float] = []
    for ident in ids:
        folder = RAW / ident
        present = [(folder / f"pass{number}.json").is_file() for number in (1, 2, 3)]
        passes = [count + int(exists) for count, exists in zip(passes, present, strict=True)]
        if all(present):
            valid += 1
            terminal_times.append((folder / "pass3.json").stat().st_mtime)
        elif (failure := folder / "failure.json").is_file():
            failed += 1
            terminal_times.append(failure.stat().st_mtime)
        elif (recovery_failure := folder / "recovery_failure.json").is_file():
            failed += 1
            terminal_times.append(recovery_failure.stat().st_mtime)
        elif any(present):
            partial += 1

    processed = valid + failed
    recent = sorted(terminal_times)[-60:]
    rate = None
    eta = None
    if len(recent) >= 3 and recent[-1] > recent[0]:
        rate = (len(recent) - 1) * 3600 / (recent[-1] - recent[0])
        if rate > 0 and processed < total:
            eta = (total - processed) * 3600 / rate

    stages = [{"name": name, "complete": (OUT / filename).is_file()}
              for filename, name in FINAL_STAGES]
    stages.insert(2, {"name": "Отчёт", "complete":
                   (OUT.parents[2] / "docs/geolocatability_v1_report.md").is_file()})
    running = subprocess.run(["pgrep", "-f", "^.*python -m ml\\.research\\.geolocatability_v1\\.run_chunks$"],
                             capture_output=True, check=False).returncode == 0
    if stages[-1]["complete"]:
        state = "complete"
    elif processed == total:
        state = "finalizing"
    elif running:
        state = "running"
    else:
        state = "stopped"
    return {
        "total": total, "valid": valid, "failed": failed, "processed": processed,
        "partial": partial, "pending": total - processed - partial,
        "passes": passes, "pass_total": total * 3,
        "rate_per_hour": round(rate, 1) if rate is not None else None,
        "eta_seconds": round(eta) if eta is not None else None,
        "last_result_at": (datetime.fromtimestamp(max(terminal_times), UTC).isoformat()
                           if terminal_times else None),
        "server_time": datetime.now(UTC).isoformat(),
        "state": state, "stages": stages,
    }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/api/status":
            try:
                payload = json.dumps(status(), ensure_ascii=False).encode()
                code = 200
            except Exception as exc:
                payload = json.dumps({"error": f"Не удалось прочитать прогресс: {type(exc).__name__}"}).encode()
                code = 503
            content_type = "application/json; charset=utf-8"
        elif self.path in ("/", "/index.html"):
            payload = HTML.read_bytes()
            code = 200
            content_type = "text/html; charset=utf-8"
        else:
            payload = b"Not found"
            code = 404
            content_type = "text/plain; charset=utf-8"
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        pass


def run(port: int) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(json.dumps({"url": f"http://127.0.0.1:{port}/", "started_at": time.time()}), flush=True)
    server.serve_forever(poll_interval=0.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8770)
    run(parser.parse_args().port)
