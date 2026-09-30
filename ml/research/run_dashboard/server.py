"""Local, read-only progress server for current and future research runs."""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import subprocess
import time
from datetime import UTC, datetime
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlsplit

from PIL import Image, ImageOps

from ml.research.geolocatability_v1.gallery_extreme_candidates import reasons as technical_reasons

ROOT = Path(__file__).resolve().parents[3]
ASSETS = Path(__file__).parent
GALLERY_DIR = ROOT / "data/evaluation/gallery_extreme_quality_v1_20260929"
GALLERY_MANIFEST = ROOT / "data/evaluation/geographic_v8_20260928/staged/gallery.parquet"
RUN_FILES = ROOT / "data/evaluation/research_run_dashboard/runs"
GALLERY_TOTAL = 112_163
CHUNK_TOTAL = 225


def iso(timestamp: float | None = None) -> str:
    return datetime.fromtimestamp(timestamp or time.time(), UTC).isoformat()


def process_lines() -> list[str]:
    result = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True, text=True, check=False)
    return result.stdout.splitlines()


def process_active(lines: list[str], fragment: str) -> bool:
    return any(fragment in line and "run_dashboard.server" not in line for line in lines)


def recent_rate(samples: list[tuple[int, float]]) -> tuple[float | None, list[dict]]:
    if len(samples) < 2:
        return None, []
    samples = sorted(samples, key=lambda item: item[1])[-20:]
    points = []
    for (first_count, first_time), (second_count, second_time) in zip(samples, samples[1:], strict=False):
        delta = second_time - first_time
        if delta > 0:
            points.append({"at": iso(second_time), "per_second": round((second_count - first_count) / delta, 2)})
    window = samples[-10:]
    span = window[-1][1] - window[0][1]
    rate = (window[-1][0] - window[0][0]) / span if span > 0 else None
    return rate, points


def scan_chunks() -> list[tuple[int, int, Path, float]]:
    chunks = []
    for path in (GALLERY_DIR / "chunks").glob("*.jsonl"):
        match = re.fullmatch(r"(\d{6})-(\d{6})\.jsonl", path.name)
        if not match or not path.with_suffix(".sha256").is_file():
            continue
        start, end = map(int, match.groups())
        chunks.append((start, end, path, path.stat().st_mtime))
    return sorted(chunks)


def last_images(path: Path, count: int = 7) -> list[dict]:
    try:
        records = [json.loads(line) for line in path.read_text().splitlines()]
    except (OSError, ValueError):
        return []
    if not records:
        return []
    suspicious = [index for index, row in enumerate(records) if technical_reasons(row)]
    regular = [round(index * (len(records) - 1) / max(count - 1, 1)) for index in range(min(count, len(records)))]
    positions = list(dict.fromkeys([*suspicious[:2], *regular]))[:count]
    output = []
    for position in positions:
        row = records[position]
        output.append({"id": row["id"], "source": row["source"], "status": row["status"],
                       "image_url": f"/image/{quote(row['id'], safe='')}", "index": position,
                       "suspicious": bool(technical_reasons(row))})
    return output


def gallery_run(lines: list[str]) -> dict | None:
    contract = GALLERY_DIR / "contract.json"
    if not contract.exists():
        return None
    chunks = scan_chunks()
    completed = sum(end - start + 1 for start, end, _, _ in chunks)
    samples = [(sum(item[1] - item[0] + 1 for item in chunks[:i + 1]), item[3])
               for i, item in enumerate(chunks)]
    rate, history = recent_rate(samples)
    latest = max(chunks, key=lambda item: item[3]) if chunks else None
    scanner_running = process_active(lines, "ml.research.geolocatability_v1.scan_gallery_technical")
    vlm_running = process_active(lines, "ml.research.geolocatability_v1.gallery_extreme_vlm_review")
    candidate_file = GALLERY_DIR / "full_gallery_candidates.jsonl"
    candidates = [json.loads(line) for line in candidate_file.read_text().splitlines()] if candidate_file.exists() else []
    candidate_count = len(candidates) if candidate_file.exists() else None
    raw = GALLERY_DIR / "vlm_review/full_gallery_candidates/raw"
    pass_files = list(raw.glob("*/pass[12].json")) if raw.exists() else []
    reviewed_passes = len(pass_files)
    finished = (GALLERY_DIR / "final_review_receipt.json").exists()
    now = time.time()
    last_at = max([path.stat().st_mtime for path in pass_files], default=latest[3] if latest else None)
    if finished:
        phase, state = "Итоги проверки", "complete"
        current, total, unit = candidate_count or 0, candidate_count or 0, "снимков"
        current_rate = None
    elif completed < GALLERY_TOTAL:
        phase = "Техническая проверка"
        stale = latest is not None and now - latest[3] > 300
        state = "stalled" if scanner_running and stale else "running" if scanner_running else "stopped"
        current, total, unit = completed, GALLERY_TOTAL, "снимков"
        current_rate = rate if scanner_running else None
    elif not candidate_file.exists():
        phase, state = "Формируем список для перепроверки", "waiting"
        current, total, unit = completed, GALLERY_TOTAL, "снимков"
        current_rate = None
    else:
        phase = "Проверка подозрительных кадров"
        latest_pass_at = max([path.stat().st_mtime for path in pass_files], default=0)
        state = "running" if vlm_running else "stalled" if reviewed_passes and now - latest_pass_at > 300 else "waiting"
        current, total, unit = reviewed_passes, (candidate_count or 0) * 2, "проверок"
        pass_times = sorted(path.stat().st_mtime for path in pass_files)[-30:]
        current_rate = (len(pass_times) - 1) / (pass_times[-1] - pass_times[0]) if vlm_running and len(pass_times) >= 2 and pass_times[-1] > pass_times[0] else None
        history = []
    eta = (total - current) / current_rate if current_rate and current < total else None
    stages = [
        {"name": "Измерить качество", "done": completed == GALLERY_TOTAL, "detail": f"{completed:,} / {GALLERY_TOTAL:,}"},
        {"name": "Найти подозрительные", "done": candidate_file.exists(), "detail": f"{candidate_count:,} кадров" if candidate_count is not None else "после полного прохода"},
        {"name": "Две независимые VLM-проверки", "done": candidate_count is not None and reviewed_passes >= candidate_count * 2,
         "detail": f"{reviewed_passes:,} / {(candidate_count or 0) * 2:,}" if candidate_count is not None else "ожидает"},
        {"name": "Сверить результаты", "done": finished, "detail": "список исключений" if finished else "ожидает"},
    ]
    grid = [{"index": i, "state": "done" if i < len(chunks) else "pending",
             "start": i * 500 + 1, "end": min((i + 1) * 500, GALLERY_TOTAL)} for i in range(CHUNK_TOTAL)]
    if not finished and completed < GALLERY_TOTAL and len(chunks) < CHUNK_TOTAL:
        grid[len(chunks)]["state"] = "working" if scanner_running else "pending"
    photos = last_images(latest[2]) if latest else []
    if pass_files and candidate_file.exists():
        recent_ids = list(dict.fromkeys(path.parent.name for path in sorted(pass_files, key=lambda p: p.stat().st_mtime, reverse=True)))[:7]
        source_by_id = {item["id"]: item["source"] for item in candidates}
        photos = [{"id": ident, "source": source_by_id.get(ident, ""), "status": "vlm_review",
                   "image_url": f"/image/{quote(ident, safe='')}"} for ident in recent_ids]
    return {"id": "gallery-extreme-20260929", "title": "Качество базы снимков", "description": "112 163 фотографий фиксированной галереи",
            "phase": phase, "state": state, "completed": current, "total": total, "unit": unit,
            "rate_per_second": round(current_rate, 2) if current_rate is not None else None,
            "eta_seconds": round(eta) if eta is not None else None, "last_update": iso(last_at) if last_at else None,
            "gallery_scanned": completed, "candidate_count": candidate_count,
            "reviewed_passes": reviewed_passes, "stages": stages, "grid": grid,
            "history": history, "photos": photos,
            "note": "Кадры появляются после сохранения каждой партии из 500 снимков. Оригиналы не изменяются."}


def registered_runs() -> list[dict]:
    runs = []
    for path in RUN_FILES.glob("*.json"):
        try:
            value = json.loads(path.read_text())
            required = {"id", "title", "phase", "state", "completed", "total", "updated_at"}
            if not required.issubset(value) or not isinstance(value["completed"], (int, float)) or not isinstance(value["total"], (int, float)):
                continue
            state = value["state"]
            if state == "running" and time.time() - datetime.fromisoformat(value["updated_at"]).timestamp() > 300:
                state = "stalled"
            runs.append({"id": value["id"], "title": value["title"], "description": value.get("description", "Исследовательский прогон"),
                         "phase": value["phase"], "state": state, "completed": value["completed"],
                         "total": value["total"], "unit": value.get("unit", "элементов"),
                         "rate_per_second": value.get("rate_per_second"), "eta_seconds": value.get("eta_seconds"),
                         "last_update": value["updated_at"], "gallery_scanned": None, "candidate_count": None,
                         "reviewed_passes": None, "stages": value.get("stages", []), "grid": [], "history": value.get("history", []),
                         "photos": [], "note": value.get("note", "")})
        except (OSError, ValueError, TypeError):
            continue
    return runs


@lru_cache(maxsize=4)
def _status_for_tick(tick: int) -> dict:
    lines = process_lines()
    runs = [run for run in [gallery_run(lines), *registered_runs()] if run is not None]
    priority = {"running": 0, "stalled": 1, "waiting": 2, "stopped": 3, "complete": 4}
    runs.sort(key=lambda run: (priority.get(run["state"], 5), run["id"]))
    unknown = []
    for line in lines:
        if "ml.research." not in line or "run_dashboard" in line or "ps -axo" in line:
            continue
        if any(name in line for name in ("scan_gallery_technical", "gallery_extreme_vlm_review",
                                         "annotation_server", "progress_dashboard")):
            continue
        unknown.append(line.strip()[:160])
    return {"server_time": iso(), "runs": runs, "other_processes": unknown[:6]}


def status() -> dict:
    return _status_for_tick(int(time.monotonic() * 2))


@lru_cache(maxsize=1)
def gallery_paths() -> dict[str, str]:
    import pandas as pd

    frame = pd.read_parquet(GALLERY_MANIFEST, columns=["id", "image_path"])
    return dict(zip(frame.id, frame.image_path, strict=True))


@lru_cache(maxsize=1)
def gallery_positions() -> dict[str, int]:
    import pandas as pd

    frame = pd.read_parquet(GALLERY_MANIFEST, columns=["id"])
    return {ident: index for index, ident in enumerate(frame.id)}


def photo_details(ident: str) -> dict:
    position = gallery_positions()[ident]
    start = position // 500 * 500
    end = min(start + 500, GALLERY_TOTAL) - 1
    path = GALLERY_DIR / "chunks" / f"{start:06d}-{end:06d}.jsonl"
    record = None
    if path.is_file() and path.with_suffix(".sha256").is_file():
        with path.open() as source:
            for line in source:
                candidate = json.loads(line)
                if candidate["id"] == ident:
                    record = candidate
                    break
    reviews = []
    for source in ("full_gallery_candidates", "pilot_candidates_1000"):
        folder = GALLERY_DIR / "vlm_review" / source / "raw" / ident
        passes = []
        for number in (1, 2):
            review_path = folder / f"pass{number}.json"
            if review_path.is_file():
                value = json.loads(review_path.read_text())
                attempt = value["attempts"][-1]
                passes.append({"pass": number, "valid": value["valid"],
                               "answer": attempt.get("parsed"), "raw_text": attempt.get("raw_text"),
                               "error": attempt.get("error")})
        if passes:
            contract_path = folder.parents[1] / "contract.json"
            contract = json.loads(contract_path.read_text()) if contract_path.is_file() else {}
            reviews.append({"source": "Полный проход" if source == "full_gallery_candidates" else "Пилотная проверка",
                            "model": contract.get("model_repo"), "model_revision": contract.get("model_revision"),
                            "passes": passes})
    return {"id": ident, "source": record["source"] if record else None,
            "scan_status": record["status"] if record else "not_yet_committed",
            "features": record.get("features") if record else None,
            "technical_flags": technical_reasons(record) if record else [],
            "reviews": reviews,
            "image_url": f"/image/{quote(ident, safe='')}"}


@lru_cache(maxsize=256)
def thumbnail(ident: str) -> bytes:
    path = Path(gallery_paths()[ident])
    with Image.open(path) as source:
        image = ImageOps.exif_transpose(source).convert("RGB")
        image.thumbnail((720, 460), Image.Resampling.LANCZOS)
        buffer = io.BytesIO()
        image.save(buffer, "JPEG", quality=79, optimize=True)
        return buffer.getvalue()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        route = urlsplit(self.path)
        try:
            if route.path == "/api/status":
                self.respond(200, json.dumps(status(), ensure_ascii=False).encode(), "application/json; charset=utf-8")
            elif route.path == "/api/chunk":
                index = int(parse_qs(route.query).get("index", ["-1"])[0])
                chunks = scan_chunks()
                if not 0 <= index < len(chunks):
                    self.respond(404, b"{}", "application/json")
                else:
                    self.respond(200, json.dumps({"index": index, "photos": last_images(chunks[index][2], 10)}).encode(), "application/json")
            elif route.path.startswith("/api/photo/"):
                ident = unquote(route.path.removeprefix("/api/photo/"))
                if len(ident) > 160 or ident not in gallery_paths():
                    self.respond(404, b"{}", "application/json")
                else:
                    self.respond(200, json.dumps(photo_details(ident), ensure_ascii=False).encode(), "application/json; charset=utf-8")
            elif route.path.startswith("/image/"):
                ident = unquote(route.path.removeprefix("/image/"))
                if len(ident) > 160 or ident not in gallery_paths():
                    self.respond(404, b"Not found", "text/plain")
                else:
                    self.respond(200, thumbnail(ident), "image/jpeg", "private, max-age=60")
            elif route.path in ("/", "/index.html"):
                self.respond(200, (ASSETS / "index.html").read_bytes(), "text/html; charset=utf-8")
            elif route.path in ("/style.css", "/app.js"):
                name = route.path[1:]
                self.respond(200, (ASSETS / name).read_bytes(), "text/css; charset=utf-8" if name.endswith("css") else "text/javascript; charset=utf-8")
            else:
                self.respond(404, b"Not found", "text/plain")
        except (OSError, ValueError, KeyError) as exc:
            self.respond(503, json.dumps({"error": f"{type(exc).__name__}: {str(exc)[:160]}"}).encode(), "application/json")

    def respond(self, code: int, payload: bytes, content_type: str, cache: str = "no-store") -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", cache)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8771)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(json.dumps({"url": f"http://127.0.0.1:{args.port}/", "pid": os.getpid()}), flush=True)
    server.serve_forever(poll_interval=0.5)


if __name__ == "__main__":
    main()
