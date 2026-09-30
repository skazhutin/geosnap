"""Local, outcome-blind image annotation server with append-only per-rater labels."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import threading
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from ml.research.gallery_scale_storage import digest
from ml.research.geographic_v9.common import OUT, SOURCE

FOLDER = OUT / "annotations"
UI = SOURCE / "annotation_ui"
REASONS = ["motion_blur", "out_of_focus", "too_dark", "overexposed",
    "compression_or_corruption", "obstructed_view", "close_surface", "mostly_ground",
    "mostly_sky", "inside_vehicle", "indoor", "vegetation_dominated", "generic_road",
    "repetitive_scene", "no_stable_landmarks", "insufficient_scene_context"]
EVIDENCE = ["distinctive_buildings", "intersection_or_road_geometry", "street_furniture",
    "signage_or_text", "transit_infrastructure", "distinctive_landscape",
    "strong_architectural_cues", "multiple_independent_landmarks"]
ACTIONS = ["hold_camera_still", "step_away_from_close_surface", "turn_toward_street",
    "turn_toward_buildings", "include_intersection", "include_signs",
    "include_wider_field_of_view", "avoid_ground_or_sky", "move_away_from_obstruction",
    "try_another_direction"]
SCHEMA = {"reason_labels": REASONS, "positive_evidence_labels": EVIDENCE,
    "geolocatability_scale": {"0": "Effectively unusable", "1": "Very weak",
        "2": "Potentially usable", "3": "Good", "4": "Very informative"},
    "retake_actions": ACTIONS}


class AnnotationState:
    def __init__(self, rater, sample="full"):
        if not re.fullmatch(r"[A-Za-z0-9_-]{2,32}", rater):
            raise ValueError("Rater ID must have 2–32 letters, digits, underscores or dashes")
        if sample not in {"full", "quick20"}:
            raise ValueError("Unknown annotation sample")
        prefix = "" if sample == "full" else "quick20_"
        receipt = json.loads((FOLDER / f"{prefix}sample_receipt.json").read_text())
        private_path = FOLDER / f"{prefix}private_sample.json"
        if digest(private_path) != receipt["private_sample_sha256"]:
            raise RuntimeError("Blind annotation sample changed")
        sample_data = json.loads(private_path.read_text())
        self.rater = rater
        self.sample = sample
        self.items = {row["token"]: row for row in sample_data["items"]}
        self.order = sorted(self.items, key=lambda token: hashlib.sha256(
            f"v9-{rater}-{token}".encode()).digest())
        self.path = FOLDER / f"{prefix}rater_{rater}.jsonl"
        self.lock = threading.Lock()
        self.completed = set()
        if self.path.exists():
            for line in self.path.read_text().splitlines():
                item = json.loads(line)
                if item["rater"] != rater or item["token"] not in self.items or item["token"] in self.completed:
                    raise RuntimeError("Rater annotation log is corrupt or duplicated")
                self.completed.add(item["token"])

    def current(self):
        with self.lock:
            token = next((token for token in self.order if token not in self.completed), None)
            return {"token": token, "completed": len(self.completed), "total": len(self.order),
                "rater": self.rater, "done": token is None}

    def validate(self, payload):
        token = payload.get("token")
        if token not in self.items:
            raise ValueError("Unknown image token")
        score = payload.get("geolocatability")
        if type(score) is not int or score not in range(5):
            raise ValueError("Choose a geolocatability score from 0 to 4")
        reasons = payload.get("reasons")
        evidence = payload.get("positive_evidence")
        if (not isinstance(reasons, list) or not isinstance(evidence, list)
                or any(not isinstance(value, str) for value in reasons + evidence)
                or len(set(reasons)) != len(reasons) or len(set(evidence)) != len(evidence)
                or set(reasons) - set(REASONS) or set(evidence) - set(EVIDENCE)):
            raise ValueError("Invalid reason or positive-evidence labels")
        retake = payload.get("would_request_another_photo")
        if type(retake) is not bool:
            raise ValueError("Choose whether another photo is needed")
        action = payload.get("recommended_retake_action")
        if retake and action not in ACTIONS:
            raise ValueError("Choose a specific retake action")
        if not retake and action not in (None, ""):
            raise ValueError("A retake action requires a retake recommendation")
        note = payload.get("note", "")
        if not isinstance(note, str) or len(note) > 500:
            raise ValueError("Note must be at most 500 characters")
        return {"token": token, "rater": self.rater, "geolocatability": score,
            "reasons": sorted(reasons), "positive_evidence": sorted(evidence),
            "would_request_another_photo": retake,
            "recommended_retake_action": action if retake else None,
            "note": note.strip(), "labeled_at": datetime.now(UTC).isoformat()}

    def save(self, payload):
        item = self.validate(payload)
        with self.lock:
            if item["token"] in self.completed:
                raise FileExistsError("This image was already labeled by this rater")
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            self.completed.add(item["token"])
        return self.current()


def handler_for(state):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, status, content, mime="application/json; charset=utf-8"):
            self.send_response(status)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; img-src 'self'; style-src 'self'; script-src 'self'")
            self.end_headers()
            self.wfile.write(content)

        def _json(self, status, data):
            self._send(status, json.dumps(data, ensure_ascii=False).encode())

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/api/current":
                return self._json(HTTPStatus.OK, state.current())
            if path == "/api/schema":
                return self._json(HTTPStatus.OK, SCHEMA)
            if path.startswith("/image/"):
                token = path.removeprefix("/image/")
                row = state.items.get(token)
                if row is None:
                    return self._json(HTTPStatus.NOT_FOUND, {"error": "Image not found"})
                image = Path(row["image_path"])
                if digest(image) != row["image_sha256"]:
                    return self._json(HTTPStatus.CONFLICT, {"error": "Image hash changed"})
                return self._send(HTTPStatus.OK, image.read_bytes(), "image/jpeg")
            static = {"/": ("index.html", "text/html; charset=utf-8"),
                "/app.js": ("app.js", "text/javascript; charset=utf-8"),
                "/style.css": ("style.css", "text/css; charset=utf-8"),
                "/favicon.svg": ("favicon.svg", "image/svg+xml")}
            if path in static:
                filename, mime = static[path]
                return self._send(HTTPStatus.OK, (UI / filename).read_bytes(), mime)
            return self._json(HTTPStatus.NOT_FOUND, {"error": "Not found"})

        def do_POST(self):
            if urlparse(self.path).path != "/api/annotation":
                return self._json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
            origin = self.headers.get("Origin")
            expected = f"http://{self.headers.get('Host')}"
            if origin and origin != expected:
                return self._json(HTTPStatus.FORBIDDEN, {"error": "Cross-origin write blocked"})
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if size < 2 or size > 32768:
                    raise ValueError("Invalid annotation size")
                payload = json.loads(self.rfile.read(size))
                if not isinstance(payload, dict):
                    raise ValueError("Annotation must be an object")
                result = state.save(payload)
            except FileExistsError as error:
                return self._json(HTTPStatus.CONFLICT, {"error": str(error)})
            except (ValueError, KeyError, json.JSONDecodeError) as error:
                return self._json(HTTPStatus.BAD_REQUEST, {"error": str(error)})
            return self._json(HTTPStatus.CREATED, result)

        def log_message(self, _format, *_args):
            # URLs include only opaque tokens; suppress request logs as a privacy default.
            return

    return Handler


def serve(rater, port, sample="full"):
    state = AnnotationState(rater, sample)
    server = ThreadingHTTPServer(("127.0.0.1", port), handler_for(state))
    print(f"Blinded GeoSnap annotation: http://127.0.0.1:{port}/  rater={rater} sample={sample}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rater", required=True)
    parser.add_argument("--port", type=int, default=8769)
    parser.add_argument("--sample", choices=["full", "quick20"], default="full")
    arguments = parser.parse_args()
    serve(arguments.rater, arguments.port, arguments.sample)
