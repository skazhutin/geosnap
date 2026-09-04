from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

_LOG_FIELDS = (
    "event",
    "request_id",
    "method",
    "endpoint",
    "http_status",
    "duration_ms",
    "localization_status",
    "embedding_ms",
    "retrieval_ms",
    "policy_ms",
    "preprocess_ms",
    "error_category",
    "model",
    "device",
    "index_id",
    "gallery_count",
    "top_k",
    "config_sha256",
    "checkpoint_sha256",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in _LOG_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.http_requests = Counter(
            "geosnap_http_requests_total",
            "HTTP requests handled.",
            ("method", "endpoint", "status"),
            registry=self.registry,
        )
        self.http_latency = Histogram(
            "geosnap_http_request_duration_seconds",
            "HTTP request duration.",
            ("method", "endpoint"),
            registry=self.registry,
        )
        self.localization_requests = Counter(
            "geosnap_localization_requests_total",
            "Localization outcomes.",
            ("status",),
            registry=self.registry,
        )
        self.invalid_uploads = Counter(
            "geosnap_invalid_uploads_total",
            "Rejected image uploads.",
            ("category",),
            registry=self.registry,
        )
        self.rate_limited = Counter(
            "geosnap_rate_limited_total",
            "Requests rejected by the localization rate limiter.",
            registry=self.registry,
        )
        self.server_errors = Counter(
            "geosnap_http_5xx_total",
            "HTTP 5xx responses.",
            registry=self.registry,
        )
        self.embedding_latency = Histogram(
            "geosnap_embedding_duration_seconds",
            "Query embedding duration.",
            registry=self.registry,
        )
        self.retrieval_latency = Histogram(
            "geosnap_retrieval_duration_seconds",
            "Exact retrieval duration.",
            registry=self.registry,
        )
        self.policy_latency = Histogram(
            "geosnap_policy_duration_seconds",
            "Geographic aggregation and policy duration.",
            registry=self.registry,
        )
        self.backend_ready = Gauge(
            "geosnap_backend_ready",
            "Whether all required localization components are ready.",
            registry=self.registry,
        )
        self.model_load = Gauge(
            "geosnap_model_load_duration_seconds",
            "Most recent model load duration.",
            registry=self.registry,
        )
        self.index_load = Gauge(
            "geosnap_index_load_duration_seconds",
            "Most recent index load duration.",
            registry=self.registry,
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)
