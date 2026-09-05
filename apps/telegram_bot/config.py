from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None else int(value)


def _float(name: str, default: float) -> float:
    value = os.getenv(name)
    return default if value is None else float(value)


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


@dataclass(frozen=True, slots=True)
class BotSettings:
    token: str
    backend_url: str = "http://backend:8000"
    cooldown_seconds: float = 8.0
    max_concurrency: int = 2
    max_download_bytes: int = 10 * 1024 * 1024
    backend_timeout_seconds: float = 30.0
    validate_only: bool = False
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> BotSettings:
        return cls(
            token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            backend_url=os.getenv("TELEGRAM_BACKEND_URL", "http://backend:8000"),
            cooldown_seconds=_float("TELEGRAM_USER_COOLDOWN_SECONDS", 8.0),
            max_concurrency=_int("TELEGRAM_MAX_CONCURRENCY", 2),
            max_download_bytes=_int("TELEGRAM_MAX_DOWNLOAD_BYTES", 10 * 1024 * 1024),
            backend_timeout_seconds=_float("TELEGRAM_BACKEND_TIMEOUT_SECONDS", 30.0),
            validate_only=_bool("TELEGRAM_VALIDATE_ONLY", False),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )

    def validate(self) -> None:
        if not self.validate_only and not self.token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required")
        if self.token and (len(self.token) > 256 or any(char.isspace() for char in self.token)):
            raise ValueError("TELEGRAM_BOT_TOKEN is malformed")
        if not self.backend_url.startswith("http://") or self.backend_url.rstrip("/") != self.backend_url:
            raise ValueError("TELEGRAM_BACKEND_URL must be an internal HTTP origin without a trailing slash")
        if any(
            value <= 0
            for value in (
                self.cooldown_seconds,
                self.max_concurrency,
                self.max_download_bytes,
                self.backend_timeout_seconds,
            )
        ):
            raise ValueError("Telegram bot limits must be positive")
