"""Bounded, transient photo collections; stores Telegram IDs, never image bytes."""
from __future__ import annotations

import asyncio
import secrets
from dataclasses import dataclass, field
from typing import Any

CollectionKey = tuple[int, int, int | None]


@dataclass
class PhotoCollection:
    expires_at: float
    media_group_id: str | None = None
    token: str = field(default_factory=lambda: secrets.token_hex(8))
    photos: list[Any] = field(default_factory=list)
    identities: set[str] = field(default_factory=set)
    prompt: Any = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
