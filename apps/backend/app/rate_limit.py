from __future__ import annotations

import asyncio
import ipaddress
import math
import time
from dataclasses import dataclass

from fastapi import Request


@dataclass(slots=True)
class _Bucket:
    tokens: float
    updated_at: float
    last_seen: float


class TokenBucketLimiter:
    def __init__(self, *, rate_per_minute: float, burst: int, max_clients: int = 10_000) -> None:
        if rate_per_minute <= 0 or burst <= 0 or max_clients <= 0:
            raise ValueError("rate limiter values must be positive")
        self.rate_per_second = rate_per_minute / 60.0
        self.burst = burst
        self.max_clients = max_clients
        self._buckets: dict[str, _Bucket] = {}
        self._lock = asyncio.Lock()

    async def allow(self, key: str) -> tuple[bool, int]:
        now = time.monotonic()
        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                if len(self._buckets) >= self.max_clients:
                    oldest = min(self._buckets, key=lambda item: self._buckets[item].last_seen)
                    self._buckets.pop(oldest, None)
                bucket = _Bucket(float(self.burst), now, now)
                self._buckets[key] = bucket
            bucket.tokens = min(
                float(self.burst),
                bucket.tokens + (now - bucket.updated_at) * self.rate_per_second,
            )
            bucket.updated_at = now
            bucket.last_seen = now
            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return True, 0
            retry_after = max(1, math.ceil((1.0 - bucket.tokens) / self.rate_per_second))
            return False, retry_after


def client_ip(request: Request, *, trust_forwarded_for: bool) -> str:
    if trust_forwarded_for:
        forwarded = request.headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
        try:
            return str(ipaddress.ip_address(forwarded))
        except ValueError:
            pass
    host = request.client.host if request.client else "unknown"
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        return "unknown"
