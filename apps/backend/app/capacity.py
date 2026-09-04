from __future__ import annotations

import asyncio


class CapacityUnavailable(RuntimeError):
    pass


class CapacityLease:
    def __init__(self, owner: LocalizationCapacity) -> None:
        self._owner = owner
        self._released = False
        self._deferred = False

    def defer_until(self, task: asyncio.Task[object]) -> None:
        if self._released or self._deferred:
            raise RuntimeError("capacity lease cannot be deferred")
        self._deferred = True
        task.add_done_callback(lambda _: self.release())

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._owner._semaphore.release()

    async def __aenter__(self) -> CapacityLease:
        return self

    async def __aexit__(self, *_: object) -> None:
        if not self._deferred:
            self.release()


class LocalizationCapacity:
    def __init__(self, *, concurrency: int, queue_limit: int, queue_timeout_seconds: float) -> None:
        if concurrency <= 0 or queue_limit < 0 or queue_timeout_seconds <= 0:
            raise ValueError("invalid localization capacity values")
        self._semaphore = asyncio.Semaphore(concurrency)
        self._queue_limit = queue_limit
        self._queue_timeout = queue_timeout_seconds
        self._waiters = 0
        self._lock = asyncio.Lock()

    async def acquire(self) -> CapacityLease:
        async with self._lock:
            if self._semaphore.locked() and self._waiters >= self._queue_limit:
                raise CapacityUnavailable("localization queue is full")
            self._waiters += 1
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=self._queue_timeout)
        except TimeoutError as exc:
            raise CapacityUnavailable("localization queue wait timed out") from exc
        finally:
            async with self._lock:
                self._waiters -= 1
        return CapacityLease(self)
