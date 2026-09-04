from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from apps.telegram_bot.backend import (
    BackendRateLimited,
    BackendRejectedImage,
    BackendTimeout,
    BackendUnavailable,
    InvalidBackendResponse,
)
from apps.telegram_bot.bot import HELP_TEXT, START_TEXT, GeoSnapBot
from apps.telegram_bot.config import BotSettings


class FakeBackend:
    def __init__(self, result=None, error: Exception | None = None) -> None:
        self.result = result or ok_result()
        self.error = error
        self.calls: list[tuple[bytes, str]] = []

    async def localize(self, payload: bytes, *, request_id: str):
        self.calls.append((payload, request_id))
        if self.error:
            raise self.error
        return self.result


class FakePhoto:
    def __init__(self, payload: bytes = b"\xff\xd8\xffimage", *, width=640, height=480, file_size=None) -> None:
        self.payload = payload
        self.width = width
        self.height = height
        self.file_size = len(payload) if file_size is None else file_size
        self.get_file = AsyncMock(
            return_value=SimpleNamespace(download_as_bytearray=AsyncMock(return_value=bytearray(payload)))
        )


def ok_result() -> dict:
    return {
        "status": "ok",
        "prediction": {"lat": 55.751244, "lon": 37.618423, "confidence": 0.947},
        "matches": [{"source": "mapillary"}, {"source": "kartaview"}],
    }


def settings(**overrides) -> BotSettings:
    values = {
        "token": "123:test",
        "backend_url": "http://backend:8000",
        "cooldown_seconds": 8.0,
        "max_concurrency": 2,
        "max_download_bytes": 1024,
        "backend_timeout_seconds": 10.0,
    }
    values.update(overrides)
    return BotSettings(**values)


def update_with(*photos: FakePhoto):
    message = SimpleNamespace(
        photo=list(photos),
        reply_text=AsyncMock(),
        reply_location=AsyncMock(),
    )
    return SimpleNamespace(message=message, effective_user=SimpleNamespace(id=42))


@pytest.mark.asyncio
async def test_start() -> None:
    update = update_with()
    await GeoSnapBot(settings(), FakeBackend()).start(update, None)
    update.message.reply_text.assert_awaited_once_with(START_TEXT)


@pytest.mark.asyncio
async def test_help() -> None:
    update = update_with()
    await GeoSnapBot(settings(), FakeBackend()).help(update, None)
    update.message.reply_text.assert_awaited_once_with(HELP_TEXT)


@pytest.mark.asyncio
async def test_valid_photo_uses_highest_resolution_and_renders_ok() -> None:
    low = FakePhoto(b"low", width=320, height=240)
    high = FakePhoto(b"high", width=1280, height=720)
    backend = FakeBackend()
    update = update_with(low, high)
    await GeoSnapBot(settings(), backend).photo(update, None)
    assert backend.calls[0][0] == b"high"
    high.get_file.assert_awaited_once()
    update.message.reply_location.assert_awaited_once_with(latitude=55.751244, longitude=37.618423)
    text = update.message.reply_text.await_args.args[0]
    assert "Estimated location" in text
    assert "not a probability" in text
    assert "Mapillary" not in text
    assert "mapillary" in text and "kartaview" in text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("low_confidence", "evidence is insufficient"),
        ("out_of_coverage", "not sufficiently represented"),
    ],
)
async def test_abstention_statuses_never_send_pin(status: str, expected: str) -> None:
    update = update_with(FakePhoto())
    await GeoSnapBot(settings(), FakeBackend({"status": status, "prediction": None, "matches": []})).photo(
        update, None
    )
    assert expected in update.message.reply_text.await_args.args[0]
    update.message.reply_location.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (BackendRejectedImage("bad"), "could not be read"),
        (BackendUnavailable("down"), "temporarily unavailable"),
        (BackendTimeout("slow"), "timed out"),
        (BackendRateLimited("busy"), "too many requests"),
        (InvalidBackendResponse("bad json"), "unexpected response"),
    ],
)
async def test_backend_errors_are_safe(error: Exception, expected: str) -> None:
    update = update_with(FakePhoto())
    await GeoSnapBot(settings(), FakeBackend(error=error)).photo(update, None)
    assert expected in update.message.reply_text.await_args.args[0]
    update.message.reply_location.assert_not_awaited()


@pytest.mark.asyncio
async def test_oversized_photo_is_rejected_before_download() -> None:
    photo = FakePhoto(file_size=2048)
    backend = FakeBackend()
    update = update_with(photo)
    await GeoSnapBot(settings(max_download_bytes=1024), backend).photo(update, None)
    assert "too large" in update.message.reply_text.await_args.args[0]
    photo.get_file.assert_not_awaited()
    assert not backend.calls


@pytest.mark.asyncio
async def test_unsupported_message() -> None:
    update = update_with()
    await GeoSnapBot(settings(), FakeBackend()).unsupported(update, None)
    assert "send a street photo" in update.message.reply_text.await_args.args[0]


@pytest.mark.asyncio
async def test_per_user_cooldown() -> None:
    now = 100.0
    backend = FakeBackend()
    bot = GeoSnapBot(settings(cooldown_seconds=10), backend, clock=lambda: now)
    first = update_with(FakePhoto())
    second = update_with(FakePhoto())
    await bot.photo(first, None)
    await bot.photo(second, None)
    assert len(backend.calls) == 1
    assert "wait 10 seconds" in second.message.reply_text.await_args.args[0]


@pytest.mark.asyncio
async def test_bot_concurrency_is_bounded() -> None:
    active = 0
    maximum = 0

    class SlowBackend(FakeBackend):
        async def localize(self, payload: bytes, *, request_id: str):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.01)
            active -= 1
            return ok_result()

    bot = GeoSnapBot(settings(max_concurrency=1), SlowBackend())
    updates = [
        SimpleNamespace(
            message=update_with(FakePhoto()).message,
            effective_user=SimpleNamespace(id=index),
        )
        for index in range(3)
    ]
    await asyncio.gather(*(bot.photo(update, None) for update in updates))
    assert maximum == 1
