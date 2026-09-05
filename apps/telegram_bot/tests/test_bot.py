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
from apps.telegram_bot.bot import (
    CHOOSE_LANGUAGE,
    LANGUAGE_KEY,
    PENDING_PHOTO_KEY,
    TEXT,
    GeoSnapBot,
)
from apps.telegram_bot.config import BotSettings
from apps.telegram_bot.map_links import google_maps_url, yandex_maps_url


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


def low_confidence_result() -> dict:
    return {
        "status": "low_confidence",
        "prediction": {"lat": 55.701234, "lon": 37.665432, "confidence": 0.62},
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


def context(language: str | None = "en"):
    data = {} if language is None else {LANGUAGE_KEY: language}
    return SimpleNamespace(user_data=data)


def update_with(*photos: FakePhoto, user_id: int = 42):
    progress = SimpleNamespace(edit_text=AsyncMock())
    message = SimpleNamespace(
        photo=list(photos),
        reply_text=AsyncMock(return_value=progress),
        reply_location=AsyncMock(),
    )
    return SimpleNamespace(message=message, effective_user=SimpleNamespace(id=user_id)), progress


@pytest.mark.asyncio
async def test_start_is_language_first() -> None:
    update, _ = update_with()
    ctx = context("en")
    await GeoSnapBot(settings(), FakeBackend()).start(update, ctx)
    assert LANGUAGE_KEY not in ctx.user_data
    assert update.message.reply_text.await_args.args[0] == CHOOSE_LANGUAGE
    labels = [button.text for button in update.message.reply_text.await_args.kwargs["reply_markup"].inline_keyboard[0]]
    assert labels == ["🇷🇺 Русский", "🇬🇧 English"]


@pytest.mark.asyncio
@pytest.mark.parametrize("language", ["ru", "en"])
async def test_language_selection_onboards_in_selected_language(language: str) -> None:
    query = SimpleNamespace(
        data=f"language:{language}",
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
        message=None,
    )
    update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=42))
    ctx = context(None)
    await GeoSnapBot(settings(), FakeBackend()).choose_language(update, ctx)
    assert ctx.user_data[LANGUAGE_KEY] == language
    query.answer.assert_awaited_once()
    query.edit_message_text.assert_awaited_once_with(TEXT[language]["ready"])


@pytest.mark.asyncio
@pytest.mark.parametrize("language", ["ru", "en"])
async def test_help_is_localized(language: str) -> None:
    update, _ = update_with()
    await GeoSnapBot(settings(), FakeBackend()).help(update, context(language))
    update.message.reply_text.assert_awaited_once_with(TEXT[language]["help"])


@pytest.mark.asyncio
async def test_photo_before_language_is_retained_and_processed_after_choice() -> None:
    photo = FakePhoto(b"pending")
    first_update, _ = update_with(photo)
    ctx = context(None)
    backend = FakeBackend()
    bot = GeoSnapBot(settings(), backend)
    await bot.photo(first_update, ctx)
    assert ctx.user_data[PENDING_PHOTO_KEY] is photo
    first_update.message.reply_text.assert_awaited_once()

    progress = SimpleNamespace(edit_text=AsyncMock())
    callback_message = SimpleNamespace(
        reply_text=AsyncMock(return_value=progress),
        reply_location=AsyncMock(),
    )
    query = SimpleNamespace(
        data="language:en",
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
        message=callback_message,
    )
    await bot.choose_language(
        SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=42)),
        ctx,
    )
    assert PENDING_PHOTO_KEY not in ctx.user_data
    assert backend.calls[0][0] == b"pending"
    callback_message.reply_location.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("language, expected", [("en", "Estimated location"), ("ru", "Предполагаемое место")])
async def test_valid_photo_uses_highest_resolution_and_localized_ok(language: str, expected: str) -> None:
    low = FakePhoto(b"low", width=320, height=240)
    high = FakePhoto(b"high", width=1280, height=720)
    backend = FakeBackend()
    update, progress = update_with(low, high)
    await GeoSnapBot(settings(), backend).photo(update, context(language))
    assert backend.calls[0][0] == b"high"
    high.get_file.assert_awaited_once()
    update.message.reply_location.assert_awaited_once_with(latitude=55.751244, longitude=37.618423)
    text = progress.edit_text.await_args.args[0]
    assert expected in text
    assert "55.7512, 37.6184" in text
    assert ("not a probability" in text) if language == "en" else ("не вероятность" in text)
    keyboard = progress.edit_text.await_args.kwargs["reply_markup"]
    urls = [button.url for button in keyboard.inline_keyboard[0]]
    assert urls == [google_maps_url(55.751244, 37.618423), yandex_maps_url(55.751244, 37.618423)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("language", "heading", "warning"),
    [
        ("en", "⚠️ Tentative location", "may be significantly wrong"),
        ("ru", "⚠️ Примерное место", "может быть сильно ошибочным"),
    ],
)
async def test_low_confidence_shows_tentative_point_without_native_pin(
    language: str,
    heading: str,
    warning: str,
) -> None:
    update, progress = update_with(FakePhoto())
    await GeoSnapBot(settings(), FakeBackend(low_confidence_result())).photo(
        update, context(language)
    )
    rendered = progress.edit_text.await_args.args[0]
    assert heading in rendered
    assert warning in rendered
    assert "55.7012, 37.6654" in rendered
    keyboard = progress.edit_text.await_args.kwargs["reply_markup"]
    urls = [button.url for button in keyboard.inline_keyboard[0]]
    assert urls == [google_maps_url(55.701234, 37.665432), yandex_maps_url(55.701234, 37.665432)]
    update.message.reply_location.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("language", ["ru", "en"])
async def test_low_confidence_without_prediction_is_safe_abstention(language: str) -> None:
    update, progress = update_with(FakePhoto())
    await GeoSnapBot(
        settings(),
        FakeBackend({"status": "low_confidence", "prediction": None, "matches": []}),
    ).photo(update, context(language))
    assert progress.edit_text.await_args.args[0] == TEXT[language]["low_confidence_no_prediction"]
    assert "reply_markup" not in progress.edit_text.await_args.kwargs
    update.message.reply_location.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("language", ["ru", "en"])
async def test_out_of_coverage_is_localized_and_never_sends_point(language: str) -> None:
    update, progress = update_with(FakePhoto())
    await GeoSnapBot(
        settings(),
        FakeBackend({"status": "out_of_coverage", "prediction": None, "matches": []}),
    ).photo(update, context(language))
    assert progress.edit_text.await_args.args[0] == TEXT[language]["out_of_coverage"]
    assert "reply_markup" not in progress.edit_text.await_args.kwargs
    update.message.reply_location.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "key"),
    [
        (BackendRejectedImage("bad"), "too_large"),
        (BackendUnavailable("down"), "unavailable"),
        (BackendTimeout("slow"), "timeout"),
        (BackendRateLimited("busy"), "rate_limited"),
        (InvalidBackendResponse("bad json"), "malformed"),
    ],
)
@pytest.mark.parametrize("language", ["ru", "en"])
async def test_backend_errors_are_safe_and_localized(error: Exception, key: str, language: str) -> None:
    update, progress = update_with(FakePhoto())
    await GeoSnapBot(settings(), FakeBackend(error=error)).photo(update, context(language))
    assert progress.edit_text.await_args.args[0] == TEXT[language][key]
    update.message.reply_location.assert_not_awaited()


@pytest.mark.asyncio
async def test_oversized_photo_is_rejected_before_download() -> None:
    photo = FakePhoto(file_size=2048)
    backend = FakeBackend()
    update, _ = update_with(photo)
    await GeoSnapBot(settings(max_download_bytes=1024), backend).photo(update, context("en"))
    assert update.message.reply_text.await_args.args[0] == TEXT["en"]["too_large"]
    photo.get_file.assert_not_awaited()
    assert not backend.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("language", ["ru", "en"])
async def test_unsupported_message_is_localized(language: str) -> None:
    update, _ = update_with()
    await GeoSnapBot(settings(), FakeBackend()).unsupported(update, context(language))
    update.message.reply_text.assert_awaited_once_with(TEXT[language]["unsupported"])


@pytest.mark.asyncio
async def test_per_user_cooldown_is_localized() -> None:
    backend = FakeBackend()
    bot = GeoSnapBot(settings(cooldown_seconds=10), backend, clock=lambda: 100.0)
    first, _ = update_with(FakePhoto())
    second, _ = update_with(FakePhoto())
    await bot.photo(first, context("ru"))
    await bot.photo(second, context("ru"))
    assert len(backend.calls) == 1
    assert second.message.reply_text.await_args.args[0] == TEXT["ru"]["cooldown"].format(seconds=10)


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
    updates = [update_with(FakePhoto(), user_id=index)[0] for index in range(3)]
    await asyncio.gather(*(bot.photo(update, context("en")) for update in updates))
    assert maximum == 1


def test_map_links_keep_lat_lon_order() -> None:
    assert "query=55.751244,37.618423" in google_maps_url(55.751244, 37.618423)
    yandex = yandex_maps_url(55.751244, 37.618423)
    assert "ll=37.618423%2C55.751244" in yandex
    assert "pt=37.618423%2C55.751244%2Cpm2rdm" in yandex
