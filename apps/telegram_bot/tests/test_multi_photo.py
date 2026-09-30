from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from apps.telegram_bot.backend import BackendClient, InvalidBackendResponse
from apps.telegram_bot.bot import GeoSnapBot

from .test_bot import FakeBackend, FakePhoto, context, settings, update_with


def multi_result(n=2, agreement="consensus"):
    return {"status": "low_confidence", "prediction": {"lat": 55.75, "lon": 37.62, "confidence": 0.6} if agreement == "consensus" else None,
            "matches": [], "multi_photo": {"submitted_images": n, "unique_images": n, "duplicate_images": 0,
            "supporting_images": n, "agreement": agreement}}


def album_update(i=0, *, user_id=42, chat_id=10, group="album"):
    photo = FakePhoto(f"image-{i}".encode())
    photo.file_unique_id = f"photo-{i}"
    update, progress = update_with(photo, user_id=user_id)
    update.message.chat_id = chat_id
    update.message.media_group_id = group
    return update, progress


def make_bot(**limits):
    backend = FakeBackend()
    backend.localize_multi = AsyncMock(return_value=multi_result(10))
    return GeoSnapBot(settings(**limits), backend), backend


@pytest.mark.asyncio
async def test_album_waits_for_explicit_done_then_submits_ten_once():
    bot, backend = make_bot()
    for i in range(10):
        update, progress = album_update(i)
        await bot.photo(update, context("ru"))
    backend.localize_multi.assert_not_awaited()
    await bot.done(update, context("ru"))
    backend.localize_multi.assert_awaited_once()
    assert len(backend.localize_multi.await_args.args[0]) == 10
    assert "10" in progress.edit_text.await_args.args[0]
    update.message.reply_location.assert_not_awaited()
    await bot.done(update, context("ru"))
    backend.localize_multi.assert_awaited_once()


@pytest.mark.asyncio
async def test_explicit_session_accepts_separate_photos_and_rejects_duplicates_and_overflow():
    bot, backend = make_bot()
    update, _ = album_update(group=None)
    await bot.photos(update, context())
    for i in [0, 0, *range(1, 11)]:
        update, _ = album_update(i, group=None)
        await bot.photo(update, context())
    assert len(next(iter(bot._collections.values())).photos) == 10
    assert backend.calls == []
    await bot.done(update, context())
    backend.localize_multi.assert_awaited_once()
    assert len(backend.localize_multi.await_args.args[0]) == 10


@pytest.mark.asyncio
async def test_collection_callback_cannot_submit_another_users_or_chats_photos():
    bot, backend = make_bot()
    update, _ = album_update()
    await bot.photo(update, context())
    token = next(iter(bot._collections.values())).token
    for user, chat in [(99, 10), (42, 99)]:
        foreign, _ = album_update(user_id=user, chat_id=chat)
        query = SimpleNamespace(data=f"photos:done:{token}", message=foreign.message, answer=AsyncMock())
        await bot.collection_action(SimpleNamespace(callback_query=query, effective_user=foreign.effective_user), context())
    assert backend.calls == []
    assert len(bot._collections) == 1
    await bot.cancel(update, context())
    assert not bot._collections


@pytest.mark.asyncio
async def test_expired_session_and_unrelated_album_do_not_mix():
    bot, backend = make_bot(collection_ttl_seconds=10)
    now = [0.0]
    bot.clock = lambda: now[0]
    first, _ = album_update()
    await bot.photo(first, context())
    other, _ = album_update(1, group="different")
    await bot.photo(other, context())
    assert len(next(iter(bot._collections.values())).photos) == 1
    now[0] = 11
    await bot.done(first, context())
    assert backend.calls == []
    assert not bot._collections


@pytest.mark.asyncio
async def test_busy_user_keeps_pending_collection_for_retry():
    bot, _ = make_bot()
    first, _ = album_update()
    await bot.photo(first, context())
    bot._inflight.add(42)
    await bot.done(first, context())
    assert len(bot._collections) == 1
    bot._inflight.clear()
    await bot.done(first, context())
    assert not bot._collections


@pytest.mark.asyncio
async def test_batch_download_limit_stops_before_backend():
    bot, backend = make_bot(max_batch_bytes=10)
    for i in range(2):
        update, progress = album_update(i)
        await bot.photo(update, context())
    await bot.done(update, context())
    backend.localize_multi.assert_not_awaited()
    assert "fewer" in progress.edit_text.await_args.args[0].lower()
    assert not bot._inflight


@pytest.mark.asyncio
async def test_ambiguous_batch_never_returns_coordinates_or_native_pin():
    bot, backend = make_bot()
    backend.localize_multi.return_value = multi_result(2, "ambiguous")
    for i in range(2):
        update, progress = album_update(i)
        await bot.photo(update, context())
    await bot.done(update, context())
    assert "55." not in progress.edit_text.await_args.args[0]
    update.message.reply_location.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_http_contract_and_separate_timeout():
    async def handle(request):
        assert request.url.path == "/localize/multi"
        assert request.content.count(b'name="images"') == 2
        assert request.extensions["timeout"]["read"] == 210
        return httpx.Response(200, json=multi_result())
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = BackendClient("http://backend", 30, client=http)
        assert (await client.localize_multi([b"one", b"two"], request_id="batch"))["status"] == "low_confidence"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [{"unique_images": 5}, {"supporting_images": 1}, {"agreement": "single"}])
async def test_batch_client_rejects_inconsistent_evidence(change):
    response = multi_result()
    response["multi_photo"].update(change)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=response))) as http:
        with pytest.raises(InvalidBackendResponse):
            await BackendClient("http://backend", 30, client=http).localize_multi([b"one", b"two"], request_id="batch")
