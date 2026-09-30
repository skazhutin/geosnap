from __future__ import annotations

import asyncio

import pytest
from app.api.localize import localize
from app.api.multi_photo import localize_multi
from app.capacity import CapacityUnavailable
from app.main import create_app
from fastapi import Request

from .test_api import FakeService, encoded_image, make_settings, multipart


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", [False, True], ids=["single-photo", "multi-photo"])
async def test_cancelled_request_keeps_capacity_until_inference_finishes(batch: bool) -> None:
    started = asyncio.Event()
    finish = asyncio.Event()
    completed = asyncio.Event()

    class BlockingService(FakeService):
        async def localize(self, query):
            started.set()
            await finish.wait()
            completed.set()
            return self.result

    app = create_app(
        settings=make_settings(localization_queue_limit=0),
        service_factory=BlockingService,
    )
    body, headers = multipart(encoded_image(), field_name="images" if batch else "image")

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    request = Request({
        "type": "http", "app": app,
        "headers": [(key.encode(), value.encode()) for key, value in headers.items()],
        "state": {"request_id": "cancelled-request"},
    }, receive)
    async with app.router.lifespan_context(app):
        task = asyncio.create_task((localize_multi if batch else localize)(request))
        try:
            await asyncio.wait_for(started.wait(), timeout=2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(CapacityUnavailable):
                await app.state.localization_capacity.acquire()
        finally:
            finish.set()
            await asyncio.wait_for(completed.wait(), timeout=2)
        # Finish the task and its deferred lease callback before the next request.
        for _ in range(100):
            try:
                lease = await app.state.localization_capacity.acquire()
            except CapacityUnavailable:
                await asyncio.sleep(0.01)
            else:
                lease.release()
                break
        else:
            pytest.fail("inference completed but request capacity stayed reserved")
