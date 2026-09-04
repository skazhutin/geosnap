from __future__ import annotations

import inspect
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from PIL import Image

from app.api.health import get_localization_service

router = APIRouter(tags=["references"])


async def _call(function: Any, *args: Any) -> Any:
    if inspect.iscoroutinefunction(function):
        return await function(*args)
    result = await run_in_threadpool(function, *args)
    if inspect.isawaitable(result):
        return await result
    return result


@router.get(
    "/thumbnails/{reference_id:path}",
    responses={
        200: {"content": {"image/jpeg": {}}, "description": "Bounded reference thumbnail."},
        404: {"description": "Reference image is unavailable."},
    },
)
async def reference_thumbnail(request: Request, reference_id: str) -> Response:
    """Resolve a thumbnail by opaque indexed ID without exposing storage paths."""

    if not reference_id or len(reference_id) > 256 or ".." in reference_id or "\\" in reference_id:
        raise HTTPException(status_code=404, detail="Reference thumbnail not found.")
    service = get_localization_service(request)
    getter = getattr(service, "get_thumbnail", None)
    if getter is None:
        raise HTTPException(status_code=404, detail="Reference thumbnail not found.")
    try:
        image = await _call(getter, reference_id)
        if not isinstance(image, Image.Image):
            raise HTTPException(status_code=404, detail="Reference thumbnail not found.")
        output = io.BytesIO()
        image.convert("RGB").save(output, format="JPEG", quality=84, optimize=True)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Reference thumbnail not found.") from exc
    return Response(
        content=output.getvalue(),
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=86400"},
    )
