from __future__ import annotations

import io
import warnings
from dataclasses import dataclass
from email.message import Message
from email.parser import BytesParser
from email.policy import default as email_policy
from time import perf_counter

from fastapi import Request
from PIL import Image, ImageOps, UnidentifiedImageError

from app.config import Settings
from app.services.errors import (
    ImageTooLargeError,
    InvalidImageError,
    PublicAPIError,
    UnsupportedFormatError,
)
from ml.query_quality import measure_query_image_quality

_ALLOWED_TYPES = {
    "image/jpeg": ("JPEG", {".jpg", ".jpeg"}),
    "image/png": ("PNG", {".png"}),
    "image/webp": ("WEBP", {".webp"}),
}


@dataclass(frozen=True, slots=True)
class ImageDiagnostics:
    width: int
    height: int
    sharpness: float
    brightness: float
    exposure: float
    preprocess_ms: float


@dataclass(frozen=True, slots=True)
class PreparedImage:
    """Web-independent query passed across the localization service boundary."""

    image: Image.Image
    detected_format: str
    content_type: str
    diagnostics: ImageDiagnostics


@dataclass(frozen=True, slots=True)
class UploadedPart:
    payload: bytes
    content_type: str
    filename: str | None


def _signature_format(payload: bytes) -> str | None:
    if payload.startswith(b"\xff\xd8\xff"):
        return "JPEG"
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return "PNG"
    if len(payload) >= 12 and payload[:4] == b"RIFF" and payload[8:12] == b"WEBP":
        return "WEBP"
    return None


async def read_image_part(request: Request, settings: Settings) -> UploadedPart:
    content_type = request.headers.get("content-type", "")
    if len(content_type) > 512 or "\r" in content_type or "\n" in content_type:
        raise InvalidImageError("Malformed Content-Type header.")
    parsed_content_type = Message()
    parsed_content_type["content-type"] = content_type
    if parsed_content_type.get_content_type().lower() != "multipart/form-data":
        raise UnsupportedFormatError("Expected multipart/form-data with an image field.")
    boundary = parsed_content_type.get_boundary()
    if boundary is None or len(boundary) > 200 or "\r" in boundary or "\n" in boundary:
        raise InvalidImageError("Malformed multipart boundary.")

    header = f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("utf-8", "strict")
    body_limit = settings.max_upload_bytes + settings.multipart_overhead_bytes
    declared_length = request.headers.get("content-length")
    if declared_length:
        try:
            if int(declared_length) > body_limit:
                raise ImageTooLargeError()
        except ValueError as exc:
            raise InvalidImageError("Invalid Content-Length header.") from exc

    chunks: list[bytes] = []
    size = 0
    try:
        async for chunk in request.stream():
            size += len(chunk)
            if size > body_limit:
                raise ImageTooLargeError()
            chunks.append(chunk)
    except PublicAPIError:
        raise
    except Exception as exc:
        raise InvalidImageError("The upload could not be read.") from exc

    try:
        message = BytesParser(policy=email_policy).parsebytes(header + b"".join(chunks))
    except Exception as exc:
        raise InvalidImageError("Malformed multipart upload.") from exc
    if not message.is_multipart():
        raise InvalidImageError("Malformed multipart upload.")

    image_parts: list[UploadedPart] = []
    for part in message.iter_parts():
        if part.get_content_disposition() != "form-data":
            continue
        if part.get_param("name", header="content-disposition") != "image":
            continue
        try:
            payload = part.get_payload(decode=True)
        except Exception as exc:
            raise InvalidImageError("The image field could not be decoded.") from exc
        if not isinstance(payload, bytes) or not payload:
            raise InvalidImageError("The image field is empty.")
        if len(payload) > settings.max_upload_bytes:
            raise ImageTooLargeError()
        image_parts.append(
            UploadedPart(
                payload=payload,
                content_type=part.get_content_type().lower(),
                filename=part.get_filename(),
            )
        )

    if len(image_parts) != 1:
        raise InvalidImageError("Provide exactly one multipart field named 'image'.")
    return image_parts[0]


def prepare_image(upload: UploadedPart, settings: Settings) -> PreparedImage:
    started = perf_counter()
    declared = _ALLOWED_TYPES.get(upload.content_type)
    if declared is None:
        raise UnsupportedFormatError()
    expected_format, allowed_suffixes = declared

    signature_format = _signature_format(upload.payload)
    if signature_format is None:
        raise InvalidImageError("The image file signature is invalid.")
    if signature_format != expected_format:
        raise UnsupportedFormatError("Image MIME type and file signature do not match.")

    if upload.filename and "." in upload.filename:
        suffix = "." + upload.filename.rsplit(".", 1)[-1].lower()
        if suffix not in allowed_suffixes:
            raise UnsupportedFormatError("Image filename extension does not match its format.")

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(upload.payload)) as source:
                if source.format != expected_format:
                    raise UnsupportedFormatError("Decoded image format does not match its signature.")
                width, height = source.size
                if (
                    width <= 0
                    or height <= 0
                    or width > settings.max_image_dimension
                    or height > settings.max_image_dimension
                    or width * height > settings.max_image_pixels
                ):
                    raise ImageTooLargeError("The decoded image dimensions are too large.")
                if width < 32 or height < 32:
                    raise InvalidImageError("The image dimensions are too small for localization.")
                if getattr(source, "n_frames", 1) != 1:
                    raise UnsupportedFormatError("Animated images are not supported.")
                source.load()
                oriented = ImageOps.exif_transpose(source)
                rgb = oriented.convert("RGB")
    except (ImageTooLargeError, InvalidImageError, UnsupportedFormatError):
        raise
    except (UnidentifiedImageError, OSError, SyntaxError, ValueError, Image.DecompressionBombError) as exc:
        raise InvalidImageError() from exc

    quality = measure_query_image_quality(rgb)
    diagnostics = ImageDiagnostics(
        width=rgb.width,
        height=rgb.height,
        sharpness=quality.sharpness,
        brightness=quality.brightness,
        exposure=quality.exposure,
        preprocess_ms=(perf_counter() - started) * 1000.0,
    )
    return PreparedImage(
        image=rgb,
        detected_format=expected_format,
        content_type=upload.content_type,
        diagnostics=diagnostics,
    )
