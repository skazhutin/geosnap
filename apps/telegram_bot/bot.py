from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from telegram import Update
from telegram.error import TelegramError
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from .backend import (
    BackendClient,
    BackendFailure,
    BackendImageTooLarge,
    BackendRateLimited,
    BackendRejectedImage,
    BackendTimeout,
    InvalidBackendResponse,
)
from .config import BotSettings

logger = logging.getLogger("geosnap.telegram")

START_TEXT = (
    "GeoSnap estimates where a Moscow street photo was taken. Send a street image to try it. "
    "Results are approximate, and GeoSnap may decline to return a location when the visual evidence is weak."
)
HELP_TEXT = (
    "Send one JPEG photo as an image message. GeoSnap has experimental, incomplete coverage of Moscow street scenes "
    "and may abstain when current references do not provide enough evidence. Photos are not permanently retained by GeoSnap."
)


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
                "level": record.levelname.lower(),
                "event": getattr(record, "event", record.getMessage()),
                "request_id": getattr(record, "request_id", None),
                "error_category": getattr(record, "error_category", None),
            },
            separators=(",", ":"),
        )


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)


class GeoSnapBot:
    def __init__(
        self,
        settings: BotSettings,
        backend: BackendClient,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self.backend = backend
        self.clock = clock
        self._last_request: dict[int, float] = {}
        self._cooldown_lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(settings.max_concurrency)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        if update.message:
            await update.message.reply_text(START_TEXT)

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        if update.message:
            await update.message.reply_text(HELP_TEXT)

    async def unsupported(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        if update.message:
            await update.message.reply_text("Please send a street photo as an image. Use /help for details.")

    async def _cooldown_remaining(self, user_id: int) -> int:
        now = self.clock()
        async with self._cooldown_lock:
            previous = self._last_request.get(user_id)
            if previous is not None and now - previous < self.settings.cooldown_seconds:
                return max(1, int(self.settings.cooldown_seconds - (now - previous) + 0.999))
            self._last_request[user_id] = now
        return 0

    async def photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        message = update.message
        user = update.effective_user
        if message is None or user is None or not message.photo:
            return
        wait_seconds = await self._cooldown_remaining(int(user.id))
        if wait_seconds:
            await message.reply_text(f"Please wait {wait_seconds} seconds before sending another photo.")
            return
        photo = max(message.photo, key=lambda item: (item.width * item.height, item.file_size or 0))
        if photo.file_size and photo.file_size > self.settings.max_download_bytes:
            await message.reply_text("That photo is too large. Please send a smaller JPEG image.")
            return

        request_id = uuid.uuid4().hex
        try:
            async with self._semaphore:
                telegram_file = await photo.get_file()
                payload = bytes(await telegram_file.download_as_bytearray())
                if not payload:
                    raise BackendRejectedImage("empty Telegram image")
                if len(payload) > self.settings.max_download_bytes:
                    raise BackendImageTooLarge("Telegram image exceeds bot limit")
                result = await self.backend.localize(payload, request_id=request_id)
            await self._render(message, result)
            logger.info("photo localization complete", extra={"event": "photo_complete", "request_id": request_id})
        except (BackendRejectedImage, BackendImageTooLarge):
            await message.reply_text("The image could not be read or is too large. Please send a smaller valid photo.")
        except BackendRateLimited:
            await message.reply_text("GeoSnap is receiving too many requests. Please wait a moment and try again.")
        except BackendTimeout:
            await message.reply_text("Localization timed out. Please try again in a moment.")
        except InvalidBackendResponse:
            await message.reply_text("GeoSnap returned an unexpected response. Please try again later.")
        except BackendFailure:
            await message.reply_text("GeoSnap is temporarily unavailable. Please try again later.")
        except TelegramError:
            logger.exception(
                "Telegram API error",
                extra={"event": "telegram_api_error", "request_id": request_id, "error_category": "telegram_api"},
            )
        except Exception as exc:
            logger.exception(
                "unexpected bot error",
                extra={
                    "event": "bot_error",
                    "request_id": request_id,
                    "error_category": type(exc).__name__,
                },
            )
            try:
                await message.reply_text("GeoSnap could not process the photo. Please try again later.")
            except TelegramError:
                pass

    async def _render(self, message: Any, result: dict[str, Any]) -> None:
        status = result["status"]
        if status == "low_confidence":
            await message.reply_text(
                "Potential matches were found, but the evidence is insufficient to provide a reliable location."
            )
            return
        if status == "out_of_coverage":
            await message.reply_text(
                "This scene is not sufficiently represented by GeoSnap's current Moscow reference coverage."
            )
            return
        prediction = result["prediction"]
        lat = float(prediction["lat"])
        lon = float(prediction["lon"])
        score = float(prediction["confidence"])
        map_url = f"https://www.openstreetmap.org/?mlat={lat:.6f}&mlon={lon:.6f}#map=17/{lat:.6f}/{lon:.6f}"
        sources = sorted(
            {
                str(match.get("source"))
                for match in result.get("matches", [])[:6]
                if isinstance(match, dict) and match.get("source")
            }
        )
        attribution = f" Reference imagery: {', '.join(sources)}." if sources else ""
        await message.reply_text(
            f"Estimated location\n{lat:.6f}, {lon:.6f}\nEvidence score: {score:.3f} "
            f"(ranking signal, not a probability).\nMap: {map_url}\n{attribution.strip()}"
        )
        await message.reply_location(latitude=lat, longitude=lon)


def build_application(settings: BotSettings) -> Application:
    settings.validate()
    client = BackendClient(settings.backend_url, settings.backend_timeout_seconds)
    bot = GeoSnapBot(settings, client)
    application = Application.builder().token(settings.token).concurrent_updates(settings.max_concurrency).build()
    application.add_handler(CommandHandler("start", bot.start))
    application.add_handler(CommandHandler("help", bot.help))
    application.add_handler(MessageHandler(filters.PHOTO, bot.photo))
    application.add_handler(MessageHandler(filters.ALL, bot.unsupported))

    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        del update
        logger.error(
            "unhandled Telegram update error",
            extra={
                "event": "update_error",
                "error_category": type(context.error).__name__ if context.error else "unknown",
            },
        )

    application.add_error_handler(error_handler)
    return application


def user_key_for_diagnostics(user_id: int, salt: str) -> str:
    """Return a non-reversible short key if aggregate diagnostics ever need one."""

    return hashlib.sha256(f"{salt}:{user_id}".encode()).hexdigest()[:12]
