from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Literal

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

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
from .map_links import google_maps_url, yandex_maps_url

logger = logging.getLogger("geosnap.telegram")
Language = Literal["ru", "en"]
LANGUAGE_KEY = "language"
PENDING_PHOTO_KEY = "pending_photo"
CHOOSE_LANGUAGE = "Выберите язык · Choose your language"

TEXT: dict[Language, dict[str, str]] = {
    "en": {
        "ready": "Send a Moscow street photo. GeoSnap is experimental and may decline when evidence is weak.",
        "help": "Send one JPEG photo as an image. Coverage of Moscow street scenes is incomplete, so GeoSnap may abstain. GPS metadata is not used and photos are not permanently retained by default. Use /language to switch language.",
        "unsupported": "Please send a street photo as an image. Use /help for details.",
        "processing": "Comparing visual evidence…",
        "cooldown": "Please wait {seconds} seconds before sending another photo.",
        "too_large": "That image is too large or unreadable. Send a valid JPEG no larger than 10 MiB.",
        "rate_limited": "GeoSnap is receiving too many requests. Wait a moment and try again.",
        "timeout": "Localization timed out. Please try again in a moment.",
        "malformed": "GeoSnap returned an unexpected response. Please try again later.",
        "unavailable": "GeoSnap is temporarily unavailable. Please try again later.",
        "internal": "GeoSnap could not process the photo. Please try again later.",
        "low_confidence": "⚠️ Tentative location\n{lat:.4f}, {lon:.4f}\n\nGeoSnap found a possible point, but the evidence is below the acceptance threshold. This result may be significantly wrong.{attribution}\n\nYou can send another photo.",
        "low_confidence_no_prediction": "Potential visual matches were found, but GeoSnap could not produce a usable location. No point is shown. Try another angle with distinctive buildings or signs.\n\nYou can send another photo.",
        "out_of_coverage": "This scene is not sufficiently represented by GeoSnap’s current Moscow reference gallery. The photo itself may still be valid. No pin was sent.\n\nSend another photo when ready.",
        "ok": "Estimated location\n{lat:.4f}, {lon:.4f}\n\nStrong visual evidence passed GeoSnap’s acceptance policy. The evidence score ({score:.3f}) is a ranking signal, not a probability.{attribution}\n\nSend another photo when ready.",
        "google": "Google Maps",
        "yandex": "Yandex Maps",
    },
    "ru": {
        "ready": "Отправьте уличную фотографию Москвы. GeoSnap работает экспериментально и может не дать координаты при слабых совпадениях.",
        "help": "Отправьте одно фото JPEG как изображение. Покрытие улиц Москвы неполное, поэтому GeoSnap иногда воздерживается от ответа. GPS-метаданные не используются, фото по умолчанию не сохраняются постоянно. Сменить язык: /language.",
        "unsupported": "Отправьте уличную фотографию как изображение. Подробнее: /help.",
        "processing": "Сравниваем визуальные признаки…",
        "cooldown": "Подождите {seconds} секунд перед следующим фото.",
        "too_large": "Изображение слишком большое или не читается. Отправьте корректный JPEG не больше 10 МиБ.",
        "rate_limited": "GeoSnap получил слишком много запросов. Немного подождите и повторите попытку.",
        "timeout": "Время локализации истекло. Повторите попытку через минуту.",
        "malformed": "GeoSnap вернул неожиданный ответ. Повторите попытку позже.",
        "unavailable": "GeoSnap временно недоступен. Повторите попытку позже.",
        "internal": "Не удалось обработать фото. Повторите попытку позже.",
        "low_confidence": "⚠️ Примерное место\n{lat:.4f}, {lon:.4f}\n\nGeoSnap нашёл возможную точку, но результат не прошёл порог уверенности. Он может быть сильно ошибочным.{attribution}\n\nМожно отправить следующее фото.",
        "low_confidence_no_prediction": "Визуальные совпадения найдены, но GeoSnap не смог определить пригодную для показа точку. Координаты не отображаются. Попробуйте другой ракурс с заметными зданиями или вывесками.\n\nМожно отправить следующее фото.",
        "out_of_coverage": "Сцена недостаточно представлена в текущей эталонной галерее Москвы. Само фото может быть корректным. Метка не отправлена.\n\nМожно отправить следующее фото.",
        "ok": "Предполагаемое место\n{lat:.4f}, {lon:.4f}\n\nСильные визуальные свидетельства прошли порог GeoSnap. Оценка ({score:.3f}) — сигнал ранжирования, а не вероятность.{attribution}\n\nМожно отправить следующее фото.",
        "google": "Google Карты",
        "yandex": "Яндекс Карты",
    },
}


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("🇷🇺 Русский", callback_data="language:ru"),
            InlineKeyboardButton("🇬🇧 English", callback_data="language:en"),
        ]]
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


def _language(context: Any) -> Language | None:
    value = context.user_data.get(LANGUAGE_KEY)
    return value if value in {"ru", "en"} else None


def _reference_attribution(
    result: dict[str, Any],
    language: Language,
    *,
    tentative: bool = False,
) -> str:
    sources = sorted(
        {
            str(match.get("source"))
            for match in result.get("matches", [])[:6]
            if isinstance(match, dict) and match.get("source")
        }
    )
    names = [
        "Mapillary" if source.lower() == "mapillary"
        else "KartaView" if source.lower() == "kartaview"
        else source
        for source in sources
    ]
    if not names:
        return ""
    if language == "en":
        prefix = "\nPossible reference imagery: " if tentative else "\nReference imagery: "
    else:
        prefix = "\nВозможные эталонные снимки: " if tentative else "\nЭталонные снимки: "
    return prefix + ", ".join(names)


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
        context.user_data.pop(LANGUAGE_KEY, None)
        context.user_data.pop(PENDING_PHOTO_KEY, None)
        if update.message:
            await update.message.reply_text(CHOOSE_LANGUAGE, reply_markup=language_keyboard())

    async def language(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message:
            await update.message.reply_text(CHOOSE_LANGUAGE, reply_markup=language_keyboard())

    async def choose_language(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None or query.data not in {"language:ru", "language:en"}:
            return
        await query.answer()
        language: Language = "ru" if query.data.endswith(":ru") else "en"
        context.user_data[LANGUAGE_KEY] = language
        pending = context.user_data.pop(PENDING_PHOTO_KEY, None)
        await query.edit_message_text(TEXT[language]["ready"])
        if pending is not None and query.message is not None and update.effective_user is not None:
            await self._process_photo(query.message, int(update.effective_user.id), pending, language)

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return
        language = _language(context)
        if language is None:
            await update.message.reply_text(CHOOSE_LANGUAGE, reply_markup=language_keyboard())
            return
        await update.message.reply_text(TEXT[language]["help"])

    async def unsupported(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return
        language = _language(context)
        if language is None:
            await update.message.reply_text(CHOOSE_LANGUAGE, reply_markup=language_keyboard())
            return
        await update.message.reply_text(TEXT[language]["unsupported"])

    async def _cooldown_remaining(self, user_id: int) -> int:
        now = self.clock()
        async with self._cooldown_lock:
            previous = self._last_request.get(user_id)
            if previous is not None and now - previous < self.settings.cooldown_seconds:
                return max(1, int(self.settings.cooldown_seconds - (now - previous) + 0.999))
            self._last_request[user_id] = now
        return 0

    async def photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.message
        user = update.effective_user
        if message is None or user is None or not message.photo:
            return
        photo = max(message.photo, key=lambda item: (item.width * item.height, item.file_size or 0))
        language = _language(context)
        if language is None:
            context.user_data[PENDING_PHOTO_KEY] = photo
            await message.reply_text(CHOOSE_LANGUAGE, reply_markup=language_keyboard())
            return
        await self._process_photo(message, int(user.id), photo, language)

    async def _process_photo(self, message: Any, user_id: int, photo: Any, language: Language) -> None:
        text = TEXT[language]
        wait_seconds = await self._cooldown_remaining(user_id)
        if wait_seconds:
            await message.reply_text(text["cooldown"].format(seconds=wait_seconds))
            return
        if photo.file_size and photo.file_size > self.settings.max_download_bytes:
            await message.reply_text(text["too_large"])
            return

        request_id = uuid.uuid4().hex
        progress = await message.reply_text(text["processing"])
        try:
            async with self._semaphore:
                telegram_file = await photo.get_file()
                payload = bytes(await telegram_file.download_as_bytearray())
                if not payload:
                    raise BackendRejectedImage("empty Telegram image")
                if len(payload) > self.settings.max_download_bytes:
                    raise BackendImageTooLarge("Telegram image exceeds bot limit")
                result = await self.backend.localize(payload, request_id=request_id)
            await self._render(message, progress, result, language)
            logger.info("photo localization complete", extra={"event": "photo_complete", "request_id": request_id})
        except (BackendRejectedImage, BackendImageTooLarge):
            await self._finish(progress, message, text["too_large"])
        except BackendRateLimited:
            await self._finish(progress, message, text["rate_limited"])
        except BackendTimeout:
            await self._finish(progress, message, text["timeout"])
        except InvalidBackendResponse:
            await self._finish(progress, message, text["malformed"])
        except BackendFailure:
            await self._finish(progress, message, text["unavailable"])
        except TelegramError:
            logger.exception("Telegram API error", extra={"event": "telegram_api_error", "request_id": request_id, "error_category": "telegram_api"})
        except Exception as exc:
            logger.exception("unexpected bot error", extra={"event": "bot_error", "request_id": request_id, "error_category": type(exc).__name__})
            try:
                await self._finish(progress, message, text["internal"])
            except TelegramError:
                pass

    async def _finish(self, progress: Any, message: Any, text: str, **kwargs: Any) -> None:
        try:
            await progress.edit_text(text, **kwargs)
        except (AttributeError, TelegramError):
            await message.reply_text(text, **kwargs)

    async def _render(
        self,
        message: Any,
        progress: Any,
        result: dict[str, Any],
        language: Language,
    ) -> None:
        status = result["status"]
        text = TEXT[language]
        if status == "low_confidence":
            prediction = result.get("prediction")
            if not isinstance(prediction, dict):
                await self._finish(progress, message, text["low_confidence_no_prediction"])
                return
            lat = float(prediction["lat"])
            lon = float(prediction["lon"])
            keyboard = InlineKeyboardMarkup(
                [[
                    InlineKeyboardButton(text["google"], url=google_maps_url(lat, lon)),
                    InlineKeyboardButton(text["yandex"], url=yandex_maps_url(lat, lon)),
                ]]
            )
            await self._finish(
                progress,
                message,
                text["low_confidence"].format(
                    lat=lat,
                    lon=lon,
                    attribution=_reference_attribution(result, language, tentative=True),
                ),
                reply_markup=keyboard,
            )
            return
        if status == "out_of_coverage":
            await self._finish(progress, message, text["out_of_coverage"])
            return
        prediction = result["prediction"]
        lat = float(prediction["lat"])
        lon = float(prediction["lon"])
        score = float(prediction["confidence"])
        attribution = _reference_attribution(result, language)
        keyboard = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton(text["google"], url=google_maps_url(lat, lon)),
                InlineKeyboardButton(text["yandex"], url=yandex_maps_url(lat, lon)),
            ]]
        )
        await self._finish(
            progress,
            message,
            text["ok"].format(lat=lat, lon=lon, score=score, attribution=attribution),
            reply_markup=keyboard,
        )
        await message.reply_location(latitude=lat, longitude=lon)


def build_application(settings: BotSettings) -> Application:
    settings.validate()
    client = BackendClient(settings.backend_url, settings.backend_timeout_seconds)
    bot = GeoSnapBot(settings, client)
    application = Application.builder().token(settings.token).concurrent_updates(settings.max_concurrency).build()
    application.add_handler(CommandHandler("start", bot.start))
    application.add_handler(CommandHandler("help", bot.help))
    application.add_handler(CommandHandler("language", bot.language))
    application.add_handler(CallbackQueryHandler(bot.choose_language, pattern=r"^language:(ru|en)$"))
    application.add_handler(MessageHandler(filters.PHOTO, bot.photo))
    application.add_handler(MessageHandler(filters.ALL, bot.unsupported))

    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        del update
        logger.error("unhandled Telegram update error", extra={"event": "update_error", "error_category": type(context.error).__name__ if context.error else "unknown"})

    application.add_error_handler(error_handler)
    return application


def user_key_for_diagnostics(user_id: int, salt: str) -> str:
    """Return a non-reversible short key if aggregate diagnostics ever need one."""

    return hashlib.sha256(f"{salt}:{user_id}".encode()).hexdigest()[:12]
