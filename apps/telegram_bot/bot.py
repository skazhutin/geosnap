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
from .collections import CollectionKey, PhotoCollection
from .config import BotSettings
from .map_links import google_maps_url, yandex_maps_url

logger = logging.getLogger("geosnap.telegram")
Language = Literal["ru", "en"]
LANGUAGE_KEY = "language"
PENDING_PHOTO_KEY = "pending_photo"
CHOOSE_LANGUAGE = "Выберите язык · Choose your language"

TEXT: dict[Language, dict[str, str]] = {
    "en": {
        "ready": "Send a Moscow street photo, or an album of up to 10 views from the same place. Use /photos to collect separate photos. GeoSnap is experimental.",
        "help": "Send a JPEG photo as an image for an immediate result. For several views of ONE place, send an album or use /photos, add up to 10 photos, then tap Locate or send /done. Use /cancel to discard the set. Include buildings, intersections or signs; avoid duplicate views. More photos may help, but accuracy is not guaranteed. Collections expire after 10 minutes. Moscow coverage is incomplete. GPS metadata is not used; photos are processed in memory. Use /language to switch language.",
        "collecting": "Photos collected: {count}/10. All photos must be taken from the same place, facing different directions. Wait for the complete album, then tap Locate or send /done. Add more photos or use /cancel.\nThis temporary set expires after {minutes:g} minutes of inactivity.",
        "locate": "Locate",
        "cancel": "Cancel",
        "cancelled": "Photo set cleared. Send a new photo or use /photos.",
        "empty_collection": "No photos in this set. Send an album or use /photos, add photos, then /done.",
        "stale_collection": "This set has expired, was cancelled, or belongs to another user. Start your own with /photos.",
        "collection_limit": "This set already has 10 photos. Tap Locate or /done; the extra photo was not added.",
        "duplicate": "This photo is already in the set. Add a different direction.",
        "different_album": "Finish the current album with /done or cancel it before sending a different album. Use /photos to combine separate uploads from one place.",
        "collection_busy": "Too many pending photo sets. Please retry shortly.",
        "already_processing": "Your previous photos are still being processed. Please wait for the result.",
        "batch_processing": "Comparing {count} photos from one place… This can take up to three minutes.",
        "batch_too_large": "The whole set exceeds {limit_mib:g} MiB. Send fewer or smaller photos.",
        "batch_result": "⚠️ Tentative location from several views\n{lat:.4f}, {lon:.4f}\n\n{support} of {count} distinct photos support this area. This experimental agreement does not guarantee a correct location.{attribution}\n\nYou can start another set with /photos.",
        "batch_ambiguous": "The photos did not establish one shared location. No point is shown. Check that they were taken from the same place and try views with buildings, intersections or signs. Start again with /photos.",
        "duplicates_ignored": "Repeated photos ignored: {count}.",
        "unsupported": "Please send a street photo as an image. Use /help for details.",
        "processing": "Comparing visual evidence…",
        "cooldown": "Please wait {seconds} seconds before sending another photo.",
        "too_large": "That image is too large or unreadable. Send a valid JPEG no larger than 10 MiB.",
        "rate_limited": "GeoSnap is receiving too many requests. Wait a moment and try again.",
        "timeout": "Localization timed out. Please try again in a moment.",
        "malformed": "GeoSnap returned an unexpected response. Please try again later.",
        "unavailable": "GeoSnap is temporarily unavailable. Please try again later.",
        "internal": "GeoSnap could not process the photo. Please try again later.",
        "low_confidence": "⚠️ Tentative location\n{lat:.4f}, {lon:.4f}\n\nGeoSnap found a possible point, but its reliability has not been established. This result may be significantly wrong.{attribution}\n\nYou can send another photo.",
        "low_confidence_no_prediction": "Potential visual matches were found, but GeoSnap could not produce a usable location. No point is shown. Try another angle with distinctive buildings or signs.\n\nYou can send another photo.",
        "out_of_coverage": "This scene is not sufficiently represented by GeoSnap’s current Moscow reference gallery. The photo itself may still be valid. No pin was sent.\n\nSend another photo when ready.",
        "ok": "Estimated location\n{lat:.4f}, {lon:.4f}\n\nStrong visual evidence passed GeoSnap’s acceptance policy. The evidence score ({score:.3f}) is a ranking signal, not a probability.{attribution}\n\nSend another photo when ready.",
        "google": "Google Maps",
        "yandex": "Yandex Maps",
    },
    "ru": {
        "ready": "Отправьте уличную фотографию Москвы или альбом до 10 ракурсов одного места. Для сбора отдельных снимков используйте /photos. GeoSnap работает экспериментально.",
        "help": "Одно фото JPEG как изображение — сразу получить результат. Для нескольких ракурсов ОДНОГО места отправьте альбом или нажмите /photos, добавьте до 10 фото и нажмите «Определить» или /done. /cancel — очистить набор. Снимайте здания, перекрёстки и вывески с разных сторон. Несколько фото могут помочь, но точность не гарантирована. Набор хранится 10 минут. Покрытие Москвы неполное. GPS-метаданные не используются, фото обрабатываются в памяти. Сменить язык: /language.",
        "collecting": "Собрано фото: {count}/10. Все снимки должны быть из одного места, с разными направлениями камеры. Дождитесь загрузки всего альбома и нажмите «Определить» или /done. Можно добавить фото или отменить набор: /cancel.\nНабор истекает через {minutes:g} минут без новых фото.",
        "locate": "Определить",
        "cancel": "Отмена",
        "cancelled": "Набор очищен. Отправьте новое фото или начните /photos.",
        "empty_collection": "В наборе нет фотографий. Отправьте альбом или начните /photos, добавьте фото и нажмите /done.",
        "stale_collection": "Набор истёк, отменён или принадлежит другому пользователю. Начните свой: /photos.",
        "collection_limit": "В наборе уже 10 фото. Нажмите «Определить» или /done; дополнительное фото не добавлено.",
        "duplicate": "Это фото уже есть в наборе. Добавьте другой ракурс.",
        "different_album": "Завершите текущий альбом командой /done или отмените его. Для объединения отдельных загрузок одного места используйте /photos.",
        "collection_busy": "Сейчас слишком много незавершённых наборов. Попробуйте немного позже.",
        "already_processing": "Предыдущие фотографии ещё обрабатываются. Дождитесь результата.",
        "batch_processing": "Сравниваем {count} фото одного места… Это может занять до трёх минут.",
        "batch_too_large": "Общий размер набора превышает {limit_mib:g} МиБ. Отправьте меньше фото или уменьшите их размер.",
        "batch_result": "⚠️ Примерное место по нескольким ракурсам\n{lat:.4f}, {lon:.4f}\n\nЭту область поддерживают {support} из {count} разных фото. Это экспериментальное совпадение, оно не гарантирует правильный адрес.{attribution}\n\nНачать новый набор: /photos.",
        "batch_ambiguous": "Фотографии не дали однозначного общего места. Координаты не показаны. Проверьте, что все снимки сделаны из одной точки, и попробуйте ракурсы со зданиями, перекрёстками или вывесками. Начать заново: /photos.",
        "duplicates_ignored": "Повторные фотографии не учитывались: {count}.",
        "unsupported": "Отправьте уличную фотографию как изображение. Подробнее: /help.",
        "processing": "Сравниваем визуальные признаки…",
        "cooldown": "Подождите {seconds} секунд перед следующим фото.",
        "too_large": "Изображение слишком большое или не читается. Отправьте корректный JPEG не больше 10 МиБ.",
        "rate_limited": "GeoSnap получил слишком много запросов. Немного подождите и повторите попытку.",
        "timeout": "Время локализации истекло. Повторите попытку через минуту.",
        "malformed": "GeoSnap вернул неожиданный ответ. Повторите попытку позже.",
        "unavailable": "GeoSnap временно недоступен. Повторите попытку позже.",
        "internal": "Не удалось обработать фото. Повторите попытку позже.",
        "low_confidence": "⚠️ Примерное место\n{lat:.4f}, {lon:.4f}\n\nGeoSnap нашёл возможную точку, но её надёжность пока не подтверждена. Результат может быть сильно ошибочным.{attribution}\n\nМожно отправить следующее фото.",
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
    def __init__(self, secret: str = "") -> None:
        super().__init__()
        self.secret = secret

    def format(self, record: logging.LogRecord) -> str:
        output = json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
                "level": record.levelname.lower(),
                "event": getattr(record, "event", record.getMessage()),
                "request_id": getattr(record, "request_id", None),
                "error_category": getattr(record, "error_category", None),
            },
            separators=(",", ":"),
        )
        return output.replace(self.secret, "[REDACTED]") if self.secret else output


def configure_logging(level: str, *, secret: str = "") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter(secret))
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
        self._collections: dict[CollectionKey, PhotoCollection] = {}
        self._closed_albums: dict[tuple[CollectionKey, str], float] = {}
        self._inflight: set[int] = set()

    @staticmethod
    def _key(message: Any, user_id: int) -> CollectionKey:
        return (int(getattr(message, "chat_id", user_id)), user_id, getattr(message, "message_thread_id", None))

    def _expire_collections(self) -> None:
        now = self.clock()
        self._collections = {key: value for key, value in self._collections.items() if value.expires_at > now}
        self._closed_albums = {key: value for key, value in self._closed_albums.items() if value > now}

    async def _show_collection(self, message: Any, collection: PhotoCollection, language: Language) -> None:
        text = TEXT[language]
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(text["locate"], callback_data=f"photos:done:{collection.token}"),
            InlineKeyboardButton(text["cancel"], callback_data=f"photos:cancel:{collection.token}"),
        ]])
        content = text["collecting"].format(count=len(collection.photos), minutes=self.settings.collection_ttl_seconds / 60)
        if collection.prompt is None:
            collection.prompt = await message.reply_text(content, reply_markup=keyboard)
        else:
            await self._finish(collection.prompt, message, content, reply_markup=keyboard)

    async def photos(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is None or update.effective_user is None:
            return
        self._expire_collections()
        key = self._key(update.message, int(update.effective_user.id))
        collection = self._collections.get(key)
        language = _language(context)
        if collection is None:
            if len(self._collections) >= self.settings.max_pending_collections:
                await update.message.reply_text(TEXT[language or "en"]["collection_busy"])
                return
            collection = self._collections[key] = PhotoCollection(self.clock() + self.settings.collection_ttl_seconds)
        async with collection.lock:
            if language is None:
                await update.message.reply_text(CHOOSE_LANGUAGE, reply_markup=language_keyboard())
            else:
                await self._show_collection(update.message, collection, language)

    async def done(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is not None and update.effective_user is not None:
            await self._finish_collection(update.message, int(update.effective_user.id), _language(context))

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message is not None and update.effective_user is not None:
            await self._finish_collection(update.message, int(update.effective_user.id), _language(context), cancel=True)

    async def collection_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None or query.message is None or update.effective_user is None:
            return
        await query.answer()
        _, action, token = str(query.data).split(":", 2)
        await self._finish_collection(query.message, int(update.effective_user.id), _language(context), cancel=action == "cancel", token=token)

    async def _finish_collection(self, message: Any, user_id: int, language: Language | None, *, cancel: bool = False, token: str | None = None) -> None:
        self._expire_collections()
        if language is None:
            await message.reply_text(CHOOSE_LANGUAGE, reply_markup=language_keyboard())
            return
        key = self._key(message, user_id)
        collection = self._collections.get(key)
        if collection is None or (token is not None and token != collection.token):
            await message.reply_text(TEXT[language]["stale_collection" if token else "empty_collection"])
            return
        async with collection.lock:
            if self._collections.get(key) is not collection:
                return
            if not cancel and not collection.photos:
                await message.reply_text(TEXT[language]["empty_collection"])
                return
            if not cancel and not await self._reserve_request(message, user_id, collection.photos, language):
                return
            self._collections.pop(key)
            if collection.media_group_id:
                self._closed_albums[(key, collection.media_group_id)] = self.clock() + self.settings.collection_ttl_seconds
            photos = list(collection.photos)
            if collection.prompt is not None:
                try:
                    await collection.prompt.edit_reply_markup(reply_markup=None)
                except (AttributeError, TelegramError):
                    pass
        if cancel:
            await message.reply_text(TEXT[language]["cancelled"])
        else:
            await self._process_photos(message, user_id, photos, language, reserved=True)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        context.user_data.pop(LANGUAGE_KEY, None)
        context.user_data.pop(PENDING_PHOTO_KEY, None)
        if update.message is not None and update.effective_user is not None:
            self._collections.pop(self._key(update.message, int(update.effective_user.id)), None)
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
        self._expire_collections()
        if query.message is not None and update.effective_user is not None:
            collection = self._collections.get(self._key(query.message, int(update.effective_user.id)))
            if collection is not None:
                async with collection.lock:
                    collection.prompt = query.message
                    await self._show_collection(query.message, collection, language)
                return
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

    async def _reserve_request(self, message: Any, user_id: int, photos: list[Any], language: Language) -> bool:
        text = TEXT[language]
        if any(photo.file_size and photo.file_size > self.settings.max_download_bytes for photo in photos):
            await message.reply_text(text["too_large"])
            return False
        now = self.clock()
        async with self._cooldown_lock:
            self._last_request = {key: value for key, value in self._last_request.items() if now - value < self.settings.cooldown_seconds}
            previous = self._last_request.get(user_id)
            if user_id in self._inflight:
                rejection = text["already_processing"]
            elif previous is not None and now - previous < self.settings.cooldown_seconds:
                rejection = text["cooldown"].format(seconds=max(1, int(self.settings.cooldown_seconds - (now - previous) + 0.999)))
            else:
                self._last_request[user_id] = now
                self._inflight.add(user_id)
                return True
        await message.reply_text(rejection)
        return False

    async def photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.message
        user = update.effective_user
        if message is None or user is None or not message.photo:
            return
        photo = max(message.photo, key=lambda item: (item.width * item.height, item.file_size or 0))
        language = _language(context)
        self._expire_collections()
        key = self._key(message, int(user.id))
        group = getattr(message, "media_group_id", None)
        if group and (key, group) in self._closed_albums:
            await message.reply_text(TEXT[language or "en"]["stale_collection"])
            return
        collection = self._collections.get(key)
        if group or collection is not None:
            if collection is None:
                if len(self._collections) >= self.settings.max_pending_collections:
                    await message.reply_text(TEXT[language or "en"]["collection_busy"])
                    return
                collection = self._collections[key] = PhotoCollection(self.clock() + self.settings.collection_ttl_seconds, media_group_id=group)
            async with collection.lock:
                if self._collections.get(key) is not collection:
                    return
                if group and collection.media_group_id and group != collection.media_group_id:
                    await message.reply_text(TEXT[language or "en"]["different_album"])
                    return
                identity = getattr(photo, "file_unique_id", None) or getattr(photo, "file_id", None) or str(id(photo))
                if identity in collection.identities:
                    await message.reply_text(TEXT[language or "en"]["duplicate"])
                    return
                if len(collection.photos) >= 10:
                    await message.reply_text(TEXT[language or "en"]["collection_limit"])
                    return
                if photo.file_size and photo.file_size > self.settings.max_download_bytes:
                    await message.reply_text(TEXT[language or "en"]["too_large"])
                    return
                collection.photos.append(photo)
                collection.identities.add(identity)
                collection.expires_at = self.clock() + self.settings.collection_ttl_seconds
                if language is None:
                    if collection.prompt is None:
                        collection.prompt = await message.reply_text(CHOOSE_LANGUAGE, reply_markup=language_keyboard())
                else:
                    await self._show_collection(message, collection, language)
            return
        if language is None:
            context.user_data[PENDING_PHOTO_KEY] = photo
            await message.reply_text(CHOOSE_LANGUAGE, reply_markup=language_keyboard())
            return
        await self._process_photo(message, int(user.id), photo, language)

    async def _process_photo(self, message: Any, user_id: int, photo: Any, language: Language) -> None:
        await self._process_photos(message, user_id, [photo], language)

    async def _process_photos(self, message: Any, user_id: int, photos: list[Any], language: Language, *, reserved: bool = False) -> None:
        text = TEXT[language]
        if not reserved and not await self._reserve_request(message, user_id, photos, language):
            return

        request_id = uuid.uuid4().hex
        progress = None
        try:
            progress = await message.reply_text(text["processing"] if len(photos) == 1 else text["batch_processing"].format(count=len(photos)))
            async with self._semaphore:
                payloads = []
                total = 0
                for photo in photos:
                    telegram_file = await photo.get_file()
                    payload = bytes(await telegram_file.download_as_bytearray())
                    if not payload:
                        raise BackendRejectedImage("empty Telegram image")
                    if len(payload) > self.settings.max_download_bytes:
                        raise BackendImageTooLarge("Telegram image exceeds bot limit")
                    total += len(payload)
                    if total > self.settings.max_batch_bytes:
                        await self._finish(progress, message, text["batch_too_large"].format(limit_mib=self.settings.max_batch_bytes / 1024**2))
                        return
                    payloads.append(payload)
                result = (await self.backend.localize(payloads[0], request_id=request_id) if len(payloads) == 1
                          else await self.backend.localize_multi(payloads, request_id=request_id))
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
        finally:
            self._inflight.discard(user_id)

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
        evidence = result.get("multi_photo")
        if isinstance(evidence, dict):
            if evidence["duplicate_images"]:
                await message.reply_text(text["duplicates_ignored"].format(count=evidence["duplicate_images"]))
            if evidence["unique_images"] > 1:
                prediction = result.get("prediction")
                if evidence["agreement"] != "consensus" or not prediction:
                    await self._finish(progress, message, text["batch_ambiguous"])
                    return
                lat, lon = float(prediction["lat"]), float(prediction["lon"])
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton(text["google"], url=google_maps_url(lat, lon)),
                    InlineKeyboardButton(text["yandex"], url=yandex_maps_url(lat, lon)),
                ]])
                await self._finish(progress, message, text["batch_result"].format(
                    lat=lat, lon=lon, support=evidence["supporting_images"], count=evidence["unique_images"],
                    attribution=_reference_attribution(result, language, tentative=True)), reply_markup=keyboard)
                return
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
    client = BackendClient(settings.backend_url, settings.backend_timeout_seconds, batch_timeout_seconds=settings.batch_timeout_seconds)
    bot = GeoSnapBot(settings, client)
    application = Application.builder().token(settings.token).concurrent_updates(settings.max_concurrency).build()
    application.add_handler(CommandHandler("start", bot.start))
    application.add_handler(CommandHandler("help", bot.help))
    application.add_handler(CommandHandler("language", bot.language))
    application.add_handler(CommandHandler("photos", bot.photos))
    application.add_handler(CommandHandler("done", bot.done))
    application.add_handler(CommandHandler("cancel", bot.cancel))
    application.add_handler(CallbackQueryHandler(bot.collection_action, pattern=r"^photos:(done|cancel):[0-9a-f]{16}$"))
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
