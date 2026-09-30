# Telegram bot

The Telegram client is a lightweight, long-polling `python-telegram-bot` service in `apps/telegram_bot`. It sends image bytes to the internal FastAPI `/localize` endpoint and contains no Torch, FAISS, SAGE code, checkpoint or index.

## Setup

Create a bot with BotFather, keep the token server-side, and set:

```dotenv
TELEGRAM_BOT_TOKEN=<secret>
TELEGRAM_VALIDATE_ONLY=false
TELEGRAM_USER_COOLDOWN_SECONDS=8
TELEGRAM_MAX_CONCURRENCY=2
TELEGRAM_MAX_DOWNLOAD_BYTES=10485760
TELEGRAM_BACKEND_TIMEOUT_SECONDS=30
TELEGRAM_BATCH_TIMEOUT_SECONDS=210
TELEGRAM_MAX_BATCH_BYTES=41943040
TELEGRAM_COLLECTION_TTL_SECONDS=600
TELEGRAM_BOT_PUBLIC_URL=https://t.me/<public-bot-name>
```

`TELEGRAM_BOT_PUBLIC_URL` is optional and public; it only enables the website CTA. `TELEGRAM_BOT_TOKEN` is required for polling and must never enter Git, logs, a Dockerfile or the frontend. `TELEGRAM_VALIDATE_ONLY=true` validates configuration without contacting Telegram.

The opt-in cleaned SAGE-L local deployment overrides the single-photo timeout to 120 seconds and the multi-photo timeout to 620 seconds because CPU inference is much slower. These are separate from the default frozen release settings above. Docker Desktop is currently stopped on the original Mac; the bot is not polling until it is started again.

## Language and commands

`/start` first presents inline `🇷🇺 Русский` and `🇬🇧 English` choices. The selection lives only in process-memory `context.user_data`. `/language` reopens the selector and `/help` is localized. If a photo arrives before selection, its transient Telegram photo object is retained in memory and processed immediately after language choice; nothing is written to persistent storage.

Every supported response is localized in Russian and English:

- `ok`: processing text is replaced by “Estimated location” / “Предполагаемое место”, four-decimal coordinates, strong-evidence wording, an explicit “not a probability” caveat, Google and Yandex inline buttons, provider attribution, a native location pin, and a prompt for the next photo;
- `low_confidence` with a prediction: sends a localized “Tentative location” / “Примерное место” warning, four-decimal coordinates, Google/Yandex buttons, and lightweight possible-reference attribution. It deliberately sends no native Telegram location because that presentation looks too authoritative;
- `low_confidence` without a prediction: explains that no usable point is available and sends no coordinates, map buttons, or native location;
- `out_of_coverage`: explains the current gallery gap without calling the photo invalid, and sends no pin, coordinates or map buttons;
- rate limit, timeout, backend unavailable, malformed response, invalid/oversize image, cooldown and unsupported-message paths provide concise localized recovery text.

The highest-resolution Telegram photo variant is downloaded in memory, checked before and after download, forwarded with a generated request ID, and released after the handler returns. Telegram metadata and user location are not localization inputs.

## Several views from one place

Send an album containing up to **10 photos**, wait for the counter to show the complete set, then press **Locate / Определить**. The bot does not guess when an album has finished arriving. For separate messages, start with `/photos`, send the photos, then `/done`; use `/cancel` to discard the set. Take different directions while staying in the same place. Telegram compression is supported; send photos rather than file attachments.

Sets are isolated by user, chat and forum topic; callback buttons are bound to the owning set. Repeated Telegram file IDs are ignored; the backend also removes exact decoded-pixel duplicates. A different album cannot silently join an unfinished album. Up to 128 pending sets can be retained; inactive sets expire after 10 minutes and are purged on the next update. A restart discards them. Only file references are held before submission, and image bytes exist transiently during inference. Telegram itself has its own storage policy.

For multiple unique photos, the backend first checks agreement among the per-photo selected locations; if none wins unambiguously, it combines lower-ranked candidate hypotheses at the location level. The bot displays support counts, a tentative estimate and map links when one place wins; conflicting evidence requests a different view. It never sends a native location pin for uncalibrated multi-photo consensus. One unique image retains the single-photo behavior. Details and API examples: [multi-photo protocol](multi_photo.md).

## Capacity and privacy

The bot has a per-user cooldown, a process-wide semaphore, a 10 MiB default per-photo download limit and a 40 MiB set limit. Requests still pass through backend rate and concurrency controls. Only one request per user runs at a time; a pending set remains available if submission is rejected by cooldown. Logs contain event category and request ID, never photo bytes, token, username or chat content. There is no user database.

## Tests and future webhook migration

```bash
.venv/bin/pytest -q apps/telegram_bot/tests
```

Tests mock Telegram and backend calls and require no real token. They cover language selection, photo-before-language, `/help`, accepted and tentative rendering in both languages, the no-prediction fallback, true out-of-coverage and errors, map-link coordinate order, the tentative/native-location distinction, size limits, cooldown and concurrency.

A later webhook deployment would add a public HTTPS Telegram route and secret verification while keeping the same handlers and internal backend client. Long polling remains simpler for the current single-instance MVP.
