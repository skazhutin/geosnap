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
TELEGRAM_BOT_PUBLIC_URL=https://t.me/<public-bot-name>
```

`TELEGRAM_BOT_PUBLIC_URL` is optional and public; it only enables the website CTA. `TELEGRAM_BOT_TOKEN` is required for polling and must never enter Git, logs, a Dockerfile or the frontend. `TELEGRAM_VALIDATE_ONLY=true` validates configuration without contacting Telegram.

## Language and commands

`/start` first presents inline `🇷🇺 Русский` and `🇬🇧 English` choices. The selection lives only in process-memory `context.user_data`. `/language` reopens the selector and `/help` is localized. If a photo arrives before selection, its transient Telegram photo object is retained in memory and processed immediately after language choice; nothing is written to persistent storage.

Every supported response is localized in Russian and English:

- `ok`: processing text is replaced by “Estimated location” / “Предполагаемое место”, four-decimal coordinates, strong-evidence wording, an explicit “not a probability” caveat, Google and Yandex inline buttons, provider attribution, a native location pin, and a prompt for the next photo;
- `low_confidence`: explains that evidence is insufficient and sends no pin, coordinates or map buttons;
- `out_of_coverage`: explains the current gallery gap without calling the photo invalid, and sends no pin, coordinates or map buttons;
- rate limit, timeout, backend unavailable, malformed response, invalid/oversize image, cooldown and unsupported-message paths provide concise localized recovery text.

The highest-resolution Telegram photo variant is downloaded in memory, checked before and after download, forwarded with a generated request ID, and released after the handler returns. Telegram metadata and user location are not localization inputs.

## Capacity and privacy

The bot has a per-user cooldown, a process-wide semaphore and a 10 MiB default download limit. Requests still pass through backend rate and concurrency controls. Logs contain event category and request ID, never photo bytes, token, username or chat content. There is no user database.

## Tests and future webhook migration

```bash
.venv/bin/pytest -q apps/telegram_bot/tests
```

Tests mock Telegram and backend calls and require no real token. They cover language selection, photo-before-language, `/help`, both languages for success/abstention/errors, map-link coordinate order, size limits, cooldown and concurrency.

A later webhook deployment would add a public HTTPS Telegram route and secret verification while keeping the same handlers and internal backend client. Long polling remains simpler for the current single-instance MVP.
