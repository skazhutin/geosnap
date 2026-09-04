# Telegram bot

The GeoSnap Telegram bot is a first-class but lightweight client of the shared FastAPI backend. It never imports Torch, FAISS, SAGE, the checkpoint, or the production index. `python-telegram-bot` 22.8 handles the Bot API, and production uses long polling so no additional public webhook route is required.

## Configuration and startup

Create a bot with BotFather and place its token only in the server-side `TELEGRAM_BOT_TOKEN` environment variable. Never use a `VITE_*` variable for the token. Relevant settings are documented in `.env.example`:

- `TELEGRAM_BOT_TOKEN`: required except in validation-only mode.
- `TELEGRAM_BACKEND_URL`: internal origin; production Compose fixes this to `http://backend:8000`.
- `TELEGRAM_USER_COOLDOWN_SECONDS`: per-user in-memory cooldown, default 8 seconds.
- `TELEGRAM_MAX_CONCURRENCY`: maximum simultaneous bot update/localization work, default 2.
- `TELEGRAM_MAX_DOWNLOAD_BYTES`: maximum Telegram download, default 10 MiB.
- `TELEGRAM_BACKEND_TIMEOUT_SECONDS`: bot-to-backend timeout, default 30 seconds.
- `TELEGRAM_VALIDATE_ONLY`: validates startup and keeps an idle container without contacting Telegram; useful when no token is available for a local Compose smoke.

Production is started with the rest of the platform using `make production-up`. For a tokenless configuration smoke set `TELEGRAM_VALIDATE_ONLY=true` and leave the token empty. The bot remains idle after validation in this mode; it does not poll or fake a live service. CI uses `python -m apps.telegram_bot --validate-config` for a one-shot check.

## User behavior

`/start` explains that GeoSnap estimates supported Moscow street scenes, results are approximate, and weak evidence can lead to abstention. `/help` explains accepted photo input, experimental/incomplete Moscow coverage, abstention, and default non-retention.

For a photo the bot selects Telegram's highest available photo resolution, enforces the download limit both before and after download, holds the bytes only in memory, creates a request ID, and posts the image to the internal backend `/localize` endpoint. Telegram metadata and user location are never used as localization evidence.

- `ok`: sends “Estimated location,” coordinates, a native Telegram location pin, an OpenStreetMap link, and an evidence score explicitly described as a ranking signal—not a probability. Reference providers are attributed when returned.
- `low_confidence`: says potential matches exist but evidence is insufficient and sends no pin or coordinates.
- `out_of_coverage`: says current Moscow references do not sufficiently represent the scene and sends no pin or coordinates.
- Invalid/oversized image, backend 429, timeout, not-ready/5xx, malformed responses, Telegram errors, and unexpected failures map to concise retry-safe messages. Stack traces and internal details are never sent to the user.

Bot-side cooldown and concurrency are defense in depth. Every photo still calls the backend, so the backend's IP/client rate limit, bounded inference concurrency, queue, upload validation, and timeout remain authoritative.

## Privacy and logging

The bot does not permanently store uploaded images, messages, usernames, or raw Telegram IDs. Images are downloaded into memory and discarded after the request. Logs contain a generated request ID and error category but no bot token, raw bytes, exact predicted coordinates, username, or message content. Cooldown identifiers are held only in process memory.

The Bot API itself necessarily transfers the user's photo through Telegram; deployments should reflect that platform boundary in any future public privacy policy.

## Tests and verification

`apps/telegram_bot/tests` mocks both Telegram and backend I/O. It covers commands, supported and unsupported inputs, all three product statuses, invalid and oversized images, backend unavailability, timeout, 429, malformed payloads, cooldown, concurrency, and multipart forwarding. No real token is needed.

The production verifier runs the bot image in validation-only mode. A real Bot API smoke is optional and must be performed manually only when a token is already available; do not print or record it.

## Future webhook migration

If operational requirements later favor webhooks, expose a dedicated HTTPS bot endpoint, validate Telegram's secret token header, remove long polling, and keep the bot-to-backend request internal. This is a deployment change only: localization must remain exclusively in FastAPI.
