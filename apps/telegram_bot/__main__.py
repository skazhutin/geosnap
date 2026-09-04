from __future__ import annotations

import sys
import threading
from dataclasses import replace

from .bot import build_application, configure_logging
from .config import BotSettings


def main() -> None:
    settings = BotSettings.from_env()
    validate_once = sys.argv[1:] == ["--validate-config"]
    if sys.argv[1:] and not validate_once:
        raise SystemExit("usage: python -m apps.telegram_bot [--validate-config]")
    if validate_once:
        settings = replace(settings, validate_only=True)
    settings.validate()
    configure_logging(settings.log_level)
    if validate_once:
        print("GeoSnap Telegram bot configuration is valid; polling disabled.")
        return
    if settings.validate_only:
        print("GeoSnap Telegram bot configuration is valid; waiting without Telegram network access.")
        threading.Event().wait()
        return
    application = build_application(settings)
    application.run_polling(
        allowed_updates=["message"],
        drop_pending_updates=False,
        close_loop=True,
    )


if __name__ == "__main__":
    main()
