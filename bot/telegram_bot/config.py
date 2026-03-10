import os


TELEGRAM_BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"


def get_telegram_bot_token() -> str:
    """Return Telegram bot token from env."""

    return (os.getenv(TELEGRAM_BOT_TOKEN_ENV) or "").strip()
