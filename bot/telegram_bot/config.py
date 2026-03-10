import os


TELEGRAM_BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
TELEGRAM_API_TOKEN_ENV = "TELEGRAM_API_TOKEN"


def get_telegram_bot_token() -> str:
    """Return Telegram bot token from env with backward-compatible fallback.

    Priority:
    1) TELEGRAM_BOT_TOKEN
    2) TELEGRAM_API_TOKEN
    """

    primary = (os.getenv(TELEGRAM_BOT_TOKEN_ENV) or "").strip()
    if primary:
        return primary

    return (os.getenv(TELEGRAM_API_TOKEN_ENV) or "").strip()
