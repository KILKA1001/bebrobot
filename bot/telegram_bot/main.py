"""Telegram bot entrypoint scaffold.

Keeps Telegram runtime separated from Discord runtime.
"""

from bot.telegram_bot.config import TELEGRAM_BOT_TOKEN_ENV, get_telegram_bot_token


def main() -> None:
    token = get_telegram_bot_token()
    if not token:
        raise RuntimeError(
            f"Не задана переменная окружения {TELEGRAM_BOT_TOKEN_ENV}. "
            "Добавьте её в Render перед запуском Telegram-процесса."
        )

    # Runtime scaffold only. Actual Telegram polling/webhook wiring will be added next.
    print("✅ Telegram scaffold готов: токен найден, можно подключать runtime и команды.")


if __name__ == "__main__":
    main()
