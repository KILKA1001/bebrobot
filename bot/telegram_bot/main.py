"""Telegram runtime module, called from unified launcher in `bot/main.py`.

Not a required separate process entrypoint anymore; we keep it as an isolated
Telegram runtime function to avoid mixing Telegram code with Discord logic.
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
