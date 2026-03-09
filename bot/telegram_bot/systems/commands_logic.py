from bot.telegram_bot.link_handler import handle_link_command


HELPY_TEXT = (
    "📚 Список команд:\n"
    "/link <код> — привязать Telegram к Discord аккаунту\n"
    "/helpy — показать это сообщение"
)


def get_helpy_text() -> str:
    return HELPY_TEXT


def process_link_command(message_text: str, telegram_user_id: int | None) -> str:
    text = (message_text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return "Использование: /link <код>"

    if telegram_user_id is None:
        return "Не удалось определить пользователя Telegram."

    code = parts[1].strip()
    success, payload = handle_link_command(telegram_user_id, code)
    prefix = "✅" if success else "❌"
    return f"{prefix} {payload}"
