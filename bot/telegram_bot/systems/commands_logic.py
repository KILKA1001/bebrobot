from bot.services import AccountsService
from bot.telegram_bot.link_handler import handle_link_command
from bot.systems.linking_logic import issue_telegram_discord_link_code, register_telegram_account


HELPY_TEXT = (
    "📚 Список команд:\n"
    "/register — зарегистрировать общий аккаунт\n"
    "/profile — показать профиль общего аккаунта\n"
    "/link <код> — привязать Telegram к аккаунту по коду из Discord\n"
    "/link_discord — получить код для привязки Discord\n"
    "/helpy — показать это сообщение"
)


def get_helpy_text() -> str:
    return HELPY_TEXT


def process_register_command(telegram_user_id: int | None) -> str:
    if telegram_user_id is None:
        return "Не удалось определить пользователя Telegram."

    success, payload = register_telegram_account(telegram_user_id)
    prefix = "✅" if success else "❌"
    return f"{prefix} {payload}"


def process_profile_command(telegram_user_id: int | None, display_name: str | None = None) -> str:
    if telegram_user_id is None:
        return "❌ Не удалось определить пользователя Telegram."

    data = AccountsService.get_profile("telegram", str(telegram_user_id), display_name=display_name)
    if not data:
        return "❌ Профиль не найден. Сначала выполните /register"

    title_name = display_name or data["custom_nick"]
    return (
        f"👤 {title_name}\n\n"
        f"Описание: {data['description'][:100]}\n\n"
        f"🔗 TG ↔ DC: {data['link_status']}\n"
        f"🛡️ Null's Brawl: {data['nulls_status']}\n"
        f"Айди в Null's Brawl: {data['nulls_brawl_id']}"
    )


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


def process_link_discord_command(telegram_user_id: int | None) -> str:
    if telegram_user_id is None:
        return "Не удалось определить пользователя Telegram."

    success, payload = issue_telegram_discord_link_code(telegram_user_id)
    if not success:
        return f"❌ {payload}"

    return (
        "🔗 Код привязки Discord сгенерирован.\n"
        f"Код: `{payload}`\n"
        f"Срок действия: {AccountsService.LINK_TTL_MINUTES} минут.\n"
        "Используйте в Discord: `/link <код>`"
    )
