import logging
from html import escape

from bot.services import AccountsService
from bot.legacy_identity_logging import (
    log_identity_resolve_error,
    log_transport_identity_error,
)


logger = logging.getLogger(__name__)


HELPY_TEXT = (
    "📚 Список команд:\n"
    "/register — зарегистрировать общий аккаунт\n"
    "/profile — показать профиль общего аккаунта\n"
    "/profile_roles — показать все роли пользователя по категориям\n"
    "/profile_edit — открыть меню редактирования профиля\n"
    "/link <код> — привязать Telegram к аккаунту по коду из Discord\n"
    "/link_discord — получить код для привязки Discord\n"
    "/points [reply|id] — меню управления баллами\n"
    "/balance [reply|id] — показать баланс пользователя\n"
    "/tickets [reply|id] — меню управления билетами\n"
    "/roles_admin — панель ролей с кнопками и встроенной справкой\n"
    "/helpy — показать это сообщение"
)



def get_helpy_text() -> str:
    return HELPY_TEXT


def process_register_command(telegram_user_id: int | None) -> str:
    if telegram_user_id is None:
        log_transport_identity_error(
            logger,
            module=__name__,
            handler="process_register_command",
            field="telegram_user_id",
            action="extract_platform_user_id",
            continue_execution=False,
        )
        return "Не удалось определить пользователя Telegram."

    from bot.systems.linking_logic import register_telegram_account

    success, payload = register_telegram_account(telegram_user_id)
    prefix = "✅" if success else "❌"
    return f"{prefix} {payload}"


def process_profile_command(
    telegram_user_id: int | None,
    display_name: str | None = None,
    target_telegram_user_id: int | None = None,
    target_display_name: str | None = None,
) -> str:
    lookup_user_id = target_telegram_user_id or telegram_user_id
    lookup_display_name = target_display_name or display_name

    if lookup_user_id is None:
        log_transport_identity_error(
            logger,
            module=__name__,
            handler="process_profile_command",
            field="telegram_user_id",
            action="extract_platform_user_id",
            continue_execution=False,
        )
        return "❌ Не удалось определить пользователя Telegram."

    account_id = AccountsService.resolve_account_id("telegram", str(lookup_user_id))
    if not account_id:
        log_identity_resolve_error(
            logger,
            module=__name__,
            handler="process_profile_command",
            field="telegram_user_id",
            action="resolve_account_id",
            continue_execution=False,
            provider="telegram",
            provider_user_id=lookup_user_id,
        )
        return "❌ Профиль не найден. Сначала выполните /register"

    data = AccountsService.get_profile_by_account(account_id, display_name=lookup_display_name)
    if not data:
        return "❌ Профиль не найден. Сначала выполните /register"

    title_name = escape(data["custom_nick"])
    target_platform_name = (lookup_display_name or "").strip()
    if target_platform_name and target_platform_name != data["custom_nick"]:
        title_name = f"{title_name} ({escape(target_platform_name)})"
    safe_description = escape(data["description"][:100])
    safe_nulls_id = escape(data["nulls_brawl_id"])
    safe_link_status = escape(data["link_status"])
    safe_nulls_status = escape(data["nulls_status"])
    safe_points = escape(str(data["points"]))
    safe_titles_text = escape(str(data.get("titles_text") or "Нет званий"))
    visible_roles = data.get("visible_roles") or []
    safe_roles_text = "\n".join(f"• {escape(str(role))}" for role in visible_roles) if visible_roles else "Нет назначенных ролей"

    return (
        "👤 <b><a href=\"tg://user?id={telegram_user_id}\">{title_name}</a></b>\n"
        "━━━━━━━━━━━━━━\n"
        "<b>Общая информация</b>\n"
        "Звания: {safe_titles_text}\n"
        "Айди в Null's Brawl: <code>{safe_nulls_id}</code> ({safe_nulls_status})\n"
        "Баллы: {safe_points}\n"
        "━━━━━━━━━━━━━━\n"
        "<b>Описание</b>\n"
        "{safe_description}\n"
        "━━━━━━━━━━━━━━\n"
        "<b>Дополнительная информация</b>\n"
        "🔗 TG ↔ DC: {safe_link_status}\n"
        "━━━━━━━━━━━━━━\n"
        "<b>Роли</b>\n"
        "{safe_roles_text}"
    ).format(
        telegram_user_id=lookup_user_id,
        title_name=title_name,
        safe_nulls_id=safe_nulls_id,
        safe_description=safe_description,
        safe_link_status=safe_link_status,
        safe_nulls_status=safe_nulls_status,
        safe_points=safe_points,
        safe_titles_text=safe_titles_text,
        safe_roles_text=safe_roles_text,
    )


def process_link_command(
    message_text: str,
    telegram_user_id: int | None,
    is_private_chat: bool = True,
) -> str:
    if not is_private_chat:
        return "❌ Команда привязки доступна только в личных сообщениях с ботом."

    text = (message_text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return "Использование: /link <код>"

    if telegram_user_id is None:
        log_transport_identity_error(
            logger,
            module=__name__,
            handler="process_link_command",
            field="telegram_user_id",
            action="extract_platform_user_id",
            continue_execution=False,
        )
        return "Не удалось определить пользователя Telegram."

    code = parts[1].strip()
    from bot.telegram_bot.link_handler import handle_link_command

    success, payload = handle_link_command(telegram_user_id, code)
    prefix = "✅" if success else "❌"
    return f"{prefix} {payload}"


def process_link_discord_command(
    telegram_user_id: int | None,
    is_private_chat: bool = True,
) -> str:
    if not is_private_chat:
        return "❌ Команда привязки доступна только в личных сообщениях с ботом."

    if telegram_user_id is None:
        log_transport_identity_error(
            logger,
            module=__name__,
            handler="process_link_discord_command",
            field="telegram_user_id",
            action="extract_platform_user_id",
            continue_execution=False,
        )
        return "Не удалось определить пользователя Telegram."

    from bot.systems.linking_logic import issue_telegram_discord_link_code

    success, payload = issue_telegram_discord_link_code(telegram_user_id)
    if not success:
        return f"❌ {payload}"

    return (
        "🔗 Код привязки Discord сгенерирован.\n"
        f"Код: `{payload}`\n"
        f"Срок действия: {AccountsService.LINK_TTL_MINUTES} минут.\n"
        "Используйте в Discord: `/link <код>`"
    )


def process_profile_roles_command(
    telegram_user_id: int | None,
    display_name: str | None = None,
    target_telegram_user_id: int | None = None,
    target_display_name: str | None = None,
) -> str:
    lookup_user_id = target_telegram_user_id or telegram_user_id
    lookup_display_name = target_display_name or display_name

    if lookup_user_id is None:
        log_transport_identity_error(
            logger,
            module=__name__,
            handler="process_profile_roles_command",
            field="telegram_user_id",
            action="extract_platform_user_id",
            continue_execution=False,
        )
        return "❌ Не удалось определить пользователя Telegram."

    account_id = AccountsService.resolve_account_id("telegram", str(lookup_user_id))
    if not account_id:
        log_identity_resolve_error(
            logger,
            module=__name__,
            handler="process_profile_roles_command",
            field="telegram_user_id",
            action="resolve_account_id",
            continue_execution=False,
            provider="telegram",
            provider_user_id=lookup_user_id,
        )
        return "❌ Профиль не найден. Сначала выполните /register"

    data = AccountsService.get_profile_by_account(account_id, display_name=lookup_display_name)
    if not data:
        return "❌ Профиль не найден. Сначала выполните /register"

    roles_by_category = data.get("roles_by_category") or {}
    if not roles_by_category:
        return "🏅 <b>Роли пользователя</b>\nНет назначенных ролей"

    parts = ["🏅 <b>Роли пользователя</b>"]
    for category in sorted(roles_by_category.keys()):
        role_names = roles_by_category.get(category) or []
        if not role_names:
            continue
        rendered = "\n".join(f"• {escape(str(role_name))}" for role_name in role_names)
        parts.append(f"\n<b>{escape(str(category))}</b>\n{rendered}")

    return "\n".join(parts)
