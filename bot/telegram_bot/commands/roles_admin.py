import logging
import time
from dataclasses import dataclass
from typing import Any

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramConflictError, TelegramNetworkError
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.data import db
from bot.services import AccountsService, AuthorityService, RoleManagementService
from bot.telegram_bot.identity import persist_telegram_identity_from_user
from bot.services.role_management_service import (
    DELETE_ROLE_REASON_DISCORD_MANAGED,
    DELETE_ROLE_REASON_NOT_FOUND,
    USER_ACQUIRE_HINT_PLACEHOLDER,
)

logger = logging.getLogger(__name__)
router = Router()

_ROLES_PAGE_SIZE = 5
_MAX_ROLE_BUTTONS = 8
_PENDING_TTL_SECONDS = 300
_SECTION_LABELS = {
    "categories": "Категории",
    "roles": "Роли",
    "users": "Пользователи",
}
_SECTION_OPERATIONS = {
    "categories": ("category_create", "category_order", "category_delete"),
    "roles": ("role_create", "role_edit_acquire_hint", "role_move", "role_order", "role_delete"),
    "users": ("user_roles", "user_grant", "user_revoke"),
}


@dataclass
class PendingRolesAdminAction:
    operation: str
    created_at: float
    payload: dict[str, Any] | None = None


_PENDING_ACTIONS: dict[int, PendingRolesAdminAction] = {}


@dataclass(frozen=True)
class RolesAdminVisibilityContext:
    actor_level: int
    actor_titles: tuple[str, ...]
    can_manage_categories: bool
    hidden_sections: tuple[str, ...]


def _role_catalog_note() -> str:
    return (
        "Команды списка и изменения ролей стараются автоматически подтягивать актуальные Discord-роли в "
        "канонический каталог <code>roles</code>. Внешние Discord-роли можно <code>move/order</code>, "
        "но нельзя <code>delete</code>. Если роль не видна, обнови экран, при необходимости запусти "
        "<code>/rolesadmin sync_discord_roles</code> в Discord и проверь консольные логи."
    )


def _delete_role_denied_message() -> str:
    return (
        "❌ Эту внешнюю Discord-роль нельзя удалить из каталога.\n"
        "Её можно переместить в другую категорию или поменять порядок, "
        "но сама роль управляется внешней синхронизацией Discord."
    )


def _delete_role_result_message(result: dict[str, object]) -> str:
    if result.get("reason") == DELETE_ROLE_REASON_DISCORD_MANAGED:
        return _delete_role_denied_message()
    if result.get("reason") == DELETE_ROLE_REASON_NOT_FOUND:
        return (
            "❌ Роль не найдена в каноническом каталоге `roles`.\n"
            "Обнови экран или дождись автосинхронизации Discord-ролей, затем попробуй ещё раз."
        )
    return "❌ Не удалось удалить роль (смотри логи)."


def _canonical_role_missing_message() -> str:
    return (
        "❌ Роль не найдена в каталоге `roles`.\n"
        "Сначала дождись синхронизации Discord-ролей или запусти `/rolesadmin sync_discord_roles` в Discord, потом попробуй ещё раз."
    )


def _telegram_user_lookup_hint() -> str:
    return (
        "Порядок такой: в Telegram ЛС используй @username / username, в Telegram группе — reply, "
        "в Discord — mention / username / display_name. Для явного провайдера можно указать "
        "tg:@username или ds:username. ID оставь только как резерв."
    )


async def _sync_linked_discord_role(target: dict[str, str], role_name: str, *, revoke: bool) -> None:
    try:
        provider = str(target.get("provider") or "").strip()
        provider_user_id = str(target.get("provider_user_id") or "").strip()
        account_id = str(target.get("account_id") or "").strip() or AccountsService.resolve_account_id(provider, provider_user_id)
        if not account_id or not db.supabase:
            return
        role_info = RoleManagementService.get_role(role_name)
        discord_role_id = str((role_info or {}).get("discord_role_id") or "").strip()
        if not discord_role_id:
            return
        identity_resp = (
            db.supabase.table("account_identities")
            .select("provider_user_id")
            .eq("account_id", str(account_id))
            .eq("provider", "discord")
            .limit(1)
            .execute()
        )
        if not identity_resp.data:
            return
        discord_user_id = int(identity_resp.data[0].get("provider_user_id") or 0)
        if not discord_user_id:
            return

        from bot.commands.base import bot as discord_bot

        if not getattr(discord_bot, "guilds", None):
            logger.warning(
                "discord sync skipped: bot guilds unavailable account_id=%s provider=%s provider_user_id=%s",
                account_id,
                provider,
                provider_user_id,
            )
            return
        for guild in discord_bot.guilds:
            member = guild.get_member(discord_user_id)
            guild_role = guild.get_role(int(discord_role_id))
            if not member or not guild_role:
                continue
            try:
                if revoke:
                    await member.remove_roles(guild_role, reason=f"telegram roles_admin revoke by {provider}:{provider_user_id}")
                else:
                    await member.add_roles(guild_role, reason=f"telegram roles_admin grant by {provider}:{provider_user_id}")
            except Exception:
                logger.exception(
                    "telegram roles_admin discord sync failed account_id=%s provider=%s provider_user_id=%s discord_user_id=%s role_id=%s revoke=%s guild_id=%s",
                    account_id,
                    provider,
                    provider_user_id,
                    discord_user_id,
                    discord_role_id,
                    revoke,
                    guild.id,
                )
            return
        logger.warning(
            "telegram roles_admin discord sync target not found account_id=%s provider=%s provider_user_id=%s discord_user_id=%s role_id=%s revoke=%s",
            account_id,
            provider,
            provider_user_id,
            discord_user_id,
            discord_role_id,
            revoke,
        )
    except Exception:
        logger.exception(
            "telegram roles_admin discord sync crashed provider=%s provider_user_id=%s account_id=%s role=%s revoke=%s",
            target.get("provider"),
            target.get("provider_user_id"),
            target.get("account_id"),
            role_name,
            revoke,
        )


async def _sync_discord_roles_catalog() -> None:
    """Sync live Discord guild roles into local catalog for Telegram role operations."""
    try:
        from bot.commands.base import bot as discord_bot

        guilds = list(getattr(discord_bot, "guilds", []) or [])
        if not guilds:
            logger.warning("telegram roles_admin discord catalog sync skipped: no guilds attached")
            return

        guild_roles: list[dict[str, str | int]] = []
        for guild in guilds:
            try:
                guild_roles.extend(
                    {
                        "id": str(role.id),
                        "name": role.name,
                        "position": int(role.position),
                        "guild_id": str(guild.id),
                    }
                    for role in guild.roles
                    if not role.is_default()
                )
            except Exception:
                logger.exception(
                    "telegram roles_admin discord catalog sync failed to read guild roles guild_id=%s",
                    getattr(guild, "id", None),
                )

        if not guild_roles:
            logger.warning(
                "telegram roles_admin discord catalog sync has no roles guild_count=%s",
                len(guilds),
            )
            return

        result = RoleManagementService.sync_discord_guild_roles(guild_roles)
        logger.info(
            "telegram roles_admin discord catalog sync completed guild_count=%s roles=%s upserted=%s removed=%s",
            len(guilds),
            len(guild_roles),
            result.get("upserted", 0),
            result.get("removed", 0),
        )
    except Exception:
        logger.exception("telegram roles_admin discord catalog sync crashed")

def _format_telegram_lookup_candidate(candidate: dict[str, Any]) -> str:
    provider = str(candidate.get("provider") or "").strip()
    username = candidate.get("username")
    display_name = candidate.get("display_name")
    provider_user_id = candidate.get("provider_user_id")
    matched_by = candidate.get("matched_by")
    parts = []
    if provider:
        parts.append(provider)
    if username:
        parts.append(f"@{str(username).lstrip('@')}")
    if display_name:
        parts.append(str(display_name))
    if provider_user_id:
        parts.append(f"id={provider_user_id}")
    if matched_by:
        parts.append(f"via={matched_by}")
    return " | ".join(parts) or "неизвестный пользователь"


def _user_not_found_message() -> str:
    return (
        "❌ Пользователь ещё не появлялся в локальном реестре identity lookup. "
        "Пусть он один раз напишет боту или выполнит /register, /link или /profile. "
        + _telegram_user_lookup_hint()
    )


def _user_without_account_message() -> str:
    return (
        "❌ Пользователь найден в локальном реестре, но у него ещё нет общего аккаунта. "
        "Пусть он завершит /register или /link, после чего попробуйте снова."
    )


def _roles_admin_lookup_log(
    *,
    actor_id: int | None,
    lookup_value: str | None,
    provider: str | None,
    provider_user_id: str | None,
    account_id: str | None,
    operation: str,
    reason: str,
    candidates_count: int,
    source: str,
) -> None:
    logger.info(
        "roles_admin lookup actor_id=%s lookup_value=%s provider=%s provider_user_id=%s account_id=%s operation=%s reason=%s candidates=%s source=%s",
        actor_id,
        str(lookup_value or "").strip() or None,
        provider,
        provider_user_id,
        account_id,
        operation,
        reason,
        candidates_count,
        source,
    )


def _build_resolved_target(
    *,
    account_id: str | None,
    provider: str,
    provider_user_id: str,
    username: str | None,
    display_name: str | None,
    matched_by: str,
) -> dict[str, str | None]:
    normalized_username = str(username or "").strip().lstrip("@") or None
    normalized_display_name = str(display_name or "").strip() or None
    label = (
        f"@{normalized_username}"
        if normalized_username
        else f"{normalized_display_name} ({provider}:{provider_user_id})"
        if normalized_display_name
        else f"{provider}:{provider_user_id}"
    )
    return {
        "account_id": str(account_id or "").strip() or None,
        "provider": provider,
        "provider_user_id": provider_user_id,
        "label": label,
        "matched_by": matched_by,
    }


def _resolve_telegram_target(
    *,
    actor_id: int | None,
    raw_target: str | None = None,
    reply_user: Any | None = None,
    operation: str,
    source: str,
) -> dict[str, str | None] | None:
    token = str(raw_target or "").strip()
    location = "telegram_group" if source == "group" else "telegram_dm"

    if token and not token.isdigit():
        lookup = AccountsService.resolve_user_lookup(token, default_provider="telegram")
        candidates = list(lookup.get("candidates") or [])
        if lookup.get("status") == "ok":
            resolved = dict(lookup.get("result") or {})
            provider = str(resolved.get("provider") or "").strip()
            provider_user_id = str(resolved.get("provider_user_id") or "").strip()
            account_id = str(resolved.get("account_id") or "").strip() or AccountsService.resolve_account_id(provider, provider_user_id)
            result = _build_resolved_target(
                account_id=account_id,
                provider=provider,
                provider_user_id=provider_user_id,
                username=resolved.get("username"),
                display_name=resolved.get("display_name"),
                matched_by=str(resolved.get("matched_by") or "identity_lookup"),
            )
            _roles_admin_lookup_log(
                actor_id=actor_id,
                lookup_value=token,
                provider=provider,
                provider_user_id=provider_user_id,
                account_id=result.get("account_id"),
                operation=operation,
                reason="resolved_username_lookup",
                candidates_count=len(candidates) or 1,
                source=location,
            )
            return result

        if lookup.get("status") == "multiple":
            _roles_admin_lookup_log(
                actor_id=actor_id,
                lookup_value=token,
                provider="telegram",
                provider_user_id=None,
                account_id=None,
                operation=operation,
                reason="multiple_matches",
                candidates_count=len(candidates),
                source=location,
            )
            return {
                "error": "multiple",
                "message": (
                    "❌ Найдено несколько кандидатов в локальном реестре. "
                    "Уточни username точнее или укажи провайдер tg:/ds:. Кандидаты:\n"
                    + "\n".join(f"• {_format_telegram_lookup_candidate(candidate)}" for candidate in candidates[:5])
                    + "\n\n"
                    + _telegram_user_lookup_hint()
                ),
            }

        _roles_admin_lookup_log(
            actor_id=actor_id,
            lookup_value=token,
            provider="telegram",
            provider_user_id=None,
            account_id=None,
            operation=operation,
            reason=str(lookup.get("reason") or "not_found"),
            candidates_count=len(candidates),
            source=location,
        )
        return {
            "error": "not_found" if lookup.get("status") == "not_found" else "invalid_format",
            "message": _user_not_found_message(),
        }

    if reply_user and not getattr(reply_user, "is_bot", False):
        persist_telegram_identity_from_user(reply_user)
        account_id = AccountsService.resolve_account_id("telegram", str(reply_user.id))
        result = _build_resolved_target(
            account_id=account_id,
            provider="telegram",
            provider_user_id=str(reply_user.id),
            username=getattr(reply_user, "username", None),
            display_name=getattr(reply_user, "full_name", None),
            matched_by="reply",
        )
        _roles_admin_lookup_log(
            actor_id=actor_id,
            lookup_value=token or f"reply:{reply_user.id}",
            provider="telegram",
            provider_user_id=str(reply_user.id),
            account_id=result.get("account_id"),
            operation=operation,
            reason="resolved_reply_target",
            candidates_count=1,
            source=location,
        )
        return result

    if token.isdigit():
        account_id = AccountsService.resolve_account_id("telegram", token)
        result = _build_resolved_target(
            account_id=account_id,
            provider="telegram",
            provider_user_id=token,
            username=None,
            display_name=None,
            matched_by="exact_id_fallback",
        )
        _roles_admin_lookup_log(
            actor_id=actor_id,
            lookup_value=token,
            provider="telegram",
            provider_user_id=token,
            account_id=result.get("account_id"),
            operation=operation,
            reason="resolved_id_fallback" if account_id else "id_fallback_without_account",
            candidates_count=1 if account_id else 0,
            source=location,
        )
        return result

    _roles_admin_lookup_log(
        actor_id=actor_id,
        lookup_value=token,
        provider="telegram",
        provider_user_id=None,
        account_id=None,
        operation=operation,
        reason="empty_target",
        candidates_count=0,
        source=location,
    )
    return None

def _normalize_page(page: int, total_items: int, page_size: int) -> int:
    if total_items <= 0:
        return 0
    max_page = max((total_items - 1) // page_size, 0)
    return min(max(page, 0), max_page)


def _resolve_visibility_context(provider: str, provider_user_id: str) -> RolesAdminVisibilityContext:
    authority = AuthorityService.resolve_authority(provider, provider_user_id)
    can_manage_categories = AuthorityService.can_manage_role_categories(provider, provider_user_id)
    hidden_sections = tuple(section for section in ("categories",) if not can_manage_categories)
    return RolesAdminVisibilityContext(
        actor_level=authority.level,
        actor_titles=tuple(authority.titles),
        can_manage_categories=can_manage_categories,
        hidden_sections=hidden_sections,
    )


def _log_roles_admin_navigation(
    *,
    actor_id: int | None,
    actor_level: int,
    actor_titles: tuple[str, ...],
    hidden_sections: tuple[str, ...],
    screen: str,
) -> None:
    logger.info(
        "roles_admin navigation actor_id=%s actor_level=%s actor_titles=%s hidden_sections=%s screen=%s source=%s",
        actor_id,
        actor_level,
        list(actor_titles),
        list(hidden_sections),
        screen,
        "telegram",
    )


def _section_for_operation(operation: str | None) -> str | None:
    operation_key = str(operation or "").strip()
    for section, operations in _SECTION_OPERATIONS.items():
        if operation_key in operations:
            return section
    return None


def _build_home_keyboard(
    actor_id: int,
    *,
    can_manage_categories: bool | None = None,
) -> InlineKeyboardMarkup:
    allow_categories = (
        AuthorityService.can_manage_role_categories("telegram", str(actor_id))
        if can_manage_categories is None
        else can_manage_categories
    )
    rows = []
    if allow_categories:
        rows.append([InlineKeyboardButton(text="🗂 Категории", callback_data=f"roles_admin:{actor_id}:actions:categories")])
    rows.extend(
        [
            [InlineKeyboardButton(text="🪪 Роли", callback_data=f"roles_admin:{actor_id}:actions:roles")],
            [InlineKeyboardButton(text="👥 Пользователи", callback_data=f"roles_admin:{actor_id}:actions:users")],
            [InlineKeyboardButton(text="📋 Категории и роли", callback_data=f"roles_admin:{actor_id}:list:0")],
            [InlineKeyboardButton(text="🆘 Не работают кнопки?", callback_data=f"roles_admin:{actor_id}:fallback")],
            [InlineKeyboardButton(text="ℹ️ Что делает каждая функция", callback_data=f"roles_admin:{actor_id}:help")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"roles_admin:{actor_id}:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_actions_keyboard(
    actor_id: int,
    section: str | None = None,
    *,
    can_manage_categories: bool | None = None,
) -> InlineKeyboardMarkup:
    allow_categories = (
        AuthorityService.can_manage_role_categories("telegram", str(actor_id))
        if can_manage_categories is None
        else can_manage_categories
    )
    rows: list[list[InlineKeyboardButton]] = []

    if section == "categories":
        if allow_categories:
            rows.extend(
                [
                    [InlineKeyboardButton(text="🗂 Создать категорию", callback_data=f"roles_admin:{actor_id}:start:category_create")],
                    [InlineKeyboardButton(text="↕️ Порядок категории", callback_data=f"roles_admin:{actor_id}:start:category_order")],
                    [InlineKeyboardButton(text="🗑 Удалить категорию", callback_data=f"roles_admin:{actor_id}:start:category_delete")],
                ]
            )
    elif section == "roles":
        rows.extend(
            [
                [InlineKeyboardButton(text="➕ Создать роль", callback_data=f"roles_admin:{actor_id}:start:role_create")],
                [InlineKeyboardButton(text="🧭 Как получить роль", callback_data=f"roles_admin:{actor_id}:start:role_edit_acquire_hint")],
                [InlineKeyboardButton(text="🚚 Переместить роль", callback_data=f"roles_admin:{actor_id}:start:role_move")],
                [InlineKeyboardButton(text="🔢 Порядок роли", callback_data=f"roles_admin:{actor_id}:start:role_order")],
                [InlineKeyboardButton(text="🗑 Удалить роль", callback_data=f"roles_admin:{actor_id}:start:role_delete")],
            ]
        )
    elif section == "users":
        rows.extend(
            [
                [InlineKeyboardButton(text="🧾 Роли пользователя", callback_data=f"roles_admin:{actor_id}:start:user_roles")],
                [InlineKeyboardButton(text="✅ Выдать роль", callback_data=f"roles_admin:{actor_id}:start:user_grant")],
                [InlineKeyboardButton(text="❌ Снять роль", callback_data=f"roles_admin:{actor_id}:start:user_revoke")],
            ]
        )
    else:
        if allow_categories:
            rows.append([InlineKeyboardButton(text="🗂 Категории", callback_data=f"roles_admin:{actor_id}:actions:categories")])
        rows.extend(
            [
                [InlineKeyboardButton(text="🪪 Роли", callback_data=f"roles_admin:{actor_id}:actions:roles")],
                [InlineKeyboardButton(text="👥 Пользователи", callback_data=f"roles_admin:{actor_id}:actions:users")],
            ]
        )

    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data=f"roles_admin:{actor_id}:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _render_hidden_sections_note(hidden_sections: tuple[str, ...]) -> str:
    if not hidden_sections:
        return ""
    return (
        "⚠️ Некоторые кнопки скрыты, потому что у вас нет нужных полномочий.\n"
        f"Скрытые разделы: {', '.join(_SECTION_LABELS.get(section, section) for section in hidden_sections)}.\n\n"
    )


def _render_actions_text(section: str | None = None, *, hidden_sections: tuple[str, ...] = tuple()) -> str:
    hidden_note = _render_hidden_sections_note(hidden_sections)
    common_tail = (
        "Нажми кнопку, затем следуй подсказкам на экране.\n"
        "Разделитель параметров: <code>|</code>.\n"
        "Для отмены ввода отправь: <code>отмена</code>.\n\n"
        "Для create/move/order после выбора категории бот покажет текущий порядок ролей и отдельный экран выбора позиции.\n"
        "Для пользовательского интерфейса старайся заполнять и описание, и поле «как получить» — так бот лучше объясняет роль людям.\n"
        "Если позицию не менять, роль будет добавлена последней.\n\n"
        f"{_role_catalog_note()}"
    )
    if section == "categories":
        return (
            "🗂 <b>Раздел «Категории»</b>\n\n"
            f"{hidden_note}"
            "Здесь собраны только действия по структуре каталога: создание, изменение порядка и удаление категорий.\n"
            "Используй этот раздел, когда меняешь верхний уровень навигации ролей.\n\n"
            f"{common_tail}"
        )
    if section == "roles":
        return (
            "🪪 <b>Раздел «Роли»</b>\n\n"
            f"{hidden_note}"
            "Здесь собраны действия по самим ролям: создание, инструкция «как получить», перенос между категориями, порядок и удаление.\n"
            "Старайся заполнять описание и способ получения — так пользователю проще понять роль прямо в интерфейсе бота.\n\n"
            f"{common_tail}"
        )
    if section == "users":
        return (
            "👥 <b>Раздел «Пользователи»</b>\n\n"
            f"{hidden_note}"
            "Здесь доступны только действия над ролями пользователей: посмотреть, выдать или снять роль.\n"
            "Для поиска пользователя в ЛС удобнее @username / username, в группе — reply, для Discord — ds:username.\n\n"
            f"{common_tail}"
        )
    return (
        "⚡ <b>Действия кнопками</b>\n\n"
        f"{hidden_note}"
        "Выберите раздел: категории, роли или пользователи.\n"
        "Внутри раздела бот покажет только относящиеся к нему действия, чтобы экран было проще читать и сложнее нажать не ту кнопку.\n\n"
        f"{common_tail}"
    )


def _operation_hint(operation: str) -> str:
    hints = {
        "category_create": "Отправь: <code>Название категории | position(опционально)</code>",
        "category_order": "Отправь: <code>Название категории | position</code>",
        "category_delete": "Отправь: <code>Название категории</code>",
        "role_create": "Отправь: <code>Название роли | Категория | Описание | Как получить(опц) | discord_role_id(опц) | position(опц)</code>. Описание и способ получения можно оставить пустыми. Если позицию не указывать, роль будет добавлена последней.",
        "role_create_enter_name": "Отправь: <code>Название роли | Описание | Как получить(опц) | discord_role_id(опц)</code>. Категория и позиция уже выбраны кнопками.",
        "role_edit_description": "Отправь: <code>Название роли | Описание</code>. Так роль будет понятнее пользователям прямо в интерфейсе.",
        "role_edit_acquire_hint": "Отправь: <code>Название роли | Как получить</code>. Пиши коротко и понятно: через активность, выдачу админа, турнир, заявку и т.д.",
        "role_move": "Отправь: <code>Название роли | Категория | position(опц)</code>. Если позицию не указывать, роль будет добавлена последней. Внешнюю Discord-роль можно переместить.",
        "role_order": "Отправь: <code>Название роли | Категория | position</code>. Внешнюю Discord-роль можно отсортировать.",
        "role_delete": "Отправь: <code>Название роли</code>. Внешние Discord-роли удалить нельзя.",
        "user_roles": "Отправь: <code>@username</code> / <code>username</code> / <code>tg:@username</code> / <code>ds:username</code>. В группе удобнее reply.",
        "user_grant": "Кнопочный режим: сначала укажи пользователя, потом выбирай роли по категориям и подтверждай пакет. Текстовый fallback: <code>@username | Название роли</code> или reply + <code>Название роли</code>. Для Discord можно <code>ds:username | Роль</code>.",
        "user_revoke": "Кнопочный режим: сначала укажи пользователя, потом выбирай роли по категориям и подтверждай пакет. Текстовый fallback: <code>@username | Название роли</code> или reply + <code>Название роли</code>. Для Discord можно <code>ds:username | Роль</code>.",
    }
    return hints.get(operation, "Неизвестная операция")


def _parse_pipe_args(raw: str) -> list[str]:
    parts = [part.strip() for part in raw.split("|")]
    while parts and not parts[-1]:
        parts.pop()
    return parts


def _looks_like_discord_role_id(value: str | None) -> bool:
    token = str(value or "").strip()
    return bool(token) and token.isdigit()


def _parse_role_create_metadata_args(args: list[str]) -> dict[str, Any]:
    role_name = args[0] if args else ""
    category = args[1] if len(args) > 1 else ""
    description = args[2] if len(args) > 2 else None
    extras = list(args[3:]) if len(args) > 3 else []

    position = None
    if extras and str(extras[-1]).lstrip("-").isdigit():
        position = int(str(extras.pop()))

    acquire_hint = None
    discord_role_id = None
    if len(extras) >= 2:
        acquire_hint = extras[0] or None
        discord_role_id = extras[1] or None
    elif len(extras) == 1:
        if _looks_like_discord_role_id(extras[0]):
            discord_role_id = extras[0]
        else:
            acquire_hint = extras[0] or None

    return {
        "role_name": role_name,
        "category": category,
        "description": description,
        "acquire_hint": acquire_hint,
        "discord_role_id": discord_role_id,
        "position": position,
    }


def _format_role_line(role: dict[str, object], *, numbered: int | None = None) -> str:
    prefix = f"{numbered}. " if numbered is not None else "• "
    suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
    external_note = " — внешняя Discord-роль, удаление скрыто" if role.get("is_discord_managed") else ""
    description = str(role.get("description") or "").strip()
    acquire_hint = str(role.get("acquire_hint") or "").strip() or USER_ACQUIRE_HINT_PLACEHOLDER
    description_note = f"\n   ↳ Описание: {description}" if description else ""
    acquire_hint_note = f"\n   ↳ Как получить: {acquire_hint}"
    return f"{prefix}{role['name']}{suffix}{external_note}{description_note}{acquire_hint_note}"


def _is_pending_action_expired(pending: PendingRolesAdminAction) -> bool:
    return (time.time() - pending.created_at) > _PENDING_TTL_SECONDS


def has_pending_roles_admin_action(telegram_user_id: int | None) -> bool:
    if not telegram_user_id:
        return False
    pending = _PENDING_ACTIONS.get(telegram_user_id)
    if not pending:
        return False
    if _is_pending_action_expired(pending):
        logger.info(
            "roles_admin pending action expired user_id=%s operation=%s ttl_seconds=%s",
            telegram_user_id,
            pending.operation,
            _PENDING_TTL_SECONDS,
        )
        _PENDING_ACTIONS.pop(telegram_user_id, None)
        return False
    return True




def _flatten_roles(grouped: list[dict]) -> list[dict[str, object]]:
    flattened: list[dict[str, object]] = []
    for item in grouped:
        category = str(item.get("category") or "Без категории")
        for role in item.get("roles", []):
            role_name = str(role.get("name") or "").strip()
            if role_name:
                flattened.append(
                    {
                        "role": role_name,
                        "category": category,
                        "is_discord_managed": bool(role.get("is_discord_managed")),
                        "discord_role_id": str(role.get("discord_role_id") or "").strip() or None,
                    }
                )
    if flattened:
        return flattened

    fallback: list[dict[str, object]] = []
    for item in grouped:
        category = str(item.get("category") or "Без категории")
        for role in item.get("roles", []):
            role_name = str(role.get("name") or "").strip()
            if role_name:
                fallback.append(
                    {
                        "role": role_name,
                        "category": category,
                        "is_discord_managed": bool(role.get("is_discord_managed")),
                        "discord_role_id": str(role.get("discord_role_id") or "").strip() or None,
                    }
                )
    return fallback


def _normalize_role_names(role_names: list[str] | tuple[str, ...] | set[str] | None) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for item in list(role_names or []):
        role_key = str(item or "").strip()
        if not role_key or role_key in seen:
            continue
        seen.add(role_key)
        normalized.append(role_key)
    return normalized


def _get_user_role_flow_pending(actor_id: int | None) -> PendingRolesAdminAction | None:
    pending = _PENDING_ACTIONS.get(actor_id or 0)
    if not pending or pending.operation != "user_role_flow_panel":
        return None
    return pending


def _user_role_flow_summary_lists(action: str, selected_roles: list[str]) -> tuple[list[str], list[str]]:
    normalized = _normalize_role_names(selected_roles)
    if action == "revoke":
        return [], normalized
    return normalized, []


def _render_user_role_flow_text(
    *,
    target_label: str,
    action: str,
    selected_roles: list[str],
    current_category: str | None = None,
) -> str:
    grant_roles, revoke_roles = _user_role_flow_summary_lists(action, selected_roles)
    action_title = "выдачи" if action == "grant" else "снятия"
    category_note = f"Текущая категория: <b>{current_category}</b>\n" if current_category else ""
    grant_text = "\n".join(f"• {item}" for item in grant_roles) if grant_roles else "• —"
    revoke_text = "\n".join(f"• {item}" for item in revoke_roles) if revoke_roles else "• —"
    return (
        f"👤 Пользователь: <b>{target_label}</b>\n"
        f"🧺 Панель пакетного {action_title} ролей\n"
        f"{category_note}"
        f"🔢 Уже выбрано ролей: <b>{len(_normalize_role_names(selected_roles))}</b>\n\n"
        "<b>Будет выдано:</b>\n"
        f"{grant_text}\n\n"
        "<b>Будет снято:</b>\n"
        f"{revoke_text}\n\n"
        "ℹ️ Выбор можно продолжать по другим категориям до явного выхода из панели.\n"
        "Сначала выберите категорию, затем отмечайте одну или несколько ролей, возвращайтесь к категориям и подтверждайте пакет только когда всё готово."
    )


def _build_user_role_categories_keyboard(
    grouped: list[dict],
    actor_id: int,
    action: str,
    selected_roles: list[str],
) -> InlineKeyboardMarkup:
    selected_set = set(_normalize_role_names(selected_roles))
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(grouped[:20]):
        role_names = {
            str(role.get("name") or "").strip()
            for role in list(item.get("roles") or [])
            if str(role.get("name") or "").strip()
        }
        selected_count = len(role_names & selected_set)
        suffix = f" [{selected_count}]" if selected_count else ""
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📂 {item['category']}{suffix}"[:64],
                    callback_data=f"roles_admin:{actor_id}:user_role_category:{action}:{idx}",
                )
            ]
        )
    total_selected = len(selected_set)
    if total_selected:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🚀 Подтвердить пакет ({total_selected})",
                    callback_data=f"roles_admin:{actor_id}:user_role_apply:{action}",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="🧹 Очистить выбор",
                    callback_data=f"roles_admin:{actor_id}:user_role_clear:{action}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="✖️ Выйти из панели",
                callback_data=f"roles_admin:{actor_id}:user_role_exit:{action}",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_user_role_picker_keyboard(
    grouped: list[dict],
    actor_id: int,
    action: str,
    category_idx: int,
    selected_roles: list[str],
    page: int = 0,
) -> InlineKeyboardMarkup:
    if category_idx < 0 or category_idx >= len(grouped):
        return _build_user_role_categories_keyboard(grouped, actor_id, action, selected_roles)
    category_item = grouped[category_idx]
    roles = [
        role
        for role in list(category_item.get("roles") or [])
        if str(role.get("name") or "").strip()
    ]
    safe_page = _normalize_page(page, len(roles), _MAX_ROLE_BUTTONS)
    start = safe_page * _MAX_ROLE_BUTTONS
    items = roles[start : start + _MAX_ROLE_BUTTONS]
    selected_set = set(_normalize_role_names(selected_roles))

    rows: list[list[InlineKeyboardButton]] = []
    for idx, role in enumerate(items):
        role_name = str(role.get("name") or "").strip()
        marker = "✅" if role_name in selected_set else "⬜️"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{marker} {role_name}"[:64],
                    callback_data=f"roles_admin:{actor_id}:user_role_toggle:{action}:{category_idx}:{safe_page}:{idx}",
                )
            ]
        )

    nav: list[InlineKeyboardButton] = []
    if safe_page > 0:
        nav.append(
            InlineKeyboardButton(
                text="⬅️",
                callback_data=f"roles_admin:{actor_id}:user_role_page:{action}:{category_idx}:{safe_page - 1}",
            )
        )
    nav.append(
        InlineKeyboardButton(
            text="🔄",
            callback_data=f"roles_admin:{actor_id}:user_role_page:{action}:{category_idx}:{safe_page}",
        )
    )
    if (safe_page + 1) * _MAX_ROLE_BUTTONS < len(roles):
        nav.append(
            InlineKeyboardButton(
                text="➡️",
                callback_data=f"roles_admin:{actor_id}:user_role_page:{action}:{category_idx}:{safe_page + 1}",
            )
        )
    if nav:
        rows.append(nav)

    total_selected = len(selected_set)
    if total_selected:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🚀 Подтвердить пакет ({total_selected})",
                    callback_data=f"roles_admin:{actor_id}:user_role_apply:{action}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text="🗂 К категориям",
                callback_data=f"roles_admin:{actor_id}:user_role_categories:{action}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="✖️ Выйти из панели",
                callback_data=f"roles_admin:{actor_id}:user_role_exit:{action}",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _log_role_position_error(
    *,
    actor_id: int | None,
    operation: str,
    role_name: str | None,
    category: str | None,
    requested_position: int | None,
    computed_last_position: int | None,
    source: str,
    message: str,
) -> None:
    logger.warning(
        "%s actor_id=%s operation=%s role_name=%s category=%s requested_position=%s computed_last_position=%s source=%s",
        message,
        actor_id,
        operation,
        role_name,
        category,
        requested_position,
        computed_last_position,
        source,
    )


def _render_category_role_preview(preview: dict[str, Any]) -> str:
    roles = list(preview.get("current_roles") or [])
    if not roles:
        return "• Категория пока пустая."
    return "\n".join(
        f"• #{idx}. {str(role.get('name') or 'Без названия')}"
        for idx, role in enumerate(roles, start=1)
    )


def _render_position_picker_text(
    *,
    mode: str,
    category_name: str,
    preview: dict[str, Any],
    role_name: str | None = None,
) -> str:
    action_line = {
        "create": "Новая роль будет вставлена в выбранную категорию.",
        "move": f"Роль: <b>{role_name}</b>",
        "order": f"Роль: <b>{role_name}</b>",
    }.get(mode, "")
    return (
        f"{action_line}\n"
        f"Категория: <b>{category_name}</b>\n\n"
        "<b>Текущий порядок ролей:</b>\n"
        f"{_render_category_role_preview(preview)}\n\n"
        "ℹ️ Если ничего не менять, роль будет добавлена последней.\n"
        f"Сейчас: <b>{preview.get('position_description')}</b>\n\n"
        "Выберите позицию кнопкой ниже."
    )


def _build_position_choice_keyboard(actor_id: int, operation: str, preview: dict[str, Any]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for item in list(preview.get("insertion_positions") or []):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"#{item['human_index']} — {item['description']}"[:64],
                    callback_data=f"roles_admin:{actor_id}:set_position:{operation}:{item['position']}",
                )
            ]
        )
    back_section = _section_for_operation(
        "category_order" if operation == "category_order" else "role_move"
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"roles_admin:{actor_id}:actions:{back_section}" if back_section else f"roles_admin:{actor_id}:actions",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_pick_category_keyboard(grouped: list[dict], actor_id: int, operation: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(grouped[:20]):
        rows.append([
            InlineKeyboardButton(
                text=f"📂 {item['category']}"[:64],
                callback_data=f"roles_admin:{actor_id}:pick_category:{operation}:{idx}",
            )
        ])
    back_section = _section_for_operation(operation)
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"roles_admin:{actor_id}:actions:{back_section}" if back_section else f"roles_admin:{actor_id}:actions",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_pick_role_keyboard(grouped: list[dict], actor_id: int, operation: str, page: int = 0) -> InlineKeyboardMarkup:
    flattened = _flatten_roles(grouped)
    if operation == "role_delete":
        flattened = [item for item in flattened if not item.get("is_discord_managed")]
    page_size = 8
    safe_page = _normalize_page(page, len(flattened), page_size)
    start = safe_page * page_size
    items = flattened[start : start + page_size]
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(items):
        rows.append([
            InlineKeyboardButton(
                text=f"🎭 {item['role']} [{item['category']}]"[:64],
                callback_data=f"roles_admin:{actor_id}:pick_role:{operation}:{safe_page}:{idx}",
            )
        ])
    nav: list[InlineKeyboardButton] = []
    if safe_page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"roles_admin:{actor_id}:pick_role_page:{operation}:{safe_page - 1}"))
    nav.append(InlineKeyboardButton(text="🔄", callback_data=f"roles_admin:{actor_id}:pick_role_page:{operation}:{safe_page}"))
    if (safe_page + 1) * page_size < len(flattened):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"roles_admin:{actor_id}:pick_role_page:{operation}:{safe_page + 1}"))
    rows.append(nav)
    back_section = _section_for_operation(operation)
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"roles_admin:{actor_id}:actions:{back_section}" if back_section else f"roles_admin:{actor_id}:actions",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_list_keyboard(grouped: list[dict], actor_id: int, page: int) -> InlineKeyboardMarkup:
    safe_page = _normalize_page(page, len(grouped), _ROLES_PAGE_SIZE)
    start = safe_page * _ROLES_PAGE_SIZE
    page_items = grouped[start : start + _ROLES_PAGE_SIZE]

    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(page_items):
        role_count = len(item.get("roles", []))
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📂 {item['category']} ({role_count})"[:64],
                    callback_data=f"roles_admin:{actor_id}:category:{safe_page}:{idx}",
                )
            ]
        )

    nav: list[InlineKeyboardButton] = []
    if safe_page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"roles_admin:{actor_id}:list:{safe_page - 1}"))
    nav.append(InlineKeyboardButton(text="🔄", callback_data=f"roles_admin:{actor_id}:list:{safe_page}"))
    if (safe_page + 1) * _ROLES_PAGE_SIZE < len(grouped):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"roles_admin:{actor_id}:list:{safe_page + 1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data=f"roles_admin:{actor_id}:home")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_category_keyboard(
    actor_id: int,
    page: int,
    category_idx: int,
    roles: list[dict],
    *,
    can_manage_categories: bool,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="⬅️ Назад к списку", callback_data=f"roles_admin:{actor_id}:list:{page}")],
    ]
    if can_manage_categories:
        rows.append(
            [InlineKeyboardButton(text="🗑 Удалить категорию", callback_data=f"roles_admin:{actor_id}:delete_category:{page}:{category_idx}")]
        )
    custom_roles = [role for role in roles[:_MAX_ROLE_BUTTONS] if not bool(role.get("is_discord_managed"))]
    for role in custom_roles:
        role_idx = roles.index(role)
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🗑 {str(role.get('name') or f'Роль #{role_idx + 1}')}"[:64],
                    callback_data=f"roles_admin:{actor_id}:delete_role:{page}:{category_idx}:{role_idx}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data=f"roles_admin:{actor_id}:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _render_home_text(*, hidden_sections: tuple[str, ...] = tuple()) -> str:
    return (
        "🛠 <b>Панель управления ролями</b>\n\n"
        f"{_render_hidden_sections_note(hidden_sections)}"
        "Все обновления идут в <b>одном сообщении</b> через кнопки.\n\n"
        "Главный экран разделён на <b>Категории</b>, <b>Роли</b> и <b>Пользователи</b>, чтобы быстрее попадать в нужный блок.\n"
        "Внутри каждого раздела бот показывает только относящиеся к нему действия.\n"
        "Если кнопки не срабатывают, открой <b>🆘 Не работают кнопки?</b> — там резервные команды и примеры.\n"
        "Для create/move/order бот показывает роли внутри выбранной категории и даёт выбрать точную позицию вставки.\n"
        "Если позицию не задавать, роль будет добавлена в конец категории.\n"
        "\nНужны пояснения по функциям? Нажми кнопку <b>ℹ️ Что делает каждая функция</b>.\n\n"
        "Как указывать пользователя: в ЛС — <code>@username</code> / <code>username</code>, в группе — reply. "
        "Для Discord-аккаунта можно использовать <code>ds:username</code>. ID нужен только как резерв.\n\n"
        f"{_role_catalog_note()}"
    )


def _render_fallback_text() -> str:
    return (
        "🆘 <b>Не работают кнопки?</b>\n\n"
        "Если Telegram-кнопки не срабатывают (лаг, старый клиент, проблемы сети), используй резервные команды.\n"
        "Формат: отправляй команду одной строкой после <code>/roles_admin</code>. "
        "Для команд с описанием используй разделитель <code>|</code>, чтобы спокойно писать текст с пробелами.\n\n"
        "<b>Категории</b>\n"
        "<code>/roles_admin category_create &lt;name&gt; [position]</code>\n"
        "<code>/roles_admin category_order &lt;name&gt; &lt;position&gt;</code>\n"
        "<code>/roles_admin category_delete &lt;name&gt;</code>\n\n"
        "<b>Роли</b>\n"
        "<code>/roles_admin role_create &lt;name&gt; | &lt;category&gt; | &lt;description&gt; | [&lt;как получить&gt;] | [discord_role_id] | [position]</code>\n"
        "<code>/roles_admin role_edit_description &lt;name&gt; | &lt;description&gt;</code>\n"
        "<code>/roles_admin role_edit_acquire_hint &lt;name&gt; | &lt;как получить&gt;</code>\n"
        "<code>/roles_admin role_move &lt;name&gt; &lt;category&gt; [position]</code>\n"
        "<code>/roles_admin role_order &lt;role_name&gt; &lt;category&gt; &lt;position&gt;</code>\n"
        "<code>/roles_admin role_delete &lt;name&gt;</code>\n"
        "Описание и поле «как получить» можно оставить пустыми: тогда бот покажет понятную заглушку пользователю.\n"
        "Если не указывать <code>position</code> в <code>role_create</code> или <code>role_move</code>, роль будет добавлена последней.\n"
        "Кнопочный режим показывает список ролей категории и отдельный экран выбора точной позиции вставки.\n"
        "Внешние Discord-роли не удаляются из каталога: их можно только перемещать и сортировать.\n\n"
        "<b>Пользователи</b>\n"
        "<code>/roles_admin user_roles [reply|@username|username|tg:@username|ds:username|id]</code>\n"
        "<code>/roles_admin user_grant &lt;@username|ds:username&gt; &lt;role_name&gt;</code>\n"
        "<code>/roles_admin user_revoke &lt;@username|ds:username&gt; &lt;role_name&gt;</code>\n"
        "Кнопочный режим для выдачи/снятия ролей теперь поддерживает пакетный выбор: выбери пользователя, отмечай несколько ролей в категории, возвращайся к категориям и продолжай выбор до явного выхода из панели.\n"
        "В ЛС удобнее всего @username / username, в группе — reply. Для Discord используй mention / username / display_name или <code>ds:username</code>. ID оставлен только как резерв для админов.\n"
        "Если найдено несколько совпадений, бот покажет кандидатов с provider, username, display и matched_by.\n\n"
        f"{_role_catalog_note()}"
    )




def _render_help_text() -> str:
    return (
        "ℹ️ <b>Что делает /roles_admin</b>\n\n"
        "<b>Категории</b>\n"
        "• <code>category_create &lt;name&gt; [position]</code> — создать/обновить категорию (<b>только Глава клуба/Главный вице</b>).\n"
        "• <code>category_order &lt;name&gt; &lt;position&gt;</code> — выставить порядок категории (<b>только Глава клуба/Главный вице</b>).\n"
        "• <code>category_delete &lt;name&gt;</code> — удалить категорию (роли уйдут в 'Без категории', <b>только Глава клуба/Главный вице</b>).\n\n"
        "<b>Роли</b>\n"
        "• <code>role_create &lt;name&gt; | &lt;category&gt; | &lt;description&gt; | [&lt;как получить&gt;] | [discord_role_id] | [position]</code> — добавить роль в каталог.\n"
        "• <code>role_edit_description &lt;name&gt; | &lt;description&gt;</code> — обновить описание роли без перемещения.\n"
        "• <code>role_edit_acquire_hint &lt;name&gt; | &lt;как получить&gt;</code> — обновить инструкцию, как получить роль.\n"
        "• <code>role_move &lt;name&gt; &lt;category&gt; [position]</code> — переместить роль в другую категорию.\n"
        "• <code>role_order &lt;role_name&gt; &lt;category&gt; &lt;position&gt;</code> — выставить очередь роли в категории.\n"
        "• <code>role_delete &lt;name&gt;</code> — удалить роль из каталога. Внешние Discord-роли удалять нельзя: только move/order.\n"
        "• После выбора категории бот показывает текущий список ролей и отдельный экран выбора позиции.\n"
        "• Описание роли и блок «как получить» видны в карточках и списках, чтобы пользователи сразу понимали назначение роли и путь к ней.\n"
        "• Если позицию не указывать в <code>role_create</code> или <code>role_move</code>, роль будет добавлена последней.\n\n"
        "<b>Роли пользователей</b>\n"
        "• <code>user_roles [reply|@username|username|tg:@username|ds:username|id]</code> — показать роли пользователя.\n"
        "• <code>user_grant &lt;@username|ds:username&gt; &lt;role_name&gt;</code> — выдать роль в БД.\n"
        "• <code>user_revoke &lt;@username|ds:username&gt; &lt;role_name&gt;</code> — снять роль в БД.\n"
        "• Кнопочная панель выдачи/снятия ролей поддерживает пакетный выбор: можно заходить в разные категории, отмечать несколько ролей и подтверждать всё одной кнопкой.\n"
        "• Порядок ввода одинаковый: Telegram ЛС — <code>@username</code> / <code>username</code>, Telegram группа — reply, Discord — mention / username / display_name.\n"
        "• Если нужен Discord fallback, укажи <code>ds:username</code>; для Telegram можно явно написать <code>tg:@username</code>; ID оставь как резерв.\n"
        "• Если найдено несколько совпадений, бот попросит уточнение, а не выберет первого молча.\n\n"
        "<b>Кнопки в панели</b>\n"
        "• 'Категории и роли' — просмотр списка и переход по категориям.\n"
        "• Внутри категории доступны кнопки удаления категории/ролей.\n"
        "• Все экраны обновляются в одном сообщении без спама.\n\n"
        f"{_role_catalog_note()}"
    )


async def _safe_callback_answer(
    callback: CallbackQuery,
    text: str | None = None,
    *,
    show_alert: bool = False,
) -> None:
    try:
        await callback.answer(text, show_alert=show_alert)
    except Exception:
        logger.exception(
            "roles_admin callback answer failed callback_data=%s actor_id=%s text=%s show_alert=%s",
            callback.data,
            callback.from_user.id if callback.from_user else None,
            text,
            show_alert,
        )


async def _safe_edit_message_text(callback: CallbackQuery, *args, **kwargs) -> bool:
    if not callback.message:
        return False
    try:
        await callback.message.edit_text(*args, **kwargs)
        return True
    except TelegramBadRequest as error:
        if "message is not modified" in str(error).lower():
            logger.info(
                "roles_admin edit skipped message not modified callback_data=%s actor_id=%s",
                callback.data,
                callback.from_user.id if callback.from_user else None,
            )
            return False
        raise

def _render_list_text(grouped: list[dict], page: int) -> str:
    safe_page = _normalize_page(page, len(grouped), _ROLES_PAGE_SIZE)
    start = safe_page * _ROLES_PAGE_SIZE
    page_items = grouped[start : start + _ROLES_PAGE_SIZE]
    total_pages = max((len(grouped) - 1) // _ROLES_PAGE_SIZE + 1, 1)

    lines = [f"🧩 <b>Роли по категориям</b> (стр. {safe_page + 1}/{total_pages})"]
    if not page_items:
        lines.append("\n📭 Категории отсутствуют.")
    for item in page_items:
        lines.append(f"\n• <i>{item['category']}</i>")
        roles = item.get("roles", [])
        if not roles:
            lines.append("  • —")
            continue
        for role in roles:
            lines.append("  " + _format_role_line(role))

    lines.append("\nНажми на категорию ниже для действий (удаление категории/ролей).")
    return "\n".join(lines)


def _resolve_category(grouped: list[dict], page: int, category_idx: int) -> dict | None:
    safe_page = _normalize_page(page, len(grouped), _ROLES_PAGE_SIZE)
    start = safe_page * _ROLES_PAGE_SIZE
    page_items = grouped[start : start + _ROLES_PAGE_SIZE]
    if category_idx < 0 or category_idx >= len(page_items):
        return None
    return page_items[category_idx]


async def _ensure_roles_admin(message: Message) -> bool:
    if not message.from_user:
        await message.answer("❌ Не удалось определить пользователя Telegram.")
        return False
    authority = AuthorityService.resolve_authority("telegram", str(message.from_user.id))
    if authority.level < 80:
        await message.answer("❌ Недостаточно полномочий для управления ролями.")
        return False
    return True


def _can_manage_categories(provider: str, provider_user_id: str) -> bool:
    return AuthorityService.can_manage_role_categories(provider, provider_user_id)


@router.message(Command("roles_admin"))
async def roles_admin_command(message: Message) -> None:
    try:
        persist_telegram_identity_from_user(message.from_user)
        if message.reply_to_message:
            persist_telegram_identity_from_user(message.reply_to_message.from_user)
        if not await _ensure_roles_admin(message):
            return

        await _sync_discord_roles_catalog()

        text = (message.text or "").strip()
        parts = text.split()
        if len(parts) < 2:
            if not message.from_user:
                await message.answer("❌ Не удалось определить пользователя Telegram.")
                return
            visibility = _resolve_visibility_context("telegram", str(message.from_user.id))
            _log_roles_admin_navigation(
                actor_id=message.from_user.id,
                actor_level=visibility.actor_level,
                actor_titles=visibility.actor_titles,
                hidden_sections=visibility.hidden_sections,
                screen="home",
            )
            await message.answer(
                _render_home_text(hidden_sections=visibility.hidden_sections),
                parse_mode="HTML",
                reply_markup=_build_home_keyboard(
                    message.from_user.id,
                    can_manage_categories=visibility.can_manage_categories,
                ),
            )
            return

        subcommand = parts[1].lower()
        args = parts[2:]


        if subcommand == "list":
            grouped = RoleManagementService.list_roles_grouped()
            if not grouped:
                await message.answer("📭 Список ролей пуст или БД недоступна.")
                return
            await message.answer(_render_list_text(grouped, 0), parse_mode="HTML")
            return

        if subcommand == "category_create" and len(args) >= 1:
            if not message.from_user or not _can_manage_categories("telegram", str(message.from_user.id)):
                await message.answer("❌ Категориями может управлять только Глава клуба или Главный вице.")
                return
            position = int(args[-1]) if len(args) > 1 and args[-1].isdigit() else 0
            name = " ".join(args[:-1] if len(args) > 1 and args[-1].isdigit() else args)
            ok = RoleManagementService.create_category(name, position)
            await message.answer("✅ Категория сохранена." if ok else "❌ Не удалось создать категорию (смотри логи).")
            return

        if subcommand == "category_order" and len(args) >= 2:
            if not message.from_user or not _can_manage_categories("telegram", str(message.from_user.id)):
                await message.answer("❌ Категориями может управлять только Глава клуба или Главный вице.")
                return
            position_raw = args[-1]
            name = " ".join(args[:-1]).strip()
            if not position_raw.lstrip("-").isdigit() or not name:
                await message.answer("❌ Формат: /roles_admin category_order <name> <position>")
                return
            ok = RoleManagementService.create_category(name, int(position_raw))
            await message.answer("✅ Порядок категории обновлён." if ok else "❌ Не удалось обновить порядок категории (смотри логи).")
            return

        if subcommand == "category_delete" and len(args) >= 1:
            if not message.from_user or not _can_manage_categories("telegram", str(message.from_user.id)):
                await message.answer("❌ Категориями может управлять только Глава клуба или Главный вице.")
                return
            ok = RoleManagementService.delete_category(" ".join(args))
            await message.answer("✅ Категория удалена." if ok else "❌ Не удалось удалить категорию (смотри логи).")
            return

        if subcommand == "role_create":
            raw_payload = text.split(None, 2)[2] if len(parts) >= 3 else ""
            pipe_args = _parse_pipe_args(raw_payload)
            if len(pipe_args) < 2:
                await message.answer(
                    "❌ Формат: /roles_admin role_create <Название роли> | <Категория> | <Описание> | [Как получить] | [discord_role_id] | [position]"
                )
                return
            parsed = _parse_role_create_metadata_args(pipe_args)
            ok = RoleManagementService.create_role(
                parsed["role_name"],
                parsed["category"],
                description=parsed["description"],
                acquire_hint=parsed["acquire_hint"],
                discord_role_id=parsed["discord_role_id"],
                position=parsed["position"],
                actor_id=str(message.from_user.id) if message.from_user else None,
                operation="role_create",
            )
            await message.answer("✅ Роль создана." if ok else "❌ Не удалось создать роль (смотри логи).")
            return

        if subcommand == "role_edit_description":
            raw_payload = text.split(None, 2)[2] if len(parts) >= 3 else ""
            pipe_args = _parse_pipe_args(raw_payload)
            if len(pipe_args) < 2:
                await message.answer("❌ Формат: /roles_admin role_edit_description <Название роли> | <Описание>")
                return
            ok = RoleManagementService.update_role_description(
                pipe_args[0],
                pipe_args[1],
                actor_id=str(message.from_user.id) if message.from_user else None,
                operation="role_edit_description",
            )
            await message.answer("✅ Описание роли обновлено." if ok else "❌ Не удалось обновить описание роли (смотри логи).")
            return

        if subcommand == "role_edit_acquire_hint":
            raw_payload = text.split(None, 2)[2] if len(parts) >= 3 else ""
            pipe_args = _parse_pipe_args(raw_payload)
            if len(pipe_args) < 2:
                await message.answer("❌ Формат: /roles_admin role_edit_acquire_hint <Название роли> | <Как получить>")
                return
            ok = RoleManagementService.update_role_acquire_hint(
                pipe_args[0],
                pipe_args[1],
                actor_id=str(message.from_user.id) if message.from_user else None,
                operation="role_edit_acquire_hint",
            )
            await message.answer("✅ Способ получения роли обновлён." if ok else "❌ Не удалось обновить способ получения роли (смотри логи).")
            return

        if subcommand == "role_delete" and len(args) >= 1:
            result = RoleManagementService.delete_role(
                args[0],
                actor_id=str(message.from_user.id) if message.from_user else None,
                telegram_user_id=str(message.from_user.id) if message.from_user else None,
            )
            await message.answer("✅ Роль удалена." if result["ok"] else _delete_role_result_message(result))
            return

        if subcommand == "role_move" and len(args) >= 2:
            preview = RoleManagementService.get_category_role_positioning(
                args[1],
                requested_position=int(args[2]) if len(args) >= 3 and args[2].lstrip("-").isdigit() else None,
                exclude_role_name=args[0],
            )
            available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
            if args[0] not in available_roles:
                _log_role_position_error(
                    actor_id=message.from_user.id if message.from_user else None,
                    operation="role_move",
                    role_name=args[0],
                    category=args[1],
                    requested_position=int(args[2]) if len(args) >= 3 and args[2].lstrip("-").isdigit() else None,
                    computed_last_position=int(preview.get("computed_last_position", 0)),
                    source="fallback_text_command",
                    message="roles_admin role_move denied role missing from canonical catalog",
                )
                await message.answer(_canonical_role_missing_message())
                return
            position = int(args[2]) if len(args) >= 3 and args[2].lstrip("-").isdigit() else None
            ok = RoleManagementService.move_role(
                args[0],
                args[1],
                position,
                actor_id=str(message.from_user.id) if message.from_user else None,
                operation="role_move",
            )
            if not ok:
                _log_role_position_error(
                    actor_id=message.from_user.id if message.from_user else None,
                    operation="role_move",
                    role_name=args[0],
                    category=args[1],
                    requested_position=position,
                    computed_last_position=int(preview.get("computed_last_position", 0)),
                    source="fallback_text_command",
                    message="roles_admin role_move failed",
                )
            await message.answer("✅ Роль перемещена." if ok else "❌ Не удалось переместить роль. Проверь синхронизацию каталога и логи.")
            return

        if subcommand == "role_order" and len(args) >= 3:
            role_name = args[0]
            category = args[1]
            position_raw = args[2]
            preview = RoleManagementService.get_category_role_positioning(
                category,
                requested_position=int(position_raw) if position_raw.lstrip("-").isdigit() else None,
                exclude_role_name=role_name,
            )
            available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
            if role_name not in available_roles:
                _log_role_position_error(
                    actor_id=message.from_user.id if message.from_user else None,
                    operation="role_order",
                    role_name=role_name,
                    category=category,
                    requested_position=int(position_raw) if position_raw.lstrip("-").isdigit() else None,
                    computed_last_position=int(preview.get("computed_last_position", 0)),
                    source="fallback_text_command",
                    message="roles_admin role_order denied role missing from canonical catalog",
                )
                await message.answer(_canonical_role_missing_message())
                return
            if not position_raw.lstrip("-").isdigit():
                await message.answer("❌ Формат: /roles_admin role_order <role_name> <category> <position>")
                return
            ok = RoleManagementService.move_role(
                role_name,
                category,
                int(position_raw),
                actor_id=str(message.from_user.id) if message.from_user else None,
                operation="role_order",
            )
            if not ok:
                _log_role_position_error(
                    actor_id=message.from_user.id if message.from_user else None,
                    operation="role_order",
                    role_name=role_name,
                    category=category,
                    requested_position=int(position_raw),
                    computed_last_position=int(preview.get("computed_last_position", 0)),
                    source="fallback_text_command",
                    message="roles_admin role_order failed",
                )
            await message.answer("✅ Очередность роли обновлена." if ok else "❌ Не удалось обновить очередь роли. Проверь синхронизацию каталога и логи.")
            return

        if subcommand == "user_roles":
            raw_target = " ".join(args).strip() if args else None
            resolved = _resolve_telegram_target(
                actor_id=message.from_user.id if message.from_user else None,
                raw_target=raw_target,
                reply_user=message.reply_to_message.from_user if message.reply_to_message else None,
                operation="user_roles",
                source="fallback_text_command",
            )
            if not resolved:
                await message.answer(f"❌ Укажи пользователя по правилам поиска. {_telegram_user_lookup_hint()}")
                return
            if resolved.get("error"):
                await message.answer(str(resolved.get("message") or "❌ Не удалось найти пользователя."))
                return
            account_id = str(resolved.get("account_id") or "").strip()
            if not account_id:
                await message.answer(_user_without_account_message())
                return
            roles = RoleManagementService.get_user_roles_by_account(account_id)
            if not roles:
                await message.answer("📭 У пользователя нет ролей.")
                return
            lines = [f"🧾 Роли пользователя {resolved['label']}:"]
            for role in roles:
                description = str(role.get("description") or "").strip()
                line = f"• {role['name']} ({role['category']})"
                if description:
                    line += f" — {description}"
                lines.append(line)
            await message.answer("\n".join(lines))
            return

        if subcommand in {"user_grant", "user_revoke"} and (len(args) >= 2 or (len(args) >= 1 and message.reply_to_message)):
            reply_user = message.reply_to_message.from_user if message.reply_to_message else None
            raw_target = args[0] if len(args) >= 2 else None
            role_name = " ".join(args[1:]) if len(args) >= 2 else " ".join(args)
            resolved = _resolve_telegram_target(
                actor_id=message.from_user.id if message.from_user else None,
                raw_target=raw_target,
                reply_user=reply_user,
                operation=subcommand,
                source="fallback_text_command",
            )
            if not resolved:
                await message.answer(f"❌ Укажи пользователя по правилам поиска. {_telegram_user_lookup_hint()}")
                return
            if resolved.get("error"):
                await message.answer(str(resolved.get("message") or "❌ Не удалось найти пользователя."))
                return
            account_id = str(resolved.get("account_id") or "").strip()
            if not account_id:
                await message.answer(_user_without_account_message())
                return
            if subcommand == "user_grant":
                role_info = RoleManagementService.get_role(role_name)
                category = role_info.get("category_name") if role_info else None
                ok = RoleManagementService.assign_user_role_by_account(
                    account_id,
                    role_name,
                    category=category,
                )
                if ok:
                    await _sync_linked_discord_role(resolved, role_name, revoke=False)
                await message.answer(
                    f"✅ Роль выдана пользователю {resolved['label']}."
                    if ok
                    else f"❌ Не удалось выдать роль. {_telegram_user_lookup_hint()}"
                )
            else:
                ok = RoleManagementService.revoke_user_role_by_account(
                    account_id,
                    role_name,
                )
                if ok:
                    await _sync_linked_discord_role(resolved, role_name, revoke=True)
                await message.answer(
                    f"✅ Роль снята у пользователя {resolved['label']}."
                    if ok
                    else f"❌ Не удалось снять роль. {_telegram_user_lookup_hint()}"
                )
            return

        await message.answer("❌ Неверная команда или аргументы. Напишите /roles_admin для панели управления.")
    except Exception:
        logger.exception(
            "roles_admin command failed actor_id=%s text=%s",
            message.from_user.id if message.from_user else None,
            message.text,
        )
        await message.answer("❌ Ошибка выполнения команды ролей.")


@router.callback_query(F.data.startswith("roles_admin:"))
async def roles_admin_callback(callback: CallbackQuery) -> None:
    try:
        persist_telegram_identity_from_user(callback.from_user)
        if not callback.data or not callback.from_user or not callback.message:
            await callback.answer("Некорректный callback", show_alert=True)
            return

        parts = callback.data.split(":")
        if len(parts) < 3:
            await callback.answer("Некорректный callback", show_alert=True)
            return

        owner_id = int(parts[1]) if parts[1].isdigit() else 0
        visibility = _resolve_visibility_context("telegram", str(callback.from_user.id))
        actor_can_manage_categories = visibility.can_manage_categories
        if owner_id != callback.from_user.id:
            logger.warning(
                "roles_admin callback denied foreign actor callback_data=%s actor_id=%s owner_id=%s",
                callback.data,
                callback.from_user.id,
                owner_id,
            )
            await callback.answer("Эта панель открыта другим администратором.", show_alert=True)
            return

        action = parts[2]
        await _sync_discord_roles_catalog()
        grouped = RoleManagementService.list_roles_grouped() or []

        if action == "help":
            _log_roles_admin_navigation(
                actor_id=callback.from_user.id,
                actor_level=visibility.actor_level,
                actor_titles=visibility.actor_titles,
                hidden_sections=visibility.hidden_sections,
                screen="help",
            )
            await _safe_edit_message_text(callback, 
                _render_help_text(),
                parse_mode="HTML",
                reply_markup=_build_home_keyboard(owner_id, can_manage_categories=actor_can_manage_categories),
            )
            await callback.answer()
            return

        if action == "fallback":
            logger.info("roles_admin fallback opened actor_id=%s", callback.from_user.id)
            _log_roles_admin_navigation(
                actor_id=callback.from_user.id,
                actor_level=visibility.actor_level,
                actor_titles=visibility.actor_titles,
                hidden_sections=visibility.hidden_sections,
                screen="fallback",
            )
            await _safe_edit_message_text(
                callback,
                _render_fallback_text(),
                parse_mode="HTML",
                reply_markup=_build_home_keyboard(owner_id, can_manage_categories=actor_can_manage_categories),
            )
            await callback.answer()
            return

        if action == "home":
            _log_roles_admin_navigation(
                actor_id=callback.from_user.id,
                actor_level=visibility.actor_level,
                actor_titles=visibility.actor_titles,
                hidden_sections=visibility.hidden_sections,
                screen="home",
            )
            await _safe_edit_message_text(callback, 
                _render_home_text(hidden_sections=visibility.hidden_sections),
                parse_mode="HTML",
                reply_markup=_build_home_keyboard(owner_id, can_manage_categories=actor_can_manage_categories),
            )
            await callback.answer()
            return

        if action == "list":
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            await _safe_edit_message_text(callback, 
                _render_list_text(grouped, page),
                parse_mode="HTML",
                reply_markup=_build_list_keyboard(grouped, owner_id, page),
            )
            await callback.answer()
            return

        if action == "actions":
            section = parts[3] if len(parts) > 3 else None
            if section == "categories" and not actor_can_manage_categories:
                logger.warning(
                    "roles_admin section denied actor_id=%s actor_level=%s actor_titles=%s hidden_sections=%s section=%s",
                    callback.from_user.id,
                    visibility.actor_level,
                    list(visibility.actor_titles),
                    list(visibility.hidden_sections),
                    section,
                )
                await callback.answer("Раздел скрыт: категориями может управлять только Глава клуба или Главный вице.", show_alert=True)
                return
            _log_roles_admin_navigation(
                actor_id=callback.from_user.id,
                actor_level=visibility.actor_level,
                actor_titles=visibility.actor_titles,
                hidden_sections=visibility.hidden_sections,
                screen=f"actions:{section or 'hub'}",
            )
            await _safe_edit_message_text(callback, 
                _render_actions_text(section, hidden_sections=visibility.hidden_sections),
                parse_mode="HTML",
                reply_markup=_build_actions_keyboard(
                    owner_id,
                    section,
                    can_manage_categories=actor_can_manage_categories,
                ),
            )
            await callback.answer()
            return

        if action == "start":
            operation = parts[3] if len(parts) > 3 else ""
            if operation.startswith("category_") and not actor_can_manage_categories:
                await callback.answer("Категориями может управлять только Глава клуба или Главный вице.", show_alert=True)
                return
            if operation in {"user_grant", "user_revoke"}:
                flow_action = "grant" if operation == "user_grant" else "revoke"
                _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                    operation="user_role_flow_target",
                    created_at=time.time(),
                    payload={"action": flow_action},
                )
                await callback.answer("Сначала выберите пользователя", show_alert=True)
                await callback.message.reply(
                    (
                        "👤 Сначала укажите пользователя: <code>@username</code>, <code>username</code>, "
                        "<code>tg:@username</code>, <code>ds:username</code> или reply.\n"
                        "После выбора откроется кнопочная панель категорий и ролей.\n"
                        "ℹ️ Выбор можно продолжать по другим категориям до явного выхода из панели."
                    ),
                    parse_mode="HTML",
                )
                return
            if operation in {"category_order", "category_delete", "role_create"}:
                await _safe_edit_message_text(callback, 
                    "Выберите категорию:",
                    reply_markup=_build_pick_category_keyboard(grouped, owner_id, operation),
                )
                await callback.answer()
                return
            if operation in {"role_move", "role_order", "role_delete", "role_edit_acquire_hint"}:
                flattened_roles = _flatten_roles(grouped)
                if operation == "role_delete":
                    flattened_roles = [item for item in flattened_roles if not item.get("is_discord_managed")]
                if not flattened_roles:
                    logger.error(
                        "roles_admin start=%s has no roles to pick actor_id=%s grouped_categories=%s",
                        operation,
                        callback.from_user.id,
                        len(grouped),
                    )
                    empty_message = (
                        "Нет кастомных ролей для удаления: внешние Discord-роли можно только перемещать и сортировать."
                        if operation == "role_delete"
                        else "В каталоге ролей пока нет ни одной роли"
                    )
                    await callback.answer(empty_message, show_alert=True)
                    return
                await _safe_edit_message_text(callback, 
                    "Выберите роль:",
                    reply_markup=_build_pick_role_keyboard(grouped, owner_id, operation, 0),
                )
                await callback.answer()
                return
            _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(operation=operation, created_at=time.time())
            await callback.answer("Ожидаю ввод параметров", show_alert=True)
            await callback.message.reply(_operation_hint(operation), parse_mode="HTML")
            return

        if action == "pick_role_page":
            operation = parts[3] if len(parts) > 3 else ""
            page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
            flattened_roles = _flatten_roles(grouped)
            if operation == "role_delete":
                flattened_roles = [item for item in flattened_roles if not item.get("is_discord_managed")]
            if not flattened_roles:
                await callback.answer(
                    "Нет кастомных ролей для удаления: внешние Discord-роли можно только перемещать и сортировать."
                    if operation == "role_delete"
                    else "В каталоге ролей пока нет ни одной роли",
                    show_alert=True,
                )
                return
            await _safe_edit_message_text(callback, 
                "Выберите роль:",
                reply_markup=_build_pick_role_keyboard(grouped, owner_id, operation, page),
            )
            await callback.answer()
            return

        if action == "pick_category":
            operation = parts[3] if len(parts) > 3 else ""
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            if category_idx < 0 or category_idx >= len(grouped):
                await callback.answer("Категория не найдена", show_alert=True)
                return
            category_name = str(grouped[category_idx]["category"])
            if operation == "category_delete":
                ok = RoleManagementService.delete_category(category_name)
                await callback.answer("Категория удалена" if ok else "Не удалось удалить категорию", show_alert=not ok)
                await _safe_edit_message_text(
                    callback,
                    _render_actions_text("categories", hidden_sections=visibility.hidden_sections),
                    parse_mode="HTML",
                    reply_markup=_build_actions_keyboard(
                        owner_id,
                        "categories",
                        can_manage_categories=actor_can_manage_categories,
                    ),
                )
                return
            if operation == "category_order":
                _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                    operation="category_order_pick_position",
                    created_at=time.time(),
                    payload={"category": category_name},
                )
                preview = {
                    "insertion_positions": [
                        {"position": 0, "human_index": 1, "description": "будет добавлено в начало (#1)"},
                        {
                            "position": max(len(grouped) - 1, 0),
                            "human_index": max(len(grouped), 1),
                            "description": f"будет добавлено в конец (#{max(len(grouped), 1)})",
                        },
                    ]
                }
                await _safe_edit_message_text(callback, 
                    f"Выбрана категория: <b>{category_name}</b>\nВыберите новую позицию:",
                    parse_mode="HTML",
                    reply_markup=_build_position_choice_keyboard(owner_id, "category_order", preview),
                )
                await callback.answer()
                return
            if operation == "role_create":
                preview = RoleManagementService.get_category_role_positioning(category_name)
                _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                    operation="role_create_pick_position",
                    created_at=time.time(),
                    payload={"category": category_name},
                )
                await _safe_edit_message_text(
                    callback,
                    _render_position_picker_text(mode="create", category_name=category_name, preview=preview),
                    parse_mode="HTML",
                    reply_markup=_build_position_choice_keyboard(owner_id, "role_create_position", preview),
                )
                await callback.answer()
                return
            if operation in {"role_move_target", "role_order_target"}:
                pending = _PENDING_ACTIONS.get(callback.from_user.id)
                if not pending or not pending.payload or not pending.payload.get("role"):
                    await callback.answer("Сессия устарела, начните заново", show_alert=True)
                    return
                available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
                if pending.payload["role"] not in available_roles:
                    _log_role_position_error(
                        actor_id=callback.from_user.id if callback.from_user else None,
                        operation="role_move" if operation == "role_move_target" else "role_order",
                        role_name=pending.payload["role"],
                        category=category_name,
                        requested_position=None,
                        computed_last_position=int(
                            RoleManagementService.get_category_role_positioning(
                                category_name,
                                exclude_role_name=pending.payload["role"],
                            ).get("computed_last_position", 0)
                        ),
                        source="button",
                        message="roles_admin pending role target denied role missing from canonical catalog",
                    )
                    _PENDING_ACTIONS.pop(callback.from_user.id, None)
                    await callback.answer("Роль больше не найдена в каталоге", show_alert=True)
                    await callback.message.reply(_canonical_role_missing_message())
                    return
                pending.operation = "role_pick_position"
                pending.payload["category"] = category_name
                pending.payload["mode"] = "move" if operation == "role_move_target" else "order"
                pending.created_at = time.time()
                _PENDING_ACTIONS[callback.from_user.id] = pending
                preview = RoleManagementService.get_category_role_positioning(
                    category_name,
                    exclude_role_name=pending.payload["role"],
                )
                await _safe_edit_message_text(callback, 
                    _render_position_picker_text(
                        mode="move" if operation == "role_move_target" else "order",
                        category_name=category_name,
                        preview=preview,
                        role_name=pending.payload["role"],
                    ),
                    parse_mode="HTML",
                    reply_markup=_build_position_choice_keyboard(owner_id, "role_position", preview),
                )
            await callback.answer()
            return

        if action == "user_role_categories":
            flow_action = parts[3] if len(parts) > 3 else ""
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await callback.answer("Панель выбора устарела, начните заново.", show_alert=True)
                return
            payload = pending.payload or {}
            await _safe_edit_message_text(
                callback,
                _render_user_role_flow_text(
                    target_label=str(payload.get("label") or "неизвестный пользователь"),
                    action=flow_action,
                    selected_roles=_normalize_role_names(payload.get("selected_roles")),
                ),
                parse_mode="HTML",
                reply_markup=_build_user_role_categories_keyboard(
                    grouped,
                    owner_id,
                    flow_action,
                    _normalize_role_names(payload.get("selected_roles")),
                ),
            )
            await callback.answer()
            return

        if action == "user_role_category":
            flow_action = parts[3] if len(parts) > 3 else ""
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await callback.answer("Панель выбора устарела, начните заново.", show_alert=True)
                return
            if category_idx < 0 or category_idx >= len(grouped):
                await callback.answer("Категория не найдена", show_alert=True)
                return
            payload = pending.payload or {}
            payload["selected_roles"] = _normalize_role_names(payload.get("selected_roles"))
            pending.payload = payload
            _PENDING_ACTIONS[callback.from_user.id] = pending
            await _safe_edit_message_text(
                callback,
                _render_user_role_flow_text(
                    target_label=str(payload.get("label") or "неизвестный пользователь"),
                    action=flow_action,
                    selected_roles=payload["selected_roles"],
                    current_category=str(grouped[category_idx]["category"]),
                ),
                parse_mode="HTML",
                reply_markup=_build_user_role_picker_keyboard(
                    grouped,
                    owner_id,
                    flow_action,
                    category_idx,
                    payload["selected_roles"],
                    0,
                ),
            )
            await callback.answer()
            return

        if action == "user_role_page":
            flow_action = parts[3] if len(parts) > 3 else ""
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            page = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await callback.answer("Панель выбора устарела, начните заново.", show_alert=True)
                return
            if category_idx < 0 or category_idx >= len(grouped):
                await callback.answer("Категория не найдена", show_alert=True)
                return
            payload = pending.payload or {}
            selected_roles = _normalize_role_names(payload.get("selected_roles"))
            await _safe_edit_message_text(
                callback,
                _render_user_role_flow_text(
                    target_label=str(payload.get("label") or "неизвестный пользователь"),
                    action=flow_action,
                    selected_roles=selected_roles,
                    current_category=str(grouped[category_idx]["category"]),
                ),
                parse_mode="HTML",
                reply_markup=_build_user_role_picker_keyboard(
                    grouped,
                    owner_id,
                    flow_action,
                    category_idx,
                    selected_roles,
                    page,
                ),
            )
            await callback.answer()
            return

        if action == "user_role_toggle":
            flow_action = parts[3] if len(parts) > 3 else ""
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            page = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0
            role_idx = int(parts[6]) if len(parts) > 6 and parts[6].isdigit() else -1
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await callback.answer("Панель выбора устарела, начните заново.", show_alert=True)
                return
            if category_idx < 0 or category_idx >= len(grouped):
                await callback.answer("Категория не найдена", show_alert=True)
                return
            category_roles = [
                role
                for role in list(grouped[category_idx].get("roles") or [])
                if str(role.get("name") or "").strip()
            ]
            safe_page = _normalize_page(page, len(category_roles), _MAX_ROLE_BUTTONS)
            item_index = safe_page * _MAX_ROLE_BUTTONS + role_idx
            if role_idx < 0 or item_index >= len(category_roles):
                await callback.answer("Роль не найдена", show_alert=True)
                return
            role_name = str(category_roles[item_index].get("name") or "").strip()
            payload = pending.payload or {}
            selected_set = set(_normalize_role_names(payload.get("selected_roles")))
            if role_name in selected_set:
                selected_set.remove(role_name)
                toast = f"Убрано из пакета: {role_name}"
            else:
                selected_set.add(role_name)
                toast = f"Добавлено в пакет: {role_name}"
            payload["selected_roles"] = sorted(selected_set)
            pending.payload = payload
            pending.created_at = time.time()
            _PENDING_ACTIONS[callback.from_user.id] = pending
            await _safe_edit_message_text(
                callback,
                _render_user_role_flow_text(
                    target_label=str(payload.get("label") or "неизвестный пользователь"),
                    action=flow_action,
                    selected_roles=payload["selected_roles"],
                    current_category=str(grouped[category_idx]["category"]),
                ),
                parse_mode="HTML",
                reply_markup=_build_user_role_picker_keyboard(
                    grouped,
                    owner_id,
                    flow_action,
                    category_idx,
                    payload["selected_roles"],
                    safe_page,
                ),
            )
            await callback.answer(toast)
            return

        if action == "user_role_clear":
            flow_action = parts[3] if len(parts) > 3 else ""
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await callback.answer("Панель выбора устарела, начните заново.", show_alert=True)
                return
            payload = pending.payload or {}
            payload["selected_roles"] = []
            pending.payload = payload
            pending.created_at = time.time()
            _PENDING_ACTIONS[callback.from_user.id] = pending
            await _safe_edit_message_text(
                callback,
                _render_user_role_flow_text(
                    target_label=str(payload.get("label") or "неизвестный пользователь"),
                    action=flow_action,
                    selected_roles=[],
                ),
                parse_mode="HTML",
                reply_markup=_build_user_role_categories_keyboard(grouped, owner_id, flow_action, []),
            )
            await callback.answer("Выбор очищен")
            return

        if action == "user_role_exit":
            flow_action = parts[3] if len(parts) > 3 else ""
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if pending and str((pending.payload or {}).get("action") or "") == flow_action:
                _PENDING_ACTIONS.pop(callback.from_user.id, None)
            await _safe_edit_message_text(
                callback,
                _render_actions_text("users", hidden_sections=visibility.hidden_sections),
                parse_mode="HTML",
                reply_markup=_build_actions_keyboard(
                    owner_id,
                    "users",
                    can_manage_categories=actor_can_manage_categories,
                ),
            )
            await callback.answer("Панель выбора закрыта")
            return

        if action == "user_role_apply":
            flow_action = parts[3] if len(parts) > 3 else ""
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await callback.answer("Панель выбора устарела, начните заново.", show_alert=True)
                return
            payload = pending.payload or {}
            selected_roles = _normalize_role_names(payload.get("selected_roles"))
            account_id = str(payload.get("account_id") or "").strip()
            if not account_id or not selected_roles:
                await callback.answer("Сначала выберите хотя бы одну роль.", show_alert=True)
                return
            grant_roles, revoke_roles = _user_role_flow_summary_lists(flow_action, selected_roles)
            result = RoleManagementService.apply_user_role_changes_by_account(
                account_id,
                actor_id=str(callback.from_user.id) if callback.from_user else None,
                grant_roles=grant_roles,
                revoke_roles=revoke_roles,
            )
            sync_target = {
                "provider": payload.get("provider"),
                "provider_user_id": payload.get("provider_user_id"),
                "account_id": account_id,
            }
            for role_name in list(result.get("grant_success") or []):
                await _sync_linked_discord_role(sync_target, role_name, revoke=False)
            for role_name in list(result.get("revoke_success") or []):
                await _sync_linked_discord_role(sync_target, role_name, revoke=True)
            _PENDING_ACTIONS.pop(callback.from_user.id, None)
            success_lines = []
            if result.get("grant_success"):
                success_lines.append("✅ Выдано: " + ", ".join(result["grant_success"]))
            if result.get("revoke_success"):
                success_lines.append("✅ Снято: " + ", ".join(result["revoke_success"]))
            if result.get("grant_failed"):
                success_lines.append("❌ Не выдано: " + ", ".join(result["grant_failed"]))
            if result.get("revoke_failed"):
                success_lines.append("❌ Не снято: " + ", ".join(result["revoke_failed"]))
            if result.get("conflicting_roles"):
                success_lines.append("⚠️ Пропущены конфликтующие роли: " + ", ".join(result["conflicting_roles"]))
            await _safe_edit_message_text(
                callback,
                (
                    f"👤 Пользователь: <b>{payload.get('label') or 'неизвестный пользователь'}</b>\n"
                    "Пакетная операция завершена.\n\n"
                    + ("\n".join(success_lines) if success_lines else "⚠️ Ничего не было применено.")
                    + "\n\nℹ️ Чтобы продолжить работу, откройте раздел пользователей снова."
                ),
                parse_mode="HTML",
                reply_markup=_build_actions_keyboard(
                    owner_id,
                    "users",
                    can_manage_categories=actor_can_manage_categories,
                ),
            )
            await callback.answer("Пакет применён" if result.get("ok") else "Пакет применён с ошибками", show_alert=not result.get("ok"))
            return

        if action == "pick_role":
            operation = parts[3] if len(parts) > 3 else ""
            page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
            role_idx = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else -1
            flattened = _flatten_roles(grouped)
            page_size = 8
            safe_page = _normalize_page(page, len(flattened), page_size)
            item_index = safe_page * page_size + role_idx
            if role_idx < 0 or item_index >= len(flattened):
                await callback.answer("Роль не найдена", show_alert=True)
                return
            role_name = flattened[item_index]["role"]
            if operation == "role_delete":
                result = RoleManagementService.delete_role(
                    role_name,
                    actor_id=str(callback.from_user.id) if callback.from_user else None,
                    telegram_user_id=str(callback.from_user.id) if callback.from_user else None,
                )
                await callback.answer(
                    f"Роль {role_name} удалена" if result["ok"] else _delete_role_result_message(result),
                    show_alert=not result["ok"],
                )
                await _safe_edit_message_text(
                    callback,
                    _render_actions_text("roles", hidden_sections=visibility.hidden_sections),
                    parse_mode="HTML",
                    reply_markup=_build_actions_keyboard(
                        owner_id,
                        "roles",
                        can_manage_categories=actor_can_manage_categories,
                    ),
                )
                return
            if operation == "role_edit_acquire_hint":
                _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                    operation="role_edit_acquire_hint",
                    created_at=time.time(),
                    payload={"role": role_name},
                )
                await callback.answer("Ожидаю текст для блока «Как получить»", show_alert=True)
                await callback.message.reply(
                    f"Выбрана роль: <b>{role_name}</b>\n"
                    "Отправь: <code>Название роли | Как получить</code> или просто <code>Как получить</code>.\n"
                    "Пиши коротко и понятно: через активность, турнир, заявку, выдачу админа и т.д.",
                    parse_mode="HTML",
                )
                return
            if operation in {"role_move", "role_order"}:
                available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
                if role_name not in available_roles:
                    _log_role_position_error(
                        actor_id=callback.from_user.id if callback.from_user else None,
                        operation=operation,
                        role_name=role_name,
                        category=str(flattened[item_index].get("category") or ""),
                        requested_position=None,
                        computed_last_position=int(
                            RoleManagementService.get_category_role_positioning(
                                str(flattened[item_index].get("category") or ""),
                                exclude_role_name=role_name,
                            ).get("computed_last_position", 0)
                        ),
                        source="button",
                        message="roles_admin pick_role denied role missing from canonical catalog",
                    )
                    await callback.answer("Роль не найдена в каталоге", show_alert=True)
                    await callback.message.reply(_canonical_role_missing_message())
                    return
                _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                    operation="role_pick_category",
                    created_at=time.time(),
                    payload={"role": role_name, "mode": "move" if operation == "role_move" else "order"},
                )
                next_operation = "role_move_target" if operation == "role_move" else "role_order_target"
                await _safe_edit_message_text(callback, 
                    f"Роль: <b>{role_name}</b>\nВыберите целевую категорию:",
                    parse_mode="HTML",
                    reply_markup=_build_pick_category_keyboard(grouped, owner_id, next_operation),
                )
                await callback.answer()
                return

        if action == "set_position":
            op = parts[3] if len(parts) > 3 else ""
            value = parts[4] if len(parts) > 4 else ""
            pending = _PENDING_ACTIONS.get(callback.from_user.id)
            if op == "category_order":
                if not pending or pending.operation != "category_order_pick_position" or not pending.payload:
                    await callback.answer("Сессия устарела, начните заново", show_alert=True)
                    return
                category_name = pending.payload.get("category", "")
                new_pos = int(value) if value.lstrip("-").isdigit() else max(len(grouped) - 1, 0)
                ok = RoleManagementService.create_category(category_name, new_pos)
                _PENDING_ACTIONS.pop(callback.from_user.id, None)
                await callback.answer("Порядок категории обновлён" if ok else "Не удалось обновить порядок", show_alert=not ok)
                await _safe_edit_message_text(
                    callback,
                    _render_actions_text("categories", hidden_sections=visibility.hidden_sections),
                    parse_mode="HTML",
                    reply_markup=_build_actions_keyboard(
                        owner_id,
                        "categories",
                        can_manage_categories=actor_can_manage_categories,
                    ),
                )
                return
            if op == "role_create_position":
                if not pending or pending.operation != "role_create_pick_position" or not pending.payload:
                    await callback.answer("Сессия устарела, начните заново", show_alert=True)
                    return
                category_name = pending.payload.get("category", "")
                preview = RoleManagementService.get_category_role_positioning(category_name)
                new_pos = int(value) if value.lstrip("-").isdigit() else int(preview.get("computed_last_position", 0))
                pending.operation = "role_create_enter_name"
                pending.payload["position"] = str(new_pos)
                pending.created_at = time.time()
                _PENDING_ACTIONS[callback.from_user.id] = pending
                await _safe_edit_message_text(
                    callback,
                    (
                        f"Категория: <b>{category_name}</b>\n"
                        f"Позиция: <b>{preview.get('insertion_positions', [])[new_pos]['description'] if preview.get('insertion_positions') else preview.get('position_description')}</b>\n\n"
                        "Теперь отправь: <code>Название роли | Описание | discord_role_id(опц)</code>\n"
                        "Если описание или Discord role id не нужны, можно оставить только название."
                    ),
                    parse_mode="HTML",
                    reply_markup=_build_actions_keyboard(
                        owner_id,
                        "roles",
                        can_manage_categories=actor_can_manage_categories,
                    ),
                )
                await callback.answer()
                return
            if op == "role_position":
                if not pending or pending.operation != "role_pick_position" or not pending.payload:
                    await callback.answer("Сессия устарела, начните заново", show_alert=True)
                    return
                role_name = pending.payload.get("role", "")
                category_name = pending.payload.get("category", "")
                preview = RoleManagementService.get_category_role_positioning(
                    category_name,
                    exclude_role_name=role_name,
                )
                available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
                if role_name not in available_roles:
                    _log_role_position_error(
                        actor_id=callback.from_user.id if callback.from_user else None,
                        operation=pending.payload.get("mode") or "role_move",
                        role_name=role_name,
                        category=category_name,
                        requested_position=None,
                        computed_last_position=int(preview.get("computed_last_position", 0)),
                        source="button",
                        message="roles_admin role_position denied role missing from canonical catalog",
                    )
                    _PENDING_ACTIONS.pop(callback.from_user.id, None)
                    await callback.answer("Роль не найдена в каталоге", show_alert=True)
                    await callback.message.reply(_canonical_role_missing_message())
                    return
                new_pos = int(value) if value.lstrip("-").isdigit() else int(preview.get("computed_last_position", 0))
                ok = RoleManagementService.move_role(
                    role_name,
                    category_name,
                    new_pos,
                    actor_id=str(callback.from_user.id) if callback.from_user else None,
                    operation="role_move" if (pending.payload.get("mode") or "move") == "move" else "role_order",
                )
                _PENDING_ACTIONS.pop(callback.from_user.id, None)
                if not ok:
                    _log_role_position_error(
                        actor_id=callback.from_user.id if callback.from_user else None,
                        operation="role_move" if (pending.payload.get("mode") or "move") == "move" else "role_order",
                        role_name=role_name,
                        category=category_name,
                        requested_position=new_pos,
                        computed_last_position=int(preview.get("computed_last_position", 0)),
                        source="button",
                        message="roles_admin role_position failed",
                    )
                await callback.answer(
                    "Позиция роли обновлена" if ok else "Не удалось обновить позицию роли",
                    show_alert=not ok,
                )
                await _safe_edit_message_text(
                    callback,
                    _render_actions_text("roles", hidden_sections=visibility.hidden_sections),
                    parse_mode="HTML",
                    reply_markup=_build_actions_keyboard(
                        owner_id,
                        "roles",
                        can_manage_categories=actor_can_manage_categories,
                    ),
                )
                return

        if action == "category":
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            category_item = _resolve_category(grouped, page, category_idx)
            if not category_item:
                await callback.answer("Категория не найдена, обновите список.", show_alert=True)
                return

            roles = category_item.get("roles", [])
            lines = [f"📂 <b>{category_item['category']}</b>"]
            if not roles:
                lines.append("\n• В категории нет ролей")
            else:
                for idx, role in enumerate(roles[:_MAX_ROLE_BUTTONS], start=1):
                    lines.append("\n" + _format_role_line(role, numbered=idx))
                if len(roles) > _MAX_ROLE_BUTTONS:
                    lines.append(f"\n… и ещё {len(roles) - _MAX_ROLE_BUTTONS} ролей (удаляй через /roles_admin role_delete)")

            await _safe_edit_message_text(callback, 
                "\n".join(lines),
                parse_mode="HTML",
                reply_markup=_build_category_keyboard(
                    owner_id,
                    page,
                    category_idx,
                    roles,
                    can_manage_categories=actor_can_manage_categories,
                ),
            )
            await callback.answer()
            return

        if action == "delete_category":
            if not actor_can_manage_categories:
                logger.warning(
                    "roles_admin category delete denied callback_data=%s actor_id=%s",
                    callback.data,
                    callback.from_user.id,
                )
                await callback.answer("Категориями может управлять только Глава клуба или Главный вице.", show_alert=True)
                return
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            category_item = _resolve_category(grouped, page, category_idx)
            if not category_item:
                await callback.answer("Категория не найдена, обновите список.", show_alert=True)
                return

            ok = RoleManagementService.delete_category(category_item["category"])
            if not ok:
                await callback.answer("Не удалось удалить категорию (смотри логи).", show_alert=True)
                return

            grouped_after = RoleManagementService.list_roles_grouped() or []
            await _safe_edit_message_text(callback, 
                _render_list_text(grouped_after, page),
                parse_mode="HTML",
                reply_markup=_build_list_keyboard(grouped_after, owner_id, page),
            )
            await callback.answer("Категория удалена")
            return

        if action == "delete_role":
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            role_idx = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else -1
            category_item = _resolve_category(grouped, page, category_idx)
            roles = category_item.get("roles", []) if category_item else []
            if not category_item or role_idx < 0 or role_idx >= len(roles):
                await callback.answer("Роль не найдена, обновите список.", show_alert=True)
                return

            role_name = roles[role_idx]["name"]
            result = RoleManagementService.delete_role(
                role_name,
                actor_id=str(callback.from_user.id) if callback.from_user else None,
                telegram_user_id=str(callback.from_user.id) if callback.from_user else None,
            )
            if not result["ok"]:
                await callback.answer(_delete_role_result_message(result), show_alert=True)
                return

            grouped_after = RoleManagementService.list_roles_grouped() or []
            refreshed_item = _resolve_category(grouped_after, page, category_idx)
            if refreshed_item:
                refreshed_roles = refreshed_item.get("roles", [])
                lines = [f"📂 <b>{refreshed_item['category']}</b>"]
                if not refreshed_roles:
                    lines.append("\n• В категории нет ролей")
                else:
                    for idx, role in enumerate(refreshed_roles[:_MAX_ROLE_BUTTONS], start=1):
                        lines.append("\n" + _format_role_line(role, numbered=idx))
                await _safe_edit_message_text(callback, 
                    "\n".join(lines),
                    parse_mode="HTML",
                    reply_markup=_build_category_keyboard(
                        owner_id,
                        page,
                        category_idx,
                        refreshed_roles,
                        can_manage_categories=actor_can_manage_categories,
                    ),
                )
            else:
                await _safe_edit_message_text(callback, 
                    _render_list_text(grouped_after, page),
                    parse_mode="HTML",
                    reply_markup=_build_list_keyboard(grouped_after, owner_id, page),
                )
            await callback.answer(f"Роль {role_name} удалена")
            return

        await callback.answer("Неизвестное действие", show_alert=True)
    except (TelegramNetworkError, TelegramConflictError):
        logger.exception(
            "roles_admin callback transport failed (telegram runtime/session issue) callback_data=%s actor_id=%s",
            callback.data,
            callback.from_user.id if callback.from_user else None,
        )
        await _safe_callback_answer(
            callback,
            "Сеть Telegram недоступна или идёт перезапуск polling. Попробуйте ещё раз через пару секунд.",
            show_alert=True,
        )
    except Exception:
        logger.exception(
            "roles_admin callback failed callback_data=%s actor_id=%s",
            callback.data,
            callback.from_user.id if callback.from_user else None,
        )
        await _safe_callback_answer(callback, "Ошибка в панели ролей (смотри логи).", show_alert=True)


@router.message(F.from_user, F.from_user.id.func(has_pending_roles_admin_action))
async def roles_admin_pending_action_handler(message: Message) -> None:
    if not message.from_user:
        return
    persist_telegram_identity_from_user(message.from_user)
    if message.reply_to_message:
        persist_telegram_identity_from_user(message.reply_to_message.from_user)
    pending = _PENDING_ACTIONS.get(message.from_user.id)
    if not pending:
        logger.warning(
            "roles_admin pending handler invoked without state user_id=%s chat_id=%s",
            message.from_user.id,
            message.chat.id if message.chat else None,
        )
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Пустой ввод. " + _operation_hint(pending.operation), parse_mode="HTML")
        return
    if text.lower() in {"отмена", "cancel"}:
        _PENDING_ACTIONS.pop(message.from_user.id, None)
        await message.answer("🟡 Операция отменена.")
        return

    try:
        keep_pending = False
        if pending.operation.startswith("category_") and not _can_manage_categories("telegram", str(message.from_user.id)):
            await message.answer("❌ Категориями может управлять только Глава клуба или Главный вице.")
            _PENDING_ACTIONS.pop(message.from_user.id, None)
            return

        args = _parse_pipe_args(text)
        op = pending.operation
        if op == "category_create":
            if not args:
                await message.answer("❌ Формат: Название | position(опц)")
                return
            pos = int(args[1]) if len(args) > 1 and args[1].lstrip("-").isdigit() else 0
            ok = RoleManagementService.create_category(args[0], pos)
            await message.answer("✅ Категория сохранена." if ok else "❌ Не удалось создать категорию (смотри логи).")
        elif op == "category_order":
            if len(args) < 2 or not args[1].lstrip("-").isdigit():
                await message.answer("❌ Формат: Название | position")
                return
            ok = RoleManagementService.create_category(args[0], int(args[1]))
            await message.answer("✅ Порядок категории обновлён." if ok else "❌ Не удалось обновить порядок категории (смотри логи).")
        elif op == "category_delete":
            if not args:
                await message.answer("❌ Формат: Название")
                return
            ok = RoleManagementService.delete_category(args[0])
            await message.answer("✅ Категория удалена." if ok else "❌ Не удалось удалить категорию (смотри логи).")
        elif op == "role_create":
            if len(args) < 2:
                await message.answer("❌ Формат: Роль | Категория | Описание | Как получить(опц) | discord_role_id(опц) | position(опц)")
                return
            parsed = _parse_role_create_metadata_args(args)
            ok = RoleManagementService.create_role(
                parsed["role_name"],
                parsed["category"],
                description=parsed["description"],
                acquire_hint=parsed["acquire_hint"],
                discord_role_id=parsed["discord_role_id"],
                position=parsed["position"],
                actor_id=str(message.from_user.id) if message.from_user else None,
                operation="role_create",
            )
            await message.answer("✅ Роль создана." if ok else "❌ Не удалось создать роль (смотри логи).")
        elif op == "role_edit_description":
            if len(args) < 2:
                await message.answer("❌ Формат: Название роли | Описание")
                return
            ok = RoleManagementService.update_role_description(
                args[0],
                args[1],
                actor_id=str(message.from_user.id) if message.from_user else None,
                operation="role_edit_description",
            )
            await message.answer("✅ Описание роли обновлено." if ok else "❌ Не удалось обновить описание роли (смотри логи).")
        elif op == "role_edit_acquire_hint":
            role_name = str((pending.payload or {}).get("role") or "").strip()
            acquire_hint = None
            if len(args) >= 2 and args[0] == role_name:
                acquire_hint = args[1]
            elif args:
                acquire_hint = args[0]
            if not role_name or not acquire_hint:
                await message.answer("❌ Формат: Название роли | Как получить или просто Как получить после выбора роли.")
                return
            ok = RoleManagementService.update_role_acquire_hint(
                role_name,
                acquire_hint,
                actor_id=str(message.from_user.id) if message.from_user else None,
                operation="role_edit_acquire_hint",
            )
            await message.answer("✅ Способ получения роли обновлён." if ok else "❌ Не удалось обновить способ получения роли (смотри логи).")
        elif op == "role_create_enter_name":
            if not pending.payload or not pending.payload.get("category"):
                await message.answer("❌ Сессия выбора категории устарела. Начните заново: /roles_admin")
                return
            if not args:
                await message.answer("❌ Формат: Название роли | Описание | Как получить(опц) | discord_role_id(опц)")
                return
            category = str(pending.payload.get("category") or "")
            position = int(str(pending.payload.get("position") or "0"))
            parsed = _parse_role_create_metadata_args([args[0], category, args[1] if len(args) > 1 else "", *args[2:]])
            ok = RoleManagementService.create_role(
                parsed["role_name"],
                category,
                description=parsed["description"],
                acquire_hint=parsed["acquire_hint"],
                discord_role_id=parsed["discord_role_id"],
                position=position,
                actor_id=str(message.from_user.id) if message.from_user else None,
                operation="role_create",
            )
            await message.answer("✅ Роль создана." if ok else "❌ Не удалось создать роль (смотри логи).")
        elif op == "role_move":
            if len(args) < 2:
                await message.answer("❌ Формат: Роль | Категория | position(опц)")
                return
            preview = RoleManagementService.get_category_role_positioning(
                args[1],
                requested_position=int(args[2]) if len(args) > 2 and args[2].lstrip("-").isdigit() else None,
                exclude_role_name=args[0],
            )
            available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
            if args[0] not in available_roles:
                _log_role_position_error(
                    actor_id=message.from_user.id if message.from_user else None,
                    operation="role_move",
                    role_name=args[0],
                    category=args[1],
                    requested_position=int(args[2]) if len(args) > 2 and args[2].lstrip("-").isdigit() else None,
                    computed_last_position=int(preview.get("computed_last_position", 0)),
                    source="button",
                    message="roles_admin role_move denied role missing from canonical catalog",
                )
                await message.answer(_canonical_role_missing_message())
                return
            pos = int(args[2]) if len(args) > 2 and args[2].lstrip("-").isdigit() else None
            ok = RoleManagementService.move_role(
                args[0],
                args[1],
                pos,
                actor_id=str(message.from_user.id) if message.from_user else None,
                operation="role_move",
            )
            if not ok:
                _log_role_position_error(
                    actor_id=message.from_user.id if message.from_user else None,
                    operation="role_move",
                    role_name=args[0],
                    category=args[1],
                    requested_position=pos,
                    computed_last_position=int(preview.get("computed_last_position", 0)),
                    source="button",
                    message="roles_admin role_move failed",
                )
            await message.answer("✅ Роль перемещена." if ok else "❌ Не удалось переместить роль. Проверь синхронизацию каталога и логи.")
        elif op == "role_order":
            if len(args) < 3 or not args[2].lstrip("-").isdigit():
                await message.answer("❌ Формат: Роль | Категория | position")
                return
            preview = RoleManagementService.get_category_role_positioning(
                args[1],
                requested_position=int(args[2]),
                exclude_role_name=args[0],
            )
            available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
            if args[0] not in available_roles:
                _log_role_position_error(
                    actor_id=message.from_user.id if message.from_user else None,
                    operation="role_order",
                    role_name=args[0],
                    category=args[1],
                    requested_position=int(args[2]),
                    computed_last_position=int(preview.get("computed_last_position", 0)),
                    source="button",
                    message="roles_admin role_order denied role missing from canonical catalog",
                )
                await message.answer(_canonical_role_missing_message())
                return
            ok = RoleManagementService.move_role(
                args[0],
                args[1],
                int(args[2]),
                actor_id=str(message.from_user.id) if message.from_user else None,
                operation="role_order",
            )
            if not ok:
                _log_role_position_error(
                    actor_id=message.from_user.id if message.from_user else None,
                    operation="role_order",
                    role_name=args[0],
                    category=args[1],
                    requested_position=int(args[2]),
                    computed_last_position=int(preview.get("computed_last_position", 0)),
                    source="button",
                    message="roles_admin role_order failed",
                )
            await message.answer("✅ Очередность роли обновлена." if ok else "❌ Не удалось обновить очередь роли. Проверь синхронизацию каталога и логи.")
        elif op == "role_delete":
            if not args:
                await message.answer("❌ Формат: Название роли")
                return
            result = RoleManagementService.delete_role(
                args[0],
                actor_id=str(message.from_user.id) if message.from_user else None,
                telegram_user_id=str(message.from_user.id) if message.from_user else None,
            )
            await message.answer("✅ Роль удалена." if result["ok"] else _delete_role_result_message(result))
        elif op == "user_roles":
            resolved = _resolve_telegram_target(
                actor_id=message.from_user.id if message.from_user else None,
                raw_target=args[0] if args else None,
                reply_user=message.reply_to_message.from_user if message.reply_to_message else None,
                operation="user_roles",
                source="button",
            )
            if not resolved:
                await message.answer(f"❌ Формат: укажи пользователя по правилам поиска. {_telegram_user_lookup_hint()}")
                return
            if resolved.get("error"):
                await message.answer(str(resolved.get("message") or "❌ Не удалось найти пользователя."))
                return
            account_id = str(resolved.get("account_id") or "").strip()
            if not account_id:
                await message.answer(_user_without_account_message())
                return
            roles = RoleManagementService.get_user_roles_by_account(account_id)
            if not roles:
                await message.answer("📭 У пользователя нет ролей.")
            else:
                lines = [f"🧾 Роли пользователя {resolved['label']}:"]
                for role in roles:
                    description = str(role.get("description") or "").strip()
                    line = f"• {role['name']} ({role['category']})"
                    if description:
                        line += f" — {description}"
                    lines.append(line)
                await message.answer("\n".join(lines))
        elif op == "user_role_flow_target":
            flow_action = str((pending.payload or {}).get("action") or "").strip()
            resolved = _resolve_telegram_target(
                actor_id=message.from_user.id if message.from_user else None,
                raw_target=args[0] if args else None,
                reply_user=message.reply_to_message.from_user if message.reply_to_message else None,
                operation=f"user_{flow_action or 'grant'}",
                source="button",
            )
            if not resolved:
                await message.answer(f"❌ Формат: укажи пользователя по правилам поиска. {_telegram_user_lookup_hint()}")
                return
            if resolved.get("error"):
                await message.answer(str(resolved.get("message") or "❌ Не удалось найти пользователя."))
                return
            account_id = str(resolved.get("account_id") or "").strip()
            if not account_id:
                await message.answer(_user_without_account_message())
                return
            selected_action = "revoke" if flow_action == "revoke" else "grant"
            _PENDING_ACTIONS[message.from_user.id] = PendingRolesAdminAction(
                operation="user_role_flow_panel",
                created_at=time.time(),
                payload={
                    "action": selected_action,
                    "account_id": account_id,
                    "provider": str(resolved.get("provider") or ""),
                    "provider_user_id": str(resolved.get("provider_user_id") or ""),
                    "label": str(resolved.get("label") or "неизвестный пользователь"),
                    "selected_roles": [],
                },
            )
            keep_pending = True
            grouped = RoleManagementService.list_roles_grouped() or []
            await message.answer(
                _render_user_role_flow_text(
                    target_label=str(resolved.get("label") or "неизвестный пользователь"),
                    action=selected_action,
                    selected_roles=[],
                ),
                parse_mode="HTML",
                reply_markup=_build_user_role_categories_keyboard(
                    grouped,
                    message.from_user.id,
                    selected_action,
                    [],
                ),
            )
        elif op in {"user_grant", "user_revoke"}:
            reply_user = message.reply_to_message.from_user if message.reply_to_message else None
            raw_target = args[0] if args else None
            role_name = args[1] if len(args) > 1 else (args[0] if reply_user and args else None)
            if not role_name:
                await message.answer(f"❌ Формат: пользователь | Название роли. {_telegram_user_lookup_hint()}")
                return
            resolved = _resolve_telegram_target(
                actor_id=message.from_user.id if message.from_user else None,
                raw_target=raw_target if not reply_user or len(args) > 1 else None,
                reply_user=reply_user,
                operation=op,
                source="button",
            )
            if not resolved:
                await message.answer(f"❌ Формат: пользователь | Название роли. {_telegram_user_lookup_hint()}")
                return
            if resolved.get("error"):
                await message.answer(str(resolved.get("message") or "❌ Не удалось найти пользователя."))
                return
            account_id = str(resolved.get("account_id") or "").strip()
            if not account_id:
                await message.answer(_user_without_account_message())
                return
            if op == "user_grant":
                result = RoleManagementService.apply_user_role_changes_by_account(
                    account_id,
                    actor_id=str(message.from_user.id) if message.from_user else None,
                    grant_roles=[role_name],
                )
                ok = bool(result.get("grant_success"))
                if ok:
                    await _sync_linked_discord_role(resolved, role_name, revoke=False)
                await message.answer(
                    f"✅ Роль выдана пользователю {resolved['label']}."
                    if ok
                    else f"❌ Не удалось выдать роль. {_telegram_user_lookup_hint()}"
                )
            else:
                result = RoleManagementService.apply_user_role_changes_by_account(
                    account_id,
                    actor_id=str(message.from_user.id) if message.from_user else None,
                    revoke_roles=[role_name],
                )
                ok = bool(result.get("revoke_success"))
                if ok:
                    await _sync_linked_discord_role(resolved, role_name, revoke=True)
                await message.answer(
                    f"✅ Роль снята у пользователя {resolved['label']}."
                    if ok
                    else f"❌ Не удалось снять роль. {_telegram_user_lookup_hint()}"
                )
        else:
            logger.warning("roles_admin pending unknown operation user_id=%s operation=%s", message.from_user.id, op)
            await message.answer("❌ Неизвестная операция. Откройте панель заново: /roles_admin")
    except Exception:
        logger.exception(
            "roles_admin pending action failed user_id=%s operation=%s text=%s",
            message.from_user.id,
            pending.operation,
            message.text,
        )
        await message.answer("❌ Ошибка выполнения операции (смотри логи).")
    finally:
        if not locals().get("keep_pending", False):
            _PENDING_ACTIONS.pop(message.from_user.id, None)
