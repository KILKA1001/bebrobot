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
from bot.services.role_management_service import (
    DELETE_ROLE_REASON_DISCORD_MANAGED,
    DELETE_ROLE_REASON_NOT_FOUND,
)

logger = logging.getLogger(__name__)
router = Router()

_ROLES_PAGE_SIZE = 5
_MAX_ROLE_BUTTONS = 8
_PENDING_TTL_SECONDS = 300


@dataclass
class PendingRolesAdminAction:
    operation: str
    created_at: float
    payload: dict[str, str] | None = None


_PENDING_ACTIONS: dict[int, PendingRolesAdminAction] = {}


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

def _persist_telegram_identity_from_user(user: Any | None) -> None:
    if not user or getattr(user, "is_bot", False):
        return
    AccountsService.persist_identity_lookup_fields(
        "telegram",
        str(user.id),
        username=getattr(user, "username", None),
        display_name=getattr(user, "full_name", None),
    )


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
        "❌ Пользователь не найден в локальном реестре. "
        "Пусть он хотя бы раз взаимодействует с ботом: /register, /link, /profile или просто сообщение боту. "
        + _telegram_user_lookup_hint()
    )


def _resolve_telegram_target(
    *,
    actor_id: int | None,
    raw_target: str | None = None,
    reply_user: Any | None = None,
    operation: str,
    source: str,
) -> dict[str, str] | None:
    if reply_user and not getattr(reply_user, "is_bot", False):
        _persist_telegram_identity_from_user(reply_user)
        username = getattr(reply_user, "username", None)
        display = getattr(reply_user, "full_name", None)
        label = f"@{username}" if username else (display or str(reply_user.id))
        account_id = AccountsService.resolve_account_id("telegram", str(reply_user.id))
        return {
            "account_id": str(account_id or "") or None,
            "provider": "telegram",
            "provider_user_id": str(reply_user.id),
            "label": label,
            "matched_by": "reply",
        }

    token = str(raw_target or "").strip()
    if not token:
        logger.warning(
            "roles_admin user lookup failed actor_id=%s telegram_user_id=%s operation=%s source=%s lookup_value=%s role_name=%s category=%s provider=%s provider_user_id=%s reason=%s candidates=%s",
            actor_id,
            actor_id,
            operation,
            source,
            token,
            None,
            None,
            "telegram",
            None,
            "empty_target",
            0,
        )
        return None

    lookup = AccountsService.resolve_user_lookup(token, default_provider="telegram")
    candidates = list(lookup.get("candidates") or [])
    if lookup.get("status") == "ok":
        resolved = dict(lookup.get("result") or {})
        provider = str(resolved.get("provider") or "").strip()
        provider_user_id = str(resolved.get("provider_user_id") or "").strip()
        username = str(resolved.get("username") or "").strip()
        display_name = str(resolved.get("display_name") or "").strip()
        label = (
            f"@{username}"
            if username
            else f"{display_name} ({provider}:{provider_user_id})"
            if display_name
            else f"{provider}:{provider_user_id}"
        )
        return {
            "account_id": str(resolved.get("account_id") or "") or None,
            "provider": provider,
            "provider_user_id": provider_user_id,
            "label": label,
            "matched_by": str(resolved.get("matched_by") or ""),
        }

    location = "telegram_group" if source == "group" else "telegram_dm"
    if lookup.get("status") == "multiple":
        logger.warning(
            "roles_admin user lookup ambiguous actor_id=%s location=%s operation=%s source=%s lookup_value=%s role_name=%s category=%s provider=%s provider_user_id=%s reason=%s candidates=%s",
            actor_id,
            location,
            operation,
            source,
            token,
            None,
            None,
            "telegram",
            None,
            "multiple_matches",
            len(candidates),
        )
        return {
            "error": "multiple",
            "message": (
                "❌ Найдено несколько пользователей. Уточни username / provider / reply. Кандидаты:\n"
                + "\n".join(f"• {_format_telegram_lookup_candidate(candidate)}" for candidate in candidates[:5])
                + "\n\n"
                + _telegram_user_lookup_hint()
            ),
        }

    if token.isdigit():
        return {
            "account_id": None,
            "provider": "telegram",
            "provider_user_id": token,
            "label": f"telegram:{token}",
            "matched_by": "exact_id_fallback",
        }

    logger.warning(
        "roles_admin user lookup failed actor_id=%s location=%s operation=%s source=%s lookup_value=%s role_name=%s category=%s provider=%s provider_user_id=%s reason=%s candidates=%s",
        actor_id,
        location,
        operation,
        source,
        token,
        None,
        None,
        "telegram",
        None,
        str(lookup.get("reason") or "not_found"),
        len(candidates),
    )
    return {
        "error": "not_found" if lookup.get("status") == "not_found" else "invalid_format",
        "message": _user_not_found_message(),
    }


def _normalize_page(page: int, total_items: int, page_size: int) -> int:
    if total_items <= 0:
        return 0
    max_page = max((total_items - 1) // page_size, 0)
    return min(max(page, 0), max_page)


def _build_home_keyboard(actor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Категории и роли", callback_data=f"roles_admin:{actor_id}:list:0")],
            [InlineKeyboardButton(text="⚡ Действия кнопками", callback_data=f"roles_admin:{actor_id}:actions")],
            [InlineKeyboardButton(text="🆘 Не работают кнопки?", callback_data=f"roles_admin:{actor_id}:fallback")],
            [InlineKeyboardButton(text="ℹ️ Что делает каждая функция", callback_data=f"roles_admin:{actor_id}:help")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"roles_admin:{actor_id}:home")],
        ]
    )


def _build_actions_keyboard(actor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗂 Создать категорию", callback_data=f"roles_admin:{actor_id}:start:category_create")],
            [InlineKeyboardButton(text="↕️ Порядок категории", callback_data=f"roles_admin:{actor_id}:start:category_order")],
            [InlineKeyboardButton(text="🗑 Удалить категорию", callback_data=f"roles_admin:{actor_id}:start:category_delete")],
            [InlineKeyboardButton(text="➕ Создать роль", callback_data=f"roles_admin:{actor_id}:start:role_create")],
            [InlineKeyboardButton(text="🚚 Переместить роль", callback_data=f"roles_admin:{actor_id}:start:role_move")],
            [InlineKeyboardButton(text="🔢 Порядок роли", callback_data=f"roles_admin:{actor_id}:start:role_order")],
            [InlineKeyboardButton(text="🗑 Удалить роль", callback_data=f"roles_admin:{actor_id}:start:role_delete")],
            [InlineKeyboardButton(text="🧾 Роли пользователя", callback_data=f"roles_admin:{actor_id}:start:user_roles")],
            [InlineKeyboardButton(text="✅ Выдать роль", callback_data=f"roles_admin:{actor_id}:start:user_grant")],
            [InlineKeyboardButton(text="❌ Снять роль", callback_data=f"roles_admin:{actor_id}:start:user_revoke")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data=f"roles_admin:{actor_id}:home")],
        ]
    )


def _render_actions_text() -> str:
    return (
        "⚡ <b>Действия кнопками</b>\n\n"
        "Нажми кнопку, затем отправь параметры <b>в следующем сообщении</b>.\n"
        "Разделитель параметров: <code>|</code>.\n"
        "Для отмены ввода отправь: <code>отмена</code>.\n\n"
        f"{_role_catalog_note()}"
    )


def _operation_hint(operation: str) -> str:
    hints = {
        "category_create": "Отправь: <code>Название категории | position(опционально)</code>",
        "category_order": "Отправь: <code>Название категории | position</code>",
        "category_delete": "Отправь: <code>Название категории</code>",
        "role_create": "Отправь: <code>Название роли | Категория | discord_role_id(опц) | position(опц)</code>",
        "role_move": "Отправь: <code>Название роли | Категория | position(опц)</code>. Внешнюю Discord-роль можно переместить.",
        "role_order": "Отправь: <code>Название роли | Категория | position</code>. Внешнюю Discord-роль можно отсортировать.",
        "role_delete": "Отправь: <code>Название роли</code>. Внешние Discord-роли удалить нельзя.",
        "user_roles": "Отправь: <code>@username</code> / <code>username</code> / <code>tg:@username</code> / <code>ds:username</code>. В группе удобнее reply.",
        "user_grant": "Отправь: <code>@username | Название роли</code> или reply + <code>Название роли</code>. Для Discord можно <code>ds:username | Роль</code>.",
        "user_revoke": "Отправь: <code>@username | Название роли</code> или reply + <code>Название роли</code>. Для Discord можно <code>ds:username | Роль</code>.",
    }
    return hints.get(operation, "Неизвестная операция")


def _parse_pipe_args(raw: str) -> list[str]:
    return [part.strip() for part in raw.split("|") if part.strip()]


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


def _build_pick_category_keyboard(grouped: list[dict], actor_id: int, operation: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(grouped[:20]):
        rows.append([
            InlineKeyboardButton(
                text=f"📂 {item['category']}"[:64],
                callback_data=f"roles_admin:{actor_id}:pick_category:{operation}:{idx}",
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"roles_admin:{actor_id}:actions")])
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
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"roles_admin:{actor_id}:actions")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_position_choice_keyboard(actor_id: int, operation: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⏫ В начало", callback_data=f"roles_admin:{actor_id}:set_position:{operation}:start")],
            [InlineKeyboardButton(text="⏬ В конец", callback_data=f"roles_admin:{actor_id}:set_position:{operation}:end")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"roles_admin:{actor_id}:actions")],
        ]
    )

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


def _render_home_text() -> str:
    return (
        "🛠 <b>Панель управления ролями</b>\n\n"
        "Все обновления идут в <b>одном сообщении</b> через кнопки.\n\n"
        "Управление через <b>кнопки</b> в разделе <b>⚡ Действия кнопками</b>.\n"
        "Если кнопки не срабатывают, открой <b>🆘 Не работают кнопки?</b> — там резервные команды и примеры.\n"
        "\nНужны пояснения по функциям? Нажми кнопку <b>ℹ️ Что делает каждая функция</b>.\n\n"
        "Как указывать пользователя: в ЛС — <code>@username</code> / <code>username</code>, в группе — reply. "
        "Для Discord-аккаунта можно использовать <code>ds:username</code>. ID нужен только как резерв.\n\n"
        f"{_role_catalog_note()}"
    )


def _render_fallback_text() -> str:
    return (
        "🆘 <b>Не работают кнопки?</b>\n\n"
        "Если Telegram-кнопки не срабатывают (лаг, старый клиент, проблемы сети), используй резервные команды.\n"
        "Формат: отправляй команду одной строкой после <code>/roles_admin</code>.\n\n"
        "<b>Категории</b>\n"
        "<code>/roles_admin category_create &lt;name&gt; [position]</code>\n"
        "<code>/roles_admin category_order &lt;name&gt; &lt;position&gt;</code>\n"
        "<code>/roles_admin category_delete &lt;name&gt;</code>\n\n"
        "<b>Роли</b>\n"
        "<code>/roles_admin role_create &lt;name&gt; &lt;category&gt; [discord_role_id] [position]</code>\n"
        "<code>/roles_admin role_move &lt;name&gt; &lt;category&gt; [position]</code>\n"
        "<code>/roles_admin role_order &lt;role_name&gt; &lt;category&gt; &lt;position&gt;</code>\n"
        "<code>/roles_admin role_delete &lt;name&gt;</code>\n"
        "Внешние Discord-роли не удаляются из каталога: их можно только перемещать и сортировать.\n\n"
        "<b>Пользователи</b>\n"
        "<code>/roles_admin user_roles [reply|@username|username|tg:@username|ds:username|id]</code>\n"
        "<code>/roles_admin user_grant &lt;@username|ds:username&gt; &lt;role_name&gt;</code>\n"
        "<code>/roles_admin user_revoke &lt;@username|ds:username&gt; &lt;role_name&gt;</code>\n"
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
        "• <code>role_create &lt;name&gt; &lt;category&gt; [discord_role_id] [position]</code> — добавить роль в каталог.\n"
        "• <code>role_move &lt;name&gt; &lt;category&gt; [position]</code> — переместить роль в другую категорию.\n"
        "• <code>role_order &lt;role_name&gt; &lt;category&gt; &lt;position&gt;</code> — выставить очередь роли в категории.\n"
        "• <code>role_delete &lt;name&gt;</code> — удалить роль из каталога. Внешние Discord-роли удалять нельзя: только move/order.\n\n"
        "<b>Роли пользователей</b>\n"
        "• <code>user_roles [reply|@username|username|tg:@username|ds:username|id]</code> — показать роли пользователя.\n"
        "• <code>user_grant &lt;@username|ds:username&gt; &lt;role_name&gt;</code> — выдать роль в БД.\n"
        "• <code>user_revoke &lt;@username|ds:username&gt; &lt;role_name&gt;</code> — снять роль в БД.\n"
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
            suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
            lines.append(f"  • {role['name']}{suffix}")

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
        _persist_telegram_identity_from_user(message.from_user)
        if message.reply_to_message:
            _persist_telegram_identity_from_user(message.reply_to_message.from_user)
        if not await _ensure_roles_admin(message):
            return

        await _sync_discord_roles_catalog()

        text = (message.text or "").strip()
        parts = text.split()
        if len(parts) < 2:
            if not message.from_user:
                await message.answer("❌ Не удалось определить пользователя Telegram.")
                return
            await message.answer(_render_home_text(), parse_mode="HTML", reply_markup=_build_home_keyboard(message.from_user.id))
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

        if subcommand == "role_create" and len(args) >= 2:
            role_name = args[0]
            category = args[1]
            discord_role_id = args[2] if len(args) >= 3 else None
            position = int(args[3]) if len(args) >= 4 and args[3].lstrip("-").isdigit() else 0
            ok = RoleManagementService.create_role(
                role_name,
                category,
                discord_role_id=discord_role_id,
                position=position,
            )
            await message.answer("✅ Роль создана." if ok else "❌ Не удалось создать роль (смотри логи).")
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
            available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
            if args[0] not in available_roles:
                logger.warning(
                    "roles_admin role_move denied role missing from canonical catalog actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s operation=%s source=%s",
                    message.from_user.id if message.from_user else None,
                    None,
                    message.from_user.id if message.from_user else None,
                    args[0],
                    args[1],
                    "role_move",
                    "fallback_text_command",
                )
                await message.answer(_canonical_role_missing_message())
                return
            position = int(args[2]) if len(args) >= 3 and args[2].isdigit() else 0
            ok = RoleManagementService.move_role(args[0], args[1], position)
            if not ok:
                logger.warning(
                    "roles_admin role_move failed actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s position=%s operation=%s source=%s",
                    message.from_user.id if message.from_user else None,
                    None,
                    message.from_user.id if message.from_user else None,
                    args[0],
                    args[1],
                    position,
                    "role_move",
                    "fallback_text_command",
                )
            await message.answer("✅ Роль перемещена." if ok else "❌ Не удалось переместить роль. Проверь синхронизацию каталога и логи.")
            return

        if subcommand == "role_order" and len(args) >= 3:
            role_name = args[0]
            category = args[1]
            position_raw = args[2]
            available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
            if role_name not in available_roles:
                logger.warning(
                    "roles_admin role_order denied role missing from canonical catalog actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s operation=%s source=%s",
                    message.from_user.id if message.from_user else None,
                    None,
                    message.from_user.id if message.from_user else None,
                    role_name,
                    category,
                    "role_order",
                    "fallback_text_command",
                )
                await message.answer(_canonical_role_missing_message())
                return
            if not position_raw.lstrip("-").isdigit():
                await message.answer("❌ Формат: /roles_admin role_order <role_name> <category> <position>")
                return
            ok = RoleManagementService.move_role(role_name, category, int(position_raw))
            if not ok:
                logger.warning(
                    "roles_admin role_order failed actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s position=%s operation=%s source=%s",
                    message.from_user.id if message.from_user else None,
                    None,
                    message.from_user.id if message.from_user else None,
                    role_name,
                    category,
                    int(position_raw),
                    "role_order",
                    "fallback_text_command",
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
            roles = RoleManagementService.get_user_roles(str(resolved["provider"]), str(resolved["provider_user_id"]))
            if not roles:
                await message.answer("📭 У пользователя нет ролей.")
                return
            lines = [f"🧾 Роли пользователя {resolved['label']}:"]
            for role in roles:
                lines.append(f"• {role['name']} ({role['category']})")
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
            if subcommand == "user_grant":
                role_info = RoleManagementService.get_role(role_name)
                category = role_info.get("category_name") if role_info else None
                ok = RoleManagementService.assign_user_role(
                    str(resolved["provider"]),
                    str(resolved["provider_user_id"]),
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
                ok = RoleManagementService.revoke_user_role(
                    str(resolved["provider"]),
                    str(resolved["provider_user_id"]),
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
        if not callback.data or not callback.from_user or not callback.message:
            await callback.answer("Некорректный callback", show_alert=True)
            return

        parts = callback.data.split(":")
        if len(parts) < 3:
            await callback.answer("Некорректный callback", show_alert=True)
            return

        owner_id = int(parts[1]) if parts[1].isdigit() else 0
        actor_can_manage_categories = _can_manage_categories("telegram", str(callback.from_user.id))
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
            await _safe_edit_message_text(callback, 
                _render_help_text(),
                parse_mode="HTML",
                reply_markup=_build_home_keyboard(owner_id),
            )
            await callback.answer()
            return

        if action == "fallback":
            logger.info("roles_admin fallback opened actor_id=%s", callback.from_user.id)
            await _safe_edit_message_text(
                callback,
                _render_fallback_text(),
                parse_mode="HTML",
                reply_markup=_build_home_keyboard(owner_id),
            )
            await callback.answer()
            return

        if action == "home":
            await _safe_edit_message_text(callback, 
                _render_home_text(),
                parse_mode="HTML",
                reply_markup=_build_home_keyboard(owner_id),
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
            await _safe_edit_message_text(callback, 
                _render_actions_text(),
                parse_mode="HTML",
                reply_markup=_build_actions_keyboard(owner_id),
            )
            await callback.answer()
            return

        if action == "start":
            operation = parts[3] if len(parts) > 3 else ""
            if operation.startswith("category_") and not actor_can_manage_categories:
                await callback.answer("Категориями может управлять только Глава клуба или Главный вице.", show_alert=True)
                return
            button_ops = {"category_order", "category_delete", "role_move", "role_order", "role_delete"}
            if operation in {"category_order", "category_delete"}:
                await _safe_edit_message_text(callback, 
                    "Выберите категорию:",
                    reply_markup=_build_pick_category_keyboard(grouped, owner_id, operation),
                )
                await callback.answer()
                return
            if operation in {"role_move", "role_order", "role_delete"}:
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
                grouped_after = RoleManagementService.list_roles_grouped() or []
                await _safe_edit_message_text(callback, _render_actions_text(), parse_mode="HTML", reply_markup=_build_actions_keyboard(owner_id))
                return
            if operation == "category_order":
                _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                    operation="category_order_pick_position",
                    created_at=time.time(),
                    payload={"category": category_name},
                )
                await _safe_edit_message_text(callback, 
                    f"Выбрана категория: <b>{category_name}</b>\nВыберите новую позицию:",
                    parse_mode="HTML",
                    reply_markup=_build_position_choice_keyboard(owner_id, "category_order"),
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
                    logger.warning(
                        "roles_admin pending role target denied role missing from canonical catalog actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s operation=%s source=%s",
                        callback.from_user.id if callback.from_user else None,
                        None,
                        callback.from_user.id if callback.from_user else None,
                        pending.payload["role"],
                        category_name,
                        "role_move" if operation == "role_move_target" else "role_order",
                        "button",
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
                await _safe_edit_message_text(callback, 
                    f"Роль: <b>{pending.payload['role']}</b>\nКатегория: <b>{category_name}</b>\nВыберите позицию:",
                    parse_mode="HTML",
                    reply_markup=_build_position_choice_keyboard(owner_id, "role_position"),
                )
                await callback.answer()
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
                await _safe_edit_message_text(callback, _render_actions_text(), parse_mode="HTML", reply_markup=_build_actions_keyboard(owner_id))
                return
            if operation in {"role_move", "role_order"}:
                available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
                if role_name not in available_roles:
                    logger.warning(
                        "roles_admin pick_role denied role missing from canonical catalog actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s operation=%s source=%s",
                        callback.from_user.id if callback.from_user else None,
                        None,
                        callback.from_user.id if callback.from_user else None,
                        role_name,
                        flattened[item_index].get("category"),
                        operation,
                        "button",
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
                new_pos = 0 if value == "start" else max(len(grouped) - 1, 0)
                ok = RoleManagementService.create_category(category_name, new_pos)
                _PENDING_ACTIONS.pop(callback.from_user.id, None)
                await callback.answer("Порядок категории обновлён" if ok else "Не удалось обновить порядок", show_alert=not ok)
                await _safe_edit_message_text(callback, _render_actions_text(), parse_mode="HTML", reply_markup=_build_actions_keyboard(owner_id))
                return
            if op == "role_position":
                if not pending or pending.operation != "role_pick_position" or not pending.payload:
                    await callback.answer("Сессия устарела, начните заново", show_alert=True)
                    return
                role_name = pending.payload.get("role", "")
                category_name = pending.payload.get("category", "")
                available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
                if role_name not in available_roles:
                    logger.warning(
                        "roles_admin role_position denied role missing from canonical catalog actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s operation=%s source=%s",
                        callback.from_user.id if callback.from_user else None,
                        None,
                        callback.from_user.id if callback.from_user else None,
                        role_name,
                        category_name,
                        pending.payload.get("mode") or "role_move",
                        "button",
                    )
                    _PENDING_ACTIONS.pop(callback.from_user.id, None)
                    await callback.answer("Роль не найдена в каталоге", show_alert=True)
                    await callback.message.reply(_canonical_role_missing_message())
                    return
                category_item = next((item for item in grouped if str(item.get("category")) == category_name), None)
                total_roles = len((category_item or {}).get("roles", []))
                new_pos = 0 if value == "start" else total_roles
                ok = RoleManagementService.move_role(role_name, category_name, new_pos)
                _PENDING_ACTIONS.pop(callback.from_user.id, None)
                if not ok:
                    logger.warning(
                        "roles_admin role_position failed actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s position=%s operation=%s source=%s",
                        callback.from_user.id if callback.from_user else None,
                        None,
                        callback.from_user.id if callback.from_user else None,
                        role_name,
                        category_name,
                        new_pos,
                        pending.payload.get("mode") or "role_move",
                        "button",
                    )
                await callback.answer(
                    "Позиция роли обновлена" if ok else "Не удалось обновить позицию роли",
                    show_alert=not ok,
                )
                await _safe_edit_message_text(callback, _render_actions_text(), parse_mode="HTML", reply_markup=_build_actions_keyboard(owner_id))
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
                    suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
                    external_note = " — внешняя Discord-роль, удаление скрыто" if role.get("is_discord_managed") else ""
                    lines.append(f"\n{idx}. {role['name']}{suffix}{external_note}")
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
                        suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
                        external_note = " — внешняя Discord-роль, удаление скрыто" if role.get("is_discord_managed") else ""
                        lines.append(f"\n{idx}. {role['name']}{suffix}{external_note}")
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
    _persist_telegram_identity_from_user(message.from_user)
    if message.reply_to_message:
        _persist_telegram_identity_from_user(message.reply_to_message.from_user)
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
                await message.answer("❌ Формат: Роль | Категория | discord_role_id(опц) | position(опц)")
                return
            discord_role_id = args[2] if len(args) > 2 else None
            pos = int(args[3]) if len(args) > 3 and args[3].lstrip("-").isdigit() else 0
            ok = RoleManagementService.create_role(args[0], args[1], discord_role_id=discord_role_id, position=pos)
            await message.answer("✅ Роль создана." if ok else "❌ Не удалось создать роль (смотри логи).")
        elif op == "role_move":
            if len(args) < 2:
                await message.answer("❌ Формат: Роль | Категория | position(опц)")
                return
            available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
            if args[0] not in available_roles:
                logger.warning(
                    "roles_admin role_move denied role missing from canonical catalog actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s operation=%s source=%s",
                    message.from_user.id if message.from_user else None,
                    None,
                    message.from_user.id if message.from_user else None,
                    args[0],
                    args[1],
                    "role_move",
                    "button",
                )
                await message.answer(_canonical_role_missing_message())
                return
            pos = int(args[2]) if len(args) > 2 and args[2].lstrip("-").isdigit() else 0
            ok = RoleManagementService.move_role(args[0], args[1], pos)
            if not ok:
                logger.warning(
                    "roles_admin role_move failed actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s position=%s operation=%s source=%s",
                    message.from_user.id if message.from_user else None,
                    None,
                    message.from_user.id if message.from_user else None,
                    args[0],
                    args[1],
                    pos,
                    "role_move",
                    "button",
                )
            await message.answer("✅ Роль перемещена." if ok else "❌ Не удалось переместить роль. Проверь синхронизацию каталога и логи.")
        elif op == "role_order":
            if len(args) < 3 or not args[2].lstrip("-").isdigit():
                await message.answer("❌ Формат: Роль | Категория | position")
                return
            available_roles = {item["role"] for item in RoleManagementService.list_roles_available_for_admin_reorder()}
            if args[0] not in available_roles:
                logger.warning(
                    "roles_admin role_order denied role missing from canonical catalog actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s operation=%s source=%s",
                    message.from_user.id if message.from_user else None,
                    None,
                    message.from_user.id if message.from_user else None,
                    args[0],
                    args[1],
                    "role_order",
                    "button",
                )
                await message.answer(_canonical_role_missing_message())
                return
            ok = RoleManagementService.move_role(args[0], args[1], int(args[2]))
            if not ok:
                logger.warning(
                    "roles_admin role_order failed actor_id=%s guild_id=%s telegram_user_id=%s role_name=%s category=%s position=%s operation=%s source=%s",
                    message.from_user.id if message.from_user else None,
                    None,
                    message.from_user.id if message.from_user else None,
                    args[0],
                    args[1],
                    int(args[2]),
                    "role_order",
                    "button",
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
            roles = RoleManagementService.get_user_roles(str(resolved["provider"]), str(resolved["provider_user_id"]))
            if not roles:
                await message.answer("📭 У пользователя нет ролей.")
            else:
                lines = [f"🧾 Роли пользователя {resolved['label']}:"]
                for role in roles:
                    lines.append(f"• {role['name']} ({role['category']})")
                await message.answer("\n".join(lines))
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
            if op == "user_grant":
                role_info = RoleManagementService.get_role(role_name)
                category = role_info.get("category_name") if role_info else None
                ok = RoleManagementService.assign_user_role(
                    str(resolved["provider"]),
                    str(resolved["provider_user_id"]),
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
                ok = RoleManagementService.revoke_user_role(
                    str(resolved["provider"]),
                    str(resolved["provider_user_id"]),
                    role_name,
                )
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
        _PENDING_ACTIONS.pop(message.from_user.id, None)
