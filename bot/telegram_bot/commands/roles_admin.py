"""
Назначение: модуль "roles admin" реализует продуктовый контур в зоне Telegram.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Telegram.
Пользовательский вход: команда /roles_admin и связанный пользовательский сценарий.
"""

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
    PRIVILEGED_DISCORD_ROLE_MESSAGE,
    ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE,
    ROLE_ASSIGNMENT_REASON_SYNC_ONLY_DISCORD_ROLE,
    SYNC_ONLY_DISCORD_ROLE_MESSAGE,
    USER_ACQUIRE_HINT_PLACEHOLDER,
)

logger = logging.getLogger(__name__)
router = Router()

_ROLES_PAGE_SIZE = 5
_MAX_ROLE_BUTTONS = 8
_PENDING_TTL_SECONDS = 300
_TELEGRAM_MESSAGE_LIMIT = 4096
_LIST_ROLE_DESCRIPTION_LIMIT = 180
_LIST_ROLE_ACQUIRE_HINT_LIMIT = 180
_DISCORD_CATALOG_SYNC_MIN_INTERVAL_SECONDS = 45
_SECTION_LABELS = {
    "categories": "Категории",
    "roles": "Роли",
    "users": "Пользователи",
}
_SECTION_OPERATIONS = {
    "categories": ("category_create", "category_order", "category_delete"),
    "roles": ("role_create", "role_edit_acquire_hint", "role_edit_sellable", "role_move", "role_order", "role_delete"),
    "users": ("user_roles", "user_grant", "user_revoke"),
}
_SHOP_ADMIN_CATEGORY_ACTIONS: tuple[tuple[str, str], ...] = (
    ("category_create", "🗂 Создать/обновить категорию"),
    ("category_order", "↕️ Изменить порядок категории"),
    ("category_delete", "🗑 Удалить категорию"),
)


@dataclass
class PendingRolesAdminAction:
    operation: str
    created_at: float
    payload: dict[str, Any] | None = None


_PENDING_ACTIONS: dict[int, PendingRolesAdminAction] = {}
_LAST_DISCORD_CATALOG_SYNC_AT: float | None = None


@dataclass(frozen=True)
class RolesAdminVisibilityContext:
    actor_level: int
    actor_titles: tuple[str, ...]
    can_manage_categories: bool
    hidden_sections: tuple[str, ...]
    can_manage_shop_settings: bool = False


def _role_catalog_note() -> str:
    return (
        "Команды списка и изменения ролей стараются автоматически подтягивать актуальные Discord-роли в "
        "канонический каталог <code>roles</code>. Внешние Discord-роли можно <code>move/order</code>, "
        "но нельзя <code>delete</code>. Если роль не видна, обнови экран, при необходимости запусти "
        "<code>/rolesadmin</code> в Discord и в панели нажми обновление каталога (кнопочный сценарий), "
        "затем проверь консольные логи."
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
        "Сначала дождись синхронизации Discord-ролей или открой `/rolesadmin` в Discord и обнови каталог через кнопки панели, потом попробуй ещё раз."
    )


def _role_assignment_error_message(result: dict[str, object], *, default_message: str) -> str:
    if result.get("reason") == ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE:
        return f"❌ {result.get('message') or PRIVILEGED_DISCORD_ROLE_MESSAGE}"
    if result.get("reason") == ROLE_ASSIGNMENT_REASON_SYNC_ONLY_DISCORD_ROLE:
        return f"❌ {result.get('message') or SYNC_ONLY_DISCORD_ROLE_MESSAGE}"
    return default_message


def _telegram_user_lookup_hint() -> str:
    return (
        "Порядок такой: в Telegram ЛС используй @username / username, в Telegram группе — reply, "
        "в Discord — mention / username / display_name. Для явного провайдера можно указать "
        "tg:@username или ds:username. ID оставь только как резерв."
    )


async def _sync_linked_discord_role(
    target: dict[str, str],
    role_name: str,
    *,
    revoke: bool,
    source: str = "telegram_command",
) -> dict[str, str | bool]:
    try:
        provider = str(target.get("provider") or "").strip()
        provider_user_id = str(target.get("provider_user_id") or "").strip()
        account_id = str(target.get("account_id") or "").strip() or AccountsService.resolve_account_id(provider, provider_user_id)
        if not account_id or not db.supabase:
            return {"synced": False, "reason": "account_or_db_unavailable"}
        role_info = RoleManagementService.get_role(role_name)
        discord_role_id = str((role_info or {}).get("discord_role_id") or "").strip()
        if not discord_role_id:
            return {"synced": False, "reason": "role_without_discord_binding"}
        identity_resp = (
            db.supabase.table("account_identities")
            .select("provider_user_id")
            .eq("account_id", str(account_id))
            .eq("provider", "discord")
            .limit(1)
            .execute()
        )
        if not identity_resp.data:
            logger.info(
                "telegram roles_admin discord sync skipped: account has no discord identity account_id=%s provider=%s provider_user_id=%s role=%s revoke=%s source=%s",
                account_id,
                provider,
                provider_user_id,
                role_name,
                revoke,
                source,
            )
            return {"synced": False, "reason": "discord_not_linked"}
        discord_user_id = int(identity_resp.data[0].get("provider_user_id") or 0)
        if not discord_user_id:
            logger.warning(
                "telegram roles_admin discord sync skipped invalid discord identity account_id=%s provider=%s provider_user_id=%s role=%s revoke=%s source=%s",
                account_id,
                provider,
                provider_user_id,
                role_name,
                revoke,
                source,
            )
            return {"synced": False, "reason": "invalid_discord_identity"}

        from bot.commands.base import bot as discord_bot

        if not getattr(discord_bot, "guilds", None):
            logger.warning(
                "discord sync skipped: bot guilds unavailable account_id=%s provider=%s provider_user_id=%s",
                account_id,
                provider,
                provider_user_id,
            )
            RoleManagementService.record_role_change_audit(
                action="discord_role_sync_conflict",
                role_name=role_name,
                source=source,
                target_provider=provider or None,
                target_user_id=provider_user_id or None,
                target_account_id=account_id,
                before={"db_applied": True, "discord_role_id": discord_role_id},
                after={"discord_synced": False, "revoke": revoke},
                status="conflict",
                error_code="discord_bot_unavailable",
                error_message="discord bot guilds unavailable for telegram sync",
            )
            return {"synced": False, "reason": "discord_bot_unavailable"}
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
                RoleManagementService.record_role_change_audit(
                    action="discord_role_sync_conflict",
                    role_name=role_name,
                    source=source,
                    target_provider=provider or None,
                    target_user_id=provider_user_id or None,
                    target_account_id=account_id,
                    before={"db_applied": True, "discord_role_id": discord_role_id, "discord_user_id": discord_user_id},
                    after={"discord_synced": False, "revoke": revoke, "guild_id": str(guild.id)},
                    status="conflict",
                    error_code="discord_sync_failed",
                    error_message="telegram-triggered discord role sync failed",
                )
                return {"synced": False, "reason": "discord_sync_failed"}
            return {"synced": True, "reason": "ok"}
        logger.warning(
            "telegram roles_admin discord sync target not found account_id=%s provider=%s provider_user_id=%s discord_user_id=%s role_id=%s revoke=%s",
            account_id,
            provider,
            provider_user_id,
            discord_user_id,
            discord_role_id,
            revoke,
        )
        RoleManagementService.record_role_change_audit(
            action="discord_role_sync_conflict",
            role_name=role_name,
            source=source,
            target_provider=provider or None,
            target_user_id=provider_user_id or None,
            target_account_id=account_id,
            before={"db_applied": True, "discord_role_id": discord_role_id, "discord_user_id": discord_user_id},
            after={"discord_synced": False, "revoke": revoke},
            status="conflict",
            error_code="discord_target_not_found",
            error_message="linked discord member or role not found for sync",
        )
        return {"synced": False, "reason": "discord_target_not_found"}
    except Exception:
        logger.exception(
            "telegram roles_admin discord sync crashed provider=%s provider_user_id=%s account_id=%s role=%s revoke=%s",
            target.get("provider"),
            target.get("provider_user_id"),
            target.get("account_id"),
            role_name,
            revoke,
        )
        RoleManagementService.record_role_change_audit(
            action="discord_role_sync_conflict",
            role_name=role_name,
            source=source,
            target_provider=str(target.get("provider") or "").strip() or None,
            target_user_id=str(target.get("provider_user_id") or "").strip() or None,
            target_account_id=str(target.get("account_id") or "").strip() or None,
            before={"db_applied": True},
            after={"discord_synced": False, "revoke": revoke},
            status="error",
            error_code="discord_sync_crashed",
            error_message="telegram-triggered discord role sync crashed",
        )
        return {"synced": False, "reason": "discord_sync_crashed"}


def _discord_sync_status_note(sync_result: dict[str, str | bool] | None) -> str:
    if not sync_result:
        return ""
    if bool(sync_result.get("synced")):
        return "\nℹ️ Discord-синхронизация выполнена."
    reason = str(sync_result.get("reason") or "").strip()
    if reason == "discord_not_linked":
        return "\nℹ️ Роль сохранена в боте, но в Discord не выдана: у аккаунта нет привязки Discord."
    if reason == "discord_target_not_found":
        return "\nℹ️ Роль сохранена в боте, но в Discord не выдана: пользователь/роль не найдены на сервере."
    if reason in {"discord_bot_unavailable", "discord_sync_failed", "discord_sync_crashed"}:
        return "\nℹ️ Роль сохранена в боте, но Discord-синхронизация завершилась с ошибкой (смотри логи)."
    return ""


def _should_skip_implicit_discord_catalog_sync(*, force: bool = False) -> tuple[bool, float]:
    global _LAST_DISCORD_CATALOG_SYNC_AT
    now = time.monotonic()
    if force or _LAST_DISCORD_CATALOG_SYNC_AT is None:
        return False, 0.0
    elapsed = now - _LAST_DISCORD_CATALOG_SYNC_AT
    if elapsed < _DISCORD_CATALOG_SYNC_MIN_INTERVAL_SECONDS:
        return True, elapsed
    return False, elapsed


async def _sync_discord_roles_catalog(*, force: bool = False, trigger: str = "implicit") -> bool:
    """Sync live Discord guild roles into local catalog for Telegram role operations."""
    try:
        global _LAST_DISCORD_CATALOG_SYNC_AT

        skip_sync, elapsed = _should_skip_implicit_discord_catalog_sync(force=force)
        if skip_sync:
            logger.info(
                "telegram roles_admin discord catalog sync skipped trigger=%s reason=recent_sync elapsed_sec=%.3f min_interval_sec=%s",
                trigger,
                elapsed,
                _DISCORD_CATALOG_SYNC_MIN_INTERVAL_SECONDS,
            )
            return False

        from bot.commands.base import bot as discord_bot

        guilds = list(getattr(discord_bot, "guilds", []) or [])
        if not guilds:
            logger.warning("telegram roles_admin discord catalog sync skipped: no guilds attached")
            return False

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
            return False

        result = RoleManagementService.sync_discord_guild_roles(guild_roles)
        _LAST_DISCORD_CATALOG_SYNC_AT = time.monotonic()
        logger.info(
            "telegram roles_admin discord catalog sync completed trigger=%s guild_count=%s roles=%s upserted=%s removed=%s",
            trigger,
            len(guilds),
            len(guild_roles),
            result.get("upserted", 0),
            result.get("removed", 0),
        )
        return True
    except Exception:
        logger.exception("telegram roles_admin discord catalog sync crashed trigger=%s", trigger)
        return False

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
    can_manage_shop_settings = AuthorityService.is_super_admin(provider, provider_user_id)
    hidden_sections = tuple(section for section in ("categories",) if not can_manage_categories)
    return RolesAdminVisibilityContext(
        actor_level=authority.level,
        actor_titles=tuple(authority.titles),
        can_manage_categories=can_manage_categories,
        hidden_sections=hidden_sections,
        can_manage_shop_settings=can_manage_shop_settings,
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
                [InlineKeyboardButton(text="🛒 Продажа в магазине", callback_data=f"roles_admin:{actor_id}:start:role_edit_sellable")],
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
        "Нажми кнопку и следуй шагам на экране.\n"
        "Если передумал — отправь: <code>отмена</code>.\n"
        "Если экран не обновился — нажми «🔄 Обновить» в главном меню."
    )
    if section == "categories":
        return (
            "🗂 <b>Раздел «Категории»</b>\n\n"
            f"{hidden_note}"
            "Здесь можно создать, переименовать и удалить группы ролей.\n"
            "Начни с этого раздела, если добавляешь новые роли.\n\n"
            f"{common_tail}"
        )
    if section == "roles":
        return (
            "🪪 <b>Раздел «Роли»</b>\n\n"
            f"{hidden_note}"
            "Здесь создаются и настраиваются роли.\n"
            "После создания роли заполни «Описание» и «Как получить».\n\n"
            f"{common_tail}"
        )
    if section == "users":
        return (
            "👥 <b>Раздел «Пользователи»</b>\n\n"
            f"{hidden_note}"
            "Здесь можно посмотреть роли человека, выдать роль или снять роль.\n"
            "Сначала выбери человека, потом действие.\n\n"
            f"{common_tail}"
        )
    return (
        "⚡ <b>Действия кнопками</b>\n\n"
        f"{hidden_note}"
        "Выбери раздел: Категории, Роли или Пользователи.\n"
        "Внутри каждого раздела будут только нужные кнопки.\n\n"
        f"{common_tail}"
    )


def _operation_hint(operation: str) -> str:
    hints = {
        "category_create": "Отправь: <code>Название категории | позиция (необязательно)</code>",
        "category_order": "Отправь: <code>Название категории | позиция</code>",
        "category_delete": "Отправь: <code>Название категории</code>",
        "role_create": "Сначала выбери категорию. Потом отправь: <code>Название роли | Описание | Как получить (необязательно) | ID роли Discord (необязательно) | продаётся/не продаётся (необязательно) | позиция (необязательно)</code>.",
        "role_create_enter_name": "Отправь: <code>Название роли | Описание | Как получить (необязательно) | ID роли Discord (необязательно) | продаётся/не продаётся (необязательно) | позиция (необязательно)</code>.",
        "role_create_new_category_name": "Отправь только <code>Название новой категории</code>. После этого бот сразу попросит параметры роли в этой категории.",
        "role_edit_description": "Отправь: <code>Название роли | Описание</code>. Так роль будет понятнее пользователям прямо в интерфейсе.",
        "role_edit_acquire_hint": "Отправь: <code>Название роли | Как получить</code>. Пиши коротко и понятно: через активность, выдачу админа, турнир, заявку и т.д.",
        "role_edit_sellable": "Отправь: <code>Название роли | продаётся</code> или <code>Название роли | не продаётся</code>.",
        "role_move": "Отправь: <code>Название роли | Категория | позиция (необязательно)</code>.",
        "role_order": "Отправь: <code>Название роли | Категория | позиция</code>.",
        "role_delete": "Отправь: <code>Название роли</code>. Внешние Discord-роли удалить нельзя.",
        "user_roles": "Отправь: <code>@username</code> / <code>username</code> / <code>tg:@username</code> / <code>ds:username</code>. В группе удобнее reply.",
        "user_grant": "Укажи человека и роль в формате: <code>@username | Название роли</code>. В группе можно ответить на сообщение и отправить только <code>Название роли</code>.",
        "user_revoke": "Укажи человека и роль в формате: <code>@username | Название роли</code>. В группе можно ответить на сообщение и отправить только <code>Название роли</code>.",
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


def _parse_sellable_choice(value: str | None) -> bool | None:
    token = str(value or "").strip().lower()
    if not token:
        return None
    if token in {"yes", "true", "1", "sellable", "on", "продается", "продаётся"}:
        return True
    if token in {"no", "false", "0", "not_sellable", "off", "непродается", "непродаётся", "не_продается", "не_продаётся"}:
        return False
    return None


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
    is_sellable = None
    if len(extras) >= 2:
        acquire_hint = extras[0] or None
        discord_role_id = extras[1] or None
        if len(extras) >= 3:
            is_sellable = _parse_sellable_choice(extras[2])
    elif len(extras) == 1:
        parsed_sellable = _parse_sellable_choice(extras[0])
        if parsed_sellable is not None:
            is_sellable = parsed_sellable
        elif _looks_like_discord_role_id(extras[0]):
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
        "is_sellable": is_sellable,
    }


def _parse_role_create_selected_category_args(args: list[str], *, category: str) -> dict[str, Any]:
    role_name = args[0] if args else ""
    description = args[1] if len(args) > 1 else None
    extras = list(args[2:]) if len(args) > 2 else []

    position = None
    if extras and str(extras[-1]).lstrip("-").isdigit():
        position = int(str(extras.pop()))

    acquire_hint = None
    discord_role_id = None
    is_sellable = None
    if len(extras) >= 2:
        acquire_hint = extras[0] or None
        discord_role_id = extras[1] or None
        if len(extras) >= 3:
            is_sellable = _parse_sellable_choice(extras[2])
    elif len(extras) == 1:
        parsed_sellable = _parse_sellable_choice(extras[0])
        if parsed_sellable is not None:
            is_sellable = parsed_sellable
        elif _looks_like_discord_role_id(extras[0]):
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
        "is_sellable": is_sellable,
    }


def _log_role_create_category_selection(
    *,
    actor_id: int | None,
    category: str,
    source: str,
    created_new: bool = False,
) -> None:
    logger.info(
        "roles_admin role_create category selected actor_id=%s category=%s created_new=%s source=%s",
        actor_id,
        category,
        created_new,
        source,
    )


def _format_role_line(role: dict[str, object], *, numbered: int | None = None) -> str:
    prefix = f"{numbered}. " if numbered is not None else "• "
    suffix = f" (Discord ID: {role['discord_role_id']})" if role.get("discord_role_id") else ""
    external_note = " — внешняя Discord-роль, удаление скрыто" if role.get("is_discord_managed") else ""
    description = str(role.get("description") or "").strip()
    acquire_hint = str(role.get("acquire_hint") or "").strip() or USER_ACQUIRE_HINT_PLACEHOLDER
    description_note = f"\n   ↳ Описание: {description}" if description else ""
    acquire_hint_note = f"\n   ↳ Как получить: {acquire_hint}"
    return f"{prefix}{role['name']}{suffix}{external_note}{description_note}{acquire_hint_note}"


def _truncate_plain_text(value: str, limit: int) -> str:
    text = str(value or "").strip()
    if limit <= 0 or len(text) <= limit:
        return text
    if limit <= 1:
        return "…"
    return text[: limit - 1].rstrip() + "…"


def _format_role_line_for_list(role: dict[str, object]) -> str:
    trimmed_role = dict(role)
    trimmed_role["description"] = _truncate_plain_text(
        str(role.get("description") or ""),
        _LIST_ROLE_DESCRIPTION_LIMIT,
    )
    acquire_hint = str(role.get("acquire_hint") or "").strip() or USER_ACQUIRE_HINT_PLACEHOLDER
    trimmed_role["acquire_hint"] = _truncate_plain_text(acquire_hint, _LIST_ROLE_ACQUIRE_HINT_LIMIT)
    return _format_role_line(trimmed_role)


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


def _build_pick_category_keyboard(
    grouped: list[dict],
    actor_id: int,
    operation: str,
    *,
    allow_create_new: bool = False,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(grouped[:20]):
        rows.append([
            InlineKeyboardButton(
                text=f"📂 {item['category']}"[:64],
                callback_data=f"roles_admin:{actor_id}:pick_category:{operation}:{idx}",
            )
        ])
    if allow_create_new and operation == "role_create":
        rows.append(
            [
                InlineKeyboardButton(
                    text="🆕 Создать новую категорию и продолжить",
                    callback_data=f"roles_admin:{actor_id}:role_create_new_category",
                )
            ]
        )
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


def _build_shop_admin_action_keyboard(actor_id: int, selected_category: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text=label,
                callback_data=f"roles_admin:{actor_id}:shop_settings_action:{action}:{selected_category[:48]}",
            )
        ]
        for action, label in _SHOP_ADMIN_CATEGORY_ACTIONS
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Назад к категориям", callback_data=f"roles_admin:{actor_id}:actions:categories")])
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
        "Единый сценарий: <b>одна команда → кнопки → результат</b>.\n"
        "1) Открой «Категории», если нужно подготовить структуру.\n"
        "2) Открой «Роли», чтобы создать или изменить роль.\n"
        "3) Открой «Пользователи», чтобы выдать или снять роль.\n\n"
        "Если сообщение устарело, нажми «🔄 Обновить».\n"
        "Подробности по шагам есть в «ℹ️ Что делает каждая функция»."
    )



def _render_command_alias_note() -> str:
    return (
        "<b>Паритет команд:</b> Telegram — <code>/roles_admin</code> (alias <code>/rolesadmin</code>), Discord — <code>/rolesadmin</code>. "
        "На обеих платформах вход одинаковый: открой команду и работай кнопками.\n\n"
    )


def _render_fallback_text() -> str:
    return (
        "🆘 <b>Не работают кнопки?</b>\n\n"
        "Подкоманды отключены.\n"
        "Открой /roles_admin и используй кнопки.\n\n"
        "Если кнопка не отвечает:\n"
        "• Нажми «🔄 Обновить».\n"
        "• Отправь /roles_admin ещё раз.\n"
        "• Если проблема осталась — передай администратору время и скрин этого экрана."
    )





def _render_help_text() -> str:
    return (
        "ℹ️ <b>Что делает /roles_admin</b>\n\n"
        "Это экран админа ролей.\n"
        "Главное правило: используйте кнопки внутри панели.\n\n"
        "<b>Порядок работы</b>\n"
        "1) Категории → подготовить структуру.\n"
        "2) Роли → создать или изменить роль.\n"
        "3) Пользователи → выдать или снять роль.\n\n"
        "Подкоманды в сообщении больше не используются.\n"
        "Если что-то пошло не так, нажмите «🔄 Обновить» и снова откройте /roles_admin.\n\n"
        "<b>Кнопки в панели</b>\n"
        "• 'Категории и роли' — просмотр списка и переход по категориям.\n"
        "• Внутри категории доступны кнопки удаления категории/ролей.\n"
        "• Все экраны обновляются в одном сообщении без спама."
    )



async def _safe_callback_answer(
    callback: CallbackQuery,
    text: str | None = None,
    *,
    show_alert: bool = False,
) -> None:
    actor_id = callback.from_user.id if callback.from_user else None
    try:
        await callback.answer(text, show_alert=show_alert)
    except TelegramBadRequest as error:
        error_text = str(error)
        error_text_lower = error_text.lower()
        if "query is too old" in error_text_lower or "query id is invalid" in error_text_lower:
            logger.warning(
                "roles_admin callback answer skipped expired query callback_data=%s actor_id=%s text=%s show_alert=%s error=%s",
                callback.data,
                actor_id,
                text,
                show_alert,
                error_text,
            )
            return
        logger.exception(
            "roles_admin callback answer bad request callback_data=%s actor_id=%s text=%s show_alert=%s error=%s",
            callback.data,
            actor_id,
            text,
            show_alert,
            error_text,
        )
    except Exception:
        logger.exception(
            "roles_admin callback answer failed callback_data=%s actor_id=%s text=%s show_alert=%s",
            callback.data,
            actor_id,
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
        if "message_too_long" in str(error).lower():
            text = args[0] if args else kwargs.get("text")
            logger.error(
                "roles_admin edit rejected by telegram: message too long callback_data=%s actor_id=%s text_len=%s preview=%r",
                callback.data,
                callback.from_user.id if callback.from_user else None,
                len(str(text or "")),
                str(text or "")[:500],
            )
        raise

def _render_list_text(grouped: list[dict], page: int) -> str:
    safe_page = _normalize_page(page, len(grouped), _ROLES_PAGE_SIZE)
    start = safe_page * _ROLES_PAGE_SIZE
    page_items = grouped[start : start + _ROLES_PAGE_SIZE]
    total_pages = max((len(grouped) - 1) // _ROLES_PAGE_SIZE + 1, 1)

    lines = [f"🧩 <b>Роли по категориям</b> (стр. {safe_page + 1}/{total_pages})"]
    footer = "\nНажми на категорию ниже для действий (удаление категории/ролей)."
    reserved_tail = len(footer) + 128
    if not page_items:
        lines.append("\n📭 Категории отсутствуют.")
    for item in page_items:
        category_lines = [f"\n• <i>{item['category']}</i>"]
        roles = item.get("roles", [])
        if not roles:
            category_lines.append("  • —")
            candidate = "\n".join(lines + category_lines) + footer
            if len(candidate) <= _TELEGRAM_MESSAGE_LIMIT:
                lines.extend(category_lines)
            continue
        omitted_roles = 0
        for role in roles:
            role_line = "  " + _format_role_line_for_list(role)
            candidate_lines = category_lines + [role_line]
            candidate = "\n".join(lines + candidate_lines) + footer
            if len(candidate) > (_TELEGRAM_MESSAGE_LIMIT - reserved_tail):
                omitted_roles = len(roles) - (len(category_lines) - 1)
                break
            category_lines.append(role_line)

        if omitted_roles > 0:
            summary_line = (
                f"  …ещё {omitted_roles} ролей скрыто в списке, "
                "открой категорию кнопкой ниже для полного просмотра и действий."
            )
            summary_candidate = "\n".join(lines + category_lines + [summary_line]) + footer
            if len(summary_candidate) <= _TELEGRAM_MESSAGE_LIMIT:
                category_lines.append(summary_line)
            logger.warning(
                "roles_admin list text truncated actor_page=%s category=%s total_roles=%s rendered_roles=%s hidden_roles=%s",
                safe_page,
                item.get("category"),
                len(roles),
                len(category_lines) - 1,
                omitted_roles,
            )

        candidate = "\n".join(lines + category_lines) + footer
        if len(candidate) > _TELEGRAM_MESSAGE_LIMIT:
            logger.warning(
                "roles_admin list text reached telegram limit before category render page=%s category=%s current_len=%s",
                safe_page,
                item.get("category"),
                len("\n".join(lines)),
            )
            break
        lines.extend(category_lines)

    lines.append(footer)
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
        logger.warning(
            "roles_admin access denied actor_id=%s source=%s",
            message.from_user.id,
            "telegram_command",
        )
        RoleManagementService.record_role_change_audit(
            action="rolesadmin_access_denied",
            role_name="*",
            source="telegram_command",
            actor_provider="telegram",
            actor_user_id=str(message.from_user.id),
            before={"command": "roles_admin"},
            after={"allowed": False},
            status="denied",
            error_code="forbidden_role_manage",
            error_message="insufficient permissions for roles_admin",
        )
        await message.answer(
            "❌ Недостаточно прав для управления ролями.\n"
            "Что делать сейчас: обратитесь к старшему администратору.\n"
            "Что будет дальше: после выдачи прав откроется панель /roles_admin."
        )
        return False
    return True


def _can_manage_categories(provider: str, provider_user_id: str) -> bool:
    return AuthorityService.can_manage_role_categories(provider, provider_user_id)


def _can_manage_shop_settings(provider: str, provider_user_id: str) -> bool:
    return AuthorityService.is_super_admin(provider, provider_user_id)


@router.message(Command(commands=["roles_admin", "rolesadmin"]))
async def roles_admin_command(message: Message) -> None:
    try:
        persist_telegram_identity_from_user(message.from_user)
        logger.info(
            "ux_screen_open event=ux_screen_open screen=roles_admin provider=telegram actor_user_id=%s chat_id=%s",
            message.from_user.id if message.from_user else None,
            message.chat.id if message.chat else None,
        )
        if message.reply_to_message:
            persist_telegram_identity_from_user(message.reply_to_message.from_user)
        if not await _ensure_roles_admin(message):
            return

        await _sync_discord_roles_catalog(trigger="command_entry")

        text = (message.text or "").strip()
        parts = text.split()
        if len(parts) >= 2:
            logger.warning(
                "roles_admin deprecated_subcommand actor_id=%s chat_id=%s reason=%s message_text=%r",
                message.from_user.id if message.from_user else None,
                message.chat.id if message.chat else None,
                "deprecated_subcommand",
                message.text,
            )
            await message.answer("Подкоманды отключены. Откройте /roles_admin и используйте кнопки.")
            return

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
            await _safe_callback_answer(callback, "Некорректный callback", show_alert=True)
            return

        parts = callback.data.split(":")
        if len(parts) < 3:
            await _safe_callback_answer(callback, "Некорректный callback", show_alert=True)
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
            await _safe_callback_answer(callback, "Эта панель открыта другим администратором.", show_alert=True)
            return

        action = parts[2]
        await _sync_discord_roles_catalog(
            force=action == "home",
            trigger=f"callback:{action}",
        )
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
            await _safe_callback_answer(callback)
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
            await _safe_callback_answer(callback)
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
            await _safe_callback_answer(callback)
            return

        if action == "list":
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            await _safe_edit_message_text(callback, 
                _render_list_text(grouped, page),
                parse_mode="HTML",
                reply_markup=_build_list_keyboard(grouped, owner_id, page),
            )
            await _safe_callback_answer(callback)
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
                await _safe_callback_answer(callback, "Раздел скрыт: категориями может управлять только Глава клуба или Главный вице.", show_alert=True)
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
            await _safe_callback_answer(callback)
            return

        if action == "shop_settings":
            if not visibility.can_manage_shop_settings:
                logger.warning(
                    "shop_admin_denied provider=telegram actor_id=%s reason=not_superadmin source=button",
                    callback.from_user.id,
                )
                await _safe_callback_answer(callback, "Недостаточно прав", show_alert=True)
                return
            logger.info(
                "shop_admin_open provider=telegram actor_id=%s role=superadmin step=category_pick source=button",
                callback.from_user.id,
            )
            shop_grouped = RoleManagementService.list_public_roles_catalog(
                log_context="telegram:roles_admin:shop_settings",
                only_sellable=True,
            ) or []
            if not shop_grouped:
                await _safe_callback_answer(callback, "Нет ролей с продажей в магазине.", show_alert=True)
                return
            await _safe_edit_message_text(
                callback,
                (
                    "⚙️ <b>Настройка магазина</b>\n\n"
                    "Шаг 1/2: выберите категорию.\n"
                    "Шаг 2/2: выберите действие настройки категории."
                ),
                parse_mode="HTML",
                reply_markup=_build_pick_category_keyboard(
                    shop_grouped,
                    owner_id,
                    "shop_settings",
                    allow_create_new=False,
                ),
            )
            await _safe_callback_answer(callback)
            return

        if action == "shop_settings_action":
            if not visibility.can_manage_shop_settings:
                logger.warning(
                    "shop_admin_denied provider=telegram actor_id=%s reason=not_superadmin source=button_action action=%s",
                    callback.from_user.id,
                    parts[3] if len(parts) > 3 else None,
                )
                await _safe_callback_answer(callback, "Недостаточно прав", show_alert=True)
                return
            selected_action = parts[3] if len(parts) > 3 else ""
            selected_category = parts[4] if len(parts) > 4 else ""
            operation_hint = _operation_hint(selected_action)
            _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                operation=selected_action,
                created_at=time.time(),
            )
            await _safe_edit_message_text(
                callback,
                (
                    "⚙️ <b>Настройка магазина</b>\n\n"
                    f"Категория: <b>{selected_category}</b>\n"
                    f"Выбрано действие: <b>{selected_action}</b>\n\n"
                    f"{operation_hint}"
                ),
                parse_mode="HTML",
                reply_markup=_build_actions_keyboard(
                    owner_id,
                    "categories",
                    can_manage_categories=actor_can_manage_categories,
                ),
            )
            await _safe_callback_answer(callback, "Действие выбрано", show_alert=False)
            return

        if action == "start":
            operation = parts[3] if len(parts) > 3 else ""
            if operation.startswith("category_") and not actor_can_manage_categories:
                await _safe_callback_answer(callback, "Категориями может управлять только Глава клуба или Главный вице.", show_alert=True)
                return
            if operation in {"user_grant", "user_revoke"}:
                flow_action = "grant" if operation == "user_grant" else "revoke"
                _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                    operation="user_role_flow_target",
                    created_at=time.time(),
                    payload={"action": flow_action},
                )
                await _safe_callback_answer(callback, "Сначала выберите пользователя", show_alert=True)
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
                    (
                        "Сначала выберите категорию.\n"
                        "После этого бот попросит только параметры роли."
                        if operation == "role_create"
                        else "Выберите категорию:"
                    ),
                    reply_markup=_build_pick_category_keyboard(
                        grouped,
                        owner_id,
                        operation,
                        allow_create_new=actor_can_manage_categories,
                    ),
                )
                await _safe_callback_answer(callback)
                return
            if operation in {"role_move", "role_order", "role_delete", "role_edit_acquire_hint", "role_edit_sellable"}:
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
                    await _safe_callback_answer(callback, empty_message, show_alert=True)
                    return
                await _safe_edit_message_text(callback, 
                    "Выберите роль:",
                    reply_markup=_build_pick_role_keyboard(grouped, owner_id, operation, 0),
                )
                await _safe_callback_answer(callback)
                return
            _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(operation=operation, created_at=time.time())
            await _safe_callback_answer(callback, "Ожидаю ввод параметров", show_alert=True)
            await callback.message.reply(_operation_hint(operation), parse_mode="HTML")
            return

        if action == "pick_role_page":
            operation = parts[3] if len(parts) > 3 else ""
            page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
            flattened_roles = _flatten_roles(grouped)
            if operation == "role_delete":
                flattened_roles = [item for item in flattened_roles if not item.get("is_discord_managed")]
            if not flattened_roles:
                await _safe_callback_answer(callback, 
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
            await _safe_callback_answer(callback)
            return

        if action == "pick_category":
            operation = parts[3] if len(parts) > 3 else ""
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            category_source = grouped
            if operation == "shop_settings":
                category_source = RoleManagementService.list_public_roles_catalog(
                    log_context="telegram:roles_admin:shop_settings:pick",
                    only_sellable=True,
                ) or []
                if not category_source:
                    await _safe_callback_answer(callback, "Нет ролей с продажей в магазине.", show_alert=True)
                    return
            if category_idx < 0 or category_idx >= len(category_source):
                await _safe_callback_answer(callback, "Категория не найдена", show_alert=True)
                return
            category_name = str(category_source[category_idx]["category"])
            if operation == "shop_settings":
                if not visibility.can_manage_shop_settings:
                    logger.warning(
                        "shop_admin_denied provider=telegram actor_id=%s reason=not_superadmin source=category_pick operation=shop_settings_category_pick",
                        callback.from_user.id,
                    )
                    await _safe_callback_answer(callback, "Недостаточно прав", show_alert=True)
                    return
                logger.info(
                    "shop_admin_category_select provider=telegram actor_id=%s category=%s",
                    callback.from_user.id,
                    category_name,
                )
                await _safe_edit_message_text(
                    callback,
                    (
                        "⚙️ <b>Настройка магазина</b>\n\n"
                        f"Шаг 1/2 завершён: <b>{category_name}</b>\n"
                        "Шаг 2/2: выберите действие настройки категории."
                    ),
                    parse_mode="HTML",
                    reply_markup=_build_shop_admin_action_keyboard(owner_id, category_name),
                )
                await _safe_callback_answer(callback)
                return
            if operation == "category_delete":
                ok = RoleManagementService.delete_category(category_name)
                await _safe_callback_answer(callback, "Категория удалена" if ok else "Не удалось удалить категорию", show_alert=not ok)
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
                await _safe_callback_answer(callback)
                return
            if operation == "role_create":
                _log_role_create_category_selection(
                    actor_id=callback.from_user.id if callback.from_user else None,
                    category=category_name,
                    source="button_existing_category",
                )
                _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                    operation="role_create_enter_name",
                    created_at=time.time(),
                    payload={"category": category_name},
                )
                await _safe_edit_message_text(
                    callback,
                    (
                        f"Категория выбрана: <b>{category_name}</b>\n\n"
                        "Теперь отправь: <code>Название роли | Описание | Как получить(опц) | discord_role_id(опц) | sellable/not_sellable(опц) | position(опц)</code>\n"
                        "Сначала идёт название роли, затем описание, способ получения и остальные опциональные параметры."
                    ),
                    parse_mode="HTML",
                    reply_markup=_build_actions_keyboard(
                        owner_id,
                        "roles",
                        can_manage_categories=actor_can_manage_categories,
                    ),
                )
                await _safe_callback_answer(callback)
                return
            if operation in {"role_move_target", "role_order_target"}:
                pending = _PENDING_ACTIONS.get(callback.from_user.id)
                if not pending or not pending.payload or not pending.payload.get("role"):
                    await _safe_callback_answer(callback, "Сессия устарела, начните заново", show_alert=True)
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
                    await _safe_callback_answer(callback, "Роль больше не найдена в каталоге", show_alert=True)
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
            await _safe_callback_answer(callback)
            return

        if action == "user_role_categories":
            flow_action = parts[3] if len(parts) > 3 else ""
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await _safe_callback_answer(callback, "Панель выбора устарела, начните заново.", show_alert=True)
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
            await _safe_callback_answer(callback)
            return

        if action == "user_role_category":
            flow_action = parts[3] if len(parts) > 3 else ""
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await _safe_callback_answer(callback, "Панель выбора устарела, начните заново.", show_alert=True)
                return
            if category_idx < 0 or category_idx >= len(grouped):
                await _safe_callback_answer(callback, "Категория не найдена", show_alert=True)
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
            await _safe_callback_answer(callback)
            return

        if action == "user_role_page":
            flow_action = parts[3] if len(parts) > 3 else ""
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            page = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await _safe_callback_answer(callback, "Панель выбора устарела, начните заново.", show_alert=True)
                return
            if category_idx < 0 or category_idx >= len(grouped):
                await _safe_callback_answer(callback, "Категория не найдена", show_alert=True)
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
            await _safe_callback_answer(callback)
            return

        if action == "user_role_toggle":
            flow_action = parts[3] if len(parts) > 3 else ""
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            page = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0
            role_idx = int(parts[6]) if len(parts) > 6 and parts[6].isdigit() else -1
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await _safe_callback_answer(callback, "Панель выбора устарела, начните заново.", show_alert=True)
                return
            if category_idx < 0 or category_idx >= len(grouped):
                await _safe_callback_answer(callback, "Категория не найдена", show_alert=True)
                return
            category_roles = [
                role
                for role in list(grouped[category_idx].get("roles") or [])
                if str(role.get("name") or "").strip()
            ]
            safe_page = _normalize_page(page, len(category_roles), _MAX_ROLE_BUTTONS)
            item_index = safe_page * _MAX_ROLE_BUTTONS + role_idx
            if role_idx < 0 or item_index >= len(category_roles):
                await _safe_callback_answer(callback, "Роль не найдена", show_alert=True)
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
            await _safe_callback_answer(callback, toast)
            return

        if action == "user_role_clear":
            flow_action = parts[3] if len(parts) > 3 else ""
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await _safe_callback_answer(callback, "Панель выбора устарела, начните заново.", show_alert=True)
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
            await _safe_callback_answer(callback, "Выбор очищен")
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
            await _safe_callback_answer(callback, "Панель выбора закрыта")
            return

        if action == "user_role_apply":
            flow_action = parts[3] if len(parts) > 3 else ""
            pending = _get_user_role_flow_pending(callback.from_user.id if callback.from_user else None)
            if not pending or str((pending.payload or {}).get("action") or "") != flow_action:
                await _safe_callback_answer(callback, "Панель выбора устарела, начните заново.", show_alert=True)
                return
            payload = pending.payload or {}
            selected_roles = _normalize_role_names(payload.get("selected_roles"))
            account_id = str(payload.get("account_id") or "").strip()
            if not account_id or not selected_roles:
                await _safe_callback_answer(callback, "Сначала выберите хотя бы одну роль.", show_alert=True)
                return
            grant_roles, revoke_roles = _user_role_flow_summary_lists(flow_action, selected_roles)
            result = RoleManagementService.apply_user_role_changes_by_account(
                account_id,
                actor_id=str(callback.from_user.id) if callback.from_user else None,
                actor_provider="telegram",
                actor_user_id=str(callback.from_user.id) if callback.from_user else None,
                target_provider=str(payload.get("provider") or "").strip() or None,
                target_user_id=str(payload.get("provider_user_id") or "").strip() or None,
                grant_roles=grant_roles,
                revoke_roles=revoke_roles,
                source="telegram_button",
            )
            sync_target = {
                "provider": payload.get("provider"),
                "provider_user_id": payload.get("provider_user_id"),
                "account_id": account_id,
            }
            for role_name in list(result.get("grant_success") or []):
                await _sync_linked_discord_role(sync_target, role_name, revoke=False, source="telegram_button")
            for role_name in list(result.get("revoke_success") or []):
                await _sync_linked_discord_role(sync_target, role_name, revoke=True, source="telegram_button")
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
            for denied in [*(result.get("grant_denied") or []), *(result.get("revoke_denied") or [])]:
                if denied.get("reason") == ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE:
                    success_lines.append(f"❌ {denied.get('message') or PRIVILEGED_DISCORD_ROLE_MESSAGE}")
                elif denied.get("reason") == ROLE_ASSIGNMENT_REASON_SYNC_ONLY_DISCORD_ROLE:
                    success_lines.append(f"❌ {denied.get('message') or SYNC_ONLY_DISCORD_ROLE_MESSAGE}")
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
            await _safe_callback_answer(callback, "Пакет применён" if result.get("ok") else "Пакет применён с ошибками", show_alert=not result.get("ok"))
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
                await _safe_callback_answer(callback, "Роль не найдена", show_alert=True)
                return
            role_name = flattened[item_index]["role"]
            if operation == "role_delete":
                result = RoleManagementService.delete_role(
                    role_name,
                    actor_id=str(callback.from_user.id) if callback.from_user else None,
                    actor_provider="telegram",
                    actor_user_id=str(callback.from_user.id) if callback.from_user else None,
                    telegram_user_id=str(callback.from_user.id) if callback.from_user else None,
                    source="telegram_button",
                )
                await _safe_callback_answer(callback, 
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
                await _safe_callback_answer(callback, "Ожидаю текст для блока «Как получить»", show_alert=True)
                await callback.message.reply(
                    f"Выбрана роль: <b>{role_name}</b>\n"
                    "Отправь: <code>Название роли | Как получить</code> или просто <code>Как получить</code>.\n"
                    "Пиши коротко и понятно: через активность, турнир, заявку, выдачу админа и т.д.",
                    parse_mode="HTML",
                )
                return
            if operation == "role_edit_sellable":
                _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                    operation="role_edit_sellable",
                    created_at=time.time(),
                    payload={"role": role_name},
                )
                await _safe_callback_answer(callback, "Ожидаю новый статус продажи", show_alert=True)
                await callback.message.reply(
                    f"Выбрана роль: <b>{role_name}</b>\n"
                    "Отправь: <code>Название роли | sellable</code> или <code>Название роли | not_sellable</code>.\n"
                    "Можно отправить только <code>sellable</code> / <code>not_sellable</code> после выбора роли.",
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
                    await _safe_callback_answer(callback, "Роль не найдена в каталоге", show_alert=True)
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
            await _safe_callback_answer(callback)
            return

        if action == "role_create_new_category":
            if not actor_can_manage_categories:
                logger.warning(
                    "roles_admin role_create new category denied callback_data=%s actor_id=%s",
                    callback.data,
                    callback.from_user.id,
                )
                await _safe_callback_answer(callback, "Категориями может управлять только Глава клуба или Главный вице.", show_alert=True)
                return
            _PENDING_ACTIONS[callback.from_user.id] = PendingRolesAdminAction(
                operation="role_create_new_category_name",
                created_at=time.time(),
            )
            await _safe_edit_message_text(
                callback,
                (
                    "Сначала создадим категорию для новой роли.\n\n"
                    "Отправь только <code>Название новой категории</code>.\n"
                    "После создания бот сразу попросит параметры роли в этой категории."
                ),
                parse_mode="HTML",
                reply_markup=_build_actions_keyboard(
                    owner_id,
                    "roles",
                    can_manage_categories=actor_can_manage_categories,
                ),
            )
            await _safe_callback_answer(callback)
            return

        if action == "set_position":
            op = parts[3] if len(parts) > 3 else ""
            value = parts[4] if len(parts) > 4 else ""
            pending = _PENDING_ACTIONS.get(callback.from_user.id)
            if op == "category_order":
                if not pending or pending.operation != "category_order_pick_position" or not pending.payload:
                    await _safe_callback_answer(callback, "Сессия устарела, начните заново", show_alert=True)
                    return
                category_name = pending.payload.get("category", "")
                new_pos = int(value) if value.lstrip("-").isdigit() else max(len(grouped) - 1, 0)
                ok = RoleManagementService.create_category(category_name, new_pos)
                _PENDING_ACTIONS.pop(callback.from_user.id, None)
                await _safe_callback_answer(callback, "Порядок категории обновлён" if ok else "Не удалось обновить порядок", show_alert=not ok)
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
            if op == "role_position":
                if not pending or pending.operation != "role_pick_position" or not pending.payload:
                    await _safe_callback_answer(callback, "Сессия устарела, начните заново", show_alert=True)
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
                    await _safe_callback_answer(callback, "Роль не найдена в каталоге", show_alert=True)
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
                await _safe_callback_answer(callback, 
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
                await _safe_callback_answer(callback, "Категория не найдена, обновите список.", show_alert=True)
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
            await _safe_callback_answer(callback)
            return

        if action == "delete_category":
            if not actor_can_manage_categories:
                logger.warning(
                    "roles_admin category delete denied callback_data=%s actor_id=%s",
                    callback.data,
                    callback.from_user.id,
                )
                await _safe_callback_answer(callback, "Категориями может управлять только Глава клуба или Главный вице.", show_alert=True)
                return
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            category_item = _resolve_category(grouped, page, category_idx)
            if not category_item:
                await _safe_callback_answer(callback, "Категория не найдена, обновите список.", show_alert=True)
                return

            ok = RoleManagementService.delete_category(category_item["category"])
            if not ok:
                await _safe_callback_answer(callback, "Не удалось удалить категорию (смотри логи).", show_alert=True)
                return

            grouped_after = RoleManagementService.list_roles_grouped() or []
            await _safe_edit_message_text(callback, 
                _render_list_text(grouped_after, page),
                parse_mode="HTML",
                reply_markup=_build_list_keyboard(grouped_after, owner_id, page),
            )
            await _safe_callback_answer(callback, "Категория удалена")
            return

        if action == "delete_role":
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            category_idx = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else -1
            role_idx = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else -1
            category_item = _resolve_category(grouped, page, category_idx)
            roles = category_item.get("roles", []) if category_item else []
            if not category_item or role_idx < 0 or role_idx >= len(roles):
                await _safe_callback_answer(callback, "Роль не найдена, обновите список.", show_alert=True)
                return

            role_name = roles[role_idx]["name"]
            result = RoleManagementService.delete_role(
                role_name,
                actor_id=str(callback.from_user.id) if callback.from_user else None,
                actor_provider="telegram",
                actor_user_id=str(callback.from_user.id) if callback.from_user else None,
                telegram_user_id=str(callback.from_user.id) if callback.from_user else None,
                source="telegram_button",
            )
            if not result["ok"]:
                await _safe_callback_answer(callback, _delete_role_result_message(result), show_alert=True)
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
            await _safe_callback_answer(callback, f"Роль {role_name} удалена")
            return

        await _safe_callback_answer(callback, "Неизвестное действие", show_alert=True)
    except (TelegramNetworkError, TelegramConflictError):
        logger.exception(
            "roles_admin callback transport failed (telegram runtime/session issue) callback_data=%s actor_id=%s",
            callback.data,
            callback.from_user.id if callback.from_user else None,
        )
        await _safe_callback_answer(callback, 
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
                await message.answer("❌ Формат: Категория | Роль | Описание | Как получить(опц) | discord_role_id(опц) | sellable/not_sellable(опц) | position(опц)")
                return
            parsed = _parse_role_create_metadata_args([args[1], args[0], *args[2:]])
            _log_role_create_category_selection(
                actor_id=message.from_user.id if message.from_user else None,
                category=parsed["category"],
                source="button_text_fallback",
            )
            create_result = RoleManagementService.create_role_result(
                parsed["role_name"],
                parsed["category"],
                description=parsed["description"],
                acquire_hint=parsed["acquire_hint"],
                discord_role_id=parsed["discord_role_id"],
                position=parsed["position"],
                actor_id=str(message.from_user.id) if message.from_user else None,
                actor_provider="telegram",
                actor_user_id=str(message.from_user.id) if message.from_user else None,
                operation="role_create",
                source="telegram_pending_text",
            )
            if create_result.get("ok") and parsed.get("is_sellable") is not None:
                RoleManagementService.update_role_sellable(
                    parsed["role_name"],
                    bool(parsed.get("is_sellable")),
                    actor_id=str(message.from_user.id) if message.from_user else None,
                    actor_provider="telegram",
                    actor_user_id=str(message.from_user.id) if message.from_user else None,
                    operation="role_edit_sellable",
                    source="telegram_pending_text",
                )
            await message.answer("✅ Роль создана." if create_result.get("ok") else f"❌ {create_result.get('message') or 'Не удалось создать роль (смотри логи).'}")
        elif op == "role_edit_description":
            if len(args) < 2:
                await message.answer("❌ Формат: Название роли | Описание")
                return
            ok = RoleManagementService.update_role_description(
                args[0],
                args[1],
                actor_id=str(message.from_user.id) if message.from_user else None,
                actor_provider="telegram",
                actor_user_id=str(message.from_user.id) if message.from_user else None,
                operation="role_edit_description",
                source="telegram_pending_text",
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
                actor_provider="telegram",
                actor_user_id=str(message.from_user.id) if message.from_user else None,
                operation="role_edit_acquire_hint",
                source="telegram_pending_text",
            )
            await message.answer("✅ Способ получения роли обновлён." if ok else "❌ Не удалось обновить способ получения роли (смотри логи).")
        elif op == "role_edit_sellable":
            role_name = str((pending.payload or {}).get("role") or "").strip()
            value = None
            if len(args) >= 2 and args[0] == role_name:
                value = args[1]
            elif args:
                value = args[0]
            parsed_sellable = _parse_sellable_choice(value)
            if not role_name or parsed_sellable is None:
                await message.answer("❌ Формат: Название роли | sellable|not_sellable или только sellable|not_sellable после выбора роли.")
                return
            ok = RoleManagementService.update_role_sellable(
                role_name,
                parsed_sellable,
                actor_id=str(message.from_user.id) if message.from_user else None,
                actor_provider="telegram",
                actor_user_id=str(message.from_user.id) if message.from_user else None,
                operation="role_edit_sellable",
                source="telegram_pending_text",
            )
            await message.answer("✅ Признак продажи роли обновлён." if ok else "❌ Не удалось обновить признак продажи роли (смотри логи).")
        elif op == "role_create_enter_name":
            if not pending.payload or not pending.payload.get("category"):
                await message.answer("❌ Сессия выбора категории устарела. Начните заново: /roles_admin")
                return
            if not args:
                await message.answer("❌ Формат: Название роли | Описание | Как получить(опц) | discord_role_id(опц) | sellable/not_sellable(опц) | position(опц)")
                return
            category = str(pending.payload.get("category") or "")
            parsed = _parse_role_create_selected_category_args(args, category=category)
            _log_role_create_category_selection(
                actor_id=message.from_user.id if message.from_user else None,
                category=category,
                source="button_selected_category",
                created_new=bool(pending.payload.get("created_new_category")),
            )
            create_result = RoleManagementService.create_role_result(
                parsed["role_name"],
                category,
                description=parsed["description"],
                acquire_hint=parsed["acquire_hint"],
                discord_role_id=parsed["discord_role_id"],
                position=parsed["position"],
                actor_id=str(message.from_user.id) if message.from_user else None,
                actor_provider="telegram",
                actor_user_id=str(message.from_user.id) if message.from_user else None,
                operation="role_create",
                source="telegram_button",
            )
            await message.answer("✅ Роль создана." if create_result.get("ok") else f"❌ {create_result.get('message') or 'Не удалось создать роль (смотри логи).'}")
        elif op == "role_create_new_category_name":
            if not _can_manage_categories("telegram", str(message.from_user.id)):
                logger.warning(
                    "roles_admin role_create new category denied actor_id=%s source=%s",
                    message.from_user.id,
                    "pending_message",
                )
                await message.answer("❌ Категориями может управлять только Глава клуба или Главный вице.")
                return
            if not args or not args[0]:
                await message.answer("❌ Формат: Название новой категории")
                return
            category_name = args[0]
            ok = RoleManagementService.create_category(category_name, 0)
            if not ok:
                logger.error(
                    "roles_admin role_create new category failed actor_id=%s category=%s",
                    message.from_user.id,
                    category_name,
                )
                await message.answer("❌ Не удалось создать категорию (смотри логи).")
                return
            _log_role_create_category_selection(
                actor_id=message.from_user.id if message.from_user else None,
                category=category_name,
                source="button_new_category",
                created_new=True,
            )
            _PENDING_ACTIONS[message.from_user.id] = PendingRolesAdminAction(
                operation="role_create_enter_name",
                created_at=time.time(),
                payload={"category": category_name, "created_new_category": True},
            )
            keep_pending = True
            await message.answer(
                (
                    f"✅ Категория <b>{category_name}</b> создана и выбрана.\n\n"
                    "Теперь отправь: <code>Название роли | Описание | Как получить(опц) | discord_role_id(опц) | sellable/not_sellable(опц) | position(опц)</code>\n"
                    "Сначала идёт название роли, затем описание и инструкция, как получить роль."
                ),
                parse_mode="HTML",
            )
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
                actor_provider="telegram",
                actor_user_id=str(message.from_user.id) if message.from_user else None,
                telegram_user_id=str(message.from_user.id) if message.from_user else None,
                source="telegram_pending_text",
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
                    actor_provider="telegram",
                    actor_user_id=str(message.from_user.id) if message.from_user else None,
                    target_provider=str(resolved.get("provider") or "").strip() or None,
                    target_user_id=str(resolved.get("provider_user_id") or "").strip() or None,
                    grant_roles=[role_name],
                    source="telegram_pending_text",
                )
                ok = bool(result.get("grant_success"))
                sync_result = None
                if ok:
                    sync_result = await _sync_linked_discord_role(resolved, role_name, revoke=False, source="telegram_pending_text")
                await message.answer(
                    f"✅ Роль выдана пользователю {resolved['label']}."
                    f"{_discord_sync_status_note(sync_result)}"
                    if ok
                    else _role_assignment_error_message(
                        result.get("grant_denied", [{}])[0] if result.get("grant_denied") else result,
                        default_message=f"❌ Не удалось выдать роль. {_telegram_user_lookup_hint()}",
                    )
                )
            else:
                result = RoleManagementService.apply_user_role_changes_by_account(
                    account_id,
                    actor_id=str(message.from_user.id) if message.from_user else None,
                    actor_provider="telegram",
                    actor_user_id=str(message.from_user.id) if message.from_user else None,
                    target_provider=str(resolved.get("provider") or "").strip() or None,
                    target_user_id=str(resolved.get("provider_user_id") or "").strip() or None,
                    revoke_roles=[role_name],
                    source="telegram_pending_text",
                )
                ok = bool(result.get("revoke_success"))
                sync_result = None
                if ok:
                    sync_result = await _sync_linked_discord_role(resolved, role_name, revoke=True, source="telegram_pending_text")
                await message.answer(
                    f"✅ Роль снята у пользователя {resolved['label']}."
                    f"{_discord_sync_status_note(sync_result)}"
                    if ok
                    else _role_assignment_error_message(
                        result.get("revoke_denied", [{}])[0] if result.get("revoke_denied") else result,
                        default_message=f"❌ Не удалось снять роль. {_telegram_user_lookup_hint()}",
                    )
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
