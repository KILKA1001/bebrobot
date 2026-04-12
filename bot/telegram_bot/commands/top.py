"""
Назначение: модуль "top" реализует продуктовый контур в зоне Telegram.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Telegram.
Пользовательский вход: команда /top и связанный пользовательский сценарий.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, User

from bot.services import AccountsService, AuthorityService, PointsService
from bot.telegram_bot.identity import persist_telegram_identity_from_user
from bot.utils import format_points

logger = logging.getLogger(__name__)
router = Router()

_PAGE_SIZE = 5
_CALLBACK_PREFIX = "top"
_SESSION_TTL_SECONDS = 60 * 30

_PERIOD_LABELS = {
    PointsService.LEADERBOARD_PERIOD_ALL: "Все время",
    PointsService.LEADERBOARD_PERIOD_MONTH: "За месяц",
    PointsService.LEADERBOARD_PERIOD_WEEK: "За неделю",
}


@dataclass
class _TopMessageSessionState:
    resolved_names: dict[int, str] = field(default_factory=dict)
    seen_non_id_names: dict[int, str] = field(default_factory=dict)
    local_telegram_names: dict[int, str] = field(default_factory=dict)
    local_telegram_users: dict[int, User] = field(default_factory=dict)
    expires_at: float = 0.0


_TOP_MESSAGE_SESSION_STATE: dict[tuple[int, int], _TopMessageSessionState] = {}


def _normalize_period(period: str | None) -> str:
    normalized = str(period or PointsService.LEADERBOARD_PERIOD_ALL).strip().lower()
    if normalized in _PERIOD_LABELS:
        return normalized
    return PointsService.LEADERBOARD_PERIOD_ALL


def _is_id_fallback_name(name: str) -> bool:
    return str(name).startswith("ID ")


def _cleanup_expired_sessions() -> None:
    now = time.time()
    stale_keys = [key for key, state in _TOP_MESSAGE_SESSION_STATE.items() if state.expires_at <= now]
    for key in stale_keys:
        _TOP_MESSAGE_SESSION_STATE.pop(key, None)


def _get_or_create_session_state(chat_id: int, message_id: int) -> _TopMessageSessionState:
    _cleanup_expired_sessions()
    key = (int(chat_id), int(message_id))
    state = _TOP_MESSAGE_SESSION_STATE.get(key)
    if state is None:
        state = _TopMessageSessionState()
        _TOP_MESSAGE_SESSION_STATE[key] = state
    state.expires_at = time.time() + _SESSION_TTL_SECONDS
    return state


def _build_top_keyboard(*, period: str, page: int, total_pages: int) -> InlineKeyboardMarkup:
    safe_period = _normalize_period(period)
    safe_page = max(0, min(page, max(total_pages - 1, 0)))
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Все время", callback_data=f"{_CALLBACK_PREFIX}:period:{PointsService.LEADERBOARD_PERIOD_ALL}:0"),
                InlineKeyboardButton(text="За месяц", callback_data=f"{_CALLBACK_PREFIX}:period:{PointsService.LEADERBOARD_PERIOD_MONTH}:0"),
                InlineKeyboardButton(text="За неделю", callback_data=f"{_CALLBACK_PREFIX}:period:{PointsService.LEADERBOARD_PERIOD_WEEK}:0"),
            ],
            [
                InlineKeyboardButton(
                    text="◀️ Назад",
                    callback_data=f"{_CALLBACK_PREFIX}:page:{safe_period}:{max(safe_page - 1, 0)}",
                ),
                InlineKeyboardButton(text=f"Стр. {safe_page + 1}/{max(total_pages, 1)}", callback_data=f"{_CALLBACK_PREFIX}:noop"),
                InlineKeyboardButton(
                    text="Вперёд ▶️",
                    callback_data=f"{_CALLBACK_PREFIX}:page:{safe_period}:{min(safe_page + 1, max(total_pages - 1, 0))}",
                ),
            ],
        ]
    )


def _local_telegram_name_from_user(user: User | None) -> str | None:
    if not user:
        return None
    full_name = str(user.full_name or "").strip()
    if full_name:
        return full_name
    username = str(user.username or "").strip()
    if username:
        return f"@{username}"
    return None


def _schedule_soft_identity_refresh_telegram(
    *,
    provider_user_id: int,
    chat_id: int | None,
    source_handler: str,
    local_user: User | None,
) -> None:
    if local_user is None:
        logger.info(
            "telegram top soft identity refresh skipped provider=%s provider_user_id=%s chat_id=%s source_handler=%s reason=%s",
            "telegram",
            provider_user_id,
            chat_id,
            source_handler,
            "user_object_missing",
        )
        return

    async def _runner() -> None:
        try:
            AccountsService.refresh_identity_from_platform_user(
                "telegram",
                local_user,
                source_handler=source_handler,
                chat_id=chat_id,
            )
        except Exception:
            logger.exception(
                "telegram top soft identity refresh failed provider=%s provider_user_id=%s chat_id=%s source_handler=%s",
                "telegram",
                provider_user_id,
                chat_id,
                source_handler,
            )

    try:
        asyncio.get_running_loop().create_task(_runner())
        logger.info(
            "telegram top soft identity refresh launched provider=%s provider_user_id=%s chat_id=%s source_handler=%s",
            "telegram",
            provider_user_id,
            chat_id,
            source_handler,
        )
    except RuntimeError:
        logger.warning(
            "telegram top soft identity refresh skipped provider=%s provider_user_id=%s chat_id=%s source_handler=%s reason=%s",
            "telegram",
            provider_user_id,
            chat_id,
            source_handler,
            "event_loop_unavailable",
        )


def _resolve_display_name(
    user_id: int,
    *,
    period: str,
    page: int,
    session_state: _TopMessageSessionState | None = None,
    local_telegram_names: dict[int, str] | None = None,
    local_telegram_users: dict[int, User] | None = None,
    chat_id: int | None = None,
    admin_actor_user_id: int | None = None,
) -> str:
    if session_state is not None:
        cached = session_state.resolved_names.get(int(user_id))
        if cached:
            return cached

    account_id = None
    try:
        account_id = AccountsService.resolve_account_id("telegram", str(user_id))
    except Exception:
        logger.exception("telegram top resolve account_id failed platform=%s user_id=%s", "telegram", user_id)

    if not account_id:
        try:
            account_id = AccountsService.resolve_account_id("discord", str(user_id))
        except Exception:
            logger.exception("telegram top resolve account_id failed platform=%s user_id=%s", "discord", user_id)

    if account_id:
        try:
            account_best_name = AccountsService.get_best_public_name(None, None, account_id=account_id)
            if account_best_name:
                resolved = str(account_best_name)
                if session_state is not None:
                    session_state.resolved_names[int(user_id)] = resolved
                    if not _is_id_fallback_name(resolved):
                        session_state.seen_non_id_names[int(user_id)] = resolved
                return resolved
        except Exception:
            logger.exception(
                "telegram top resolve identity name failed platform=%s user_id=%s account_id=%s",
                "telegram",
                user_id,
                account_id,
            )

    local_name = (local_telegram_names or {}).get(int(user_id))
    if local_name:
        if session_state is not None:
            session_state.resolved_names[int(user_id)] = str(local_name)
            session_state.seen_non_id_names[int(user_id)] = str(local_name)
        return local_name

    _schedule_soft_identity_refresh_telegram(
        provider_user_id=int(user_id),
        chat_id=chat_id,
        source_handler="telegram.top_render",
        local_user=(local_telegram_users or {}).get(int(user_id)),
    )
    logger.warning(
        "top name fallback to id platform=%s source_user_id=%s resolved_account_id=%s period=%s page=%s",
        "telegram",
        user_id,
        account_id,
        period,
        page,
    )
    if admin_actor_user_id and AuthorityService.is_super_admin("telegram", str(admin_actor_user_id)):
        logger.info(
            "top id fallback admin hint platform=%s source_user_id=%s period=%s page=%s hint=%s",
            "telegram",
            user_id,
            period,
            page,
            "Профиль не привязан или lookup-поля пустые. Проверьте account_identities и обновление identity.",
        )
    fallback_name = f"ID {user_id}"
    if session_state is not None:
        previous_name = session_state.seen_non_id_names.get(int(user_id))
        if previous_name:
            logger.warning(
                "top_name_regressed_to_id platform=%s user_id=%s period=%s page=%s",
                "telegram",
                user_id,
                period,
                page,
            )
            session_state.resolved_names[int(user_id)] = previous_name
            return previous_name
        session_state.resolved_names[int(user_id)] = fallback_name
    return fallback_name


def _render_top_text(
    *,
    period: str,
    page: int,
    session_state: _TopMessageSessionState | None = None,
    local_telegram_names: dict[int, str] | None = None,
    local_telegram_users: dict[int, User] | None = None,
    chat_id: int | None = None,
    admin_actor_user_id: int | None = None,
) -> tuple[str, InlineKeyboardMarkup]:
    safe_period = _normalize_period(period)
    entries = PointsService.get_leaderboard_entries(safe_period)
    total_pages = max(1, (len(entries) + _PAGE_SIZE - 1) // _PAGE_SIZE)
    safe_page = max(0, min(int(page), total_pages - 1))

    start = safe_page * _PAGE_SIZE
    page_entries = entries[start : start + _PAGE_SIZE]
    period_label = _PERIOD_LABELS.get(safe_period, _PERIOD_LABELS[PointsService.LEADERBOARD_PERIOD_ALL])

    header = (
        "🏆 <b>Топ участников</b>\n"
        "Смотрите, кто сейчас впереди по количеству баллов.\n"
        "Период можно переключать кнопками ниже."
    )

    lines: list[str] = []
    if not page_entries:
        lines.append("Пока нет участников с положительным балансом баллов.")
    else:
        for idx, (user_id, points) in enumerate(page_entries, start=start + 1):
            lines.append(
                f"{idx}. <b>{_resolve_display_name(int(user_id), period=safe_period, page=safe_page, session_state=session_state, local_telegram_names=local_telegram_names, local_telegram_users=local_telegram_users, chat_id=chat_id, admin_actor_user_id=admin_actor_user_id)}</b> — {format_points(points)} баллов"
            )

    lines.extend(["", f"<b>Период:</b> {period_label}", f"<b>Страница:</b> {safe_page + 1}/{total_pages}"])

    text = f"{header}\n\n" + "\n".join(lines)
    return text, _build_top_keyboard(period=safe_period, page=safe_page, total_pages=total_pages)


@router.message(Command("top"))
async def top_command(message: Message) -> None:
    if not message.from_user:
        return
    persist_telegram_identity_from_user(message.from_user)
    actor_id = message.from_user.id
    chat_id = message.chat.id if message.chat else None
    period = PointsService.LEADERBOARD_PERIOD_ALL
    local_telegram_names = {message.from_user.id: _local_telegram_name_from_user(message.from_user)}
    local_telegram_names = {uid: name for uid, name in local_telegram_names.items() if name}
    local_telegram_users = {message.from_user.id: message.from_user}
    session_state = _TopMessageSessionState()
    session_state.local_telegram_names.update(local_telegram_names)
    session_state.local_telegram_users.update(local_telegram_users)
    try:
        text, keyboard = _render_top_text(
            period=period,
            page=0,
            session_state=session_state,
            local_telegram_names=local_telegram_names,
            local_telegram_users=local_telegram_users,
            chat_id=chat_id,
            admin_actor_user_id=actor_id,
        )
        sent = await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        if sent.chat:
            state = _get_or_create_session_state(sent.chat.id, sent.message_id)
            state.resolved_names.update(session_state.resolved_names)
            state.seen_non_id_names.update(session_state.seen_non_id_names)
            state.local_telegram_names.update(local_telegram_names)
            state.local_telegram_users.update(local_telegram_users)
    except Exception:
        logger.exception(
            "telegram top command failed platform=%s actor_id=%s chat_id=%s period=%s page=%s",
            "telegram",
            actor_id,
            chat_id,
            period,
            0,
        )
        await message.answer("❌ Не удалось открыть рейтинг. Подробности записаны в консоль.")


@router.callback_query(F.data.startswith(f"{_CALLBACK_PREFIX}:"))
async def top_callback(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.message:
        return

    parts = str(callback.data or "").split(":")
    if len(parts) == 2 and parts[1] == "noop":
        await callback.answer()
        return

    if len(parts) != 4:
        await callback.answer("Не удалось обработать действие", show_alert=True)
        return

    _, action, period, page_raw = parts
    actor_id = callback.from_user.id
    chat_id = callback.message.chat.id if callback.message else None
    mode = _normalize_period(period)
    state = _get_or_create_session_state(chat_id, callback.message.message_id)
    local_telegram_names = {callback.from_user.id: _local_telegram_name_from_user(callback.from_user)}
    local_telegram_names = {uid: name for uid, name in local_telegram_names.items() if name}
    state.local_telegram_names.update(local_telegram_names)
    state.local_telegram_users[callback.from_user.id] = callback.from_user
    _schedule_soft_identity_refresh_telegram(
        provider_user_id=callback.from_user.id,
        chat_id=chat_id,
        source_handler="telegram.top_callback",
        local_user=callback.from_user,
    )

    if action not in {"period", "page"}:
        await callback.answer("Неизвестное действие", show_alert=True)
        return

    try:
        page = int(page_raw)
        text, keyboard = _render_top_text(
            period=mode,
            page=page,
            session_state=state,
            local_telegram_names=state.local_telegram_names,
            local_telegram_users=state.local_telegram_users,
            chat_id=chat_id,
            admin_actor_user_id=actor_id,
        )
        await callback.message.edit_text(text=text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        await callback.answer()
    except Exception:
        logger.exception(
            "telegram top callback failed platform=%s actor_id=%s chat_id=%s period=%s page=%s",
            "telegram",
            actor_id,
            chat_id,
            mode,
            page_raw,
        )
        await callback.answer("Не удалось обновить рейтинг. Подробности в консоли.", show_alert=True)
