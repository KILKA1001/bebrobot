"""
Назначение: модуль "top" реализует продуктовый контур в зоне Telegram.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Telegram.
Пользовательский вход: команда /top и связанный пользовательский сценарий.
"""

from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, User

from bot.services import AccountsService, PointsService
from bot.telegram_bot.identity import persist_telegram_identity_from_user
from bot.utils import format_points

logger = logging.getLogger(__name__)
router = Router()

_PAGE_SIZE = 5
_CALLBACK_PREFIX = "top"

_PERIOD_LABELS = {
    PointsService.LEADERBOARD_PERIOD_ALL: "Все время",
    PointsService.LEADERBOARD_PERIOD_MONTH: "За месяц",
    PointsService.LEADERBOARD_PERIOD_WEEK: "За неделю",
}


def _normalize_period(period: str | None) -> str:
    normalized = str(period or PointsService.LEADERBOARD_PERIOD_ALL).strip().lower()
    if normalized in _PERIOD_LABELS:
        return normalized
    return PointsService.LEADERBOARD_PERIOD_ALL


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


def _resolve_display_name(user_id: int, *, local_telegram_names: dict[int, str] | None = None) -> str:
    try:
        account_id = AccountsService.resolve_account_id("telegram", str(user_id))
    except Exception:
        logger.exception("telegram top resolve account_id failed platform=%s user_id=%s", "telegram", user_id)
        account_id = None

    if account_id:
        try:
            account_best_name = AccountsService.get_best_public_name("discord", None, account_id=account_id)
            if account_best_name:
                return str(account_best_name)
            account_best_name = AccountsService.get_best_public_name("telegram", None, account_id=account_id)
            if account_best_name:
                return str(account_best_name)
        except Exception:
            logger.exception(
                "telegram top resolve identity name failed platform=%s user_id=%s account_id=%s",
                "telegram",
                user_id,
                account_id,
            )

    local_name = (local_telegram_names or {}).get(int(user_id))
    if local_name:
        return local_name

    logger.warning("telegram top fallback to id platform=%s user_id=%s", "telegram", user_id)
    return f"ID {user_id}"


def _render_top_text(*, period: str, page: int, local_telegram_names: dict[int, str] | None = None) -> tuple[str, InlineKeyboardMarkup]:
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
        lines.append("Пока нет данных для отображения.")
    else:
        for idx, (user_id, points) in enumerate(page_entries, start=start + 1):
            lines.append(
                f"{idx}. <b>{_resolve_display_name(int(user_id), local_telegram_names=local_telegram_names)}</b> — {format_points(points)} баллов"
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
    try:
        text, keyboard = _render_top_text(period=period, page=0, local_telegram_names=local_telegram_names)
        await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
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
    local_telegram_names = {callback.from_user.id: _local_telegram_name_from_user(callback.from_user)}
    local_telegram_names = {uid: name for uid, name in local_telegram_names.items() if name}

    if action not in {"period", "page"}:
        await callback.answer("Неизвестное действие", show_alert=True)
        return

    try:
        page = int(page_raw)
        text, keyboard = _render_top_text(period=mode, page=page, local_telegram_names=local_telegram_names)
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
