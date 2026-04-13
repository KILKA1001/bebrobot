"""
Назначение: модуль "proposal" реализует продуктовый контур в зоне Telegram.
Ответственность: единый сценарий предложений Совету в рамках одной команды.
Где используется: Telegram.
Пользовательский вход: команда /proposal и связанный пользовательский сценарий.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.services.council_feedback_service import CouncilFeedbackService
from bot.services.council_system_events_service import CouncilSystemEventsService
from bot.services.proposal_ui_texts import (
    build_status_parts,
    build_submit_success_parts,
    render_archive_empty_text,
    render_archive_lines,
    render_help_text,
    render_menu_overview,
)

logger = logging.getLogger(__name__)
router = Router()


@dataclass(slots=True)
class PendingProposal:
    title: str
    proposal_text: str
    created_at: float


_PENDING_PROPOSAL_INPUT: dict[int, float] = {}
_PENDING_PROPOSAL_CONFIRM: dict[int, PendingProposal] = {}
_PENDING_TTL_SECONDS = 900


def _menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📝 Подать предложение", callback_data="proposal:submit")],
            [InlineKeyboardButton(text="📍 Статус", callback_data="proposal:status")],
            [InlineKeyboardButton(text="📚 Архив решений", callback_data="proposal:archive")],
            [InlineKeyboardButton(text="❓ Помощь", callback_data="proposal:help")],
        ]
    )


def _confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Отправить", callback_data="proposal:confirm_send")],
            [InlineKeyboardButton(text="✏️ Изменить", callback_data="proposal:submit")],
            [InlineKeyboardButton(text="↩️ В меню", callback_data="proposal:menu")],
        ]
    )


def _cleanup_pending(user_id: int) -> None:
    _PENDING_PROPOSAL_INPUT.pop(user_id, None)
    _PENDING_PROPOSAL_CONFIRM.pop(user_id, None)


def _is_alive(created_at: float | None) -> bool:
    if not created_at:
        return False
    return (time.time() - created_at) <= _PENDING_TTL_SECONDS


@router.message(Command("proposal"))
async def proposal_command(message: Message) -> None:
    try:
        if not message.from_user:
            await message.answer("❌ Не удалось определить пользователя.")
            return
        _cleanup_pending(message.from_user.id)
        await message.answer(
            "🗂 <b>Меню предложений</b>\n" + render_menu_overview(),
            reply_markup=_menu_keyboard(),
            parse_mode="HTML",
        )
    except Exception:
        logger.exception("telegram proposal command failed actor_id=%s", getattr(message.from_user, "id", None))
        await message.answer("❌ Не удалось открыть меню предложений.")


@router.message(Command("proposal_system_channel"))
async def proposal_system_channel_command(message: Message) -> None:
    try:
        if not message.from_user:
            await message.answer("❌ Не удалось определить пользователя.")
            return
        raw_text = str(getattr(message, "text", "") or "").strip()
        parts = raw_text.split(maxsplit=1)
        args = str(parts[1] if len(parts) > 1 else "show").strip().lower()
        if args == "show":
            current = CouncilSystemEventsService.get_channel("telegram")
            if not current:
                await message.answer(
                    "ℹ️ Канал системных событий Совета пока не настроен.\n"
                    "Суперадмин может выполнить `/proposal_system_channel set_here` в нужной группе.",
                )
                return
            await message.answer(f"✅ Сейчас выбран чат `{current}` для системных событий Совета.", parse_mode="Markdown")
            return
        if args == "set_here":
            result = CouncilSystemEventsService.set_channel(
                provider="telegram",
                actor_user_id=str(message.from_user.id),
                destination_id=str(getattr(message.chat, "id", "") or ""),
            )
            await message.answer(str(result.get("message") or ("✅ Чат системных событий Совета сохранён." if result.get("ok") else "❌ Не удалось сохранить чат.")))
            return
        if args == "clear":
            result = CouncilSystemEventsService.set_channel(
                provider="telegram",
                actor_user_id=str(message.from_user.id),
                destination_id="",
            )
            await message.answer(str(result.get("message") or ("✅ Чат системных событий Совета очищен." if result.get("ok") else "❌ Не удалось очистить чат.")))
            return
        await message.answer(
            "❌ Неизвестное действие. Доступно: show, set_here, clear.\n"
            "Пример: /proposal_system_channel set_here"
        )
    except Exception:
        logger.exception("telegram proposal system channel command failed actor_id=%s", getattr(message.from_user, "id", None))
        await message.answer("❌ Ошибка настройки канала. Подробности в логах.")


@router.callback_query(F.data.startswith("proposal:"))
async def proposal_callbacks(callback: CallbackQuery) -> None:
    actor_id = callback.from_user.id if callback.from_user else None
    action = str(callback.data or "").split(":", 1)[1] if callback.data else ""
    if actor_id is None:
        await callback.answer("Ошибка пользователя", show_alert=True)
        return

    try:
        if action == "menu":
            _cleanup_pending(actor_id)
            await callback.message.edit_text(
                "🗂 <b>Меню предложений</b>\n" + render_menu_overview(),
                reply_markup=_menu_keyboard(),
                parse_mode="HTML",
            )
            await callback.answer()
            return

        if action == "submit":
            _PENDING_PROPOSAL_INPUT[actor_id] = time.time()
            _PENDING_PROPOSAL_CONFIRM.pop(actor_id, None)
            await callback.message.edit_text(
                "📝 <b>Форма подачи</b>\n"
                "Отправьте одним сообщением: заголовок и текст предложения.\n\n"
                "Формат:\n"
                "<code>Заголовок\n\nТекст предложения</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="↩️ В меню", callback_data="proposal:menu")]]
                ),
            )
            await callback.answer()
            return

        if action == "confirm_send":
            pending = _PENDING_PROPOSAL_CONFIRM.get(actor_id)
            if not pending or not _is_alive(pending.created_at):
                _cleanup_pending(actor_id)
                await callback.answer("Черновик устарел. Откройте форму снова.", show_alert=True)
                return
            result = CouncilFeedbackService.submit_proposal(
                provider="telegram",
                provider_user_id=str(actor_id),
                title=pending.title,
                proposal_text=pending.proposal_text,
            )
            if not result.get("ok"):
                await callback.message.edit_text(str(result.get("message") or "Не удалось отправить предложение."))
                await callback.answer()
                return
            _cleanup_pending(actor_id)
            success_parts = build_submit_success_parts(
                proposal_id=result.get("proposal_id"),
                status_label=result.get("status_label"),
            )
            await callback.message.edit_text(
                "✅ <b>Предложение отправлено</b>\n"
                f"<b>{success_parts['proposal_number']}</b>\n"
                f"{success_parts['status']}\n\n"
                f"{success_parts['next_step']}",
                parse_mode="HTML",
                reply_markup=_menu_keyboard(),
            )
            await callback.answer()
            return

        if action == "status":
            payload = CouncilFeedbackService.get_latest_status(provider="telegram", provider_user_id=str(actor_id))
            text = str(payload.get("message") or "")
            if payload.get("ok") and payload.get("has_data"):
                status_parts = build_status_parts(
                    proposal_id=payload.get("proposal_id"),
                    title=payload.get("title"),
                    status_label=payload.get("status_label"),
                    updated_at=payload.get("updated_at"),
                )
                text = (
                    "📍 <b>Текущий статус</b>\n"
                    f"<b>{status_parts['proposal']}</b>\n"
                    f"{status_parts['status']}\n"
                    f"<code>{status_parts['updated_at']}</code>"
                )
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=_menu_keyboard())
            await callback.answer()
            return

        if action == "archive":
            rows = CouncilFeedbackService.get_decisions_archive(limit=5)
            if not rows:
                text = f"📚 <b>{render_archive_empty_text().removeprefix('📚 ')}</b>"
            else:
                raw_lines = render_archive_lines(rows, text_limit=180)
                chunks = ["📚 <b>Архив решений</b>"]
                for line in raw_lines:
                    chunks.append(line)
                text = "\n".join(chunks)
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=_menu_keyboard())
            await callback.answer()
            return

        if action == "help":
            await callback.message.edit_text(
                render_help_text().replace("❓ Как пользоваться:", "❓ <b>Помощь</b>"),
                parse_mode="HTML",
                reply_markup=_menu_keyboard(),
            )
            await callback.answer()
            return

        await callback.answer("Неизвестное действие", show_alert=True)
    except Exception:
        logger.exception("telegram proposal callback failed actor_id=%s action=%s", actor_id, action)
        await callback.answer("❌ Ошибка выполнения. Попробуйте ещё раз.", show_alert=True)


@router.message()
async def proposal_pending_input(message: Message) -> None:
    if not message.from_user:
        return
    actor_id = message.from_user.id
    started_at = _PENDING_PROPOSAL_INPUT.get(actor_id)
    if not _is_alive(started_at):
        if started_at:
            _cleanup_pending(actor_id)
        return
    text = str(message.text or "").strip()
    if not text or text.startswith("/"):
        return

    try:
        if "\n\n" in text:
            title, body = text.split("\n\n", 1)
        else:
            parts = text.split("\n", 1)
            if len(parts) < 2:
                await message.answer("❌ Укажите заголовок и текст. Пример: Заголовок, пустая строка, затем текст предложения.")
                return
            title, body = parts[0], parts[1]

        pending = PendingProposal(title=title.strip(), proposal_text=body.strip(), created_at=time.time())
        _PENDING_PROPOSAL_CONFIRM[actor_id] = pending
        _PENDING_PROPOSAL_INPUT.pop(actor_id, None)

        await message.answer(
            "📨 <b>Подтверждение отправки</b>\n"
            f"<b>Заголовок:</b> {pending.title}\n"
            f"<b>Текст:</b> {pending.proposal_text}\n\n"
            "Проверьте данные и выберите действие:",
            parse_mode="HTML",
            reply_markup=_confirm_keyboard(),
        )
    except Exception:
        logger.exception("telegram proposal pending parse failed actor_id=%s", actor_id)
        await message.answer("❌ Не удалось обработать форму. Откройте «Подать предложение» ещё раз.")
