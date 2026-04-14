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
from bot.services.authority_service import AuthorityService
from bot.services.guiy_publish_destinations_service import GuiyPublishDestination, GuiyPublishDestinationsService
from bot.services.proposal_ui_texts import (
    ARCHIVE_PERIOD_LABELS,
    ARCHIVE_STATUS_LABELS,
    ARCHIVE_TYPE_LABELS,
    PROPOSAL_ADMIN_ACTION_BY_CODE,
    PROPOSAL_ADMIN_SECTION_BY_CODE,
    PROPOSAL_ADMIN_SECTIONS,
    build_status_parts,
    build_submit_success_parts,
    render_admin_action_result,
    render_admin_confirm_text,
    render_admin_root_text,
    render_admin_section_text,
    render_archive_empty_text,
    render_archive_filters_text,
    render_archive_lines,
    render_help_text,
    render_menu_action_explanations,
    render_menu_overview,
    render_submit_form_text,
    render_submit_review_text,
    render_events_pick_confirmation_text,
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
_PENDING_ADMIN_CONFIRM: dict[int, str] = {}
_PENDING_EVENTS_DESTINATION_PICKER: dict[int, dict[str, object]] = {}
_PENDING_TTL_SECONDS = 900
_ARCHIVE_FILTERS_BY_USER: dict[int, dict[str, str]] = {}
_TELEGRAM_EVENTS_DESTINATIONS_PAGE_SIZE = 6


def _archive_filters(user_id: int) -> dict[str, str]:
    current = _ARCHIVE_FILTERS_BY_USER.get(user_id) or {"period_code": "90d", "status_code": "all", "question_type_code": "all"}
    _ARCHIVE_FILTERS_BY_USER[user_id] = current
    return current


def _archive_keyboard(user_id: int) -> InlineKeyboardMarkup:
    current = _archive_filters(user_id)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"🗓 {ARCHIVE_PERIOD_LABELS.get(current['period_code'], '90 дней')}", callback_data="proposal:archive_period")],
            [InlineKeyboardButton(text=f"📌 {ARCHIVE_STATUS_LABELS.get(current['status_code'], 'Все статусы')}", callback_data="proposal:archive_status")],
            [InlineKeyboardButton(text=f"🧩 {ARCHIVE_TYPE_LABELS.get(current['question_type_code'], 'Все типы')}", callback_data="proposal:archive_type")],
            [InlineKeyboardButton(text="↩️ В меню", callback_data="proposal:menu")],
        ]
    )


def _menu_keyboard(*, is_superadmin: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="📝 Подать предложение", callback_data="proposal:submit")],
        [InlineKeyboardButton(text="📍 Статус", callback_data="proposal:status")],
        [InlineKeyboardButton(text="📚 Архив решений", callback_data="proposal:archive")],
        [InlineKeyboardButton(text="❓ Помощь", callback_data="proposal:help")],
    ]
    if is_superadmin:
        rows.append([InlineKeyboardButton(text="⚙️ Настройки Совета", callback_data="proposal:admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _admin_root_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"📂 {section.title}", callback_data=f"proposal:admin_section:{section.code}")] for section in PROPOSAL_ADMIN_SECTIONS]
    rows.append([InlineKeyboardButton(text="↩️ В меню", callback_data="proposal:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _admin_section_keyboard(section_code: str) -> InlineKeyboardMarkup:
    section = PROPOSAL_ADMIN_SECTION_BY_CODE.get(section_code)
    if not section:
        return _admin_root_keyboard()
    rows = [[InlineKeyboardButton(text=f"➡️ {action.title}", callback_data=f"proposal:admin_action:{action.code}")] for action in section.actions]
    rows.append([InlineKeyboardButton(text="↩️ К разделам", callback_data="proposal:admin")])
    rows.append([InlineKeyboardButton(text="↩️ В меню", callback_data="proposal:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _admin_confirm_keyboard(action_code: str, section_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"proposal:admin_confirm:{action_code}")],
            [InlineKeyboardButton(text="↩️ Отмена", callback_data=f"proposal:admin_section:{section_code}")],
            [InlineKeyboardButton(text="↩️ К разделам", callback_data="proposal:admin")],
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
    _PENDING_ADMIN_CONFIRM.pop(user_id, None)
    _PENDING_EVENTS_DESTINATION_PICKER.pop(user_id, None)


def _is_alive(created_at: float | None) -> bool:
    if not created_at:
        return False
    return (time.time() - created_at) <= _PENDING_TTL_SECONDS


def _execute_admin_action(actor_id: int, action_code: str, *, current_chat_id: str) -> str:
    if action_code == "events_show_channel":
        current = CouncilSystemEventsService.get_channel("telegram")
        status_text = (
            f"✅ Сейчас выбран чат `{current}` для системных уведомлений Совета."
            if current
            else "ℹ️ Канал системных уведомлений Совета пока не настроен."
        )
        return render_admin_action_result(action_code, custom_result=status_text)
    if action_code == "events_set_channel_here":
        result = CouncilSystemEventsService.set_channel(
            provider="telegram",
            actor_user_id=str(actor_id),
            destination_id=current_chat_id,
        )
        return render_admin_action_result(
            action_code,
            custom_result=str(result.get("message") or ("✅ Канал уведомлений сохранён." if result.get("ok") else "❌ Не удалось сохранить канал уведомлений.")),
        )
    if action_code == "events_clear_channel":
        result = CouncilSystemEventsService.set_channel(
            provider="telegram",
            actor_user_id=str(actor_id),
            destination_id="",
        )
        return render_admin_action_result(
            action_code,
            custom_result=str(result.get("message") or ("✅ Канал уведомлений очищен." if result.get("ok") else "❌ Не удалось очистить канал уведомлений.")),
        )
    logger.info("telegram proposal admin lifecycle action selected actor_id=%s action=%s", actor_id, action_code)
    return render_admin_action_result(action_code)


async def _collect_writable_telegram_destinations(bot) -> list[GuiyPublishDestination]:
    destinations = GuiyPublishDestinationsService.list_telegram_destinations()
    if not destinations:
        return []
    try:
        bot_user = await bot.get_me()
    except Exception:
        logger.exception("telegram proposal events failed to resolve bot identity")
        return []

    writable: list[GuiyPublishDestination] = []
    for destination in destinations:
        destination_id = str(destination.destination_id or "").strip()
        if not destination_id:
            continue
        try:
            member = await bot.get_chat_member(int(destination_id), bot_user.id)
        except Exception:
            logger.exception("telegram proposal events destination access lookup failed destination_id=%s", destination_id)
            continue
        status = str(getattr(member, "status", "") or "").strip()
        if status in {"left", "kicked"}:
            GuiyPublishDestinationsService.mark_telegram_chat_inactive(destination_id, reason=f"status={status}")
            logger.warning("telegram proposal events destination skipped: bot not in chat destination_id=%s", destination_id)
            continue
        if getattr(member, "can_send_messages", None) is False:
            logger.warning("telegram proposal events destination skipped: missing send permission destination_id=%s", destination_id)
            continue
        writable.append(destination)
    return writable


def _events_picker_page(destinations: list[GuiyPublishDestination], page: int) -> tuple[int, int, list[GuiyPublishDestination]]:
    total_pages = max((len(destinations) - 1) // _TELEGRAM_EVENTS_DESTINATIONS_PAGE_SIZE + 1, 1)
    safe_page = min(max(page, 0), total_pages - 1)
    start = safe_page * _TELEGRAM_EVENTS_DESTINATIONS_PAGE_SIZE
    return safe_page, total_pages, destinations[start : start + _TELEGRAM_EVENTS_DESTINATIONS_PAGE_SIZE]


def _events_picker_text(destinations: list[GuiyPublishDestination], page: int) -> str:
    if not destinations:
        return (
            "⚠️ <b>Нет доступных чатов и каналов</b>\n"
            "Бот пока не нашёл чаты, где он может отправлять сообщения.\n"
            "Проверьте, что бот добавлен в нужный чат и ему разрешено писать."
        )
    safe_page, total_pages, _items = _events_picker_page(destinations, page)
    return (
        "⚙️ <b>Канал и чат уведомлений</b>\n\n"
        "Выберите чат или канал из списка, куда бот будет отправлять системные события Совета.\n"
        f"Страница: <b>{safe_page + 1}/{total_pages}</b>"
    )


def _events_picker_keyboard(destinations: list[GuiyPublishDestination], page: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    safe_page, total_pages, items = _events_picker_page(destinations, page)
    for item in items:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📍 {item.display_label[:48]}",
                    callback_data=f"proposal:events_choose:{item.destination_id}",
                )
            ]
        )
    navigation: list[InlineKeyboardButton] = []
    if safe_page > 0:
        navigation.append(InlineKeyboardButton(text="⬅️", callback_data=f"proposal:events_page:{safe_page - 1}"))
    if safe_page + 1 < total_pages:
        navigation.append(InlineKeyboardButton(text="➡️", callback_data=f"proposal:events_page:{safe_page + 1}"))
    if navigation:
        rows.append(navigation)
    rows.append([InlineKeyboardButton(text="↩️ Отмена", callback_data="proposal:events_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _events_pick_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Сохранить", callback_data="proposal:events_save")],
            [InlineKeyboardButton(text="↩️ Отмена", callback_data="proposal:events_cancel")],
        ]
    )


@router.message(Command("proposal"))
async def proposal_command(message: Message) -> None:
    try:
        if not message.from_user:
            await message.answer("❌ Не удалось определить пользователя.")
            return
        is_superadmin = AuthorityService.is_super_admin("telegram", str(message.from_user.id))
        _cleanup_pending(message.from_user.id)
        await message.answer(
            "🗂 <b>Меню предложений</b>\n"
            + render_menu_overview()
            + "\n\n"
            + render_menu_action_explanations(),
            reply_markup=_menu_keyboard(is_superadmin=is_superadmin),
            parse_mode="HTML",
        )
    except Exception:
        logger.exception("telegram proposal command failed actor_id=%s", getattr(message.from_user, "id", None))
        await message.answer("❌ Не удалось открыть меню предложений.")


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
            is_superadmin = AuthorityService.is_super_admin("telegram", str(actor_id))
            await callback.message.edit_text(
                "🗂 <b>Меню предложений</b>\n"
                + render_menu_overview()
                + "\n\n"
                + render_menu_action_explanations(),
                reply_markup=_menu_keyboard(is_superadmin=is_superadmin),
                parse_mode="HTML",
            )
            await callback.answer()
            return
        if action == "admin":
            if not AuthorityService.is_super_admin("telegram", str(actor_id)):
                await callback.answer("Доступно только суперадмину.", show_alert=True)
                return
            _PENDING_ADMIN_CONFIRM.pop(actor_id, None)
            await callback.message.edit_text(
                render_admin_root_text(),
                reply_markup=_admin_root_keyboard(),
                parse_mode="HTML",
            )
            await callback.answer()
            return
        if action.startswith("admin_section:"):
            if not AuthorityService.is_super_admin("telegram", str(actor_id)):
                await callback.answer("Доступно только суперадмину.", show_alert=True)
                return
            section_code = action.split(":", 1)[1]
            _PENDING_ADMIN_CONFIRM.pop(actor_id, None)
            await callback.message.edit_text(
                render_admin_section_text(section_code),
                reply_markup=_admin_section_keyboard(section_code),
                parse_mode="HTML",
            )
            await callback.answer()
            return
        if action.startswith("admin_action:"):
            if not AuthorityService.is_super_admin("telegram", str(actor_id)):
                await callback.answer("Доступно только суперадмину.", show_alert=True)
                return
            action_code = action.split(":", 1)[1]
            admin_action = PROPOSAL_ADMIN_ACTION_BY_CODE.get(action_code)
            if not admin_action:
                await callback.answer("Действие не найдено.", show_alert=True)
                return
            if admin_action.requires_confirmation:
                _PENDING_ADMIN_CONFIRM[actor_id] = action_code
                section = next((item for item in PROPOSAL_ADMIN_SECTIONS if any(action.code == action_code for action in item.actions)), None)
                section_code = section.code if section else "events"
                await callback.message.edit_text(
                    render_admin_confirm_text(action_code),
                    parse_mode="HTML",
                    reply_markup=_admin_confirm_keyboard(action_code, section_code),
                )
                await callback.answer()
                return
            if action_code == "events_set_channel_here":
                destinations = await _collect_writable_telegram_destinations(callback.message.bot)
                _PENDING_EVENTS_DESTINATION_PICKER[actor_id] = {
                    "destinations": destinations,
                    "page": 0,
                    "selected_destination_id": None,
                }
                await callback.message.edit_text(
                    _events_picker_text(destinations, 0),
                    parse_mode="HTML",
                    reply_markup=_events_picker_keyboard(destinations, 0),
                )
                await callback.answer()
                return
            result_text = _execute_admin_action(
                actor_id,
                action_code,
                current_chat_id=str(getattr(callback.message.chat, "id", "") or ""),
            )
            section = next((item for item in PROPOSAL_ADMIN_SECTIONS if any(action.code == action_code for action in item.actions)), None)
            section_code = section.code if section else "events"
            await callback.message.edit_text(
                result_text,
                parse_mode="HTML",
                reply_markup=_admin_section_keyboard(section_code),
            )
            await callback.answer()
            return
        if action.startswith("admin_confirm:"):
            if not AuthorityService.is_super_admin("telegram", str(actor_id)):
                await callback.answer("Доступно только суперадмину.", show_alert=True)
                return
            action_code = action.split(":", 1)[1]
            pending = _PENDING_ADMIN_CONFIRM.get(actor_id)
            if pending != action_code:
                await callback.answer("Подтверждение устарело. Откройте действие снова.", show_alert=True)
                return
            _PENDING_ADMIN_CONFIRM.pop(actor_id, None)
            result_text = _execute_admin_action(
                actor_id,
                action_code,
                current_chat_id=str(getattr(callback.message.chat, "id", "") or ""),
            )
            section = next((item for item in PROPOSAL_ADMIN_SECTIONS if any(action.code == action_code for action in item.actions)), None)
            section_code = section.code if section else "events"
            await callback.message.edit_text(
                result_text,
                parse_mode="HTML",
                reply_markup=_admin_section_keyboard(section_code),
            )
            await callback.answer()
            return
        if action.startswith("events_page:"):
            if not AuthorityService.is_super_admin("telegram", str(actor_id)):
                await callback.answer("Доступно только суперадмину.", show_alert=True)
                return
            pending = _PENDING_EVENTS_DESTINATION_PICKER.get(actor_id) or {}
            destinations = list(pending.get("destinations") or [])
            page_raw = action.split(":", 1)[1]
            try:
                page = int(page_raw)
            except ValueError:
                page = 0
            pending["page"] = page
            _PENDING_EVENTS_DESTINATION_PICKER[actor_id] = pending
            await callback.message.edit_text(
                _events_picker_text(destinations, page),
                parse_mode="HTML",
                reply_markup=_events_picker_keyboard(destinations, page),
            )
            await callback.answer()
            return
        if action.startswith("events_choose:"):
            if not AuthorityService.is_super_admin("telegram", str(actor_id)):
                await callback.answer("Доступно только суперадмину.", show_alert=True)
                return
            pending = _PENDING_EVENTS_DESTINATION_PICKER.get(actor_id)
            if not pending:
                await callback.answer("Список устарел. Откройте выбор заново.", show_alert=True)
                return
            destination_id = action.split(":", 1)[1]
            destinations = list(pending.get("destinations") or [])
            selected = next((item for item in destinations if item.destination_id == destination_id), None)
            if not selected:
                logger.warning("telegram proposal events destination no longer available actor_id=%s destination_id=%s", actor_id, destination_id)
                await callback.answer("Этот чат больше недоступен. Выберите другой.", show_alert=True)
                return
            pending["selected_destination_id"] = selected.destination_id
            _PENDING_EVENTS_DESTINATION_PICKER[actor_id] = pending
            await callback.message.edit_text(
                render_events_pick_confirmation_text(destination_label=selected.display_label),
                parse_mode="HTML",
                reply_markup=_events_pick_confirm_keyboard(),
            )
            await callback.answer()
            return
        if action == "events_save":
            if not AuthorityService.is_super_admin("telegram", str(actor_id)):
                await callback.answer("Доступно только суперадмину.", show_alert=True)
                return
            pending = _PENDING_EVENTS_DESTINATION_PICKER.get(actor_id)
            destination_id = str((pending or {}).get("selected_destination_id") or "").strip()
            if not destination_id:
                await callback.answer("Сначала выберите чат или канал.", show_alert=True)
                return
            result = CouncilSystemEventsService.set_channel(
                provider="telegram",
                actor_user_id=str(actor_id),
                destination_id=destination_id,
            )
            if not result.get("ok"):
                logger.error(
                    "telegram proposal events save failed actor_id=%s destination_id=%s message=%s",
                    actor_id,
                    destination_id,
                    result.get("message"),
                )
            _PENDING_EVENTS_DESTINATION_PICKER.pop(actor_id, None)
            result_text = render_admin_action_result(
                "events_set_channel_here",
                custom_result=str(result.get("message") or ("✅ Канал уведомлений сохранён." if result.get("ok") else "❌ Не удалось сохранить канал уведомлений.")),
            )
            await callback.message.edit_text(
                result_text,
                parse_mode="HTML",
                reply_markup=_admin_section_keyboard("events"),
            )
            await callback.answer()
            return
        if action == "events_cancel":
            _PENDING_EVENTS_DESTINATION_PICKER.pop(actor_id, None)
            await callback.message.edit_text(
                render_admin_section_text("events"),
                parse_mode="HTML",
                reply_markup=_admin_section_keyboard("events"),
            )
            await callback.answer()
            return

        if action == "submit":
            _PENDING_PROPOSAL_INPUT[actor_id] = time.time()
            _PENDING_PROPOSAL_CONFIRM.pop(actor_id, None)
            await callback.message.edit_text(
                render_submit_form_text(),
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
                logger.error(
                    "telegram proposal submit not ok actor_id=%s message=%s",
                    actor_id,
                    result.get("message"),
                )
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
                reply_markup=_menu_keyboard(is_superadmin=AuthorityService.is_super_admin("telegram", str(actor_id))),
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
                    f"<code>{status_parts['updated_at']}</code>\n\n"
                    f"{status_parts['next_step']}"
                )
            elif not payload.get("ok"):
                logger.error(
                    "telegram proposal status not ok actor_id=%s message=%s",
                    actor_id,
                    payload.get("message"),
                )
            await callback.message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=_menu_keyboard(is_superadmin=AuthorityService.is_super_admin("telegram", str(actor_id))),
            )
            await callback.answer()
            return

        if action == "archive":
            filters = _archive_filters(actor_id)
            rows = CouncilFeedbackService.get_decisions_archive(
                limit=5,
                period_code=filters["period_code"],
                status_code=filters["status_code"],
                question_type_code=filters["question_type_code"],
            )
            if not rows:
                text = (
                    f"📚 <b>{render_archive_empty_text().removeprefix('📚 ')}</b>\n\n"
                    + render_archive_filters_text(
                        period_code=filters["period_code"],
                        status_code=filters["status_code"],
                        question_type_code=filters["question_type_code"],
                    )
                )
            else:
                raw_lines = render_archive_lines(rows, text_limit=180)
                chunks = [
                    "📚 <b>Архив решений</b>",
                    render_archive_filters_text(
                        period_code=filters["period_code"],
                        status_code=filters["status_code"],
                        question_type_code=filters["question_type_code"],
                    ),
                ]
                for line in raw_lines:
                    chunks.append(line)
                text = "\n".join(chunks)
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=_archive_keyboard(actor_id))
            await callback.answer()
            return

        if action in {"archive_period", "archive_status", "archive_type"}:
            filters = _archive_filters(actor_id)
            if action == "archive_period":
                chain = ["30d", "90d", "365d", "all"]
                current = filters["period_code"]
                filters["period_code"] = chain[(chain.index(current) + 1) % len(chain)] if current in chain else chain[0]
            elif action == "archive_status":
                chain = ["all", "accepted", "rejected", "pending"]
                current = filters["status_code"]
                filters["status_code"] = chain[(chain.index(current) + 1) % len(chain)] if current in chain else chain[0]
            else:
                chain = ["all", "general", "election", "other"]
                current = filters["question_type_code"]
                filters["question_type_code"] = chain[(chain.index(current) + 1) % len(chain)] if current in chain else chain[0]
            _ARCHIVE_FILTERS_BY_USER[actor_id] = filters
            rows = CouncilFeedbackService.get_decisions_archive(
                limit=5,
                period_code=filters["period_code"],
                status_code=filters["status_code"],
                question_type_code=filters["question_type_code"],
            )
            if not rows:
                text = (
                    f"📚 <b>{render_archive_empty_text().removeprefix('📚 ')}</b>\n\n"
                    + render_archive_filters_text(
                        period_code=filters["period_code"],
                        status_code=filters["status_code"],
                        question_type_code=filters["question_type_code"],
                    )
                )
            else:
                raw_lines = render_archive_lines(rows, text_limit=180)
                text = "\n".join(
                    [
                        "📚 <b>Архив решений</b>",
                        render_archive_filters_text(
                            period_code=filters["period_code"],
                            status_code=filters["status_code"],
                            question_type_code=filters["question_type_code"],
                        ),
                        *raw_lines,
                    ]
                )
            await callback.message.edit_text(text, parse_mode="HTML", reply_markup=_archive_keyboard(actor_id))
            await callback.answer()
            return

        if action == "help":
            await callback.message.edit_text(
                render_help_text().replace("❓ Как пользоваться:", "❓ <b>Помощь</b>"),
                parse_mode="HTML",
                reply_markup=_menu_keyboard(is_superadmin=AuthorityService.is_super_admin("telegram", str(actor_id))),
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
            render_submit_review_text(title=pending.title, proposal_text=pending.proposal_text),
            parse_mode="HTML",
            reply_markup=_confirm_keyboard(),
        )
    except Exception:
        logger.exception("telegram proposal pending parse failed actor_id=%s", actor_id)
        await message.answer("❌ Не удалось обработать форму. Откройте «Подать предложение» ещё раз.")
