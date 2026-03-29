from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.services import AuthorityService, ModerationNotificationsService, ModerationService
from bot.systems.moderation_rep_ui import (
    render_rep_apply_error_text,
    render_rep_authority_deny_text,
    render_rep_cancelled_text,
    render_rep_duplicate_submit_text,
    render_rep_expired_text,
    render_rep_foreign_actor_text,
    render_rep_preview_text,
    render_rep_preview_failed_text,
    render_rep_result_text,
    render_rep_start_text,
    render_rep_target_not_found_text,
    render_rep_target_prompt_text,
    render_violator_notification_text,
    render_rep_violation_prompt_text,
)
from bot.telegram_bot.commands.roles_admin import _resolve_telegram_target, _telegram_user_lookup_hint
from bot.telegram_bot.identity import persist_telegram_identity_from_user

logger = logging.getLogger(__name__)
router = Router()
_PENDING_TTL_SECONDS = 300


@dataclass
class PendingRepState:
    step: str
    created_at: float
    payload: dict[str, Any] = field(default_factory=dict)
    is_applying: bool = False


_PENDING_REP: dict[int, PendingRepState] = {}
_MANUAL_DURATION_PRESETS: tuple[tuple[str, int], ...] = (("15м", 15), ("1ч", 60), ("12ч", 720), ("1д", 1440))


def _friendly_rep_error_text() -> str:
    return render_rep_apply_error_text()


def _is_pending_expired(state: PendingRepState) -> bool:
    return (time.time() - state.created_at) > _PENDING_TTL_SECONDS


def has_pending_rep_action(telegram_user_id: int | None) -> bool:
    if not telegram_user_id:
        return False
    pending = _PENDING_REP.get(telegram_user_id)
    if not pending:
        return False
    if _is_pending_expired(pending):
        _PENDING_REP.pop(telegram_user_id, None)
        return False
    return True


def _cancel_keyboard(actor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data=f"rep:{actor_id}:cancel")]])


def _target_keyboard(actor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data=f"rep:{actor_id}:cancel")],
        ]
    )


def _violations_keyboard(actor_id: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(text="🔧 Мут", callback_data=f"rep:{actor_id}:manual:mute")])
    rows.append([InlineKeyboardButton(text="🔧 Пред", callback_data=f"rep:{actor_id}:manual:warn")])
    rows.append([InlineKeyboardButton(text="🔧 Бан", callback_data=f"rep:{actor_id}:manual:ban")])
    rows.append([InlineKeyboardButton(text="🔧 Кик", callback_data=f"rep:{actor_id}:manual:kick")])
    rows.append([InlineKeyboardButton(text="📚 Нарушения из правил", callback_data=f"rep:{actor_id}:rules_menu")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data=f"rep:{actor_id}:back:target")])
    rows.append([InlineKeyboardButton(text="Отмена", callback_data=f"rep:{actor_id}:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _rules_keyboard(actor_id: int, violations: list[dict[str, Any]] | None = None, *, show_escalation: bool = False) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    source = violations if violations is not None else ModerationService.list_active_violation_types()
    for violation in source[:12]:
        code = str(violation.get("code") or "").strip()
        if not code:
            continue
        rows.append([InlineKeyboardButton(text=str(violation.get("title") or code), callback_data=f"rep:{actor_id}:violation:{code}")])
    rows.append([InlineKeyboardButton(text="⬅️ К действиям", callback_data=f"rep:{actor_id}:back:actions")])
    if show_escalation:
        rows.append([InlineKeyboardButton(text="📨 Заявка старшему админу", callback_data=f"rep:{actor_id}:escalate")])
    rows.append([InlineKeyboardButton(text="Отмена", callback_data=f"rep:{actor_id}:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _manual_duration_keyboard(actor_id: int, action_key: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton(text=f"Действие: {action_key}", callback_data=f"rep:{actor_id}:noop")])
    for title, minutes in _MANUAL_DURATION_PRESETS:
        rows.append([InlineKeyboardButton(text=title, callback_data=f"rep:{actor_id}:mdur:{action_key}:{minutes}")])
    rows.append([InlineKeyboardButton(text="Свой срок", callback_data=f"rep:{actor_id}:mdur_custom:{action_key}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data=f"rep:{actor_id}:back:violation")])
    rows.append([InlineKeyboardButton(text="Отмена", callback_data=f"rep:{actor_id}:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _parse_duration_minutes(raw: str) -> int:
    text = str(raw or "").strip().lower().replace(" ", "")
    if text.endswith("m") and text[:-1].isdigit():
        return int(text[:-1])
    if text.endswith("h") and text[:-1].isdigit():
        return int(text[:-1]) * 60
    if text.endswith("d") and text[:-1].isdigit():
        return int(text[:-1]) * 24 * 60
    if text.isdigit():
        return int(text)
    return 0


def _preview_keyboard(actor_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Подтвердить", callback_data=f"rep:{actor_id}:confirm")],
            [InlineKeyboardButton(text="Назад", callback_data=f"rep:{actor_id}:back:violation")],
            [InlineKeyboardButton(text="Отмена", callback_data=f"rep:{actor_id}:cancel")],
        ]
    )


def _log_rep(
    level: str,
    *,
    message: str,
    provider: str,
    chat_id: int | None,
    actor: int | None,
    actor_account_id: str | None,
    target: str | None,
    target_account_id: str | None,
    violation_code: str | None,
    selected_actions: list[str] | None,
    case_id: Any | None,
    error_code: str | None,
) -> None:
    log_method = getattr(logger, level)
    log_method(
        "%s provider=%s chat_id=%s actor=%s actor_account_id=%s target=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
        message,
        provider,
        chat_id,
        actor,
        actor_account_id,
        target,
        target_account_id,
        violation_code,
        list(selected_actions or []),
        case_id,
        error_code,
    )


def _start_text() -> str:
    return render_rep_start_text(
        target_selection_hint="reply на сообщение нарушителя; в личке — @username / username, lookup или id только как резерв.",
        compact=True,
    )


def _target_prompt_text(target_label: str | None = None) -> str:
    return render_rep_target_prompt_text(
        target_selection_hint=f"{_telegram_user_lookup_hint()}. Reply на сообщение нарушителя — самый быстрый и безопасный вариант.",
        target_label=target_label,
        compact=True,
    )


def _violation_prompt_text(target_label: str) -> str:
    return render_rep_violation_prompt_text(target_label=target_label, compact=True)


def _actions_menu_text(target_label: str, hidden: int) -> str:
    suffix = f"\n\n🔒 Скрыто нарушений по полномочиям: {hidden}" if hidden > 0 else ""
    return (
        _violation_prompt_text(target_label)
        + "\n\nВыберите 1 из 4 ручных действий или нажмите 5-ю кнопку «Нарушения из правил»."
        + suffix
    )


@router.message(Command("rep"))
async def rep_command(message: Message) -> None:
    if not message.from_user:
        return
    persist_telegram_identity_from_user(message.from_user)
    if not AuthorityService.has_command_permission("telegram", str(message.from_user.id), "moderation_mute"):
        _log_rep(
            "warning",
            message="rep authority deny",
            provider="telegram",
            chat_id=message.chat.id,
            actor=message.from_user.id,
            actor_account_id=None,
            target=None,
            target_account_id=None,
            violation_code=None,
            selected_actions=[],
            case_id=None,
            error_code="authority_denied",
        )
        await message.answer(f"❌ {render_rep_authority_deny_text('Команда /rep доступна только ролям модерации. Если доступ должен быть, проверьте authority и попробуйте ещё раз.')}")
        return
    pending = PendingRepState(step="await_target", created_at=time.time(), payload={"chat_id": message.chat.id})
    _PENDING_REP[message.from_user.id] = pending
    _log_rep(
        "info",
        message="rep start",
        provider="telegram",
        chat_id=message.chat.id,
        actor=message.from_user.id,
        actor_account_id=None,
        target=None,
        target_account_id=None,
        violation_code=None,
        selected_actions=[],
        case_id=None,
        error_code=None,
    )

    if message.reply_to_message and message.reply_to_message.from_user:
        persist_telegram_identity_from_user(message.reply_to_message.from_user)
        resolved = _resolve_telegram_target(
            actor_id=message.from_user.id,
            raw_target=None,
            reply_user=message.reply_to_message.from_user,
            operation="rep",
            source="group" if message.chat.type != "private" else "private",
        )
        if resolved and not resolved.get("error"):
            pending.step = "await_violation"
            pending.payload["target"] = dict(resolved)
            availability = ModerationService.list_available_violation_types(
                provider="telegram",
                actor={"provider": "telegram", "provider_user_id": str(message.from_user.id), "label": str(message.from_user.id)},
                target=resolved,
                chat_id=message.chat.id,
            )
            pending.payload["available_violations"] = list(availability.get("available") or [])
            pending.payload["unavailable_count"] = len(list(availability.get("unavailable") or []))
            pending.created_at = time.time()
            hidden = int(pending.payload.get("unavailable_count") or 0)
            await message.answer(
                _actions_menu_text(str(resolved.get("label") or "неизвестный пользователь"), hidden),
                reply_markup=_violations_keyboard(message.from_user.id),
            )
            return

    await message.answer(_start_text())
    await message.answer(_target_prompt_text(), reply_markup=_target_keyboard(message.from_user.id))


@router.callback_query(F.data.startswith("rep:"))
async def rep_callback(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.message:
        return
    parts = str(callback.data or "").split(":")
    owner_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    action = parts[2] if len(parts) > 2 else ""
    if callback.from_user.id != owner_id:
        await callback.answer(render_rep_foreign_actor_text(), show_alert=True)
        return
    pending = _PENDING_REP.get(callback.from_user.id)
    if action == "cancel":
        _PENDING_REP.pop(callback.from_user.id, None)
        await callback.message.edit_text(render_rep_cancelled_text(), reply_markup=None)
        await callback.answer()
        return
    if not pending or _is_pending_expired(pending):
        _PENDING_REP.pop(callback.from_user.id, None)
        await callback.answer(render_rep_expired_text(), show_alert=True)
        return
    if action == "noop":
        await callback.answer()
        return
    if action == "back":
        destination = parts[3] if len(parts) > 3 else "target"
        if destination == "target":
            pending.step = "await_target"
            pending.created_at = time.time()
            pending.payload.pop("target", None)
            pending.payload.pop("preview", None)
            pending.payload.pop("violation_code", None)
            await callback.message.edit_text(_target_prompt_text(), reply_markup=_target_keyboard(callback.from_user.id))
        elif destination == "violation":
            target = pending.payload.get("target") or {}
            pending.step = "await_violation"
            pending.created_at = time.time()
            pending.payload.pop("preview", None)
            pending.payload.pop("violation_code", None)
            await callback.message.edit_text(
                _violation_prompt_text(str(target.get("label") or "неизвестный пользователь")),
                reply_markup=_rules_keyboard(
                    callback.from_user.id,
                    pending.payload.get("available_violations"),
                    show_escalation=int(pending.payload.get("unavailable_count") or 0) > 0,
                ),
            )
        else:
            target = pending.payload.get("target") or {}
            pending.step = "await_violation"
            pending.created_at = time.time()
            pending.payload.pop("preview", None)
            pending.payload.pop("violation_code", None)
            hidden = int(pending.payload.get("unavailable_count") or 0)
            await callback.message.edit_text(
                _actions_menu_text(str(target.get("label") or "неизвестный пользователь"), hidden),
                reply_markup=_violations_keyboard(callback.from_user.id),
            )
        _PENDING_REP[callback.from_user.id] = pending
        await callback.answer()
        return
    if action == "escalate":
        target = pending.payload.get("target") or {}
        unavailable_count = int(pending.payload.get("unavailable_count") or 0)
        moderator_label = f"@{callback.from_user.username}" if callback.from_user.username else str(callback.from_user.id)
        text = (
            "📨 Заявка на недоступное наказание\n"
            f"Модератор: {moderator_label}"
            f"\nЦель: {target.get('label') or target.get('provider_user_id') or 'неизвестно'}"
            f"\nСкрытых нарушений по полномочиям: {unavailable_count}\n"
            "Нужен старший администратор для подтверждения."
        )
        try:
            await callback.message.answer(text)
            await callback.answer("✅ Заявка отправлена в чат.")
        except Exception:
            logger.exception(
                "telegram rep escalation request failed actor=%s target=%s chat_id=%s",
                callback.from_user.id,
                target.get("provider_user_id"),
                callback.message.chat.id,
            )
            await callback.answer("❌ Не удалось отправить заявку. Подробности в консоли.", show_alert=True)
        return
    if action == "rules_menu":
        target = pending.payload.get("target") or {}
        await callback.message.edit_text(
            _violation_prompt_text(str(target.get("label") or "неизвестный пользователь")),
            reply_markup=_rules_keyboard(
                callback.from_user.id,
                pending.payload.get("available_violations"),
                show_escalation=int(pending.payload.get("unavailable_count") or 0) > 0,
            ),
        )
        await callback.answer()
        return
    if action == "violation":
        code = parts[3] if len(parts) > 3 else ""
        payload = dict(pending.payload)
        target = payload.get("target")
        if not target:
            await callback.answer("Сначала выбери нарушителя.", show_alert=True)
            return
        try:
            preview = ModerationService.prepare_moderation_payload(
                "telegram",
                {"provider": "telegram", "provider_user_id": str(callback.from_user.id), "label": f"@{callback.from_user.username}" if callback.from_user.username else str(callback.from_user.id)},
                target,
                code,
                {"chat_id": callback.message.chat.id, "source_platform": "telegram", "reason_text": ""},
            )
        except Exception:
            _log_rep(
                "exception",
                message="rep preview failure",
                provider="telegram",
                chat_id=callback.message.chat.id,
                actor=callback.from_user.id,
                actor_account_id=None,
                target=str((target or {}).get("provider_user_id") or "") or None,
                target_account_id=str((target or {}).get("account_id") or "") or None,
                violation_code=code,
                selected_actions=[],
                case_id=None,
                error_code="preview_exception",
            )
            await callback.answer(render_rep_preview_failed_text(), show_alert=True)
            return
        if not preview.get("ok"):
            _log_rep(
                "warning",
                message="rep authority deny",
                provider="telegram",
                chat_id=callback.message.chat.id,
                actor=callback.from_user.id,
                actor_account_id=((preview.get("actor") or {}).get("account_id") if isinstance(preview.get("actor"), dict) else None),
                target=str((target or {}).get("provider_user_id") or "") or None,
                target_account_id=((preview.get("target") or {}).get("account_id") if isinstance(preview.get("target"), dict) else str((target or {}).get("account_id") or "") or None),
                violation_code=code,
                selected_actions=list(preview.get("selected_actions") or []),
                case_id=None,
                error_code=str(preview.get("error_code") or "preview_failed"),
            )
            await callback.answer(render_rep_authority_deny_text(preview.get("message") or "Действие сейчас недоступно."), show_alert=True)
            return
        payload["violation_code"] = code
        payload["preview"] = preview
        pending.step = "preview"
        pending.created_at = time.time()
        pending.payload = payload
        _PENDING_REP[callback.from_user.id] = pending
        ui_payload = preview["ui_payload"]
        _log_rep(
            "info",
            message="rep preview built",
            provider="telegram",
            chat_id=callback.message.chat.id,
            actor=callback.from_user.id,
            actor_account_id=preview["actor"].get("account_id"),
            target=preview["target"].get("provider_user_id"),
            target_account_id=preview["target"].get("account_id"),
            violation_code=code,
            selected_actions=list(ui_payload.get("selected_actions") or []),
            case_id=None,
            error_code=None,
        )
        await callback.message.edit_text(render_rep_preview_text(ui_payload, compact=True), reply_markup=_preview_keyboard(callback.from_user.id))
        await callback.answer()
        return
    if action == "manual":
        action_key = parts[3] if len(parts) > 3 else ""
        if action_key not in {"mute", "warn", "ban", "kick"}:
            await callback.answer("Неизвестный тип ручного наказания.", show_alert=True)
            return
        pending.step = "await_manual_duration"
        pending.payload["manual_action"] = action_key
        pending.created_at = time.time()
        _PENDING_REP[callback.from_user.id] = pending
        await callback.message.edit_text(
            "⏱️ Выберите быстрый срок или укажите свой.\n"
            "После этого бот попросит причину (обязательно).",
            reply_markup=_manual_duration_keyboard(callback.from_user.id, action_key),
        )
        await callback.answer()
        return
    if action == "mdur":
        action_key = parts[3] if len(parts) > 3 else ""
        minutes = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
        if minutes <= 0:
            await callback.answer("Некорректный срок.", show_alert=True)
            return
        pending.step = "await_manual_reason"
        pending.payload["manual_action"] = action_key
        pending.payload["manual_duration_minutes"] = minutes
        pending.created_at = time.time()
        _PENDING_REP[callback.from_user.id] = pending
        await callback.message.edit_text(
            f"📝 Укажите причину для `{action_key}` на {minutes} мин.\n"
            "Напишите одним сообщением. Без причины наказание не будет применено.",
            reply_markup=_cancel_keyboard(callback.from_user.id),
        )
        await callback.answer()
        return
    if action == "mdur_custom":
        action_key = parts[3] if len(parts) > 3 else ""
        pending.step = "await_manual_duration_custom"
        pending.payload["manual_action"] = action_key
        pending.created_at = time.time()
        _PENDING_REP[callback.from_user.id] = pending
        await callback.message.edit_text(
            "⌨️ Введите срок вручную (например: 30m, 2h, 1d).\n"
            "После этого бот попросит причину.",
            reply_markup=_cancel_keyboard(callback.from_user.id),
        )
        await callback.answer()
        return
    if action == "confirm":
        if pending.is_applying or pending.step == "done":
            await callback.answer(render_rep_duplicate_submit_text(), show_alert=True)
            return
        payload = dict(pending.payload)
        preview = payload.get("preview") or {}
        target = payload.get("target")
        violation_code = str(payload.get("violation_code") or "")
        pending.is_applying = True
        _PENDING_REP[callback.from_user.id] = pending
        try:
            preview_ui_payload = (preview.get("ui_payload") or {}) if isinstance(preview, dict) else {}
            result = ModerationService.commit_case(
                "telegram",
                {"provider": "telegram", "provider_user_id": str(callback.from_user.id), "label": f"@{callback.from_user.username}" if callback.from_user.username else str(callback.from_user.id)},
                target,
                violation_code,
                {
                    "chat_id": callback.message.chat.id,
                    "source_platform": "telegram",
                    "reason_text": "",
                    "moderation_op_key": (preview.get("moderation_op_key") if isinstance(preview, dict) else None) or preview_ui_payload.get("moderation_op_key"),
                },
            )
            if not result.get("ok"):
                pending.is_applying = False
                _PENDING_REP[callback.from_user.id] = pending
                _log_rep(
                    "warning",
                    message="rep apply failure",
                    provider="telegram",
                    chat_id=callback.message.chat.id,
                    actor=callback.from_user.id,
                    actor_account_id=((preview.get("actor") or {}).get("account_id") if isinstance(preview.get("actor"), dict) else None),
                    target=str((target or {}).get("provider_user_id") or "") or None,
                    target_account_id=((preview.get("target") or {}).get("account_id") if isinstance(preview.get("target"), dict) else str((target or {}).get("account_id") or "") or None),
                    violation_code=violation_code,
                    selected_actions=list(result.get("selected_actions") or []),
                    case_id=None,
                    error_code=str(result.get("error_code") or "apply_failed"),
                )
                await callback.answer(result.get("user_message") or result.get("message") or _friendly_rep_error_text(), show_alert=True)
                return
            ui_payload = result["ui_payload"]
            pending.step = "done"
            pending.payload["result"] = result
            pending.created_at = time.time()
            _PENDING_REP.pop(callback.from_user.id, None)
            _log_rep(
                "info",
                message="rep apply success",
                provider="telegram",
                chat_id=callback.message.chat.id,
                actor=callback.from_user.id,
                actor_account_id=result["actor"].get("account_id"),
                target=result["target"].get("provider_user_id"),
                target_account_id=result["target"].get("account_id"),
                violation_code=violation_code,
                selected_actions=list(ui_payload.get("selected_actions") or []),
                case_id=ui_payload.get("case_id"),
                error_code=None,
            )
            await callback.message.edit_text(render_rep_result_text(ui_payload, compact=True), reply_markup=None)
            try:
                selected_actions = set(ui_payload.get("selected_actions") or [])
                text = render_violator_notification_text(ui_payload)
                if "mute" in selected_actions:
                    await ModerationNotificationsService.dispatch_notification(
                        runtime_bot=callback.bot,
                        provider="telegram",
                        target_account_id=ui_payload.get("target_account_id"),
                        event_type=ModerationNotificationsService.EVENT_MUTE_STARTED,
                        message_text=text,
                        case_id=ui_payload.get("case_id"),
                        source_chat_id=callback.message.chat.id,
                        requires_chat_delivery=True,
                        allow_dm_delivery=True,
                    )
                if "fine_points" in selected_actions:
                    await ModerationNotificationsService.dispatch_notification(
                        runtime_bot=callback.bot,
                        provider="telegram",
                        target_account_id=ui_payload.get("target_account_id"),
                        event_type=ModerationNotificationsService.EVENT_FINE_CREATED,
                        message_text=text,
                        case_id=ui_payload.get("case_id"),
                        source_chat_id=callback.message.chat.id,
                        requires_chat_delivery=True,
                        allow_dm_delivery=True,
                    )
                if not selected_actions.intersection({"mute", "fine_points"}):
                    target_user_id = int(str((target or {}).get("provider_user_id") or "0") or 0)
                    if target_user_id:
                        await callback.bot.send_message(target_user_id, text)
            except Exception:
                _log_rep(
                    "exception",
                    message="rep target notify failed",
                    provider="telegram",
                    chat_id=callback.message.chat.id,
                    actor=callback.from_user.id,
                    actor_account_id=ui_payload.get("actor_account_id"),
                    target=str((target or {}).get("provider_user_id") or "") or None,
                    target_account_id=ui_payload.get("target_account_id"),
                    violation_code=ui_payload.get("violation_code"),
                    selected_actions=list(ui_payload.get("selected_actions") or []),
                    case_id=ui_payload.get("case_id"),
                    error_code="target_notify_failed",
                )
            await callback.answer()
            return
        except Exception:
            pending.is_applying = False
            _PENDING_REP[callback.from_user.id] = pending
            _log_rep(
                "exception",
                message="rep apply failure",
                provider="telegram",
                chat_id=callback.message.chat.id,
                actor=callback.from_user.id,
                actor_account_id=((preview.get("actor") or {}).get("account_id") if isinstance(preview.get("actor"), dict) else None),
                target=str((target or {}).get("provider_user_id") or "") or None,
                target_account_id=((preview.get("target") or {}).get("account_id") if isinstance(preview.get("target"), dict) else str((target or {}).get("account_id") or "") or None),
                violation_code=violation_code,
                selected_actions=list((((preview.get("ui_payload") or {}).get("selected_actions")) or [])),
                case_id=None,
                error_code="apply_exception",
            )
            await callback.answer(_friendly_rep_error_text(), show_alert=True)
            return


@router.message(F.from_user, F.from_user.id.func(has_pending_rep_action))
async def rep_pending_handler(message: Message) -> None:
    if not message.from_user:
        return
    pending = _PENDING_REP.get(message.from_user.id)
    if not pending or _is_pending_expired(pending):
        _PENDING_REP.pop(message.from_user.id, None)
        await message.answer(f"❌ {render_rep_expired_text()}")
        return
    if pending.step != "await_target":
        if pending.step == "await_manual_duration_custom":
            minutes = _parse_duration_minutes(str(message.text or ""))
            if minutes <= 0:
                await message.answer("❌ Неверный срок. Пример: 30m, 2h, 1d.")
                return
            pending.step = "await_manual_reason"
            pending.payload["manual_duration_minutes"] = minutes
            pending.created_at = time.time()
            _PENDING_REP[message.from_user.id] = pending
            await message.answer(
                f"📝 Укажите причину для `{pending.payload.get('manual_action')}` на {minutes} мин.\n"
                "Без причины наказание не применяется.",
                reply_markup=_cancel_keyboard(message.from_user.id),
            )
            return
        if pending.step == "await_manual_reason":
            reason_text = str(message.text or "").strip()
            action_key = str(pending.payload.get("manual_action") or "").strip()
            minutes = int(pending.payload.get("manual_duration_minutes") or 0)
            target = pending.payload.get("target")
            if not reason_text or not action_key or minutes <= 0 or not target:
                await message.answer("❌ Не удалось собрать данные (цель/срок/причина). Запустите /rep заново.")
                return
            result = ModerationService.commit_manual_action(
                "telegram",
                {"provider": "telegram", "provider_user_id": str(message.from_user.id), "label": f"@{message.from_user.username}" if message.from_user.username else str(message.from_user.id)},
                target,
                action_key,
                duration_minutes=minutes,
                reason_text=reason_text,
                context={"chat_id": message.chat.id, "source_platform": "telegram"},
            )
            if not result.get("ok"):
                logger.warning(
                    "telegram rep manual apply failed actor=%s target=%s action=%s error_code=%s",
                    message.from_user.id,
                    (target or {}).get("provider_user_id"),
                    action_key,
                    result.get("error_code"),
                )
                await message.answer(f"❌ {result.get('message') or _friendly_rep_error_text()}")
                return
            _PENDING_REP.pop(message.from_user.id, None)
            await message.answer(
                "✅ Ручное наказание применено.\n"
                f"Действие: {action_key}\n"
                f"Срок: {minutes} мин\n"
                f"Причина: {reason_text}"
            )
            return
        return
    if message.reply_to_message and message.reply_to_message.from_user:
        persist_telegram_identity_from_user(message.reply_to_message.from_user)
    resolved = _resolve_telegram_target(
        actor_id=message.from_user.id,
        raw_target=message.text,
        reply_user=message.reply_to_message.from_user if message.reply_to_message else None,
        operation="rep",
        source="group" if message.chat.type != "private" else "private",
    )
    if not resolved or resolved.get("error"):
        _log_rep(
            "warning",
            message="rep target resolve failed",
            provider="telegram",
            chat_id=message.chat.id,
            actor=message.from_user.id,
            actor_account_id=None,
            target=str(message.text or "") or None,
            target_account_id=None,
            violation_code=None,
            selected_actions=[],
            case_id=None,
            error_code="target_not_found",
        )
        await message.answer((resolved or {}).get("message") or render_rep_target_not_found_text(target_selection_hint="reply на сообщение нарушителя; в ЛС можно ввести @username / username"))
        return
    pending.step = "await_violation"
    pending.created_at = time.time()
    availability = ModerationService.list_available_violation_types(
        provider="telegram",
        actor={"provider": "telegram", "provider_user_id": str(message.from_user.id), "label": str(message.from_user.id)},
        target=resolved,
        chat_id=message.chat.id,
    )
    pending.payload = {
        "chat_id": message.chat.id,
        "target": dict(resolved),
        "available_violations": list(availability.get("available") or []),
        "unavailable_count": len(list(availability.get("unavailable") or [])),
    }
    _PENDING_REP[message.from_user.id] = pending
    _log_rep(
        "info",
        message="rep target selected",
        provider="telegram",
        chat_id=message.chat.id,
        actor=message.from_user.id,
        actor_account_id=None,
        target=str(resolved.get("provider_user_id") or "") or None,
        target_account_id=str(resolved.get("account_id") or "") or None,
        violation_code=None,
        selected_actions=[],
        case_id=None,
        error_code=None,
    )
    hidden = int(pending.payload.get("unavailable_count") or 0)
    await message.answer(
        _actions_menu_text(str(resolved.get("label") or "неизвестный пользователь"), hidden),
        reply_markup=_violations_keyboard(message.from_user.id),
    )
