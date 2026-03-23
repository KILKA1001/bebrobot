from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.services import AuthorityService, ModerationService
from bot.systems.moderation_rep_ui import (
    render_rep_apply_error_text,
    render_rep_cancelled_text,
    render_rep_duplicate_submit_text,
    render_rep_expired_text,
    render_rep_foreign_actor_text,
    render_rep_preview_text,
    render_rep_result_text,
    render_rep_start_text,
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
    for violation in ModerationService.list_active_violation_types()[:12]:
        code = str(violation.get("code") or "").strip()
        if not code:
            continue
        rows.append([InlineKeyboardButton(text=str(violation.get("title") or code), callback_data=f"rep:{actor_id}:violation:{code}")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data=f"rep:{actor_id}:back:target")])
    rows.append([InlineKeyboardButton(text="Отмена", callback_data=f"rep:{actor_id}:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
        target_selection_hint="reply на сообщение нарушителя; в личке можно использовать @username / username, lookup или id как резерв."
    )


def _target_prompt_text(target_label: str | None = None) -> str:
    return render_rep_target_prompt_text(
        target_selection_hint=f"{_telegram_user_lookup_hint()}. Reply на сообщение нарушителя — самый быстрый и безопасный вариант.",
        target_label=target_label,
    )


def _violation_prompt_text(target_label: str) -> str:
    return render_rep_violation_prompt_text(target_label=target_label)


@router.message(Command("rep"))
async def rep_command(message: Message) -> None:
    if not message.from_user:
        return
    persist_telegram_identity_from_user(message.from_user)
    if not AuthorityService.has_command_permission("telegram", str(message.from_user.id), "moderation_mute"):
        await message.answer("❌ Команда /rep доступна только ролям модерации. Если доступ должен быть, проверьте authority и попробуйте ещё раз.")
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
            pending.created_at = time.time()
            await message.answer(_violation_prompt_text(str(resolved.get("label") or "неизвестный пользователь")), reply_markup=_violations_keyboard(message.from_user.id))
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
    if action == "back":
        destination = parts[3] if len(parts) > 3 else "target"
        if destination == "target":
            pending.step = "await_target"
            pending.created_at = time.time()
            pending.payload.pop("target", None)
            pending.payload.pop("preview", None)
            pending.payload.pop("violation_code", None)
            await callback.message.edit_text(_target_prompt_text(), reply_markup=_target_keyboard(callback.from_user.id))
        else:
            target = pending.payload.get("target") or {}
            pending.step = "await_violation"
            pending.created_at = time.time()
            pending.payload.pop("preview", None)
            pending.payload.pop("violation_code", None)
            await callback.message.edit_text(_violation_prompt_text(str(target.get("label") or "неизвестный пользователь")), reply_markup=_violations_keyboard(callback.from_user.id))
        _PENDING_REP[callback.from_user.id] = pending
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
            await callback.answer(_friendly_rep_error_text(), show_alert=True)
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
            await callback.answer(preview.get("message") or "Действие сейчас недоступно.", show_alert=True)
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
        await callback.message.edit_text(render_rep_preview_text(ui_payload), reply_markup=_preview_keyboard(callback.from_user.id))
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
            await callback.message.edit_text(render_rep_result_text(ui_payload), reply_markup=None)
            target_user_id = int(str((target or {}).get("provider_user_id") or "0") or 0)
            if target_user_id:
                try:
                    await callback.bot.send_message(target_user_id, render_violator_notification_text(ui_payload))
                except Exception:
                    _log_rep(
                        "exception",
                        message="rep target notify failed",
                        provider="telegram",
                        chat_id=callback.message.chat.id,
                        actor=callback.from_user.id,
                        actor_account_id=ui_payload.get("actor_account_id"),
                        target=str(target_user_id),
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
        await message.answer((resolved or {}).get("message") or "❌ Не удалось определить нарушителя. Попробуй ещё раз.")
        return
    pending.step = "await_violation"
    pending.created_at = time.time()
    pending.payload = {"chat_id": message.chat.id, "target": dict(resolved)}
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
    await message.answer(_violation_prompt_text(str(resolved.get("label") or "неизвестный пользователь")), reply_markup=_violations_keyboard(message.from_user.id))
