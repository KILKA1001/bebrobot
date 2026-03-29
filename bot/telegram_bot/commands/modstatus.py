from __future__ import annotations

import logging
from typing import Any

from aiogram import Router
from aiogram import F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, ChatPermissions, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.services import AccountsService, AuthorityService, ModerationNotificationsService, ModerationService
from bot.telegram_bot.commands.fines import send_legacy_fines_panel
from bot.telegram_bot.commands.roles_admin import _resolve_telegram_target
from bot.telegram_bot.identity import persist_telegram_identity_from_user

logger = logging.getLogger(__name__)
router = Router()
_PAYMENT_HINT = ModerationService.MODSTATUS_PAYMENT_HINT
_OPEN_LEGACY_FINES_CALLBACK = "modstatus:open_legacy_fines"
_ROLLBACK_CALLBACK = "modstatus:rollback"
_ROLLBACK_SELECT_CALLBACK = "modstatus:rollback_select"


def _snapshot_has_payable_manual_fines(snapshot: dict[str, Any]) -> bool:
    for fine in list(snapshot.get("active_fines") or []):
        kind = str(fine.get("kind") or "").strip().lower()
        if kind == "legacy_fine":
            return True
        if kind != "case_fine":
            continue
        payment_mode = str(
            fine.get("payment_mode") or ModerationService.FINE_PAYMENT_MODE_MANUAL
        ).strip().lower()
        if payment_mode != ModerationService.FINE_PAYMENT_MODE_INSTANT:
            return True
    return False

_TELEGRAM_UNMUTE_PERMISSIONS = ChatPermissions(
    can_send_messages=True,
    can_send_audios=True,
    can_send_documents=True,
    can_send_photos=True,
    can_send_videos=True,
    can_send_video_notes=True,
    can_send_voice_notes=True,
    can_send_polls=True,
    can_send_other_messages=True,
    can_add_web_page_previews=True,
    can_change_info=False,
    can_invite_users=True,
    can_pin_messages=False,
    can_manage_topics=False,
)


async def _rollback_telegram_runtime_sanctions(
    *,
    callback: CallbackQuery,
    target_user_id: str,
    rollback_result: dict[str, Any],
) -> None:
    chat_id = callback.message.chat.id if callback.message else None
    actor_id = callback.from_user.id if callback.from_user else None
    normalized_target_user_id = int(str(target_user_id or "0") or 0)
    if not chat_id or not normalized_target_user_id:
        logger.error(
            "telegram modstatus rollback runtime skipped reason=%s actor_id=%s target_id=%s chat_id=%s case_id=%s",
            "invalid_target_or_chat",
            actor_id,
            target_user_id,
            chat_id,
            rollback_result.get("case_id"),
        )
        return

    if rollback_result.get("had_mute"):
        try:
            await callback.bot.restrict_chat_member(
                chat_id=chat_id,
                user_id=normalized_target_user_id,
                permissions=_TELEGRAM_UNMUTE_PERMISSIONS,
                use_independent_chat_permissions=False,
            )
            logger.info(
                "telegram modstatus rollback runtime mute removed actor_id=%s target_id=%s chat_id=%s case_id=%s",
                actor_id,
                normalized_target_user_id,
                chat_id,
                rollback_result.get("case_id"),
            )
        except Exception:
            logger.exception(
                "telegram modstatus rollback runtime unmute failed actor_id=%s target_id=%s chat_id=%s case_id=%s",
                actor_id,
                normalized_target_user_id,
                chat_id,
                rollback_result.get("case_id"),
            )

    if rollback_result.get("had_ban_or_kick"):
        try:
            await callback.bot.unban_chat_member(
                chat_id=chat_id,
                user_id=normalized_target_user_id,
                only_if_banned=True,
            )
            logger.info(
                "telegram modstatus rollback runtime unban success actor_id=%s target_id=%s chat_id=%s case_id=%s",
                actor_id,
                normalized_target_user_id,
                chat_id,
                rollback_result.get("case_id"),
            )
        except Exception:
            logger.exception(
                "telegram modstatus rollback runtime unban failed actor_id=%s target_id=%s chat_id=%s case_id=%s",
                actor_id,
                normalized_target_user_id,
                chat_id,
                rollback_result.get("case_id"),
            )


@router.message(Command("modstatus"))
async def modstatus_command(message: Message) -> None:
    if not message.from_user:
        return
    persist_telegram_identity_from_user(message.from_user)
    chat_id = message.chat.id
    viewer_id = str(message.from_user.id)
    logger.info(
        "ux_screen_open event=ux_screen_open screen=modstatus provider=telegram actor_user_id=%s chat_id=%s",
        viewer_id,
        chat_id,
    )
    viewer_account_id = AccountsService.resolve_account_id("telegram", viewer_id)
    if not viewer_account_id:
        logger.warning(
            "telegram modstatus viewer unresolved provider=%s chat_id=%s viewer_id=%s target_id=%s account_id=%s",
            "telegram",
            chat_id,
            viewer_id,
            None,
            None,
        )
        await message.answer(
            "❌ Общий профиль пока не найден.\n"
            "Что делать сейчас: откройте /register или /link в личном чате с ботом.\n"
            "Что будет дальше: после привязки откроется экран /modstatus."
        )
        return

    target_subject: dict[str, Any] | None = None
    selected_via_reply = False
    explicit_target = False
    try:
        command_text = str(message.text or "").strip()
        raw_target = command_text.split(maxsplit=1)[1].strip() if len(command_text.split(maxsplit=1)) > 1 else ""
        allow_lookup_others = AuthorityService.has_command_permission("telegram", viewer_id, "moderation_view_cases")

        if message.reply_to_message and message.reply_to_message.from_user and not message.reply_to_message.from_user.is_bot:
            persist_telegram_identity_from_user(message.reply_to_message.from_user)
            target_subject = _resolve_telegram_target(
                actor_id=message.from_user.id,
                raw_target=None,
                reply_user=message.reply_to_message.from_user,
                operation="modstatus",
                source="group" if message.chat.type != "private" else "private",
            )
            selected_via_reply = True
            explicit_target = True
        elif raw_target:
            explicit_target = True
            if message.chat.type != "private" and raw_target not in {viewer_id, f"@{message.from_user.username}" if message.from_user.username else ""}:
                logger.warning(
                    "telegram modstatus non-reply foreign lookup provider=%s chat_id=%s viewer_id=%s target_id=%s account_id=%s",
                    "telegram",
                    chat_id,
                    viewer_id,
                    raw_target,
                    viewer_account_id,
                )
            target_subject = _resolve_telegram_target(
                actor_id=message.from_user.id,
                raw_target=raw_target,
                reply_user=None,
                operation="modstatus",
                source="group" if message.chat.type != "private" else "private",
            )
            if target_subject and target_subject.get("error"):
                logger.warning(
                    "telegram modstatus target resolve failed provider=%s chat_id=%s viewer_id=%s target_id=%s account_id=%s",
                    "telegram",
                    chat_id,
                    viewer_id,
                    raw_target,
                    viewer_account_id,
                )
                await message.answer(str(target_subject.get("message") or "❌ Не удалось найти пользователя."))
                return

        target_account_id = str((target_subject or {}).get("account_id") or "").strip() or str(viewer_account_id)
        snapshot = ModerationService.get_user_moderation_snapshot(
            target_account_id,
            str(viewer_account_id),
            "telegram",
            chat_id,
            {
                "viewer_id": viewer_id,
                "target_id": (target_subject or {}).get("provider_user_id") or viewer_id,
                "selected_via_reply": selected_via_reply,
                "explicit_target": explicit_target,
                "allow_lookup_others": allow_lookup_others,
                "is_private": message.chat.type == "private",
            },
        )
        if not snapshot.get("ok"):
            logger.warning(
                "telegram modstatus snapshot denied provider=%s chat_id=%s viewer_id=%s target_id=%s account_id=%s error_code=%s",
                "telegram",
                chat_id,
                viewer_id,
                (target_subject or {}).get("provider_user_id") or viewer_id,
                target_account_id,
                snapshot.get("error_code"),
            )
            await message.answer(f"❌ {snapshot.get('message') or 'Не удалось загрузить модерационный статус.'}")
            return

        can_open_payment = bool(snapshot.get("target_is_self")) and _snapshot_has_payable_manual_fines(snapshot)
        can_rollback = (
            target_subject
            and str((target_subject or {}).get("provider_user_id") or "").strip()
            and str((target_subject or {}).get("provider_user_id")).strip().lower() not in {"none", "null"}
            and AuthorityService.has_command_permission("telegram", viewer_id, "moderation_mute")
        )
        reply_rows: list[list[InlineKeyboardButton]] = []
        if can_open_payment:
            reply_rows.append([InlineKeyboardButton(text="💳 Оплатить штраф", callback_data=_OPEN_LEGACY_FINES_CALLBACK)])
        if can_rollback:
            callback = f"{_ROLLBACK_SELECT_CALLBACK}:{str(target_subject.get('provider_user_id')).strip()}"
            reply_rows.append([InlineKeyboardButton(text="🧹 Убрать наказание", callback_data=callback)])
        reply_markup = InlineKeyboardMarkup(inline_keyboard=reply_rows) if reply_rows else None
        await message.answer(
            ModerationService.render_user_moderation_snapshot(snapshot, payment_hint=_PAYMENT_HINT),
            reply_markup=reply_markup,
        )
    except Exception:
        logger.exception(
            "telegram modstatus command failed provider=%s chat_id=%s viewer_id=%s target_id=%s account_id=%s",
            "telegram",
            chat_id,
            viewer_id,
            (target_subject or {}).get("provider_user_id") if target_subject else None,
            (target_subject or {}).get("account_id") if target_subject else viewer_account_id,
        )
        await message.answer("❌ Не удалось загрузить модерационный статус. Подробности записаны в консоль.")


@router.callback_query(F.data == _OPEN_LEGACY_FINES_CALLBACK)
async def modstatus_open_legacy_fines(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.message:
        return
    try:
        actor_id = str(callback.from_user.id)
        actor_account_id = AccountsService.resolve_account_id("telegram", actor_id)
        if not actor_account_id:
            logger.warning(
                "telegram modstatus payment callback denied reason=%s actor_id=%s chat_id=%s",
                "account_unresolved",
                callback.from_user.id,
                callback.message.chat.id,
            )
            await callback.answer("❌ Сначала привяжите общий аккаунт.", show_alert=True)
            return
        snapshot = ModerationService.get_user_moderation_snapshot(
            str(actor_account_id),
            str(actor_account_id),
            "telegram",
            callback.message.chat.id,
            {
                "viewer_id": actor_id,
                "target_id": actor_id,
                "selected_via_reply": False,
                "explicit_target": False,
                "allow_lookup_others": False,
                "is_private": callback.message.chat.type == "private",
            },
        )
        if (not snapshot.get("ok")) or (not snapshot.get("target_is_self")) or (not _snapshot_has_payable_manual_fines(snapshot)):
            logger.warning(
                "telegram modstatus payment callback denied reason=%s actor_id=%s chat_id=%s snapshot_ok=%s target_is_self=%s",
                "snapshot_not_payable",
                callback.from_user.id,
                callback.message.chat.id,
                snapshot.get("ok"),
                snapshot.get("target_is_self"),
            )
            await callback.answer("❌ Нет доступных штрафов для ручной оплаты.", show_alert=True)
            return
        await send_legacy_fines_panel(message=callback.message, telegram_user_id=int(callback.from_user.id))
        await callback.answer()
    except Exception:
        logger.exception(
            "telegram modstatus open legacy fines failed provider=%s chat_id=%s viewer_id=%s",
            "telegram",
            callback.message.chat.id,
            callback.from_user.id,
        )
        await callback.answer("❌ Не удалось открыть панель оплаты. Подробности в консоли.", show_alert=True)


@router.callback_query(F.data.startswith(f"{_ROLLBACK_CALLBACK}:"))
async def modstatus_rollback_case(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.message:
        return
    try:
        raw_data = str(callback.data or "").strip()
        prefix = f"{_ROLLBACK_CALLBACK}:"
        payload = raw_data[len(prefix):].strip() if raw_data.startswith(prefix) else ""
        parts = payload.split(":", maxsplit=1)
        target_user_id = str(parts[0] or "").strip()
        case_id = str(parts[1] or "").strip() if len(parts) > 1 else ""
    except Exception:
        await callback.answer("❌ Некорректные данные кнопки.", show_alert=True)
        return
    if not target_user_id or target_user_id.lower() in {"none", "null"}:
        await callback.answer("❌ Не удалось определить цель для отката.", show_alert=True)
        return
    if not AuthorityService.has_command_permission("telegram", str(callback.from_user.id), "moderation_mute"):
        logger.warning(
            "telegram modstatus rollback denied reason=%s actor_id=%s target_id=%s chat_id=%s",
            "no_permission",
            callback.from_user.id,
            target_user_id,
            callback.message.chat.id,
        )
        await callback.answer("❌ Недостаточно прав для отката наказания.", show_alert=True)
        return
    target_account_id = AccountsService.resolve_account_id("telegram", target_user_id)
    if not target_account_id:
        logger.warning(
            "telegram modstatus rollback denied reason=%s actor_id=%s target_id=%s chat_id=%s",
            "target_account_unresolved",
            callback.from_user.id,
            target_user_id,
            callback.message.chat.id,
        )
        await callback.answer("❌ Цель не привязана к общему аккаунту.", show_alert=True)
        return
    valid_case_ids = {
        str((item.get("case") or {}).get("id") or "").strip()
        for item in list(ModerationService.list_recent_cases(str(target_account_id), limit=10).get("items") or [])
        if str((item.get("case") or {}).get("status") or "").strip().lower() == ModerationService.STATUS_APPLIED
    }
    if case_id and case_id not in valid_case_ids:
        logger.warning(
            "telegram modstatus rollback denied reason=%s actor_id=%s target_id=%s case_id=%s chat_id=%s",
            "invalid_case_selection",
            callback.from_user.id,
            target_user_id,
            case_id,
            callback.message.chat.id,
        )
        await callback.answer("❌ Выбранный кейс недоступен для отката.", show_alert=True)
        return
    try:
        result = ModerationService.rollback_latest_case(
            "telegram",
            {"provider": "telegram", "provider_user_id": str(callback.from_user.id), "label": f"@{callback.from_user.username}" if callback.from_user.username else str(callback.from_user.id)},
            {"provider": "telegram", "provider_user_id": target_user_id, "label": target_user_id},
            chat_id=callback.message.chat.id,
            case_id=case_id or None,
        )
    except Exception:
        logger.exception("telegram modstatus rollback failed actor_id=%s target_id=%s", callback.from_user.id, target_user_id)
        await callback.answer("❌ Не удалось снять наказание. Подробности в консоли.", show_alert=True)
        return
    if not result.get("ok"):
        await callback.answer(str(result.get("message") or "Не удалось снять наказание."), show_alert=True)
        return
    await _rollback_telegram_runtime_sanctions(
        callback=callback,
        target_user_id=target_user_id,
        rollback_result=result,
    )
    await callback.answer("✅ Наказание снято.", show_alert=True)
    if result.get("had_ban_or_kick"):
        text = (
            "ℹ️ Предыдущее наказание (бан/кик) снято как ошибочное. "
            "Вы можете заново зайти в чат по ссылке-приглашению группы."
        )
        try:
            await ModerationNotificationsService.dispatch_notification(
                runtime_bot=callback.bot,
                provider="telegram",
                target_account_id=(result.get("target") or {}).get("account_id"),
                event_type="punishment_revoked",
                message_text=text,
                case_id=result.get("case_id"),
                source_chat_id=callback.message.chat.id,
                requires_chat_delivery=False,
                allow_dm_delivery=True,
            )
        except Exception:
            logger.exception("telegram modstatus rollback notify failed case_id=%s", result.get("case_id"))


@router.callback_query(F.data.startswith(f"{_ROLLBACK_SELECT_CALLBACK}:"))
async def modstatus_rollback_select_case(callback: CallbackQuery) -> None:
    if not callback.from_user or not callback.message:
        return
    raw_data = str(callback.data or "").strip()
    prefix = f"{_ROLLBACK_SELECT_CALLBACK}:"
    target_user_id = raw_data[len(prefix):].strip() if raw_data.startswith(prefix) else ""
    if not target_user_id or target_user_id.lower() in {"none", "null"}:
        await callback.answer("❌ Не удалось определить цель для отката.", show_alert=True)
        return
    if not AuthorityService.has_command_permission("telegram", str(callback.from_user.id), "moderation_mute"):
        logger.warning(
            "telegram modstatus rollback select denied reason=%s actor_id=%s target_id=%s chat_id=%s",
            "no_permission",
            callback.from_user.id,
            target_user_id,
            callback.message.chat.id,
        )
        await callback.answer("❌ Недостаточно прав для отката наказания.", show_alert=True)
        return
    target_account_id = AccountsService.resolve_account_id("telegram", target_user_id)
    if not target_account_id:
        await callback.answer("❌ Цель не привязана к общему аккаунту.", show_alert=True)
        return
    items = [
        item
        for item in list(ModerationService.list_recent_cases(str(target_account_id), limit=10).get("items") or [])
        if str((item.get("case") or {}).get("status") or "").strip().lower() == ModerationService.STATUS_APPLIED
    ]
    if not items:
        await callback.answer("❌ Нет активных кейсов для отката.", show_alert=True)
        return
    rows: list[list[InlineKeyboardButton]] = []
    for item in items[:8]:
        case_row = dict(item.get("case") or {})
        case_id = str(case_row.get("id") or "").strip()
        if not case_id:
            continue
        rows.append([InlineKeyboardButton(text=f"Кейс #{case_id}", callback_data=f"{_ROLLBACK_CALLBACK}:{target_user_id}:{case_id}")])
    rows.append([InlineKeyboardButton(text="Отмена", callback_data="noop")])
    await callback.message.answer(
        "Выберите конкретный кейс, который нужно снять:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer()


@router.callback_query(F.data == "noop")
async def modstatus_noop(callback: CallbackQuery) -> None:
    await callback.answer()
