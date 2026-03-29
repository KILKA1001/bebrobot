from __future__ import annotations

import logging
from typing import Any

from aiogram import Router
from aiogram import F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.services import AccountsService, AuthorityService, ModerationService
from bot.telegram_bot.commands.fines import send_legacy_fines_panel
from bot.telegram_bot.commands.roles_admin import _resolve_telegram_target
from bot.telegram_bot.identity import persist_telegram_identity_from_user

logger = logging.getLogger(__name__)
router = Router()
_PAYMENT_HINT = ModerationService.MODSTATUS_PAYMENT_HINT
_OPEN_LEGACY_FINES_CALLBACK = "modstatus:open_legacy_fines"
_ROLLBACK_CALLBACK = "modstatus:rollback"


@router.message(Command("modstatus"))
async def modstatus_command(message: Message) -> None:
    if not message.from_user:
        return
    persist_telegram_identity_from_user(message.from_user)
    chat_id = message.chat.id
    viewer_id = str(message.from_user.id)
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
        await message.answer("❌ Сначала привяжите общий аккаунт, затем повторите /modstatus.")
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

        reply_markup = None
        if snapshot.get("target_is_self") and list(snapshot.get("active_fines") or []):
            reply_markup = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="💳 Оплатить legacy-штраф", callback_data=_OPEN_LEGACY_FINES_CALLBACK)]
                ]
            )
        elif (
            target_subject
            and str((target_subject or {}).get("provider_user_id") or "").strip()
            and str((target_subject or {}).get("provider_user_id")).strip().lower() not in {"none", "null"}
            and AuthorityService.has_command_permission("telegram", viewer_id, "moderation_mute")
        ):
            callback = f"{_ROLLBACK_CALLBACK}:{str(target_subject.get('provider_user_id')).strip()}"
            reply_markup = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🧹 Убрать наказание", callback_data=callback)],
                ]
            )
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
        target_user_id = str(str(callback.data or "").split(":", maxsplit=1)[1]).strip()
    except Exception:
        await callback.answer("❌ Некорректные данные кнопки.", show_alert=True)
        return
    if not target_user_id or target_user_id.lower() in {"none", "null"}:
        await callback.answer("❌ Не удалось определить цель для отката.", show_alert=True)
        return
    if not AuthorityService.has_command_permission("telegram", str(callback.from_user.id), "moderation_mute"):
        await callback.answer("❌ Недостаточно прав для отката наказания.", show_alert=True)
        return
    try:
        result = ModerationService.rollback_latest_case(
            "telegram",
            {"provider": "telegram", "provider_user_id": str(callback.from_user.id), "label": f"@{callback.from_user.username}" if callback.from_user.username else str(callback.from_user.id)},
            {"provider": "telegram", "provider_user_id": target_user_id, "label": target_user_id},
            chat_id=callback.message.chat.id,
        )
    except Exception:
        logger.exception("telegram modstatus rollback failed actor_id=%s target_id=%s", callback.from_user.id, target_user_id)
        await callback.answer("❌ Не удалось снять наказание. Подробности в консоли.", show_alert=True)
        return
    if not result.get("ok"):
        await callback.answer(str(result.get("message") or "Не удалось снять наказание."), show_alert=True)
        return
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
