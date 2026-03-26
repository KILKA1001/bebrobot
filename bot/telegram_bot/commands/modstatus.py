from __future__ import annotations

import logging
from typing import Any

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.services import AccountsService, AuthorityService, ModerationService
from bot.telegram_bot.commands.roles_admin import _resolve_telegram_target
from bot.telegram_bot.identity import persist_telegram_identity_from_user

logger = logging.getLogger(__name__)
router = Router()
_PAYMENT_HINT = ModerationService.MODSTATUS_PAYMENT_HINT


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

        await message.answer(ModerationService.render_user_moderation_snapshot(snapshot, payment_hint=_PAYMENT_HINT))
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
