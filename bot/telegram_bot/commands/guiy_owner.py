import logging

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

from bot.services import AccountsService
from bot.services.guiy_admin_service import (
    GUIY_OWNER_DENIED_MESSAGE,
    GUIY_OWNER_REPLY_REQUIRED_MESSAGE,
    GUIY_OWNER_USAGE_TEXT,
    authorize_guiy_owner_action,
    parse_guiy_owner_profile_payload,
    resolve_guiy_target_account,
)
from bot.telegram_bot.identity import persist_telegram_identity_from_user

logger = logging.getLogger(__name__)
router = Router()


def _parse_action_and_payload(raw_args: str | None) -> tuple[str, str]:
    cleaned = str(raw_args or "").strip()
    if not cleaned:
        return "", ""
    parts = cleaned.split(maxsplit=1)
    action = parts[0].strip().lower()
    payload = parts[1].strip() if len(parts) > 1 else ""
    return action, payload


@router.message(Command("guiy_owner"))
async def guiy_owner_command(message: Message, command: CommandObject) -> None:
    persist_telegram_identity_from_user(message.from_user)
    persist_telegram_identity_from_user(message.reply_to_message.from_user if message.reply_to_message else None)
    actor_user_id = message.from_user.id if message.from_user else None
    target_message_id = message.reply_to_message.message_id if message.reply_to_message else None
    action, payload = _parse_action_and_payload(command.args)

    if action not in {"say", "reply", "profile"}:
        await message.answer(GUIY_OWNER_USAGE_TEXT)
        return
    if action in {"say", "reply"} and not payload:
        await message.answer(GUIY_OWNER_USAGE_TEXT)
        return
    if action == "reply" and not message.reply_to_message:
        await message.answer(GUIY_OWNER_REPLY_REQUIRED_MESSAGE)
        return

    access = authorize_guiy_owner_action(
        actor_provider="telegram",
        actor_user_id=actor_user_id,
        requested_action=action,
        target_message_id=target_message_id,
    )
    if not access.allowed:
        await message.answer(GUIY_OWNER_DENIED_MESSAGE)
        return

    try:
        bot_user = await message.bot.get_me()
    except Exception:
        logger.exception(
            "telegram guiy owner failed to resolve bot identity chat_id=%s actor_user_id=%s action=%s",
            message.chat.id if message.chat else None,
            actor_user_id,
            action,
        )
        await message.answer(GUIY_OWNER_DENIED_MESSAGE)
        return

    reply_author_user_id = message.reply_to_message.from_user.id if message.reply_to_message and message.reply_to_message.from_user else None
    target_resolution = resolve_guiy_target_account(
        provider="telegram",
        bot_user_id=bot_user.id,
        reply_author_user_id=reply_author_user_id,
        explicit_owner_command=True,
    )
    if not target_resolution.ok:
        await message.answer(target_resolution.message or GUIY_OWNER_DENIED_MESSAGE)
        return

    try:
        if action == "say":
            await message.answer(payload)
            return

        if action == "reply":
            await message.answer(payload, reply_to_message_id=message.reply_to_message.message_id)
            return

        field_name, field_value = parse_guiy_owner_profile_payload(payload)
        if not field_name:
            await message.answer(GUIY_OWNER_USAGE_TEXT)
            return

        success, response = AccountsService.update_profile_field(
            "telegram",
            str(bot_user.id),
            field_name,
            field_value or "",
        )
        prefix = "✅" if success else "❌"
        await message.answer(
            f"{prefix} {response}\n"
            "Подсказка: чтобы проверить результат, ответьте /profile на сообщение Гуя или откройте профиль его общего аккаунта."
        )
    except Exception:
        logger.exception(
            "telegram guiy owner command failed chat_id=%s actor_user_id=%s action=%s target_message_id=%s",
            message.chat.id if message.chat else None,
            actor_user_id,
            action,
            target_message_id,
        )
        await message.answer("❌ Не удалось выполнить действие. Попробуйте позже.")
