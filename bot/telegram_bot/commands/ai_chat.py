import asyncio
import logging
import re

from aiogram import F, Router
from aiogram.types import Message

from bot.services.gemini_service import generate_guiy_reply
from bot.telegram_bot.commands.engagement import has_pending_action
from bot.telegram_bot.commands.linking import has_pending_profile_edit
from bot.utils.guiy_typing import calculate_typing_delay_seconds


logger = logging.getLogger(__name__)
router = Router()

KNOWN_COMMAND_PREFIXES = (
    "/start",
    "/register",
    "/profile",
    "/profile_edit",
    "/link",
    "/link_discord",
    "/points",
    "/tickets",
    "/helpy",
)


def _is_command_text(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return False
    if not lowered.startswith("/"):
        return False
    return any(lowered == cmd or lowered.startswith(f"{cmd} ") for cmd in KNOWN_COMMAND_PREFIXES)


@router.message(F.text)
async def handle_guiy_chat(message: Message) -> None:
    text = (message.text or "").strip()
    if not text:
        return

    if _is_command_text(text):
        logger.info(
            "telegram ai skipped because message is command chat_id=%s user_id=%s text=%s",
            message.chat.id,
            message.from_user.id if message.from_user else None,
            text[:120],
        )
        return

    sender_id = message.from_user.id if message.from_user else None
    if has_pending_action(sender_id) or has_pending_profile_edit(sender_id):
        logger.info(
            "telegram ai skipped due to active command flow chat_id=%s user_id=%s",
            message.chat.id,
            sender_id,
        )
        return

    lowered = text.lower()
    is_named = re.search(r"\bгуй\b", lowered) is not None

    try:
        bot_user = await message.bot.get_me()
    except Exception:
        logger.exception(
            "telegram ai failed to fetch bot identity chat_id=%s user_id=%s",
            message.chat.id,
            sender_id,
        )
        return

    is_reply_to_bot = bool(
        message.reply_to_message
        and message.reply_to_message.from_user
        and message.reply_to_message.from_user.id == bot_user.id
    )

    if not (is_named or is_reply_to_bot):
        logger.debug(
            "telegram ai skipped because trigger not matched chat_id=%s user_id=%s text=%s",
            message.chat.id,
            sender_id,
            text[:120],
        )
        return

    try:
        logger.info(
            "telegram ai trigger matched chat_id=%s user_id=%s is_named=%s is_reply_to_bot=%s text=%s",
            message.chat.id,
            sender_id,
            is_named,
            is_reply_to_bot,
            text[:160],
        )
        reply = await generate_guiy_reply(
            text,
            provider="telegram",
            user_id=sender_id,
            conversation_id=message.chat.id,
        )
        if not reply:
            logger.warning(
                "telegram ai reply is empty chat_id=%s user_id=%s",
                message.chat.id,
                sender_id,
            )
            return

        typing_delay = calculate_typing_delay_seconds(reply)
        logger.info(
            "telegram ai typing simulation chat_id=%s user_id=%s delay=%ss reply_len=%s",
            message.chat.id,
            sender_id,
            typing_delay,
            len(reply),
        )
        try:
            await message.bot.send_chat_action(message.chat.id, "typing")
            await asyncio.sleep(typing_delay)
        except Exception:
            logger.exception(
                "telegram typing simulation failed chat_id=%s user_id=%s",
                message.chat.id,
                sender_id,
            )

        await message.answer(reply)
    except Exception:
        logger.exception(
            "telegram ai reply failed chat_id=%s user_id=%s",
            message.chat.id,
            sender_id,
        )
