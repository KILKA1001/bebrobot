import logging
import re

from aiogram import F, Router
from aiogram.types import Message

from bot.services.gemini_service import generate_guiy_reply


logger = logging.getLogger(__name__)
router = Router()


@router.message(F.text)
async def handle_guiy_chat(message: Message) -> None:
    text = (message.text or "").strip()
    if not text or text.startswith("/"):
        return

    lowered = text.lower()
    is_named = re.search(r"\bгуй\b", lowered) is not None

    bot_user = await message.bot.get_me()
    is_reply_to_bot = bool(
        message.reply_to_message
        and message.reply_to_message.from_user
        and message.reply_to_message.from_user.id == bot_user.id
    )

    if not (is_named or is_reply_to_bot):
        return

    try:
        reply = await generate_guiy_reply(text)
        if not reply:
            logger.warning(
                "telegram ai reply is empty chat_id=%s user_id=%s",
                message.chat.id,
                message.from_user.id if message.from_user else None,
            )
            return
        await message.answer(reply)
    except Exception:
        logger.exception(
            "telegram ai reply failed chat_id=%s user_id=%s",
            message.chat.id,
            message.from_user.id if message.from_user else None,
        )
