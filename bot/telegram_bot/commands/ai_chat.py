import asyncio
import logging
from aiogram import F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

from bot.services.ai_service import generate_guiy_reply
from bot.services import AccountsService
from bot.telegram_bot.commands.engagement import has_pending_action
from bot.telegram_bot.commands.linking import has_pending_profile_edit
from bot.utils.guiy_trigger import is_guiy_name_trigger
from bot.utils.guiy_typing import calculate_typing_delay_seconds
from bot.utils.conversation_activity import should_thread_reply


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
    "/roles_admin",
    "/helpy",
    "/guiy",
)


def _is_command_text(text: str) -> bool:
    lowered = text.strip().lower()
    if not lowered:
        return False
    if not lowered.startswith("/"):
        return False
    return any(lowered == cmd or lowered.startswith(f"{cmd} ") for cmd in KNOWN_COMMAND_PREFIXES)




def _is_name_trigger(text: str) -> bool:
    return is_guiy_name_trigger(text)


def _persist_telegram_identity(message: Message) -> None:
    user = message.from_user
    if not user or getattr(user, "is_bot", False):
        return
    AccountsService.persist_identity_lookup_fields(
        "telegram",
        str(user.id),
        username=getattr(user, "username", None),
        display_name=getattr(user, "full_name", None),
    )


async def _generate_and_send_reply(message: Message, text: str) -> None:
    sender_id = message.from_user.id if message.from_user else None
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

    use_reply_mark = should_thread_reply(
        f"telegram:{message.chat.id}",
        sender_id,
    )
    logger.info(
        "telegram ai reply mode resolved chat_id=%s user_id=%s message_id=%s use_reply_mark=%s",
        message.chat.id,
        sender_id,
        message.message_id,
        use_reply_mark,
    )

    try:
        if use_reply_mark:
            await message.answer(reply, reply_to_message_id=message.message_id)
        else:
            await message.answer(reply)
    except Exception:
        logger.exception(
            "telegram ai failed to send response chat_id=%s user_id=%s message_id=%s use_reply_mark=%s",
            message.chat.id,
            sender_id,
            message.message_id,
            use_reply_mark,
        )
        await message.answer(reply)


@router.message(Command("guiy"))
async def guiy_command(message: Message, command: CommandObject) -> None:
    _persist_telegram_identity(message)
    sender_id = message.from_user.id if message.from_user else None
    if has_pending_action(sender_id) or has_pending_profile_edit(sender_id):
        logger.info(
            "telegram ai /guiy skipped due to active command flow chat_id=%s user_id=%s",
            message.chat.id,
            sender_id,
        )
        return

    prompt = (command.args or "").strip()
    if not prompt:
        await message.answer("Напиши после команды текст: /guiy <сообщение>")
        return

    logger.info(
        "telegram ai /guiy trigger matched chat_id=%s user_id=%s text=%s",
        message.chat.id,
        sender_id,
        prompt[:160],
    )
    try:
        await _generate_and_send_reply(message, prompt)
    except Exception:
        logger.exception("telegram ai /guiy reply failed chat_id=%s user_id=%s", message.chat.id, sender_id)

def _is_bot_mentioned(message: Message, bot_id: int | None, bot_username: str | None) -> bool:
    entities = message.entities or []
    text = message.text or ""

    normalized_username = (bot_username or "").lstrip("@").lower()
    for entity in entities:
        if entity.type == "text_mention" and entity.user is not None and bot_id is not None:
            if entity.user.id == bot_id:
                return True

        if entity.type != "mention":
            continue

        start = entity.offset
        end = entity.offset + entity.length
        mention_text = text[start:end].strip().lstrip("@").lower()
        if mention_text and mention_text == normalized_username:
            return True

    return False


@router.message(F.text)
async def handle_guiy_chat(message: Message) -> None:
    _persist_telegram_identity(message)
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

    is_named = _is_name_trigger(text)

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
    is_bot_mention = _is_bot_mentioned(message, bot_user.id, bot_user.username)

    if not (is_named or is_reply_to_bot or is_bot_mention):
        logger.info(
            "telegram ai skipped because trigger not matched chat_id=%s user_id=%s is_named=%s "
            "is_reply_to_bot=%s is_bot_mention=%s text=%s",
            message.chat.id,
            sender_id,
            is_named,
            is_reply_to_bot,
            is_bot_mention,
            text[:120],
        )
        return

    try:
        logger.info(
            "telegram ai trigger matched chat_id=%s user_id=%s is_named=%s is_reply_to_bot=%s "
            "is_bot_mention=%s text=%s",
            message.chat.id,
            sender_id,
            is_named,
            is_reply_to_bot,
            is_bot_mention,
            text[:160],
        )
        await _generate_and_send_reply(message, text)
    except Exception:
        logger.exception(
            "telegram ai reply failed chat_id=%s user_id=%s",
            message.chat.id,
            sender_id,
        )
