import logging
import os

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

from bot.telegram_bot.identity import persist_telegram_identity_from_user
from bot.systems.shop_logic import build_shop_render_payload, check_shop_profile_access

logger = logging.getLogger(__name__)
router = Router()

SHOP_OPEN_PROMPT_TEXT = "Откройте магазин в личных сообщениях, я уже отправил вам инструкцию."
DM_FALLBACK_TEXT = (
    "❌ Не удалось отправить инструкцию в личные сообщения.\n"
    "Откройте ЛС с ботом: нажмите на профиль бота → <b>Start</b> / <b>Начать</b>, затем снова отправьте <code>/shop</code>."
)
SHOP_URL = os.getenv("SHOP_URL", "").strip()


def _shop_markup(bot_username: str | None) -> InlineKeyboardMarkup | None:
    if SHOP_URL:
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Роли", url=SHOP_URL)]])

    username = (bot_username or "").strip().lstrip("@")
    if not username:
        logger.warning("shop_empty_catalog provider=telegram reason=missing_shop_url_and_username")
        return None
    deeplink = f"https://t.me/{username}?start=shop"
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Роли", url=deeplink)]])


def _extract_dm_failure_code(error: Exception) -> str:
    if isinstance(error, TelegramForbiddenError):
        return "forbidden"
    message = str(error).lower()
    if "blocked" in message:
        return "user_blocked"
    if "forbidden" in message:
        return "forbidden"
    if "chat not found" in message or "user not found" in message:
        return "dm_closed"
    return "dm_failed"


@router.message(Command("shop"))
async def shop_command(message: Message) -> None:
    persist_telegram_identity_from_user(message.from_user)
    if message.from_user is None:
        logger.error("shop telegram actor missing provider=telegram source=unknown")
        return

    source = "dm" if message.chat.type == "private" else "group"
    logger.info(
        "shop flow step=received provider=telegram source=%s actor_user_id=%s chat_id=%s",
        source,
        message.from_user.id,
        message.chat.id if message.chat else None,
    )

    profile_check = check_shop_profile_access("telegram", message.from_user.id, register_command="/register")
    if not profile_check.ok:
        await message.answer(profile_check.user_message or "Сначала создайте профиль и повторите команду /shop.", parse_mode="HTML")
        return

    payload = build_shop_render_payload(profile_check.account_id)
    text = payload.telegram_text
    reply_markup = _shop_markup(getattr(message.bot, "username", None))

    if message.chat.type == "private":
        await message.answer(text, parse_mode="HTML", reply_markup=reply_markup)
        logger.info(
            "shop flow step=completed provider=telegram source=dm actor_user_id=%s dm_sent=true reason=ok",
            message.from_user.id,
        )
        return

    await message.answer(SHOP_OPEN_PROMPT_TEXT)
    logger.info(
        "shop flow step=group_notice_sent provider=telegram source=group actor_user_id=%s",
        message.from_user.id,
    )
    try:
        await message.bot.send_message(
            chat_id=message.from_user.id,
            text=text,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )
        logger.info(
            "shop flow step=dm_attempt provider=telegram source=group actor_user_id=%s dm_sent=true reason=ok",
            message.from_user.id,
        )
    except (TelegramForbiddenError, TelegramBadRequest) as error:
        reason = _extract_dm_failure_code(error)
        logger.warning(
            "shop flow step=dm_attempt provider=telegram source=group actor_user_id=%s dm_sent=false reason=%s error=%s",
            message.from_user.id,
            reason,
            error,
        )
        await message.answer(DM_FALLBACK_TEXT, parse_mode="HTML")
    except Exception as error:  # noqa: BLE001
        logger.exception(
            "shop flow step=dm_attempt provider=telegram source=group actor_user_id=%s dm_sent=false reason=dm_failed error=%s",
            message.from_user.id,
            error,
        )
        await message.answer(DM_FALLBACK_TEXT, parse_mode="HTML")
