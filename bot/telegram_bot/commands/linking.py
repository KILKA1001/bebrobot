import logging
import time

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from bot.services import AccountsService
from bot.telegram_bot.systems.commands_logic import (
    get_helpy_text,
    process_link_command,
    process_link_discord_command,
    process_profile_command,
    process_register_command,
)

logger = logging.getLogger(__name__)
router = Router()

_EDIT_FIELD_LABELS = {
    "custom_nick": "Никнейм",
    "description": "Описание",
    "nulls_brawl_id": "Null's Brawl ID",
}
_PENDING_EDIT_FIELD: dict[int, str] = {}


PENDING_PROFILE_EDIT_TTL_SECONDS = 900
_PENDING_EDIT_FIELD_CREATED_AT: dict[int, float] = {}


def _has_non_expired_profile_edit(telegram_user_id: int) -> bool:
    field_name = _PENDING_EDIT_FIELD.get(telegram_user_id)
    created_at = _PENDING_EDIT_FIELD_CREATED_AT.get(telegram_user_id)
    if not field_name or created_at is None:
        _PENDING_EDIT_FIELD.pop(telegram_user_id, None)
        _PENDING_EDIT_FIELD_CREATED_AT.pop(telegram_user_id, None)
        return False

    if (time.time() - created_at) > PENDING_PROFILE_EDIT_TTL_SECONDS:
        logger.info(
            "profile_edit pending state expired user_id=%s field=%s ttl_seconds=%s",
            telegram_user_id,
            field_name,
            PENDING_PROFILE_EDIT_TTL_SECONDS,
        )
        _PENDING_EDIT_FIELD.pop(telegram_user_id, None)
        _PENDING_EDIT_FIELD_CREATED_AT.pop(telegram_user_id, None)
        return False

    return True


def has_pending_profile_edit(telegram_user_id: int | None) -> bool:
    if telegram_user_id is None:
        return False
    return _has_non_expired_profile_edit(telegram_user_id)


def _is_chat_send_permissions_error(error: TelegramBadRequest) -> bool:
    return "not enough rights to send" in str(error).lower()


async def _safe_answer(
    message: Message,
    text: str,
    *,
    parse_mode: ParseMode | None = None,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> bool:
    try:
        await message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
        return True
    except TelegramBadRequest as error:
        if _is_chat_send_permissions_error(error):
            logger.warning(
                "message send skipped due to missing chat permissions chat_id=%s user_id=%s error=%s",
                message.chat.id,
                message.from_user.id if message.from_user is not None else None,
                error,
            )
            return False
        logger.exception("message send failed chat_id=%s", message.chat.id)
        return False


def _profile_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Изменить никнейм", callback_data="profile_edit:custom_nick")],
            [InlineKeyboardButton(text="📝 Изменить описание", callback_data="profile_edit:description")],
            [InlineKeyboardButton(text="🆔 Изменить Null's ID", callback_data="profile_edit:nulls_brawl_id")],
        ]
    )


@router.message(Command("helpy"))
async def helpy_command(message: Message) -> None:
    await message.answer(get_helpy_text())


@router.message(Command("register"))
async def register_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    response = process_register_command(telegram_user_id)
    await message.answer(response)


@router.message(Command("profile"))
async def profile_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    display_name = message.from_user.full_name if message.from_user is not None else None

    target_user = message.reply_to_message.from_user if message.reply_to_message else None
    target_user_id = target_user.id if target_user is not None else telegram_user_id
    target_display_name = target_user.full_name if target_user is not None else display_name

    response = process_profile_command(
        telegram_user_id,
        display_name=display_name,
        target_telegram_user_id=target_user_id,
        target_display_name=target_display_name,
    )

    if target_user_id is None:
        await _safe_answer(message, response)
        return

    reply_markup = None
    if message.chat.type == "private" and telegram_user_id == target_user_id:
        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⚙️ Настройки профиля", callback_data="profile_settings")]]
        )

    async def _send_avatar_caption(user_id: int) -> bool:
        try:
            photos = await message.bot.get_user_profile_photos(user_id, limit=1)
            if photos.total_count > 0 and photos.photos and photos.photos[0]:
                file_id = photos.photos[0][-1].file_id
                await message.answer_photo(
                    photo=file_id,
                    caption=response,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
                return True
        except TelegramBadRequest as error:
            if _is_chat_send_permissions_error(error):
                logger.warning(
                    "photo send skipped due to missing chat permissions chat_id=%s target_user_id=%s error=%s",
                    message.chat.id,
                    user_id,
                    error,
                )
                return False
            logger.exception("failed to send profile avatar due to telegram error user_id=%s", user_id)
            return False
        except Exception:
            logger.exception("failed to send profile avatar user_id=%s", user_id)
            return False
        return False

    if await _send_avatar_caption(target_user_id):
        return

    bot_user = await message.bot.get_me()
    if await _send_avatar_caption(bot_user.id):
        return

    await _safe_answer(message, response, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


@router.message(Command("profile_edit"))
async def profile_edit_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    if message.chat.type != "private":
        await message.answer("❌ Редактирование профиля доступно только в личных сообщениях с ботом.")
        return
    if telegram_user_id is None:
        await message.answer("❌ Не удалось определить пользователя Telegram.")
        return

    await message.answer(
        "⚙️ <b>Настройки профиля</b>\n"
        "Выберите, что хотите изменить:",
        parse_mode=ParseMode.HTML,
        reply_markup=_profile_settings_keyboard(),
    )


@router.callback_query(F.data == "profile_settings")
async def profile_settings_callback(callback: CallbackQuery) -> None:
    try:
        if callback.message and callback.message.chat.type != "private":
            await callback.answer("Доступно только в ЛС", show_alert=True)
            return

        await callback.message.answer(
            "⚙️ <b>Настройки профиля</b>\n"
            "Выберите поле для изменения:",
            parse_mode=ParseMode.HTML,
            reply_markup=_profile_settings_keyboard(),
        )
        await callback.answer()
    except Exception:
        logger.exception("profile_settings callback failed")
        await callback.answer("Ошибка открытия настроек", show_alert=True)


@router.callback_query(F.data.startswith("profile_edit:"))
async def profile_edit_field_callback(callback: CallbackQuery) -> None:
    try:
        if callback.from_user is None:
            await callback.answer("Не удалось определить пользователя", show_alert=True)
            return
        if callback.message and callback.message.chat.type != "private":
            await callback.answer("Доступно только в ЛС", show_alert=True)
            return

        field_name = str(callback.data).split(":", 1)[1]
        if field_name not in _EDIT_FIELD_LABELS:
            await callback.answer("Неизвестное поле", show_alert=True)
            return

        _PENDING_EDIT_FIELD[callback.from_user.id] = field_name
        _PENDING_EDIT_FIELD_CREATED_AT[callback.from_user.id] = time.time()
        await callback.message.answer(
            f"✍️ Введите новое значение для поля <b>{_EDIT_FIELD_LABELS[field_name]}</b>.\n"
            "Чтобы очистить поле, отправьте символ <code>-</code>.",
            parse_mode=ParseMode.HTML,
        )
        await callback.answer()
    except Exception:
        logger.exception("profile_edit field callback failed callback_data=%s", callback.data)
        await callback.answer("Ошибка выбора поля", show_alert=True)


@router.message(F.chat.type == "private", F.from_user, F.from_user.id.func(has_pending_profile_edit))
async def profile_edit_value_handler(message: Message) -> None:
    if not _has_non_expired_profile_edit(message.from_user.id):
        logger.warning(
            "profile_edit handler invoked without pending field user_id=%s chat_id=%s",
            message.from_user.id,
            message.chat.id if message.chat else None,
        )
        return

    pending_field = _PENDING_EDIT_FIELD.get(message.from_user.id)
    try:
        value = (message.text or "").strip()
        if value == "-":
            value = ""

        success, payload = AccountsService.update_profile_field(
            "telegram",
            str(message.from_user.id),
            pending_field,
            value,
        )

        _PENDING_EDIT_FIELD.pop(message.from_user.id, None)
        _PENDING_EDIT_FIELD_CREATED_AT.pop(message.from_user.id, None)
        prefix = "✅" if success else "❌"
        await message.answer(f"{prefix} {payload}")

        if success:
            profile_text = process_profile_command(
                telegram_user_id=message.from_user.id,
                display_name=message.from_user.full_name,
            )
            await message.answer(
                profile_text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⚙️ Настройки профиля", callback_data="profile_settings")]]
                ),
            )
    except Exception:
        logger.exception("profile_edit value handler failed user_id=%s", message.from_user.id)
        _PENDING_EDIT_FIELD.pop(message.from_user.id, None)
        _PENDING_EDIT_FIELD_CREATED_AT.pop(message.from_user.id, None)
        await message.answer("❌ Ошибка обновления профиля. Попробуйте позже.")


@router.message(Command("link"))
async def link_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    is_private_chat = message.chat.type == "private"
    response = process_link_command(message.text or "", telegram_user_id, is_private_chat=is_private_chat)
    await message.answer(response)


@router.message(Command("link_discord"))
async def link_discord_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    is_private_chat = message.chat.type == "private"
    response = process_link_discord_command(telegram_user_id, is_private_chat=is_private_chat)
    await message.answer(response)
