from aiogram import Router
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message

from bot.telegram_bot.systems.commands_logic import (
    get_helpy_text,
    process_link_command,
    process_link_discord_command,
    process_profile_command,
    process_register_command,
)

router = Router()


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
        await message.answer(response)
        return

    async def _send_avatar_caption(user_id: int) -> bool:
        try:
            photos = await message.bot.get_user_profile_photos(user_id, limit=1)
            if photos.total_count > 0 and photos.photos and photos.photos[0]:
                file_id = photos.photos[0][-1].file_id
                await message.answer_photo(photo=file_id, caption=response, parse_mode=ParseMode.HTML)
                return True
        except Exception:
            return False
        return False

    if await _send_avatar_caption(target_user_id):
        return

    bot_user = await message.bot.get_me()
    if await _send_avatar_caption(bot_user.id):
        return

    await message.answer(response, parse_mode=ParseMode.HTML)


@router.message(Command("link"))
async def link_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    response = process_link_command(message.text or "", telegram_user_id)
    await message.answer(response)


@router.message(Command("link_discord"))
async def link_discord_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    response = process_link_discord_command(telegram_user_id)
    await message.answer(response)
