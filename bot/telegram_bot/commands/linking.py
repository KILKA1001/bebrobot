from aiogram import Router
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
    response = process_profile_command(telegram_user_id, display_name=display_name)
    await message.answer(response, parse_mode="Markdown")


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
