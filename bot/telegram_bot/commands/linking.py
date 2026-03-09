from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from bot.telegram_bot.systems.commands_logic import get_helpy_text, process_link_command

router = Router()


@router.message(Command("helpy"))
async def helpy_command(message: Message) -> None:
    await message.answer(get_helpy_text())


@router.message(Command("link"))
async def link_command(message: Message) -> None:
    telegram_user_id = message.from_user.id if message.from_user is not None else None
    response = process_link_command(message.text or "", telegram_user_id)
    await message.answer(response)
