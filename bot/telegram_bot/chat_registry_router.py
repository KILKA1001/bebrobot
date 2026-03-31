"""
Назначение: модуль "chat registry router" реализует продуктовый контур в зоне Telegram.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Telegram.
"""

from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.dispatcher.event.bases import SkipHandler
from aiogram.types import CallbackQuery, ChatMemberUpdated, Message

from bot.services import GuiyPublishDestinationsService

logger = logging.getLogger(__name__)
router = Router(name="telegram_chat_registry")
_GROUP_CHAT_TYPES = {"group", "supergroup"}


def _remember_chat(chat) -> None:
    if chat is None:
        return
    chat_type = str(getattr(chat, "type", "") or "").strip()
    if chat_type not in _GROUP_CHAT_TYPES:
        return
    GuiyPublishDestinationsService.register_telegram_chat(
        chat_id=getattr(chat, "id", None),
        chat_title=getattr(chat, "title", None),
        chat_type=chat_type,
        is_active=True,
    )


@router.message(F.chat.type.in_(_GROUP_CHAT_TYPES))
async def remember_group_message(message: Message) -> None:
    _remember_chat(message.chat)
    raise SkipHandler()


@router.edited_message(F.chat.type.in_(_GROUP_CHAT_TYPES))
async def remember_group_edited_message(message: Message) -> None:
    _remember_chat(message.chat)
    raise SkipHandler()


@router.callback_query(F.message, F.message.chat.type.in_(_GROUP_CHAT_TYPES))
async def remember_group_callback(callback: CallbackQuery) -> None:
    _remember_chat(callback.message.chat if callback.message else None)
    raise SkipHandler()


@router.my_chat_member(F.chat.type.in_(_GROUP_CHAT_TYPES))
async def remember_bot_membership(update: ChatMemberUpdated) -> None:
    chat = update.chat
    status = str(getattr(getattr(update, "new_chat_member", None), "status", "") or "").strip()
    is_active = status not in {"left", "kicked"}
    logger.info(
        "telegram bot chat membership update chat_id=%s chat_title=%s chat_type=%s status=%s is_active=%s",
        getattr(chat, "id", None),
        getattr(chat, "title", None),
        getattr(chat, "type", None),
        status,
        is_active,
    )
    GuiyPublishDestinationsService.register_telegram_chat(
        chat_id=getattr(chat, "id", None),
        chat_title=getattr(chat, "title", None),
        chat_type=getattr(chat, "type", None),
        is_active=is_active,
    )
