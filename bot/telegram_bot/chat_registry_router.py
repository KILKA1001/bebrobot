"""
Назначение: модуль "chat registry router" реализует продуктовый контур в зоне Telegram.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Telegram.
"""

from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.types import CallbackQuery, ChatMemberUpdated, Message

from bot.services import AccountsService
from bot.services import GuiyPublishDestinationsService
from bot.telegram_bot.identity import persist_telegram_identity_from_user

logger = logging.getLogger(__name__)
router = Router(name="telegram_chat_registry")
_GROUP_CHAT_TYPES = {"group", "supergroup"}
_TRACKED_CHAT_TYPES = _GROUP_CHAT_TYPES | {"channel"}


def _remember_chat(chat) -> None:
    if chat is None:
        return
    chat_type = str(getattr(chat, "type", "") or "").strip()
    if chat_type not in _TRACKED_CHAT_TYPES:
        return
    try:
        GuiyPublishDestinationsService.register_telegram_chat(
            chat_id=getattr(chat, "id", None),
            chat_title=getattr(chat, "title", None),
            chat_type=chat_type,
            is_active=True,
        )
    except Exception:
        logger.exception(
            "telegram bot chat registry remember chat failed chat_id=%s chat_title=%s chat_type=%s",
            getattr(chat, "id", None),
            getattr(chat, "title", None),
            chat_type,
        )


@router.message(F.chat.type.in_(_GROUP_CHAT_TYPES))
async def remember_group_message(message: Message) -> None:
    _remember_chat(message.chat)


@router.edited_message(F.chat.type.in_(_GROUP_CHAT_TYPES))
async def remember_group_edited_message(message: Message) -> None:
    _remember_chat(message.chat)


@router.channel_post(F.chat.type == "channel")
async def remember_channel_post(message: Message) -> None:
    _remember_chat(message.chat)


@router.edited_channel_post(F.chat.type == "channel")
async def remember_channel_edited_post(message: Message) -> None:
    _remember_chat(message.chat)


@router.callback_query(F.message, F.message.chat.type.in_(_GROUP_CHAT_TYPES))
async def remember_group_callback(callback: CallbackQuery) -> None:
    _remember_chat(callback.message.chat if callback.message else None)


@router.my_chat_member(F.chat.type.in_(_TRACKED_CHAT_TYPES))
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


@router.chat_member(F.chat.type.in_(_GROUP_CHAT_TYPES))
async def remember_user_membership(update: ChatMemberUpdated) -> None:
    _remember_chat(update.chat)

    old_status = str(getattr(getattr(update, "old_chat_member", None), "status", "") or "").strip()
    new_status = str(getattr(getattr(update, "new_chat_member", None), "status", "") or "").strip()

    transitioned_to_active = old_status in {"left", "kicked"} and new_status in {"member", "administrator", "creator", "restricted"}
    transitioned_from_active = old_status in {"member", "administrator", "creator", "restricted"} and new_status in {"left", "kicked"}

    member_user = getattr(getattr(update, "new_chat_member", None), "user", None)
    if transitioned_to_active and member_user is not None and not getattr(member_user, "is_bot", False):
        persist_telegram_identity_from_user(member_user)
        logger.info(
            "telegram chat_member identity refresh started provider=%s provider_user_id=%s chat_id=%s source_handler=%s old_status=%s new_status=%s",
            "telegram",
            getattr(member_user, "id", None),
            getattr(update.chat, "id", None),
            "telegram.chat_member",
            old_status,
            new_status,
        )

    if new_status in {"left", "kicked"} and member_user is not None and not getattr(member_user, "is_bot", False):
        try:
            purged, purge_result = AccountsService.purge_unlinked_identity("telegram", str(member_user.id))
            logger.info(
                "telegram chat_member identity purge completed provider=%s provider_user_id=%s chat_id=%s purged=%s purge_result=%s old_status=%s new_status=%s",
                "telegram",
                getattr(member_user, "id", None),
                getattr(update.chat, "id", None),
                purged,
                purge_result,
                old_status,
                new_status,
            )
        except Exception:
            logger.exception(
                "telegram chat_member identity purge failed provider=%s provider_user_id=%s chat_id=%s old_status=%s new_status=%s",
                "telegram",
                getattr(member_user, "id", None),
                getattr(update.chat, "id", None),
                old_status,
                new_status,
            )

    if transitioned_to_active or transitioned_from_active:
        logger.info(
            "telegram user chat membership update chat_id=%s user_id=%s old_status=%s new_status=%s",
            getattr(update.chat, "id", None),
            getattr(member_user, "id", None),
            old_status,
            new_status,
        )
