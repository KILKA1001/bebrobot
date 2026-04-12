"""
Назначение: модуль "identity" реализует продуктовый контур в зоне Telegram.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Telegram.
"""

import logging
from typing import Any

from bot.services import AccountsService

logger = logging.getLogger(__name__)


def persist_telegram_identity_from_user(user: Any | None) -> None:
    if not user or getattr(user, "is_bot", False):
        return
    try:
        AccountsService.persist_identity_lookup_fields(
            "telegram",
            str(user.id),
            username=getattr(user, "username", None),
            display_name=getattr(user, "full_name", None),
        )
    except Exception:
        logger.exception(
            "persist_telegram_identity_from_user crashed provider=%s provider_user_id=%s chat_id=%s source_handler=%s",
            "telegram",
            getattr(user, "id", None),
            None,
            "telegram.persist_telegram_identity_from_user",
        )
