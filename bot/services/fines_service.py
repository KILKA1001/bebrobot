"""
Назначение: модуль "fines service" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
Доменные операции: операции штрафов, начислений и погашений.
"""

import logging
from datetime import datetime
from typing import List, Optional

from bot.data import db
from bot.legacy_identity_logging import (
    log_identity_resolve_error,
    log_legacy_identity_path_detected,
)
from .accounts_service import AccountsService


logger = logging.getLogger(__name__)


class FinesService:
    @staticmethod
    def _resolve_account_id(provider: str, provider_user_id: str, handler: str, *, target: str = "account_id") -> Optional[str]:
        account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
        if account_id:
            return str(account_id)
        if hasattr(db, "_inc_metric"):
            db._inc_metric("identity_resolve_errors")
        log_identity_resolve_error(
            logger,
            module=__name__,
            handler=handler,
            field=f"{provider}_user_id" if provider in {"telegram", "discord"} else "provider_user_id",
            action="resolve_account_id",
            continue_execution=False,
            provider=provider,
            provider_user_id=provider_user_id,
            target=target,
        )
        return None

    @staticmethod
    def create_fine_by_identity(
        provider: str,
        provider_user_id: str,
        author_provider_user_id: str,
        amount: float,
        fine_type: int,
        reason: str,
        due_date: datetime,
    ):
        log_legacy_identity_path_detected(
            logger,
            module=__name__,
            handler="FinesService.create_fine_by_identity",
            field=f"{provider}_user_id" if provider in {"telegram", "discord"} else "provider_user_id",
            action="resolve_account_id",
            continue_execution=True,
            provider=provider,
        )
        account_id = FinesService._resolve_account_id(provider, provider_user_id, "FinesService.create_fine_by_identity")
        if not account_id:
            return None
        author_account_id = FinesService._resolve_account_id(
            provider,
            author_provider_user_id,
            "FinesService.create_fine_by_identity",
            target="author_account_id",
        )
        if not author_account_id:
            return None
        return FinesService.create_fine_by_account(account_id, author_account_id, amount, fine_type, reason, due_date)

    @staticmethod
    def get_user_fines_by_identity(provider: str, provider_user_id: str, active_only: bool = True) -> List[dict]:
        log_legacy_identity_path_detected(
            logger,
            module=__name__,
            handler="FinesService.get_user_fines_by_identity",
            field=f"{provider}_user_id" if provider in {"telegram", "discord"} else "provider_user_id",
            action="resolve_account_id",
            continue_execution=True,
            provider=provider,
        )
        account_id = FinesService._resolve_account_id(provider, provider_user_id, "FinesService.get_user_fines_by_identity")
        if not account_id:
            return []
        return FinesService.get_user_fines_by_account(account_id, active_only=active_only)

    @staticmethod
    def create_fine(discord_user_id: int, author_id: int, amount: float, fine_type: int, reason: str, due_date: datetime):
        return FinesService.create_fine_by_identity(
            "discord",
            str(discord_user_id),
            str(author_id),
            amount,
            fine_type,
            reason,
            due_date,
        )

    @staticmethod
    def get_user_fines(discord_user_id: int, active_only: bool = True) -> List[dict]:
        return FinesService.get_user_fines_by_identity("discord", str(discord_user_id), active_only=active_only)

    @staticmethod
    def get_fine_by_id(fine_id: int) -> Optional[dict]:
        return db.get_fine_by_id(fine_id)

    @staticmethod
    def create_fine_by_account(account_id: str, author_account_id: Optional[str], amount: float, fine_type: int, reason: str, due_date: datetime):
        if not account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return None
        return db.add_fine(account_id, author_account_id, amount, fine_type, reason, due_date)

    @staticmethod
    def get_user_fines_by_account(account_id: str, active_only: bool = True) -> List[dict]:
        if not account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return []
        return db.get_user_fines_by_account(account_id, active_only=active_only)
