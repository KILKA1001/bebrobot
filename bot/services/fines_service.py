from datetime import datetime
from typing import List, Optional

from bot.data import db
from .accounts_service import AccountsService


class FinesService:
    @staticmethod
    def create_fine(discord_user_id: int, author_id: int, amount: float, fine_type: int, reason: str, due_date: datetime):
        account_id = AccountsService.resolve_account_id("discord", str(discord_user_id))
        author_account_id = AccountsService.resolve_account_id("discord", str(author_id))
        if not account_id:
            return None
        return db.add_fine(account_id, author_account_id, amount, fine_type, reason, due_date)

    @staticmethod
    def get_user_fines(discord_user_id: int, active_only: bool = True) -> List[dict]:
        account_id = AccountsService.resolve_account_id("discord", str(discord_user_id))
        if not account_id:
            return []
        return db.get_user_fines_by_account(account_id, active_only=active_only)

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
