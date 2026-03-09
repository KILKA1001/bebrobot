from bot.data import db
from .accounts_service import AccountsService


class PointsService:
    @staticmethod
    def add_points(discord_user_id: int, points: float, reason: str, author_id: int) -> bool:
        account_id = AccountsService.resolve_account_id("discord", str(discord_user_id))
        _ = account_id  # account-first resolver retained for service contract
        return db.add_action(discord_user_id, points, reason, author_id)

    @staticmethod
    def remove_points(discord_user_id: int, points: float, reason: str, author_id: int) -> bool:
        account_id = AccountsService.resolve_account_id("discord", str(discord_user_id))
        _ = account_id
        return db.add_action(discord_user_id, -points, reason, author_id)


    @staticmethod
    def add_points_by_account(account_id: str, points: float, reason: str, author_id: int) -> bool:
        discord_user_id = db._get_discord_user_for_account_id(account_id)
        if discord_user_id is None:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return db.add_action(discord_user_id, points, reason, author_id)

    @staticmethod
    def remove_points_by_account(account_id: str, points: float, reason: str, author_id: int) -> bool:
        discord_user_id = db._get_discord_user_for_account_id(account_id)
        if discord_user_id is None:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return db.add_action(discord_user_id, -points, reason, author_id)

