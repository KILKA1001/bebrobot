import logging

from bot.data import db

from .accounts_service import AccountsService


logger = logging.getLogger(__name__)


class PointsService:
    @staticmethod
    def _resolve_anchor_user_id(account_id: str) -> int | None:
        if not account_id:
            return None
        discord_user_id = db._get_discord_user_for_account_id(account_id)
        if discord_user_id is not None:
            return int(discord_user_id)
        logger.warning("points anchor discord identity missing account_id=%s", account_id)
        return None

    @staticmethod
    def add_points_by_identity(provider: str, provider_user_id: str, points: float, reason: str, author_id: int) -> bool:
        account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
        if not account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return PointsService.add_points_by_account(account_id, points, reason, author_id)

    @staticmethod
    def remove_points_by_identity(provider: str, provider_user_id: str, points: float, reason: str, author_id: int) -> bool:
        account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
        if not account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return PointsService.remove_points_by_account(account_id, points, reason, author_id)

    @staticmethod
    def add_points(discord_user_id: int, points: float, reason: str, author_id: int) -> bool:
        return PointsService.add_points_by_identity("discord", str(discord_user_id), points, reason, author_id)

    @staticmethod
    def remove_points(discord_user_id: int, points: float, reason: str, author_id: int) -> bool:
        return PointsService.remove_points_by_identity("discord", str(discord_user_id), points, reason, author_id)


    @staticmethod
    def add_points_by_account(account_id: str, points: float, reason: str, author_id: int) -> bool:
        anchor_user_id = PointsService._resolve_anchor_user_id(account_id)
        if anchor_user_id is None:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return db.add_action(anchor_user_id, points, reason, author_id)

    @staticmethod
    def remove_points_by_account(account_id: str, points: float, reason: str, author_id: int) -> bool:
        anchor_user_id = PointsService._resolve_anchor_user_id(account_id)
        if anchor_user_id is None:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return db.add_action(anchor_user_id, -points, reason, author_id)
