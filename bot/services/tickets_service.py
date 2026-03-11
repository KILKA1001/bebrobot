import logging

from bot.data import db

from .accounts_service import AccountsService


logger = logging.getLogger(__name__)


class TicketsService:
    @staticmethod
    def _resolve_anchor_user_id(account_id: str) -> int | None:
        if not account_id:
            return None
        discord_user_id = db._get_discord_user_for_account_id(account_id)
        if discord_user_id is not None:
            return int(discord_user_id)
        logger.warning("tickets anchor discord identity missing account_id=%s", account_id)
        return None

    @staticmethod
    def give_ticket_by_identity(
        provider: str,
        provider_user_id: str,
        ticket_type: str,
        amount: int,
        reason: str,
        author_id: int,
    ) -> bool:
        account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
        if not account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return TicketsService.give_ticket_by_account(account_id, ticket_type, amount, reason, author_id)

    @staticmethod
    def remove_ticket_by_identity(
        provider: str,
        provider_user_id: str,
        ticket_type: str,
        amount: int,
        reason: str,
        author_id: int,
    ) -> bool:
        account_id = AccountsService.resolve_account_id(provider, str(provider_user_id))
        if not account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return TicketsService.remove_ticket_by_account(account_id, ticket_type, amount, reason, author_id)

    @staticmethod
    def give_ticket(discord_user_id: int, ticket_type: str, amount: int, reason: str, author_id: int) -> bool:
        return TicketsService.give_ticket_by_identity(
            "discord", str(discord_user_id), ticket_type, amount, reason, author_id
        )

    @staticmethod
    def remove_ticket(discord_user_id: int, ticket_type: str, amount: int, reason: str, author_id: int) -> bool:
        return TicketsService.remove_ticket_by_identity(
            "discord", str(discord_user_id), ticket_type, amount, reason, author_id
        )

    @staticmethod
    def give_ticket_by_account(account_id: str, ticket_type: str, amount: int, reason: str, author_id: int) -> bool:
        anchor_user_id = TicketsService._resolve_anchor_user_id(account_id)
        if anchor_user_id is None:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return db.give_ticket(anchor_user_id, ticket_type, amount, reason, author_id)

    @staticmethod
    def remove_ticket_by_account(account_id: str, ticket_type: str, amount: int, reason: str, author_id: int) -> bool:
        anchor_user_id = TicketsService._resolve_anchor_user_id(account_id)
        if anchor_user_id is None:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return db.remove_ticket(anchor_user_id, ticket_type, amount, reason, author_id)
