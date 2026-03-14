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
        if not db.supabase:
            return None
        try:
            response = (
                db.supabase.table("account_identities")
                .select("provider_user_id")
                .eq("account_id", str(account_id))
                .order("provider")
                .limit(1)
                .execute()
            )
            if response.data:
                return int(response.data[0]["provider_user_id"])
        except Exception:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
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
        author_account_id = AccountsService.resolve_account_id(provider, str(author_id))
        if not author_account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            logger.error("give_ticket_by_identity: unresolved author account provider=%s author_id=%s", provider, author_id)
            return False
        return TicketsService.give_ticket_by_account(account_id, ticket_type, amount, reason, author_account_id)

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
        author_account_id = AccountsService.resolve_account_id(provider, str(author_id))
        if not author_account_id:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            logger.error("remove_ticket_by_identity: unresolved author account provider=%s author_id=%s", provider, author_id)
            return False
        return TicketsService.remove_ticket_by_account(account_id, ticket_type, amount, reason, author_account_id)

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
    def give_ticket_by_account(account_id: str, ticket_type: str, amount: int, reason: str, author_account_id: str) -> bool:
        return db.give_ticket_by_account(account_id, ticket_type, amount, reason, author_account_id)

    @staticmethod
    def remove_ticket_by_account(account_id: str, ticket_type: str, amount: int, reason: str, author_account_id: str) -> bool:
        return db.remove_ticket_by_account(account_id, ticket_type, amount, reason, author_account_id)
