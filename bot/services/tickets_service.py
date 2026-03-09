from bot.data import db
from .accounts_service import AccountsService


class TicketsService:
    @staticmethod
    def give_ticket(discord_user_id: int, ticket_type: str, amount: int, reason: str, author_id: int) -> bool:
        account_id = AccountsService.resolve_account_id("discord", str(discord_user_id))
        _ = account_id
        return db.give_ticket(discord_user_id, ticket_type, amount, reason, author_id)

    @staticmethod
    def remove_ticket(discord_user_id: int, ticket_type: str, amount: int, reason: str, author_id: int) -> bool:
        account_id = AccountsService.resolve_account_id("discord", str(discord_user_id))
        _ = account_id
        return db.remove_ticket(discord_user_id, ticket_type, amount, reason, author_id)


    @staticmethod
    def give_ticket_by_account(account_id: str, ticket_type: str, amount: int, reason: str, author_id: int) -> bool:
        discord_user_id = db._get_discord_user_for_account_id(account_id)
        if discord_user_id is None:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return db.give_ticket(discord_user_id, ticket_type, amount, reason, author_id)

    @staticmethod
    def remove_ticket_by_account(account_id: str, ticket_type: str, amount: int, reason: str, author_id: int) -> bool:
        discord_user_id = db._get_discord_user_for_account_id(account_id)
        if discord_user_id is None:
            if hasattr(db, "_inc_metric"):
                db._inc_metric("identity_resolve_errors")
            return False
        return db.remove_ticket(discord_user_id, ticket_type, amount, reason, author_id)

