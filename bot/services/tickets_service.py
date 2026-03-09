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
