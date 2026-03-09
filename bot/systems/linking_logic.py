from bot.services import AccountsService


def issue_discord_telegram_link_code(discord_user_id: int) -> tuple[bool, str]:
    """Shared logic for Discord command that issues a Telegram link code."""
    return AccountsService.issue_discord_telegram_link_code(discord_user_id)


def consume_telegram_link_code(telegram_user_id: int, code: str) -> tuple[bool, str]:
    """Shared logic for Telegram `/link <code>` command."""
    return AccountsService.consume_telegram_link_code(telegram_user_id, code)
