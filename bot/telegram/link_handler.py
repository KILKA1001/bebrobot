from bot.services import AccountsService


def handle_link_command(telegram_user_id: int, code: str) -> tuple[bool, str]:
    """Platform-agnostic TG handler entrypoint for `/link <code>`."""
    return AccountsService.consume_telegram_link_code(telegram_user_id, code)
