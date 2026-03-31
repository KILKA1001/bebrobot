"""
Назначение: модуль "link handler" реализует продуктовый контур в зоне Telegram.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Telegram.
"""

from bot.systems.linking_logic import consume_telegram_link_code


def handle_link_command(telegram_user_id: int, code: str) -> tuple[bool, str]:
    """Platform-agnostic TG handler entrypoint for `/link <code>`."""
    return consume_telegram_link_code(telegram_user_id, code)
