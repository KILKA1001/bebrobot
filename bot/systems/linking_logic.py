"""
Назначение: модуль "linking logic" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

from bot.services import AccountsService


def register_discord_account(discord_user_id: int) -> tuple[bool, str]:
    return AccountsService.register_identity("discord", str(discord_user_id))


def register_telegram_account(telegram_user_id: int) -> tuple[bool, str]:
    return AccountsService.register_identity("telegram", str(telegram_user_id))


def issue_discord_telegram_link_code(discord_user_id: int) -> tuple[bool, str]:
    return AccountsService.issue_discord_telegram_link_code(discord_user_id)


def consume_telegram_link_code(telegram_user_id: int, code: str) -> tuple[bool, str]:
    return AccountsService.consume_telegram_link_code(telegram_user_id, code)


def issue_telegram_discord_link_code(telegram_user_id: int) -> tuple[bool, str]:
    return AccountsService.issue_telegram_discord_link_code(telegram_user_id)


def consume_discord_link_code(discord_user_id: int, code: str) -> tuple[bool, str]:
    return AccountsService.consume_discord_link_code(discord_user_id, code)
