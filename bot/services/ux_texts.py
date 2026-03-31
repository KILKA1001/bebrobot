"""
Назначение: модуль "ux texts" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
Доменные операции: доменные операции модуля "ux texts".
"""

from __future__ import annotations


def compose_three_block_message(*, what: str, now: str, next_step: str, emoji: str | None = None) -> str:
    """Build a short 3-block message: what it is, what to do now, what happens next."""
    prefix = f"{emoji} " if emoji else ""
    return (
        f"{prefix}<b>Что это:</b> {what}\n"
        f"<b>Что делать сейчас:</b> {now}\n"
        f"<b>Что будет дальше:</b> {next_step}"
    )


def compose_three_block_plain(*, what: str, now: str, next_step: str, emoji: str | None = None) -> str:
    """Plain-text version for Discord and logs when HTML is not needed."""
    prefix = f"{emoji} " if emoji else ""
    return (
        f"{prefix}Что это: {what}\n"
        f"Что делать сейчас: {now}\n"
        f"Что будет дальше: {next_step}"
    )
