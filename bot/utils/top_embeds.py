"""
Назначение: модуль "top embeds" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

import discord
from typing import List, Tuple, Optional


def build_top_embed(
    title: str,
    entries: List[Tuple[str, str]],
    *,
    color: discord.Color = discord.Color.gold(),
    footer: Optional[str] = None,
    start_index: int = 1,
) -> discord.Embed:
    """Create a unified embed for top lists.

    Parameters
    ----------
    title : str
        Embed title.
    entries : List[Tuple[str, str]]
        Sequence of pairs ``(name, value)`` describing each entry.
    color : discord.Color, optional
        Color of the embed border, by default ``discord.Color.gold()``.
    footer : Optional[str], optional
        Footer text, by default ``None``.
    start_index : int, optional
        Starting number for ranking, by default ``1``.
    """
    embed = discord.Embed(title=title, color=color)

    for index, (name, value) in enumerate(entries, start=start_index):
        if index == 1:
            prefix = "🥇"
        elif index == 2:
            prefix = "🥈"
        elif index == 3:
            prefix = "🥉"
        else:
            prefix = f"{index}."
        embed.add_field(name=f"{prefix} {name}", value=value, inline=False)

    if footer:
        embed.set_footer(text=footer)

    return embed
