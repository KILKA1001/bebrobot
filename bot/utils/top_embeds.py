import discord
from typing import List, Tuple, Optional


def build_top_embed(
    title: str,
    entries: List[Tuple[str, str]],
    *,
    color: discord.Color = discord.Color.gold(),
    footer: Optional[str] = None,
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
    """
    embed = discord.Embed(title=title, color=color)

    for index, (name, value) in enumerate(entries, start=1):
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
