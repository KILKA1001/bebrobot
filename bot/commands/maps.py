import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.utils import send_temp
from bot.systems.tournament_logic import MODE_NAMES
from bot.data.tournament_db import get_map_info

@bot.hybrid_command(
    name="mapinfo",
    description='Информация о карте по её ID'
)
async def mapinfo(ctx: commands.Context, map_id: str):
    """Показать информацию о карте по её ID."""
    info = get_map_info(map_id)
    if not info:
        await send_temp(ctx, "❌ Карта не найдена.")
        return

    embed = discord.Embed(title=f"🗺️ {info.get('name', 'Карта')}", color=discord.Color.blue())
    embed.add_field(name="ID", value=map_id, inline=True)

    mode_id = info.get("mode_id")
    if mode_id is not None:
        mode_name = MODE_NAMES.get(int(mode_id), str(mode_id))
        embed.add_field(name="Режим", value=mode_name, inline=True)

    image_url = info.get("image_url")
    if image_url:
        embed.set_image(url=image_url)

    await send_temp(ctx, embed=embed)
