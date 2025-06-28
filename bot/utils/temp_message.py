import discord
from discord.ext import commands

async def send_temp(ctx: commands.Context, *args, **kwargs):
    """Send a message that auto-deletes after 5 minutes for non-admins."""
    delete_after = kwargs.pop("delete_after", None)
    if delete_after is None and not ctx.author.guild_permissions.administrator:
        delete_after = 300
    return await ctx.send(*args, delete_after=delete_after, **kwargs)
