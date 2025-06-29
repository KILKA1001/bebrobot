import discord
from discord.ext import commands

async def send_temp(ctx: commands.Context, *args, **kwargs):
    """Send a message that auto-deletes after 5 minutes unless it contains a view.

    Admin replies were previously persistent which cluttered channels. Now any
    message sent via this helper will auto-delete after 5 minutes by default for
    all users. Messages that include interactive views (e.g. tournament
    announcements) are kept unless ``delete_after`` is explicitly provided.
    """

    delete_after = kwargs.pop("delete_after", None)

    # If caller didn't specify behaviour explicitly, choose defaults.
    if delete_after is None:
        # Preserve messages with views (interactive components) by default.
        delete_after = None if "view" in kwargs else 300

    return await ctx.send(*args, delete_after=delete_after, **kwargs)
