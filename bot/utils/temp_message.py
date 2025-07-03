from discord.ext import commands
import logging
from discord.errors import HTTPException

async def send_temp(ctx: commands.Context, *args, **kwargs):
    """Send a message that auto-deletes after 5 minutes by default.

    Admin replies were previously persistent which cluttered channels. Now any
    message sent via this helper will auto-delete after 5 minutes by default for
    all users, even if the message includes an interactive view. The caller can
    override this behaviour by explicitly providing ``delete_after``.
    """

    delete_after = kwargs.pop("delete_after", None)

    # If caller didn't specify behaviour explicitly, choose default.
    if delete_after is None:
        # Delete messages after 5 minutes unless overridden.
        delete_after = 300

    try:
        return await ctx.send(*args, delete_after=delete_after, **kwargs)
    except HTTPException as e:
        if e.status == 429:
            logging.warning("send_temp hit rate limit: %s", e.text)
            return None
        raise
