import logging
from discord.errors import HTTPException
from discord.ext import commands
from .rate_limiter import rate_limiter


async def safe_send(destination, *args, delay: float | None = None, **kwargs):
    """Send a message with global rate limiting.

    Parameters
    ----------
    destination: discord.abc.Messageable
        Channel, user or interaction to send message to.
    delay: float | None
        Optional delay override in seconds. If None, env defaults are used.
    """
    await rate_limiter.wait(delay)
    try:
        if isinstance(destination, commands.Context) and destination.interaction:
            if destination.interaction.response.is_done():
                delete_after = kwargs.pop("delete_after", None)
                message = await destination.interaction.followup.send(*args, **kwargs)
                if delete_after is not None:
                    await message.delete(delay=delete_after)
                return message
            return await destination.interaction.response.send_message(*args, **kwargs)
        return await destination.send(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429:
            logging.warning("safe_send hit rate limit: %s", e.text)
            await rate_limiter.wait(delay)
            return None
        raise
