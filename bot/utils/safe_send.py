import asyncio
import logging
from discord.errors import HTTPException

async def safe_send(destination, *args, delay: float = 1.0, **kwargs):
    """Send a message with basic rate limit handling.

    Parameters
    ----------
    destination: discord.abc.Messageable
        Channel, user or interaction to send message to.
    delay: float
        Optional delay in seconds after sending to avoid bursts.
    """
    try:
        msg = await destination.send(*args, **kwargs)
        if delay:
            await asyncio.sleep(delay)
        return msg
    except HTTPException as e:
        if e.status == 429:
            logging.warning("safe_send hit rate limit: %s", e.text)
            if delay:
                await asyncio.sleep(delay)
            return None
        raise
