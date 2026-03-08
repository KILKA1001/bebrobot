import logging
from discord.errors import HTTPException
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
        return await destination.send(*args, **kwargs)
    except HTTPException as e:
        if e.status == 429:
            logging.warning("safe_send hit rate limit: %s", e.text)
            await rate_limiter.wait(delay)
            return None
        raise
