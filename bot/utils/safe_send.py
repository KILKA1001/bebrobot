import logging
from discord.errors import HTTPException
from .rate_limiter import rate_limiter

async def safe_send(destination, *args, delay: float = 1.5, **kwargs):
    """Send a message with global rate limiting.

    Parameters
    ----------
    destination: discord.abc.Messageable
        Channel, user or interaction to send message to.
    delay: float
        Optional delay in seconds after sending to avoid bursts.
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
