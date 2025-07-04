import asyncio
import time

class RateLimiter:
    """Ensures a minimum delay between API requests."""
    def __init__(self):
        self._lock = asyncio.Lock()
        self._next_time = 0.0

    async def wait(self, delay: float) -> None:
        async with self._lock:
            now = time.monotonic()
            if self._next_time > now:
                await asyncio.sleep(self._next_time - now)
            self._next_time = time.monotonic() + delay

rate_limiter = RateLimiter()
