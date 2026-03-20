import asyncio
import os
import random
import time


class RateLimiter:
    """Ensures a minimum delay between API requests."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self._next_time = 0.0
        self._base_delay = max(0.5, float(os.getenv("BOT_API_DELAY_SECONDS", "2.5")))
        self._jitter = max(0.0, float(os.getenv("BOT_API_DELAY_JITTER", "1")))

    def _normalize_delay(self, delay: float | None) -> float:
        """Return an effective delay with optional jitter.

        If caller passes a delay, we keep backwards compatibility and use it.
        Otherwise default to env-controlled pacing to distribute requests.
        """

        base = self._base_delay if delay is None else max(0.0, delay)
        if self._jitter <= 0:
            return base
        return base + random.uniform(0, self._jitter)

    async def wait(self, delay: float | None = None) -> None:
        effective_delay = self._normalize_delay(delay)
        async with self._lock:
            now = time.monotonic()
            if self._next_time > now:
                await asyncio.sleep(self._next_time - now)
            self._next_time = time.monotonic() + effective_delay


rate_limiter = RateLimiter()
