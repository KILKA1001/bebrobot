import asyncio
import os
import random
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class RateLimitWaitResult:
    bucket: str
    requested_delay: float | None
    effective_delay: float
    waited_for: float
    next_available_at: float


class RateLimiter:
    """Ensures independent pacing buckets for different Discord API request types."""

    def __init__(self):
        default_delay = max(0.5, float(os.getenv("BOT_API_DELAY_SECONDS", "1.5")))
        default_jitter = max(0.0, float(os.getenv("BOT_API_DELAY_JITTER", "0.5")))

        self._bucket_defaults = {
            "interaction_ack": {
                "delay": 0.0,
                "jitter": 0.0,
            },
            "followup": {
                "delay": default_delay,
                "jitter": default_jitter,
            },
            "channel_send": {
                "delay": default_delay,
                "jitter": default_jitter,
            },
        }
        self._bucket_states = {
            name: {"lock": asyncio.Lock(), "next_time": 0.0}
            for name in self._bucket_defaults
        }

    def _normalize_delay(self, bucket: str, delay: float | None) -> float:
        """Return an effective delay for the bucket with optional jitter."""

        if bucket not in self._bucket_defaults:
            raise ValueError(f"Unknown rate limiter bucket: {bucket}")

        bucket_config = self._bucket_defaults[bucket]
        base = bucket_config["delay"] if delay is None else max(0.0, delay)
        jitter = bucket_config["jitter"]
        if jitter <= 0:
            return base
        return base + random.uniform(0, jitter)

    async def wait(self, bucket: str, delay: float | None = None) -> RateLimitWaitResult:
        effective_delay = self._normalize_delay(bucket, delay)
        state = self._bucket_states[bucket]

        async with state["lock"]:
            now = time.monotonic()
            waited_for = max(0.0, state["next_time"] - now)
            if waited_for > 0:
                await asyncio.sleep(waited_for)
            state["next_time"] = time.monotonic() + effective_delay
            return RateLimitWaitResult(
                bucket=bucket,
                requested_delay=delay,
                effective_delay=effective_delay,
                waited_for=waited_for,
                next_available_at=state["next_time"],
            )

    def snapshot(self) -> dict[str, float]:
        return {
            bucket: state["next_time"]
            for bucket, state in self._bucket_states.items()
        }


rate_limiter = RateLimiter()
