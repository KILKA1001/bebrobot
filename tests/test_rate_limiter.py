import os
from unittest import mock

from bot.utils.rate_limiter import RateLimiter


def test_rate_limiter_uses_3_seconds_by_default():
    with mock.patch.dict(os.environ, {}, clear=True):
        limiter = RateLimiter()

    assert limiter._base_delay == 3.0


def test_rate_limiter_still_honors_env_override():
    with mock.patch.dict(os.environ, {"BOT_API_DELAY_SECONDS": "1.75"}, clear=True):
        limiter = RateLimiter()

    assert limiter._base_delay == 1.75
