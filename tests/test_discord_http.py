import unittest

import discord

from bot.utils.discord_http import (
    build_http_exception_log_context,
    extract_retry_after_seconds,
    is_cloudflare_rate_limited_http_exception,
)


class _FakeResponse:
    def __init__(self, *, status=429, reason="Too Many Requests", headers=None):
        self.status = status
        self.reason = reason
        self.headers = headers or {}


class DiscordHttpTests(unittest.TestCase):
    def test_detects_cloudflare_1015_even_when_discord_reports_429(self):
        exc = discord.HTTPException(
            _FakeResponse(headers={"server": "cloudflare", "cf-ray": "ray-123"}),
            "<html><title>Access denied</title>Error 1015 You are being rate limited</html>",
        )

        self.assertTrue(is_cloudflare_rate_limited_http_exception(exc))

    def test_extract_retry_after_from_headers(self):
        exc = discord.HTTPException(
            _FakeResponse(headers={"Retry-After": "17.5"}),
            "rate limited",
        )

        self.assertEqual(extract_retry_after_seconds(exc), 17.5)

    def test_extract_retry_after_ignores_cloudflare_html_noise(self):
        exc = discord.HTTPException(
            _FakeResponse(headers={"server": "cloudflare", "cf-ray": "ray-123"}),
            """<!doctype html><html><body><script>var retryAfter=1555;</script>
            <h1>Error 1015</h1><p>You are being rate limited</p></body></html>""",
        )

        self.assertIsNone(extract_retry_after_seconds(exc))

    def test_build_log_context_sanitizes_html(self):
        exc = discord.HTTPException(
            _FakeResponse(headers={"server": "cloudflare", "cf-ray": "ray-999"}),
            "<html><body>Error 1015 <strong>Access denied</strong><script>var x=1</script></body></html>",
        )

        context = build_http_exception_log_context(exc, stage="unit-test")

        self.assertEqual(context["cf_ray"], "ray-999")
        self.assertEqual(context["stage"], "unit-test")
        self.assertIn("Error 1015 Access denied", context["error_excerpt"])
        self.assertNotIn("<strong>", context["error_excerpt"])
        self.assertNotIn("var x=1", context["error_excerpt"])


if __name__ == "__main__":
    unittest.main()
