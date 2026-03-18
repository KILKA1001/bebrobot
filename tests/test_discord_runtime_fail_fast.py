import asyncio
import importlib
import os
import sys
from unittest.mock import AsyncMock, patch

import discord
import pytest


class _FakeQuery:
    def select(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def execute(self):
        return type("Response", (), {"data": []})()


class _FakeSupabase:
    def table(self, *_args, **_kwargs):
        return _FakeQuery()


class DummyResponse:
    status = 429
    reason = "Too Many Requests"
    headers = {"Retry-After": "5"}


def load_bot_main():
    with (
        patch.dict(os.environ, {"SUPABASE_URL": "https://example.supabase.co", "SUPABASE_KEY": "test-key"}, clear=False),
        patch("supabase.create_client", return_value=_FakeSupabase()),
    ):
        db_module = importlib.import_module("bot.data.db")

        db_module.db.supabase = _FakeSupabase()
        sys.modules.pop("bot.main", None)
        sys.modules.pop("bot.data.tournament_db", None)
        import bot.main as bot_main
        return importlib.reload(bot_main)


def make_http_exception(status: int = 429, text: str = "rate limited") -> discord.HTTPException:
    response = DummyResponse()
    response.status = status
    response.reason = "HTTP error"
    return discord.HTTPException(response, text)


def test_run_discord_main_does_not_retry_after_http_exception():
    bot_main = load_bot_main()
    exc = make_http_exception()

    with (
        patch("bot.main.keep_alive"),
        patch("bot.main.configure_logging"),
        patch("bot.main.load_dotenv"),
        patch.dict(os.environ, {"DISCORD_TOKEN": "test-token"}, clear=False),
        patch.object(bot_main.bot, "run", side_effect=exc) as run_mock,
        patch("bot.main.log_discord_http_exception") as log_mock,
    ):
        with pytest.raises(discord.HTTPException):
            bot_main.run_discord_main("test-token")

    assert run_mock.call_count == 1
    log_mock.assert_called_once()


def test_run_both_async_keeps_telegram_running_after_discord_failure():
    bot_main = load_bot_main()
    exc = RuntimeError("discord boom")

    async def _exercise() -> None:
        telegram_started = asyncio.Event()

        async def fake_run_telegram_polling(_token: str) -> None:
            telegram_started.set()
            await asyncio.Event().wait()

        with (
            patch("bot.main.run_telegram_polling", side_effect=fake_run_telegram_polling) as run_telegram_polling_mock,
            patch.object(bot_main.bot, "start", AsyncMock(side_effect=exc)) as start_mock,
            patch.object(bot_main.bot, "close", AsyncMock()) as close_mock,
        ):
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(bot_main._run_both_async("discord-token", "telegram-token"), timeout=0.05)

            assert telegram_started.is_set()
            assert start_mock.await_count == 1
            assert close_mock.await_count >= 1
            assert run_telegram_polling_mock.call_count >= 1

    asyncio.run(_exercise())
