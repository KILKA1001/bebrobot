"""
Назначение: модуль "test telegram polling lock" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import asyncio
import hashlib
import logging
from unittest.mock import patch

import pytest

from bot.telegram_bot.main import (
    TelegramPollingAlreadyRunningInProcessError,
    TelegramPollingTransientNetworkError,
    _patch_aiogram_conflict_behavior,
    run_polling,
)


def test_run_polling_raises_when_same_process_already_owns_lock(caplog: pytest.LogCaptureFixture) -> None:
    token = "dummy-token"
    current_pid = 1234
    current_hostname = "host-a"
    owner_line = f"pid={current_pid} hostname={current_hostname} started_at=2026-03-16T23:19:42.670673+00:00"

    caplog.set_level(logging.INFO)

    with (
        patch("bot.telegram_bot.main.os.getpid", return_value=current_pid),
        patch("bot.telegram_bot.main.socket.gethostname", return_value=current_hostname),
        patch("bot.telegram_bot.main.os.open", return_value=99),
        patch("bot.telegram_bot.main.fcntl.flock", side_effect=BlockingIOError),
        patch("bot.telegram_bot.main.os.lseek", return_value=0),
        patch("bot.telegram_bot.main.os.read", return_value=owner_line.encode("utf-8")),
        patch("bot.telegram_bot.main.os.close") as close_mock,
    ):
        with pytest.raises(TelegramPollingAlreadyRunningInProcessError):
            asyncio.run(run_polling(token))

    close_mock.assert_called_once_with(99)


    log_text = caplog.text
    assert "BOT_RUNTIME" not in log_text
    assert hashlib.sha256(token.encode("utf-8")).hexdigest()[:12] in log_text


def test_patch_aiogram_conflict_behavior_stops_after_bounded_transient_retries():
    with patch("bot.telegram_bot.main.os.getenv", side_effect=lambda key, default=None: "2" if key == "TELEGRAM_POLLING_MAX_TRANSIENT_FAILURES" else default):
        _patch_aiogram_conflict_behavior()
        listen_updates = run_polling.__globals__["Dispatcher"]._listen_updates.__func__

        class DummyBot:
            id = 999
            session = type("Session", (), {"timeout": None})()

            async def __call__(self, *_args, **_kwargs):
                raise ConnectionError("boom")

        bot = DummyBot()

        async def _exercise() -> None:
            async for _ in listen_updates(run_polling.__globals__["Dispatcher"], bot):
                pass

        with pytest.raises(TelegramPollingTransientNetworkError):
            asyncio.run(_exercise())
