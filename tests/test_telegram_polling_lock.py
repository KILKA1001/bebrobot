import asyncio
from unittest.mock import patch

import pytest

from bot.telegram_bot.main import (
    TelegramPollingAlreadyRunningInProcessError,
    run_polling,
)


def test_run_polling_raises_when_same_process_already_owns_lock() -> None:
    token = "dummy-token"
    current_pid = 1234
    current_hostname = "host-a"
    owner_line = f"pid={current_pid} hostname={current_hostname} started_at=2026-03-16T23:19:42.670673+00:00"

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
