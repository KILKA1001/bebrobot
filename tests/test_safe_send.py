"""
Назначение: модуль "test safe send" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import unittest
from unittest.mock import AsyncMock, patch

from bot.utils.safe_send import safe_send


class _FakeMessage:
    def __init__(self):
        self.delete = AsyncMock()


class _FakeResponse:
    def __init__(self, done=True):
        self._done = done
        self.send_message = AsyncMock()

    def is_done(self):
        return self._done


class _FakeInteraction:
    def __init__(self, done=True):
        self.response = _FakeResponse(done=done)
        self.followup = type("_Followup", (), {})()
        self.followup.send = AsyncMock(return_value=_FakeMessage())


class _FakeContext:
    def __init__(self, done=True):
        self.interaction = _FakeInteraction(done=done)


class SafeSendTests(unittest.IsolatedAsyncioTestCase):
    async def test_followup_send_removes_delete_after_and_schedules_delete(self):
        ctx = _FakeContext(done=True)

        with patch("bot.utils.safe_send.commands.Context", _FakeContext), patch(
            "bot.utils.safe_send.rate_limiter.wait", new=AsyncMock()
        ):
            message = await safe_send(ctx, "hello", delete_after=10)

        ctx.interaction.followup.send.assert_awaited_once_with("hello")
        message.delete.assert_awaited_once_with(delay=10)

    async def test_followup_send_keeps_message_when_delete_after_none(self):
        ctx = _FakeContext(done=True)

        with patch("bot.utils.safe_send.commands.Context", _FakeContext), patch(
            "bot.utils.safe_send.rate_limiter.wait", new=AsyncMock()
        ):
            message = await safe_send(ctx, "hello", delete_after=None)

        ctx.interaction.followup.send.assert_awaited_once_with("hello")
        message.delete.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
