"""
Назначение: тесты роутера Telegram proposal.
Где используется: Telegram (тесты).
"""

import unittest
from types import SimpleNamespace

from aiogram.dispatcher.event.bases import SkipHandler

from bot.telegram_bot.commands import proposal


class TelegramProposalRouterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        proposal._PENDING_PROPOSAL_INPUT.clear()
        proposal._PENDING_PROPOSAL_CONFIRM.clear()
        proposal._ARCHIVE_FILTERS_BY_USER.clear()

    async def test_pending_input_skips_when_user_has_no_pending_form(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=111),
            text="обычное сообщение",
        )

        with self.assertRaises(SkipHandler):
            await proposal.proposal_pending_input(message)

    async def test_pending_input_skips_when_message_is_command(self):
        actor_id = 222
        proposal._PENDING_PROPOSAL_INPUT[actor_id] = proposal.time.time()
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=actor_id),
            text="/help",
        )

        with self.assertRaises(SkipHandler):
            await proposal.proposal_pending_input(message)


if __name__ == "__main__":
    unittest.main()
