import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.telegram_bot.commands.guiy_owner import guiy_owner_command


class TelegramGuiyOwnerCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_reply_action_requires_reply_message(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42, is_bot=False),
            reply_to_message=None,
            bot=SimpleNamespace(get_me=AsyncMock()),
            answer=AsyncMock(),
            chat=SimpleNamespace(id=100),
        )
        command = SimpleNamespace(args="reply привет")

        with patch("bot.telegram_bot.commands.guiy_owner.persist_telegram_identity_from_user"):
            await guiy_owner_command(message, command)

        message.answer.assert_awaited_once()
        self.assertIn("ответьте", message.answer.await_args.args[0].lower())

    async def test_owner_can_send_message_as_guiy(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42, is_bot=False),
            reply_to_message=None,
            bot=SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=999))),
            answer=AsyncMock(),
            chat=SimpleNamespace(id=100),
        )
        command = SimpleNamespace(args="say привет от гуя")

        with (
            patch("bot.telegram_bot.commands.guiy_owner.persist_telegram_identity_from_user"),
            patch(
                "bot.telegram_bot.commands.guiy_owner.authorize_guiy_owner_action",
                return_value=SimpleNamespace(allowed=True),
            ) as auth_mock,
            patch(
                "bot.telegram_bot.commands.guiy_owner.resolve_guiy_target_account",
                return_value=SimpleNamespace(ok=True, message="", target_account_id="guiy-acc"),
            ) as target_mock,
        ):
            await guiy_owner_command(message, command)

        auth_mock.assert_called_once()
        target_mock.assert_called_once()
        message.answer.assert_awaited_once_with("привет от гуя")

    async def test_non_owner_gets_neutral_denial(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=77, is_bot=False),
            reply_to_message=None,
            bot=SimpleNamespace(get_me=AsyncMock()),
            answer=AsyncMock(),
            chat=SimpleNamespace(id=100),
        )
        command = SimpleNamespace(args="say привет")

        with (
            patch("bot.telegram_bot.commands.guiy_owner.persist_telegram_identity_from_user"),
            patch(
                "bot.telegram_bot.commands.guiy_owner.authorize_guiy_owner_action",
                return_value=SimpleNamespace(allowed=False),
            ),
        ):
            await guiy_owner_command(message, command)

        message.answer.assert_awaited_once()
        self.assertIn("недоступна", message.answer.await_args.args[0].lower())


if __name__ == "__main__":
    unittest.main()


class TelegramGuiyOwnerVisibilityTests(unittest.TestCase):
    def test_guiy_owner_hidden_from_public_help(self):
        from bot.telegram_bot.systems.commands_logic import HELPY_TEXT

        self.assertNotIn("/guiy_owner", HELPY_TEXT)

    def test_guiy_owner_hidden_from_public_command_menu(self):
        from bot.telegram_bot.main import BOT_COMMANDS

        self.assertNotIn("guiy_owner", [item.command for item in BOT_COMMANDS])
