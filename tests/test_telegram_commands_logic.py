import unittest
from unittest.mock import patch

from bot.telegram_bot.systems.commands_logic import (
    process_link_command,
    process_link_discord_command,
    process_profile_command,
)


class TelegramCommandsLogicTests(unittest.TestCase):
    @patch("bot.telegram_bot.systems.commands_logic.AccountsService.get_profile")
    def test_profile_uses_target_user_from_reply(self, mock_get_profile):
        mock_get_profile.return_value = {
            "custom_nick": "Target",
            "description": "desc",
            "nulls_brawl_id": "NB123",
            "link_status": "linked",
            "nulls_status": "linked",
            "points": 10,
        }

        result = process_profile_command(
            telegram_user_id=100,
            display_name="Caller",
            target_telegram_user_id=200,
            target_display_name="Target User",
        )

        mock_get_profile.assert_called_once_with("telegram", "200", display_name="Target User")
        self.assertIn('tg://user?id=200', result)
        self.assertIn("Target User", result)

    @patch("bot.telegram_bot.systems.commands_logic.AccountsService.get_profile")
    def test_profile_without_target_uses_caller(self, mock_get_profile):
        mock_get_profile.return_value = {
            "custom_nick": "Caller",
            "description": "desc",
            "nulls_brawl_id": "NB321",
            "link_status": "linked",
            "nulls_status": "linked",
            "points": 15,
        }

        result = process_profile_command(telegram_user_id=100, display_name="Caller")

        mock_get_profile.assert_called_once_with("telegram", "100", display_name="Caller")
        self.assertIn('tg://user?id=100', result)

    @patch("bot.telegram_bot.systems.commands_logic.handle_link_command")
    def test_link_command_restricted_to_private_chat(self, mock_handle_link):
        result = process_link_command('/link ABC123', telegram_user_id=100, is_private_chat=False)

        self.assertEqual(result, '❌ Команда привязки доступна только в личных сообщениях с ботом.')
        mock_handle_link.assert_not_called()

    @patch("bot.telegram_bot.systems.commands_logic.issue_telegram_discord_link_code")
    def test_link_discord_command_restricted_to_private_chat(self, mock_issue_link_code):
        result = process_link_discord_command(telegram_user_id=100, is_private_chat=False)

        self.assertEqual(result, '❌ Команда привязки доступна только в личных сообщениях с ботом.')
        mock_issue_link_code.assert_not_called()


if __name__ == "__main__":
    unittest.main()
