import unittest
from unittest.mock import patch

from bot.telegram_bot.systems.commands_logic import (
    get_helpy_text,
    process_link_command,
    process_link_discord_command,
    process_profile_command,
)


class TelegramCommandsLogicTests(unittest.TestCase):
    @patch("bot.telegram_bot.systems.commands_logic.AccountsService.get_profile_by_account")
    @patch("bot.telegram_bot.systems.commands_logic.AccountsService.resolve_account_id")
    def test_profile_uses_target_user_from_reply(self, mock_resolve_account_id, mock_get_profile):
        mock_resolve_account_id.return_value = "acc-200"
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

        mock_resolve_account_id.assert_called_once_with("telegram", "200")
        mock_get_profile.assert_called_once_with("acc-200", display_name="Target User")
        self.assertIn('tg://user?id=200', result)
        self.assertIn("Target", result)

    @patch("bot.telegram_bot.systems.commands_logic.AccountsService.get_profile_by_account")
    @patch("bot.telegram_bot.systems.commands_logic.AccountsService.resolve_account_id")
    def test_profile_without_target_uses_caller(self, mock_resolve_account_id, mock_get_profile):
        mock_resolve_account_id.return_value = "acc-100"
        mock_get_profile.return_value = {
            "custom_nick": "Caller",
            "description": "desc",
            "nulls_brawl_id": "NB321",
            "link_status": "linked",
            "nulls_status": "linked",
            "points": 15,
        }

        result = process_profile_command(telegram_user_id=100, display_name="Caller")

        mock_resolve_account_id.assert_called_once_with("telegram", "100")
        mock_get_profile.assert_called_once_with("acc-100", display_name="Caller")
        self.assertIn('tg://user?id=100', result)

    def test_profile_transport_error_logs_explicitly(self):
        with self.assertLogs("bot.telegram_bot.systems.commands_logic", level="ERROR") as captured:
            result = process_profile_command(telegram_user_id=None)

        self.assertEqual(result, "❌ Не удалось определить пользователя Telegram.")
        self.assertIn("transport identity error", captured.output[0])
        self.assertIn("field=telegram_user_id", captured.output[0])
        self.assertIn("action=extract_platform_user_id", captured.output[0])

    def test_helpy_contains_profile_edit(self):
        self.assertIn("/profile_edit", get_helpy_text())
        self.assertIn("/points", get_helpy_text())
        self.assertIn("/balance", get_helpy_text())
        self.assertIn("/tickets", get_helpy_text())

    def test_link_command_restricted_to_private_chat(self):
        result = process_link_command('/link ABC123', telegram_user_id=100, is_private_chat=False)
        self.assertEqual(result, '❌ Команда привязки доступна только в личных сообщениях с ботом.')

    def test_link_discord_command_restricted_to_private_chat(self):
        result = process_link_discord_command(telegram_user_id=100, is_private_chat=False)
        self.assertEqual(result, '❌ Команда привязки доступна только в личных сообщениях с ботом.')


if __name__ == "__main__":
    unittest.main()
