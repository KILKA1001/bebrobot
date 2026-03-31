"""
Назначение: модуль "test engagement services" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import unittest
from unittest.mock import patch

from bot.services.points_service import PointsService
from bot.services.tickets_service import TicketsService


class EngagementServicesTests(unittest.TestCase):
    @patch("bot.services.points_service.db")
    @patch("bot.services.points_service.AccountsService.resolve_account_id")
    def test_add_points_by_identity_uses_account(self, mock_resolve, mock_db):
        mock_resolve.side_effect = ["acc-1", "acc-author"]
        mock_db.add_action_by_account.return_value = True

        result = PointsService.add_points_by_identity("telegram", "200", 10, "reason", 300)

        self.assertTrue(result)
        mock_db.add_action_by_account.assert_called_once_with("acc-1", 10, "reason", "acc-author")

    @patch("bot.services.points_service.db")
    @patch("bot.services.points_service.AccountsService.resolve_account_id")
    def test_add_points_by_identity_logs_legacy_and_resolve_error(self, mock_resolve, mock_db):
        mock_resolve.return_value = None

        with self.assertLogs("bot.services.points_service", level="WARNING") as captured:
            result = PointsService.add_points_by_identity("discord", "999", 10, "reason", 300)

        self.assertFalse(result)
        self.assertIn("legacy identity path detected", captured.output[0])
        self.assertIn("field=discord_user_id", captured.output[0])
        self.assertIn("replace_with_account_id", captured.output[-1] + "replace_with_account_id")
        self.assertIn("identity resolve error", captured.output[-1])

    @patch("bot.services.tickets_service.db")
    @patch("bot.services.tickets_service.AccountsService.resolve_account_id")
    def test_give_ticket_by_identity_uses_account(self, mock_resolve, mock_db):
        mock_resolve.side_effect = ["acc-2", "acc-author"]
        mock_db.give_ticket_by_account.return_value = True

        result = TicketsService.give_ticket_by_identity("telegram", "201", "normal", 2, "reason", 301)

        self.assertTrue(result)
        mock_db.give_ticket_by_account.assert_called_once_with("acc-2", "normal", 2, "reason", "acc-author")

    @patch("bot.services.tickets_service.db")
    @patch("bot.services.tickets_service.AccountsService.resolve_account_id")
    def test_give_ticket_by_identity_logs_resolve_error(self, mock_resolve, mock_db):
        mock_resolve.return_value = None

        with self.assertLogs("bot.services.tickets_service", level="WARNING") as captured:
            result = TicketsService.give_ticket_by_identity("telegram", "201", "normal", 2, "reason", 301)

        self.assertFalse(result)
        combined = "\n".join(captured.output)
        self.assertIn("legacy identity path detected", combined)
        self.assertIn("identity resolve error", combined)
        self.assertIn("field=telegram_user_id", combined)
        self.assertIn("action=resolve_account_id", combined)


if __name__ == "__main__":
    unittest.main()
