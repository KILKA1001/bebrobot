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

    @patch("bot.services.tickets_service.db")
    @patch("bot.services.tickets_service.AccountsService.resolve_account_id")
    def test_give_ticket_by_identity_uses_account(self, mock_resolve, mock_db):
        mock_resolve.side_effect = ["acc-2", "acc-author"]
        mock_db.give_ticket_by_account.return_value = True

        result = TicketsService.give_ticket_by_identity("telegram", "201", "normal", 2, "reason", 301)

        self.assertTrue(result)
        mock_db.give_ticket_by_account.assert_called_once_with("acc-2", "normal", 2, "reason", "acc-author")


if __name__ == "__main__":
    unittest.main()
