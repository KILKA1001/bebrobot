import unittest
from unittest.mock import patch

from bot.services.authority_service import AuthorityService
from bot.services.points_service import PointsService
from bot.services.tickets_service import TicketsService


class EngagementServicesTests(unittest.TestCase):
    @patch("bot.services.points_service.db")
    @patch("bot.services.points_service.AccountsService.resolve_account_id")
    def test_add_points_by_identity_uses_account(self, mock_resolve, mock_db):
        mock_resolve.return_value = "acc-1"
        mock_db._get_discord_user_for_account_id.return_value = 111
        mock_db.add_action.return_value = True

        result = PointsService.add_points_by_identity("telegram", "200", 10, "reason", 300)

        self.assertTrue(result)
        mock_db.add_action.assert_called_once_with(111, 10, "reason", 300)

    @patch("bot.services.tickets_service.db")
    @patch("bot.services.tickets_service.AccountsService.resolve_account_id")
    def test_give_ticket_by_identity_uses_account(self, mock_resolve, mock_db):
        mock_resolve.return_value = "acc-2"
        mock_db._get_discord_user_for_account_id.return_value = 222
        mock_db.give_ticket.return_value = True

        result = TicketsService.give_ticket_by_identity("telegram", "201", "normal", 2, "reason", 301)

        self.assertTrue(result)
        mock_db.give_ticket.assert_called_once_with(222, "normal", 2, "reason", 301)

    @patch("bot.services.points_service.db")
    @patch("bot.services.points_service.AccountsService.resolve_account_id")
    def test_add_points_by_identity_without_discord_anchor_fails(self, mock_resolve, mock_db):
        mock_resolve.return_value = "acc-3"
        mock_db._get_discord_user_for_account_id.return_value = None

        result = PointsService.add_points_by_identity("telegram", "202", 5, "reason", 302)

        self.assertFalse(result)
        mock_db.add_action.assert_not_called()

    def test_authority_allows_titles_with_suffixes(self):
        self.assertEqual(AuthorityService._title_weight("Главный вице клуба"), 100)
        self.assertEqual(AuthorityService._title_weight("Ветеран города [old]"), 30)


if __name__ == "__main__":
    unittest.main()
