import unittest
from unittest.mock import patch

from bot.services.authority_service import AuthorityService


class AuthorityServiceTests(unittest.TestCase):
    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_resolve_authority_from_titles(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-1"
        mock_titles.return_value = ["Участник клубов", "Вице города"]

        result = AuthorityService.resolve_authority("discord", "100")

        self.assertEqual(result.level, 80)
        self.assertEqual(result.rank_weight, 80)

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_permission_matrix(self, mock_resolve, mock_titles):
        mock_resolve.return_value = "acc-2"
        mock_titles.return_value = ["Ветеран города"]

        self.assertTrue(AuthorityService.has_command_permission("discord", "200", "points_manage"))
        self.assertFalse(AuthorityService.has_command_permission("discord", "200", "tickets_manage"))

    @patch("bot.services.authority_service.AccountsService.get_account_titles")
    @patch("bot.services.authority_service.AccountsService.resolve_account_id")
    def test_hierarchy_prevents_equal_roles(self, mock_resolve, mock_titles):
        def _resolve(_provider, user_id):
            return f"acc-{user_id}"

        def _titles(account_id):
            if account_id == "acc-1":
                return ["Главный вице"]
            return ["Глава клуба"]

        mock_resolve.side_effect = _resolve
        mock_titles.side_effect = _titles

        self.assertFalse(AuthorityService.can_manage_target("discord", "1", "discord", "2"))


if __name__ == "__main__":
    unittest.main()
