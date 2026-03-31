"""
Назначение: модуль "test admin api" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import unittest
from unittest.mock import patch

from bot.admin_api.app import admin_app


class AdminApiTests(unittest.TestCase):
    def setUp(self):
        self.client = admin_app.test_client()

    @patch("bot.admin_api.app._build_user_payload")
    @patch("bot.admin_api.app._resolve_account_id")
    def test_user_view_endpoint(self, mock_resolve_account, mock_payload):
        mock_resolve_account.return_value = "acc-1"
        mock_payload.return_value = {
            "account_id": "acc-1",
            "custom_roles": [{"name": "оператор", "source": "custom"}],
            "external_roles": [{"name": "vet", "source": "discord"}],
            "permissions": {"allow": ["x"], "deny": []},
        }

        response = self.client.get("/admin/api/users/discord/100")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["provider"], "discord")
        self.assertEqual(body["account_id"], "acc-1")

    @patch("bot.admin_api.app.db.supabase", None)
    @patch("bot.admin_api.app.AuthorityService.can_manage_role")
    @patch("bot.admin_api.app._resolve_account_id")
    def test_custom_role_change_returns_db_not_configured(self, mock_resolve_account, mock_can_manage):
        mock_can_manage.return_value = True
        mock_resolve_account.return_value = "acc-2"

        response = self.client.post(
            "/admin/api/users/telegram/200/roles/custom",
            json={
                "action": "assign",
                "role_name": "оператор",
                "actor_provider": "discord",
                "actor_user_id": "999",
            },
        )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.get_json()["error"], "db_not_configured")

    @patch("bot.admin_api.app._build_user_payload")
    @patch("bot.admin_api.app._resolve_account_id")
    def test_roles_ui_contains_required_sections(self, mock_resolve_account, mock_payload):
        mock_resolve_account.return_value = "acc-5"
        mock_payload.return_value = {
            "account_id": "acc-5",
            "custom_roles": [{"name": "оператор", "source": "custom"}],
            "external_roles": [{"name": "главный вице", "source": "telegram"}],
            "permissions": {"allow": [], "deny": []},
        }

        response = self.client.get("/admin/users/discord/555/roles")

        self.assertEqual(response.status_code, 200)
        text = response.get_data(as_text=True)
        self.assertIn("Кастомные роли (редактируемые)", text)
        self.assertIn("Discord/Telegram роли (только просмотр, синк)", text)


if __name__ == "__main__":
    unittest.main()
