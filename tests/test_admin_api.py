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
    @patch("bot.admin_api.app._log_admin_api_error")
    def test_custom_role_change_returns_db_not_configured(
        self,
        mock_log_admin_api_error,
        mock_resolve_account,
        mock_can_manage,
    ):
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
        self.assertTrue(mock_log_admin_api_error.called)
        self.assertEqual(mock_log_admin_api_error.call_args.kwargs["reason"], "db_not_configured")

    @patch("bot.admin_api.app._log_admin_api_error")
    def test_custom_role_change_logs_validation_error(self, mock_log_admin_api_error):
        response = self.client.post(
            "/admin/api/users/telegram/200/roles/custom",
            json={"action": "assign", "role_name": "", "actor_provider": "discord", "actor_user_id": "999"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "bad_request")
        self.assertTrue(mock_log_admin_api_error.called)
        self.assertEqual(mock_log_admin_api_error.call_args.kwargs["reason"], "validation_failed_bad_request")

    @patch("bot.admin_api.app.AuthorityService.can_manage_role", return_value=False)
    @patch("bot.admin_api.app._log_admin_api_error")
    def test_custom_role_change_logs_permission_error(self, mock_log_admin_api_error, _mock_can_manage):
        response = self.client.post(
            "/admin/api/users/telegram/200/roles/custom",
            json={"action": "assign", "role_name": "оператор", "actor_provider": "discord", "actor_user_id": "999"},
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["error"], "forbidden_role_manage")
        self.assertTrue(mock_log_admin_api_error.called)
        self.assertEqual(mock_log_admin_api_error.call_args.kwargs["reason"], "permission_denied_role_manage")

    @patch("bot.admin_api.app.AuthorityService.can_manage_role", return_value=True)
    @patch("bot.admin_api.app._resolve_account_id", return_value=None)
    @patch("bot.admin_api.app._log_admin_api_error")
    def test_custom_role_change_logs_external_api_resolution_error(
        self,
        mock_log_admin_api_error,
        _mock_resolve_account,
        _mock_can_manage,
    ):
        response = self.client.post(
            "/admin/api/users/telegram/200/roles/custom",
            json={"action": "assign", "role_name": "оператор", "actor_provider": "discord", "actor_user_id": "999"},
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_json()["error"], "user_not_found")
        self.assertTrue(mock_log_admin_api_error.called)
        self.assertEqual(mock_log_admin_api_error.call_args.kwargs["reason"], "external_api_account_not_found")

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


    @patch("bot.admin_api.app.CouncilPauseService.get_pause_status_for_admin")
    def test_admin_council_pause_api_returns_reason_and_timestamp(self, mock_pause_status):
        mock_pause_status.return_value = {
            "paused": True,
            "reason": "term_ended_without_launch_confirmation",
            "paused_at": "2026-04-13T12:00:00+00:00",
            "message": "Пауза включена",
        }

        response = self.client.get("/admin/api/council/pause")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["paused"])
        self.assertEqual(payload["reason"], "term_ended_without_launch_confirmation")
        self.assertEqual(payload["paused_at"], "2026-04-13T12:00:00+00:00")

    @patch("bot.admin_api.app.CouncilPauseService.get_pause_status_for_admin")
    def test_admin_council_pause_view_shows_reason_and_timestamp(self, mock_pause_status):
        mock_pause_status.return_value = {
            "paused": True,
            "reason": "term_ended_without_launch_confirmation",
            "paused_at": "2026-04-13T12:00:00+00:00",
            "message": "Пауза включена",
        }

        response = self.client.get("/admin/council/pause")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("term_ended_without_launch_confirmation", body)
        self.assertIn("2026-04-13T12:00:00+00:00", body)


if __name__ == "__main__":
    unittest.main()
