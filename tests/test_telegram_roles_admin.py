import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bot.telegram_bot.commands.roles_admin import _resolve_telegram_target


class TelegramRolesAdminTargetResolutionTests(unittest.TestCase):
    def test_resolve_target_prefers_reply_user(self):
        reply_user = SimpleNamespace(id=777, username="reply_target", full_name="Reply Target", is_bot=False)

        with patch("bot.telegram_bot.commands.roles_admin.AccountsService.persist_identity_lookup_fields") as persist_mock:
            result = _resolve_telegram_target(
                actor_id=100,
                raw_target="@ignored",
                reply_user=reply_user,
                operation="user_roles",
                source="button",
            )

        self.assertEqual(result["provider_user_id"], "777")
        self.assertEqual(result["label"], "@reply_target")
        persist_mock.assert_called_once()

    def test_resolve_target_returns_multiple_error_for_ambiguous_username(self):
        with patch(
            "bot.telegram_bot.commands.roles_admin.AccountsService.find_accounts_by_identity_username",
            return_value=[
                {"provider_user_id": "1", "username": "dup_user", "display_name": "One"},
                {"provider_user_id": "2", "username": "dup_user", "display_name": "Two"},
            ],
        ):
            result = _resolve_telegram_target(
                actor_id=100,
                raw_target="@dup_user",
                reply_user=None,
                operation="user_grant",
                source="fallback_text_command",
            )

        self.assertEqual(result["error"], "multiple")
        self.assertIn("Найдено несколько пользователей", result["message"])

    def test_resolve_target_returns_not_found_for_unknown_username(self):
        with patch(
            "bot.telegram_bot.commands.roles_admin.AccountsService.find_accounts_by_identity_username",
            return_value=[],
        ):
            result = _resolve_telegram_target(
                actor_id=100,
                raw_target="missing_user",
                reply_user=None,
                operation="user_revoke",
                source="fallback_text_command",
            )

        self.assertEqual(result["error"], "not_found")
        self.assertIn("Пользователь не найден", result["message"])


if __name__ == "__main__":
    unittest.main()
