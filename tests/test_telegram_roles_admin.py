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
            "bot.telegram_bot.commands.roles_admin.AccountsService.resolve_user_lookup",
            return_value={
                "status": "multiple",
                "candidates": [
                    {"provider": "telegram", "provider_user_id": "1", "username": "dup_user", "display_name": "One"},
                    {"provider": "telegram", "provider_user_id": "2", "username": "dup_user", "display_name": "Two"},
                ],
            },
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
            "bot.telegram_bot.commands.roles_admin.AccountsService.resolve_user_lookup",
            return_value={"status": "not_found", "candidates": [], "reason": "not_found"},
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

    def test_resolve_target_supports_cross_provider_prefix(self):
        with patch(
            "bot.telegram_bot.commands.roles_admin.AccountsService.resolve_user_lookup",
            return_value={
                "status": "ok",
                "result": {
                    "account_id": "acc-1",
                    "provider": "discord",
                    "provider_user_id": "555",
                    "username": "discord_target",
                    "display_name": "Discord Target",
                    "matched_by": "discord_username",
                },
                "candidates": [],
            },
        ):
            result = _resolve_telegram_target(
                actor_id=100,
                raw_target="ds:discord_target",
                reply_user=None,
                operation="user_grant",
                source="fallback_text_command",
            )

        self.assertEqual(result["provider"], "discord")
        self.assertEqual(result["provider_user_id"], "555")
        self.assertEqual(result["account_id"], "acc-1")
        self.assertEqual(result["matched_by"], "discord_username")


if __name__ == "__main__":
    unittest.main()
