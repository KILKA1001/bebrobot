import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bot.telegram_bot.commands.roles_admin import (
    _build_pick_role_keyboard,
    _resolve_telegram_target,
)


class TelegramRolesAdminTargetResolutionTests(unittest.TestCase):
    def test_resolve_target_uses_reply_when_explicit_target_missing(self):
        reply_user = SimpleNamespace(id=777, username="reply_target", full_name="Reply Target", is_bot=False)

        with patch("bot.telegram_bot.commands.roles_admin.AccountsService.resolve_account_id", return_value="acc-777"):
            result = _resolve_telegram_target(
                actor_id=100,
                raw_target=None,
                reply_user=reply_user,
                operation="user_roles",
                source="button",
            )

        self.assertEqual(result["provider_user_id"], "777")
        self.assertEqual(result["label"], "@reply_target")
        self.assertEqual(result["account_id"], "acc-777")

    def test_resolve_target_returns_multiple_error_for_ambiguous_username(self):
        with patch(
            "bot.telegram_bot.commands.roles_admin.AccountsService.resolve_user_lookup",
            return_value={
                "status": "multiple",
                "candidates": [
                    {
                        "provider": "telegram",
                        "provider_user_id": "1",
                        "username": "dup_user",
                        "display_name": "One",
                        "matched_by": "username",
                    },
                    {
                        "provider": "discord",
                        "provider_user_id": "2",
                        "username": "dup_user",
                        "display_name": "Two",
                        "matched_by": "display_name",
                    },
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
        self.assertIn("Найдено несколько кандидатов", result["message"])
        self.assertIn("telegram | @dup_user | One | id=1 | via=username", result["message"])
        self.assertIn("discord | @dup_user | Two | id=2 | via=display_name", result["message"])

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
        self.assertIn("локальном реестре", result["message"])
        self.assertIn("/register", result["message"])
        self.assertIn("reply", result["message"])

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

    def test_resolve_target_supports_telegram_dm_username_lookup(self):
        with patch(
            "bot.telegram_bot.commands.roles_admin.AccountsService.resolve_user_lookup",
            return_value={
                "status": "ok",
                "result": {
                    "account_id": "acc-2",
                    "provider": "telegram",
                    "provider_user_id": "999",
                    "username": "dm_target",
                    "display_name": "DM Target",
                    "matched_by": "username",
                },
                "candidates": [],
            },
        ):
            result = _resolve_telegram_target(
                actor_id=100,
                raw_target="@dm_target",
                reply_user=None,
                operation="user_roles",
                source="fallback_text_command",
            )

        self.assertEqual(result["provider"], "telegram")
        self.assertEqual(result["provider_user_id"], "999")
        self.assertEqual(result["label"], "@dm_target")
        self.assertEqual(result["matched_by"], "username")

    def test_role_delete_picker_hides_external_roles_but_move_keeps_them(self):
        grouped = [
            {
                "category": "General",
                "roles": [
                    {"name": "External", "is_discord_managed": True, "discord_role_id": "1"},
                    {"name": "Custom", "is_discord_managed": False, "discord_role_id": None},
                ],
            }
        ]

        delete_keyboard = _build_pick_role_keyboard(grouped, actor_id=10, operation="role_delete", page=0)
        move_keyboard = _build_pick_role_keyboard(grouped, actor_id=10, operation="role_move", page=0)
        delete_texts = [button.text for row in delete_keyboard.inline_keyboard for button in row]
        move_texts = [button.text for row in move_keyboard.inline_keyboard for button in row]

        self.assertFalse(any("External" in text for text in delete_texts))
        self.assertTrue(any("Custom" in text for text in delete_texts))
        self.assertTrue(any("External" in text for text in move_texts))


if __name__ == "__main__":
    unittest.main()
