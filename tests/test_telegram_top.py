import unittest
from unittest.mock import patch

from bot.telegram_bot.commands.top import _TopMessageSessionState, _resolve_display_name


class TelegramTopNameResolutionTests(unittest.TestCase):
    @patch("bot.telegram_bot.commands.top.AccountsService.get_public_identity_context")
    @patch("bot.telegram_bot.commands.top.AccountsService.get_best_public_name")
    @patch("bot.telegram_bot.commands.top.AccountsService.resolve_account_id")
    def test_resolve_display_name_uses_message_lifetime_cache(
        self,
        mock_resolve_account_id,
        mock_get_best_public_name,
        mock_get_public_identity_context,
    ):
        state = _TopMessageSessionState()
        mock_resolve_account_id.return_value = "acc-1"
        mock_get_best_public_name.return_value = "Player One"
        mock_get_public_identity_context.return_value = {}

        first = _resolve_display_name(
            1,
            period="all",
            page=0,
            session_state=state,
        )
        second = _resolve_display_name(
            1,
            period="all",
            page=0,
            session_state=state,
        )

        self.assertEqual(first, "Player One")
        self.assertEqual(second, "Player One")
        self.assertEqual(mock_resolve_account_id.call_count, 1)
        self.assertEqual(mock_get_best_public_name.call_count, 1)
        self.assertEqual(mock_get_public_identity_context.call_count, 0)

    @patch("bot.telegram_bot.commands.top.AccountsService.get_public_identity_context")
    @patch("bot.telegram_bot.commands.top.AccountsService.get_best_public_name")
    @patch("bot.telegram_bot.commands.top.AccountsService.resolve_account_id")
    def test_resolve_display_name_uses_discord_identity_when_preferred_name_empty(
        self,
        mock_resolve_account_id,
        mock_get_best_public_name,
        mock_get_public_identity_context,
    ):
        mock_resolve_account_id.return_value = "acc-1"
        mock_get_best_public_name.return_value = None
        mock_get_public_identity_context.return_value = {
            "display_name": "Discord Display",
            "username": "discord_user",
            "global_username": "global_user",
        }

        resolved = _resolve_display_name(
            123,
            period="all",
            page=0,
        )

        self.assertEqual(resolved, "Discord Display")
        mock_get_public_identity_context.assert_called_once_with("discord", None, account_id="acc-1")

    @patch("bot.telegram_bot.commands.top.AccountsService.get_public_identity_context")
    @patch("bot.telegram_bot.commands.top.AccountsService.get_best_public_name")
    @patch("bot.telegram_bot.commands.top.AccountsService.resolve_account_id")
    def test_resolve_display_name_keeps_previous_name_when_identity_regresses(
        self,
        mock_resolve_account_id,
        mock_get_best_public_name,
        mock_get_public_identity_context,
    ):
        state = _TopMessageSessionState()
        state.seen_non_id_names[42] = "Старое имя"
        mock_resolve_account_id.return_value = None
        mock_get_best_public_name.return_value = None
        mock_get_public_identity_context.return_value = {}

        with self.assertLogs("bot.telegram_bot.commands.top", level="WARNING") as captured:
            resolved = _resolve_display_name(
                42,
                period="week",
                page=2,
                session_state=state,
            )

        self.assertEqual(resolved, "Старое имя")
        self.assertTrue(any("top_name_regressed_to_id" in line for line in captured.output))

    @patch("bot.telegram_bot.commands.top.AuthorityService.is_super_admin")
    @patch("bot.telegram_bot.commands.top.AccountsService.get_public_identity_context")
    @patch("bot.telegram_bot.commands.top.AccountsService.get_best_public_name")
    @patch("bot.telegram_bot.commands.top.AccountsService.resolve_account_id")
    def test_resolve_display_name_logs_admin_hint_for_id_fallback(
        self,
        mock_resolve_account_id,
        mock_get_best_public_name,
        mock_get_public_identity_context,
        mock_is_super_admin,
    ):
        mock_resolve_account_id.return_value = None
        mock_get_best_public_name.return_value = None
        mock_get_public_identity_context.return_value = {}
        mock_is_super_admin.return_value = True

        with self.assertLogs("bot.telegram_bot.commands.top", level="INFO") as captured:
            resolved = _resolve_display_name(
                77,
                period="all",
                page=0,
                admin_actor_user_id=999,
            )

        self.assertEqual(resolved, "ID 77")
        self.assertTrue(any("top id fallback admin hint" in line for line in captured.output))
