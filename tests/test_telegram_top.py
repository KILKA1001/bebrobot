import unittest
from unittest.mock import patch

from bot.telegram_bot.commands.top import _TopMessageSessionState, _resolve_display_name


class TelegramTopNameResolutionTests(unittest.TestCase):
    @patch("bot.telegram_bot.commands.top.AccountsService.get_best_public_name")
    @patch("bot.telegram_bot.commands.top.AccountsService.resolve_account_id")
    def test_resolve_display_name_uses_message_lifetime_cache(self, mock_resolve_account_id, mock_get_best_public_name):
        state = _TopMessageSessionState()
        mock_resolve_account_id.return_value = "acc-1"
        mock_get_best_public_name.return_value = "Player One"

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

    @patch("bot.telegram_bot.commands.top.AccountsService.get_best_public_name")
    @patch("bot.telegram_bot.commands.top.AccountsService.resolve_account_id")
    def test_resolve_display_name_keeps_previous_name_when_identity_regresses(self, mock_resolve_account_id, mock_get_best_public_name):
        state = _TopMessageSessionState()
        state.seen_non_id_names[42] = "Старое имя"
        mock_resolve_account_id.return_value = None
        mock_get_best_public_name.return_value = None

        with self.assertLogs("bot.telegram_bot.commands.top", level="WARNING") as captured:
            resolved = _resolve_display_name(
                42,
                period="week",
                page=2,
                session_state=state,
            )

        self.assertEqual(resolved, "Старое имя")
        self.assertTrue(any("top_name_regressed_to_id" in line for line in captured.output))
