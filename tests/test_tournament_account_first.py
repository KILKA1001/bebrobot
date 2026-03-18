import unittest
from unittest.mock import patch

from bot.data.db import db as shared_db

shared_db.supabase = object()

from bot.data import tournament_db
from bot.systems import tournament_rewards_logic


class TournamentAccountFirstTests(unittest.TestCase):
    @patch("bot.systems.tournament_rewards_logic.TicketsService.give_ticket_by_account")
    @patch("bot.systems.tournament_rewards_logic.PointsService.add_points_by_account")
    @patch("bot.systems.tournament_rewards_logic.db._get_account_id_for_discord_user")
    @patch("bot.systems.tournament_rewards_logic.get_tournament_info")
    def test_distribute_rewards_uses_account_first_services(
        self,
        mock_info,
        mock_resolve_account,
        mock_add_points,
        mock_give_ticket,
    ):
        mock_info.return_value = {"name": "Cup"}
        mock_resolve_account.side_effect = ["acc-author", "acc-1", "acc-2"]

        tournament_rewards_logic.distribute_rewards(
            tournament_id=77,
            bank_total=100.0,
            first_team_ids=[11],
            second_team_ids=[22],
            author_id=999,
        )

        mock_add_points.assert_any_call("acc-1", 50.0, "🏆 1 место в турнире Cup (#77)", "acc-author")
        mock_add_points.assert_any_call("acc-2", 25.0, "🥈 2 место в турнире Cup (#77)", "acc-author")
        mock_give_ticket.assert_any_call(
            "acc-1",
            "gold",
            1,
            "🥇 Золотой билет за 1 место (турнир Cup (#77))",
            "acc-author",
        )
        mock_give_ticket.assert_any_call(
            "acc-2",
            "normal",
            1,
            "🎟 Обычный билет за 2 место (турнир Cup (#77))",
            "acc-author",
        )

    @patch("bot.systems.tournament_rewards_logic.db._get_account_id_for_discord_user")
    @patch("bot.systems.tournament_rewards_logic.get_tournament_info")
    def test_distribute_rewards_logs_unresolved_participant(self, mock_info, mock_resolve_account):
        mock_info.return_value = {}
        mock_resolve_account.side_effect = ["acc-author", None]

        with self.assertLogs("bot.systems.tournament_rewards_logic", level="ERROR") as captured:
            tournament_rewards_logic.distribute_rewards(
                tournament_id=5,
                bank_total=10.0,
                first_team_ids=[111],
                second_team_ids=[],
                author_id=999,
            )

        combined = "\n".join(captured.output)
        self.assertIn("identity resolve error", combined)
        self.assertIn("field=discord_user_id", combined)
        self.assertIn("participant_id=111", combined)
        self.assertIn("action=replace_with_account_id", combined)

    @patch("bot.data.tournament_db._get_discord_user_for_account", return_value=321)
    def test_normalize_bet_row_logs_schema_fallback(self, _mock_resolve_discord):
        with self.assertLogs("bot.data.tournament_db", level="WARNING") as captured:
            row = tournament_db._normalize_bet_row({"account_id": "acc-1", "amount": 5})

        self.assertEqual(row["user_id"], 321)
        combined = "\n".join(captured.output)
        self.assertIn("legacy schema fallback", combined)
        self.assertIn("table=tournament_bets", combined)
        self.assertIn("field=user_id", combined)
        self.assertIn("replace_with_account_id_column", combined)


if __name__ == "__main__":
    unittest.main()
