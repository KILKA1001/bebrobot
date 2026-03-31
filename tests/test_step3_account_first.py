"""
Назначение: модуль "test step3 account first" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import asyncio
import importlib
import sys
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.services.fines_service import FinesService
from bot.systems import bets_logic, fines_logic
from bot.services.accounts_service import AccountsService


class Step3AccountFirstTests(unittest.TestCase):
    @patch("bot.services.fines_service.db")
    @patch("bot.services.fines_service.AccountsService.resolve_account_id")
    def test_create_fine_by_identity_uses_account_first_and_logs_legacy_path(self, mock_resolve, mock_db):
        mock_resolve.side_effect = ["acc-target", "acc-author"]
        mock_db.add_fine.return_value = {"id": 7}

        with self.assertLogs("bot.services.fines_service", level="WARNING") as captured:
            fine = FinesService.create_fine(
                discord_user_id=111,
                author_id=222,
                amount=5.0,
                fine_type=1,
                reason="late",
                due_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )

        self.assertEqual(fine, {"id": 7})
        mock_db.add_fine.assert_called_once_with(
            "acc-target",
            "acc-author",
            5.0,
            1,
            "late",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        combined = "\n".join(captured.output)
        self.assertIn("legacy identity path detected", combined)
        self.assertIn("field=discord_user_id", combined)

    @patch("bot.services.fines_service.db")
    @patch("bot.services.fines_service.AccountsService.resolve_account_id")
    def test_get_user_fines_by_identity_logs_resolve_error(self, mock_resolve, mock_db):
        mock_resolve.return_value = None

        with self.assertLogs("bot.services.fines_service", level="WARNING") as captured:
            fines = FinesService.get_user_fines(111)

        self.assertEqual(fines, [])
        combined = "\n".join(captured.output)
        self.assertIn("legacy identity path detected", combined)
        self.assertIn("identity resolve error", combined)
        self.assertIn("action=resolve_account_id", combined)

    @patch("bot.systems.bets_logic.db")
    @patch("bot.systems.bets_logic.tournament_db")
    @patch("bot.systems.bets_logic.AccountsService.resolve_account_id")
    def test_place_bet_uses_account_balance_and_account_score_updates(self, mock_resolve, mock_tournament_db, mock_db):
        mock_resolve.return_value = "acc-1"
        mock_db.supabase.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = [
            {"points": 20}
        ]
        mock_tournament_db.get_tournament_info.return_value = {"bank_type": 1}
        mock_tournament_db.create_bet_by_account.return_value = 99

        ok, message = bets_logic.place_bet(
            tournament_id=10,
            round_no=1,
            pair_index=1,
            user_id=111,
            bet_on=555,
            amount=5,
            total_rounds=4,
        )

        self.assertTrue(ok)
        self.assertIn("99", message)
        mock_tournament_db.create_bet_by_account.assert_called_once_with(10, 1, 1, "acc-1", 555, 5, discord_user_id=111)
        mock_db.update_scores_by_account.assert_called_once_with("acc-1", -5, user_id=111)
        mock_db.update_scores.assert_not_called()

    @patch("bot.systems.bets_logic.db")
    @patch("bot.systems.bets_logic.tournament_db")
    @patch("bot.systems.bets_logic.AccountsService.resolve_account_id")
    def test_cancel_bet_legacy_row_logs_schema_fallback_and_refunds_by_account(self, mock_resolve, mock_tournament_db, mock_db):
        mock_resolve.return_value = "acc-legacy"
        mock_tournament_db.get_tournament_info.return_value = {"bank_type": 1}
        mock_tournament_db.get_bet.return_value = {
            "id": 5,
            "tournament_id": 7,
            "amount": 3,
            "user_id": 111,
            "won": None,
        }
        mock_tournament_db.delete_bet.return_value = True

        with self.assertLogs("bot.systems.bets_logic", level="WARNING") as captured:
            ok, message = bets_logic.cancel_bet(5)

        self.assertTrue(ok)
        self.assertIn("баллы возвращены", message)
        mock_db.update_scores_by_account.assert_called_once_with("acc-legacy", 3.0, user_id=111)
        mock_db.update_scores.assert_not_called()
        combined = "\n".join(captured.output)
        self.assertIn("legacy schema fallback", combined)
        self.assertIn("table=tournament_bets", combined)
        self.assertIn("field=user_id", combined)

    @patch("bot.systems.fines_logic.safe_followup_send", new_callable=unittest.mock.AsyncMock)
    @patch("bot.systems.fines_logic.db")
    @patch("bot.systems.fines_logic.AccountsService.resolve_account_id")
    def test_process_payment_uses_account_first_balance(self, mock_resolve, mock_db, mock_followup):
        mock_resolve.return_value = "acc-pay"
        mock_db.supabase.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = [
            {"points": 20}
        ]
        mock_db.record_payment_by_account.return_value = True
        interaction = SimpleNamespace(user=SimpleNamespace(id=111))
        fine = {"id": 10, "amount": 8.0, "paid_amount": 0}

        asyncio.run(fines_logic.process_payment(interaction, fine, 0.5))

        mock_db.record_payment_by_account.assert_called_once_with(
            account_id="acc-pay",
            fine_id=10,
            amount=4.0,
            author_account_id="acc-pay",
        )
        mock_followup.assert_awaited()


class AccountsProfileFallbackTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = SimpleNamespace(
            supabase=_FakeSupabase(),
            _inc_metric=lambda *_args, **_kwargs: None,
        )
        self.db_patcher = patch("bot.services.accounts_service.db", self.fake_db)
        self.roles_patcher = patch(
            "bot.services.accounts_service.RoleResolver.resolve_for_account",
            return_value=SimpleNamespace(roles=[], permissions={"allow": [], "deny": []}),
        )
        self.sync_patcher = patch(
            "bot.services.external_roles_sync_service.ExternalRolesSyncService.get_last_sync_at",
            return_value=None,
        )
        self.db_patcher.start()
        self.roles_patcher.start()
        self.sync_patcher.start()

    def tearDown(self):
        self.sync_patcher.stop()
        self.roles_patcher.stop()
        self.db_patcher.stop()

    def test_get_profile_by_account_logs_legacy_scores_user_id_fallback(self):
        with self.assertLogs("bot.services.accounts_service", level="WARNING") as captured:
            profile = AccountsService.get_profile_by_account("acc-1", display_name="Tester")

        self.assertIsNotNone(profile)
        self.assertEqual(profile["points"], "15")
        combined = "\n".join(captured.output)
        self.assertIn("legacy schema fallback", combined)
        self.assertIn("table=scores", combined)
        self.assertIn("field=user_id", combined)
        self.assertIn("developer_hint=temporary compatibility path; migrate scores rows to scores.account_id", combined)


class _FakeResponse:
    def __init__(self, data):
        self.data = data


class _FakeTable:
    def __init__(self, name):
        self.name = name
        self.filters = []
        self.limit_value = None

    def select(self, _fields):
        return self

    def eq(self, key, value):
        self.filters.append((key, value))
        return self

    def limit(self, value):
        self.limit_value = value
        return self

    def execute(self):
        rows = []
        if self.name == "account_identities":
            rows = [{"provider": "discord", "provider_user_id": "111"}]
        elif self.name == "accounts":
            rows = [{"custom_nick": "", "description": "", "nulls_brawl_id": "", "profile_visible_roles": []}]
        elif self.name == "scores":
            filters = dict(self.filters)
            if filters.get("account_id") == "acc-1":
                rows = []
            elif filters.get("user_id") == "111":
                rows = [{"points": 15}]
        elif self.name == "actions":
            rows = []
        return _FakeResponse(rows[: self.limit_value] if self.limit_value is not None else rows)


class _FakeSupabase:
    def table(self, name):
        return _FakeTable(name)


class TournamentDbSafeImportTests(unittest.TestCase):
    def test_import_is_safe_without_supabase_and_logs_on_real_call(self):
        from bot.data.db import db as shared_db

        original_supabase = shared_db.supabase
        original_module = sys.modules.get("bot.data.tournament_db")
        try:
            shared_db.supabase = None
            sys.modules.pop("bot.data.tournament_db", None)

            tournament_db = importlib.import_module("bot.data.tournament_db")

            with self.assertLogs("bot.data.tournament_db", level="ERROR") as captured:
                info = tournament_db.get_tournament_info(10)

            self.assertIsNone(info)
            combined = "\n".join(captured.output)
            self.assertIn("runtime dependency missing", combined)
            self.assertIn("field=db.supabase", combined)
            self.assertIn("action=initialize_supabase_client_before_tournament_db_call", combined)
        finally:
            shared_db.supabase = original_supabase
            if original_module is not None:
                sys.modules["bot.data.tournament_db"] = original_module
            else:
                sys.modules.pop("bot.data.tournament_db", None)


class UndoAccountFirstTests(unittest.TestCase):
    def test_undo_reads_history_via_account_first_helper(self):
        from bot.data.db import db as shared_db

        original_supabase = shared_db.supabase
        shared_db.supabase = object()
        try:
            base_commands = importlib.import_module("bot.commands.base")

            ctx = SimpleNamespace(author=SimpleNamespace(id=999))
            member = SimpleNamespace(id=111, display_name="Tester")

            fake_db = SimpleNamespace(
                history={},
                add_action=unittest.mock.Mock(),
            )

            history_rows = [{"points": 5, "reason": "bonus"}]

            with patch.object(base_commands, "db", fake_db):
                with patch.object(base_commands, "_check_command_authority", AsyncMock(return_value=True)):
                    with patch.object(base_commands, "_resolve_account_id_from_discord", return_value="acc-1"):
                        with patch.object(base_commands, "_get_action_rows_for_account", return_value=list(history_rows)) as mock_history:
                            with patch.object(base_commands, "send_temp", AsyncMock()) as mock_send:
                                with patch.object(base_commands, "update_roles", AsyncMock()) as mock_update_roles:
                                    with patch.object(base_commands, "log_action_cancellation", AsyncMock()) as mock_cancel_log:
                                        asyncio.run(base_commands.undo.callback(ctx, member, 1))

            mock_history.assert_called_once_with("acc-1", discord_user_id=111, handler="undo")
            fake_db.add_action.assert_called_once_with(
                user_id=111,
                points=-5,
                reason="Отмена действия: bonus",
                author_id=999,
                is_undo=True,
            )
            mock_send.assert_awaited()
            mock_update_roles.assert_awaited_once_with(member)
            mock_cancel_log.assert_awaited_once()
        finally:
            shared_db.supabase = original_supabase

    def test_undo_logs_legacy_fallback_when_account_cannot_be_resolved(self):
        from bot.data.db import db as shared_db

        original_supabase = shared_db.supabase
        shared_db.supabase = object()
        try:
            base_commands = importlib.import_module("bot.commands.base")

            ctx = SimpleNamespace(author=SimpleNamespace(id=999))
            member = SimpleNamespace(id=111, display_name="Tester")
            fake_db = SimpleNamespace(
                history={111: [{"points": 7, "reason": "legacy"}]},
                add_action=unittest.mock.Mock(),
            )

            with patch.object(base_commands, "db", fake_db):
                with patch.object(base_commands, "_check_command_authority", AsyncMock(return_value=True)):
                    with patch.object(base_commands, "_resolve_account_id_from_discord", return_value=None):
                        with patch.object(base_commands, "send_temp", AsyncMock()):
                            with patch.object(base_commands, "update_roles", AsyncMock()):
                                with patch.object(base_commands, "log_action_cancellation", AsyncMock()):
                                    with self.assertLogs("bot.commands.base", level="WARNING") as captured:
                                        asyncio.run(base_commands.undo.callback(ctx, member, 1))

            combined = "\n".join(captured.output)
            self.assertIn("legacy identity fallback used", combined)
            self.assertIn("handler=undo", combined)
            self.assertIn("action=fallback_to_legacy_history_cache", combined)
        finally:
            shared_db.supabase = original_supabase


class TournamentLogicAccountFirstTests(unittest.TestCase):
    def test_end_tournament_reads_balance_via_account_first_snapshot(self):
        from bot.data.db import db as shared_db

        original_supabase = shared_db.supabase
        shared_db.supabase = object()
        from bot.systems import tournament_logic
        try:
            ctx = SimpleNamespace(author=SimpleNamespace(id=111))

            with patch.object(tournament_logic, "get_tournament_info", return_value={"bank_type": 1, "manual_amount": 20.0}):
                with patch.object(tournament_logic, "_get_user_balance_for_tournament", return_value=42.0) as mock_balance:
                    with patch.object(tournament_logic.rewards, "calculate_bank", side_effect=ValueError("stop"), create=True) as mock_calc:
                        with patch.object(tournament_logic, "send_temp", AsyncMock()):
                            asyncio.run(tournament_logic.end_tournament(ctx, tournament_id=5, first=1, second=2))

            mock_balance.assert_called_once_with(111, handler="end_tournament")
            self.assertEqual(mock_calc.call_args.args[1], 42.0)
        finally:
            shared_db.supabase = original_supabase

    def test_balance_helper_logs_legacy_scores_fallback_when_identity_missing(self):
        from bot.data.db import db as shared_db

        original_supabase = shared_db.supabase
        shared_db.supabase = object()
        from bot.systems import tournament_logic
        try:
            fake_db = SimpleNamespace(scores={555: 9.5})

            with patch.object(tournament_logic, "db", fake_db):
                with patch.object(tournament_logic, "_resolve_account_id_from_discord", return_value=None):
                    with self.assertLogs("bot.systems.tournament_logic", level="WARNING") as captured:
                        balance = tournament_logic._get_user_balance_for_tournament(555, handler="finalize_tournament_logic")

            self.assertEqual(balance, 9.5)
            combined = "\n".join(captured.output)
            self.assertIn("legacy identity fallback used", combined)
            self.assertIn("action=fallback_to_legacy_scores_cache", combined)
            self.assertIn("discord_user_id=555", combined)
        finally:
            shared_db.supabase = original_supabase


if __name__ == "__main__":
    unittest.main()
