import unittest
from unittest.mock import patch

from bot.data.db import Database


class _Resp:
    def __init__(self, data=None):
        self.data = data


class _ActionsInsertOp:
    def insert(self, _payload):
        return self

    def execute(self):
        raise RuntimeError("insert failed")


class _FakeSupabase:
    def __init__(self):
        self.score_upserts = []
        self.score_table_calls = 0

    def rpc(self, _name, _payload):
        raise RuntimeError("rpc unavailable")

    def table(self, name):
        if name == "actions":
            return _ActionsInsertOp()
        if name == "scores":
            self.score_table_calls += 1
            return _ScoresUpsertOp(self)
        raise AssertionError(f"Unexpected table call: {name}")


class _ScoresUpsertOp:
    def __init__(self, supabase):
        self.supabase = supabase

    def upsert(self, payload, on_conflict=None):
        self.supabase.score_upserts.append((payload, on_conflict))
        return self

    def execute(self):
        payload, _ = self.supabase.score_upserts[-1]
        return _Resp(data=payload)


class _FakeDbForAddAction:
    def __init__(self):
        self.supabase = _FakeSupabase()
        self.scores = {}
        self.actions = []
        self.history = {}
        self._core_data_loaded = True
        self._core_data_loading = False
        self._dirty_score_keys = set()
        self.score_updates = []

    def ensure_core_data_loaded(self):
        return None

    def _get_account_id_for_discord_user(self, _user_id):
        return "acc-1"

    def _get_discord_user_for_account_id(self, _account_id):
        return 1001

    def update_scores_by_account(self, account_id, points_change, user_id=None):
        self.score_updates.append((account_id, points_change, user_id))
        return True

    def _handle_response(self, response):
        return response

    def _build_dirty_scores_payload(self):
        return Database._build_dirty_scores_payload(self)


class AddActionRollbackTests(unittest.TestCase):
    def test_fallback_insert_error_rolls_back_score_delta(self):
        fake_db = _FakeDbForAddAction()

        with patch("bot.data.db.uuid.uuid4", return_value="op-key-1"):
            result = Database.add_action(
                fake_db,
                user_id=1001,
                points=0.1,
                reason="test",
                author_id=1001,
                author_account_id="acc-author",
                account_id="acc-1",
            )

        self.assertFalse(result)
        self.assertEqual(
            fake_db.score_updates,
            [
                ("acc-1", 0.1, 1001),
                ("acc-1", -0.1, 1001),
            ],
        )

    def test_update_scores_logs_legacy_wrapper_usage(self):
        fake_db = _FakeDbForAddAction()

        with self.assertLogs("bot.data.db", level="WARNING") as captured:
            result = Database.update_scores(fake_db, 1001, 1.0)

        self.assertTrue(result)
        combined = "\n".join(captured.output)
        self.assertIn("legacy identity path detected", combined)
        self.assertIn("handler=Database.update_scores", combined)
        self.assertIn("field=user_id", combined)
        self.assertIn("action=replace_with_account_id", combined)

    def test_save_all_returns_early_without_roundtrip_when_dirty_set_empty(self):
        fake_db = _FakeDbForAddAction()
        fake_db.scores = {1001: 12.5}

        flushed = Database.save_all(fake_db)

        self.assertEqual(flushed, 0)
        self.assertEqual(fake_db.supabase.score_table_calls, 0)

    def test_save_all_flushes_only_dirty_rows_and_clears_dirty_set(self):
        fake_db = _FakeDbForAddAction()
        fake_db.scores = {1001: 12.5, 1002: 77.0}
        fake_db._dirty_score_keys = {"account:acc-1"}

        with self.assertLogs("bot.data.db", level="INFO") as captured:
            flushed = Database.save_all(fake_db)

        self.assertEqual(flushed, 1)
        self.assertEqual(
            fake_db.supabase.score_upserts,
            [([{"account_id": "acc-1", "points": 12.5}], "account_id")],
        )
        self.assertEqual(fake_db._dirty_score_keys, set())
        self.assertIn("autosave flushed 1 dirty rows", "\n".join(captured.output))


if __name__ == "__main__":
    unittest.main()
