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
    def rpc(self, _name, _payload):
        raise RuntimeError("rpc unavailable")

    def table(self, name):
        if name == "actions":
            return _ActionsInsertOp()
        raise AssertionError(f"Unexpected table call: {name}")


class _FakeDbForAddAction:
    def __init__(self):
        self.supabase = _FakeSupabase()
        self.scores = {}
        self.actions = []
        self.history = {}
        self._core_data_loaded = True
        self._core_data_loading = False
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


if __name__ == "__main__":
    unittest.main()
