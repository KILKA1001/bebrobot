import unittest
from unittest.mock import patch

from bot.services.moderation_service import ModerationService


class _Resp:
    def __init__(self, data):
        self.data = data


class _TableOp:
    def __init__(self, fake_db, table_name):
        self.fake_db = fake_db
        self.table_name = table_name
        self._filters = []
        self._limit = None
        self._payload = None
        self._action = "select"

    def select(self, _fields):
        self._action = "select"
        return self

    def eq(self, key, value):
        self._filters.append((key, value))
        return self

    def limit(self, n):
        self._limit = n
        return self

    def insert(self, payload, **_kwargs):
        self._payload = payload
        self._action = "insert"
        return self

    def update(self, payload):
        self._payload = payload
        self._action = "update"
        return self

    def execute(self):
        rows = self.fake_db.tables[self.table_name]
        if self._action == "insert":
            if self.fake_db.fail_insert_for == self.table_name:
                raise RuntimeError(f"forced insert failure for {self.table_name}")
            payload = dict(self._payload)
            if "id" not in payload:
                self.fake_db.sequences[self.table_name] += 1
                payload["id"] = self.fake_db.sequences[self.table_name]
            rows.append(payload)
            self.fake_db.operations.append({"table": self.table_name, "action": "insert", "payload": dict(payload)})
            return _Resp([dict(payload)])

        if self._action == "update":
            if self.fake_db.fail_update_for == self.table_name:
                raise RuntimeError(f"forced update failure for {self.table_name}")
            matched = []
            for row in rows:
                if all(str(row.get(k)) == str(v) for k, v in self._filters):
                    row.update(self._payload)
                    matched.append(dict(row))
            self.fake_db.operations.append({
                "table": self.table_name,
                "action": "update",
                "filters": list(self._filters),
                "payload": dict(self._payload),
            })
            return _Resp(matched)

        selected = []
        for row in rows:
            if all(str(row.get(k)) == str(v) for k, v in self._filters):
                selected.append(dict(row))
        if self._limit is not None:
            selected = selected[: self._limit]
        self.fake_db.operations.append({
            "table": self.table_name,
            "action": "select",
            "filters": list(self._filters),
        })
        return _Resp(selected)


class _FakeSupabase:
    def __init__(self, fake_db):
        self.fake_db = fake_db

    def table(self, name):
        return _TableOp(self.fake_db, name)


class _FakeDb:
    def __init__(self):
        self.tables = {
            "moderation_violation_types": [
                {"id": 1, "code": "spam", "title": "Spam", "is_active": True},
            ],
            "moderation_penalty_rules": [
                {
                    "id": 10,
                    "violation_type_id": 1,
                    "escalation_step": 1,
                    "warn_count_before": 0,
                    "apply_warn": True,
                    "mute_minutes": 15,
                    "fine_points": 3,
                    "apply_ban": False,
                    "is_active": True,
                    "description_for_admin": "Первый спам",
                    "description_for_user": "Не спамьте",
                },
                {
                    "id": 11,
                    "violation_type_id": 1,
                    "escalation_step": 5,
                    "warn_count_before": 4,
                    "apply_warn": True,
                    "mute_minutes": 0,
                    "fine_points": 0,
                    "apply_ban": True,
                    "is_active": True,
                    "description_for_admin": "Пятый пред",
                    "description_for_user": "Достигнут лимит предупреждений",
                },
            ],
            "moderation_warn_state": [],
            "moderation_cases": [],
            "moderation_actions": [],
            "moderation_mutes": [],
            "moderation_bans": [],
        }
        self.sequences = {
            "moderation_cases": 100,
            "moderation_actions": 1000,
            "moderation_warn_state": 0,
            "moderation_mutes": 200,
            "moderation_bans": 300,
        }
        self.supabase = _FakeSupabase(self)
        self.operations = []
        self.metrics = []
        self.point_actions = []
        self.fail_insert_for = None
        self.fail_update_for = None
        self.fail_point_action = False

    def _inc_metric(self, name):
        self.metrics.append(name)

    def add_action_by_account(self, account_id, points, reason, author_account_id, is_undo=False, op_key=None):
        if self.fail_point_action:
            return False
        self.point_actions.append(
            {
                "account_id": account_id,
                "points": points,
                "reason": reason,
                "author_account_id": author_account_id,
                "is_undo": is_undo,
                "op_key": op_key,
            }
        )
        return True


class ModerationServiceTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = _FakeDb()
        self.db_patcher = patch("bot.services.moderation_service.db", self.fake_db)
        self.resolve_patcher = patch("bot.services.moderation_service.AccountsService.resolve_account_id")
        self.mock_resolve = self.resolve_patcher.start()
        self.db_patcher.start()

    def tearDown(self):
        self.resolve_patcher.stop()
        self.db_patcher.stop()

    def test_apply_violation_creates_case_warn_mute_and_fine_point_action(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]

        result = ModerationService.apply_violation(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            reason_text="Flood links",
            source_platform="discord",
            source_chat_id="987",
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["warn_count_before"], 0)
        self.assertEqual(result["warn_count_after"], 1)
        self.assertEqual(result["status"], ModerationService.STATUS_APPLIED)
        self.assertFalse(result["ban_applied"])
        self.assertIsNotNone(result["op_key"])
        self.assertEqual(self.fake_db.tables["moderation_cases"][0]["penalty_rule_id"], 10)
        self.assertEqual(self.fake_db.tables["moderation_cases"][0]["status"], ModerationService.STATUS_APPLIED)
        self.assertEqual(self.fake_db.tables["moderation_warn_state"][0]["active_warn_count"], 1)
        self.assertEqual(len(self.fake_db.tables["moderation_mutes"]), 1)
        self.assertEqual(
            [row["action_type"] for row in self.fake_db.tables["moderation_actions"]],
            ["warn", "mute", "fine_points"],
        )
        self.assertEqual(self.fake_db.point_actions[0]["points"], -3.0)
        self.assertTrue(self.fake_db.point_actions[0]["op_key"].endswith(":fine_points"))
        self.assertIn("Flood links", self.fake_db.point_actions[0]["reason"])

    def test_apply_violation_bans_when_warn_threshold_reached(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]
        self.fake_db.tables["moderation_warn_state"] = [
            {"id": 1, "account_id": "acc-target", "active_warn_count": 4}
        ]

        result = ModerationService.apply_violation(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            reason_text="again",
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["warn_count_before"], 4)
        self.assertEqual(result["warn_count_after"], 5)
        self.assertTrue(result["ban_applied"])
        self.assertEqual(result["status"], ModerationService.STATUS_APPLIED)
        self.assertEqual(self.fake_db.tables["moderation_warn_state"][0]["active_warn_count"], 5)
        self.assertEqual(len(self.fake_db.tables["moderation_bans"]), 1)
        self.assertEqual(
            [row["action_type"] for row in self.fake_db.tables["moderation_actions"]],
            ["warn", "ban"],
        )

    def test_apply_violation_does_not_auto_ban_without_active_ban_rule(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]
        self.fake_db.tables["moderation_penalty_rules"] = [
            {
                "id": 20,
                "violation_type_id": 1,
                "escalation_step": 1,
                "warn_count_before": 4,
                "apply_warn": True,
                "mute_minutes": 0,
                "fine_points": 0,
                "apply_ban": False,
                "is_active": True,
                "description_for_admin": "Пятый пред без бана",
                "description_for_user": "Автобан отключён",
            },
        ]
        self.fake_db.tables["moderation_warn_state"] = [
            {"id": 1, "account_id": "acc-target", "active_warn_count": 4}
        ]

        result = ModerationService.apply_violation(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            reason_text="again",
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["warn_count_after"], 5)
        self.assertFalse(result["ban_applied"])
        self.assertEqual(
            [row["action_type"] for row in self.fake_db.tables["moderation_actions"]],
            ["warn"],
        )
        self.assertEqual(len(self.fake_db.tables["moderation_bans"]), 0)

    def test_apply_violation_returns_none_when_identity_cannot_be_resolved(self):
        self.mock_resolve.return_value = None

        with self.assertLogs("bot.services.moderation_service", level="ERROR") as captured:
            result = ModerationService.apply_violation(
                provider="discord",
                actor="111",
                target="222",
                violation_code="spam",
            )

        self.assertIsNone(result)
        self.assertIn("moderation resolve account failed", "\n".join(captured.output))
        self.assertIn("identity_resolve_errors", self.fake_db.metrics)

    def test_commit_case_is_idempotent_for_same_op_key(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target", "acc-actor", "acc-target"]

        first = ModerationService.commit_case(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            context={"moderation_op_key": "rep:fixed-op-key", "skip_authority": True},
        )
        second = ModerationService.commit_case(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            context={"moderation_op_key": "rep:fixed-op-key", "skip_authority": True},
        )

        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(first["case_id"], second["case_id"])
        self.assertEqual(second["status"], ModerationService.STATUS_DUPLICATE)
        self.assertEqual(len(self.fake_db.tables["moderation_cases"]), 1)

    def test_commit_case_rolls_back_when_mute_apply_fails_after_warn_increment(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]
        self.fake_db.fail_insert_for = "moderation_mutes"

        result = ModerationService.commit_case(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            context={"moderation_op_key": "rep:rollback-op", "skip_authority": True},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error_code"], "mute_apply_failed")
        self.assertIn(result["rollback_status"], {"rolled_back", "manual_review_required"})
        self.assertEqual(self.fake_db.tables["moderation_warn_state"][0]["active_warn_count"], 0)
        self.assertEqual(self.fake_db.tables["moderation_cases"][0]["status"], ModerationService.STATUS_ROLLED_BACK)
        self.assertEqual(len(self.fake_db.point_actions), 0)

    def test_commit_case_returns_error_if_fine_applied_but_case_finalize_fails(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]
        self.fake_db.fail_update_for = "moderation_cases"

        result = ModerationService.commit_case(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            context={"moderation_op_key": "rep:finalize-fail", "skip_authority": True},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error_code"], "finalize_case_failed")
        self.assertEqual(self.fake_db.point_actions[0]["points"], -3.0)
        self.assertEqual(self.fake_db.point_actions[1]["points"], 3.0)
        self.assertTrue(self.fake_db.point_actions[1]["is_undo"])
        self.assertEqual(result["rollback_status"], "manual_review_required")


if __name__ == "__main__":
    unittest.main()
