"""
Назначение: модуль "test moderation service" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import unittest
from unittest.mock import patch

from bot.services.accounts_service import AccountsService
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
            "moderation_warn_state_by_violation": [],
            "moderation_cases": [],
            "moderation_actions": [],
            "moderation_mutes": [],
            "moderation_bans": [],
            "moderation_case_fines": [],
            "fines": [],
            "accounts": [
                {"id": "acc-actor", "titles": ["Админ"]},
                {"id": "acc-target", "titles": ["Участник клубов"]},
            ],
            "bank": [{"id": 1, "total": 0.0}],
            "bank_history": [],
        }
        self.sequences = {
            "moderation_cases": 100,
            "moderation_actions": 1000,
            "moderation_warn_state": 0,
            "moderation_warn_state_by_violation": 0,
            "moderation_mutes": 200,
            "moderation_bans": 300,
            "moderation_case_fines": 400,
            "fines": 500,
            "bank_history": 0,
        }
        self.supabase = _FakeSupabase(self)
        self.operations = []
        self.metrics = []
        self.point_actions = []
        self.fail_insert_for = None
        self.fail_update_for = None
        self.fail_point_action = False
        self.fail_add_to_bank = False
        self.fail_log_bank_income = False

    def _inc_metric(self, name):
        self.metrics.append(name)

    def add_action_by_account(self, account_id, points, reason, author_account_id, op_key=None):
        if self.fail_point_action:
            return False
        self.point_actions.append(
            {
                "account_id": account_id,
                "points": points,
                "reason": reason,
                "author_account_id": author_account_id,
                "op_key": op_key,
            }
        )
        return True

    def add_to_bank(self, amount):
        if self.fail_add_to_bank:
            return False
        self.tables["bank"][0]["total"] += amount
        self.operations.append({"table": "bank", "action": "add", "amount": amount})
        return True

    def add_fine(self, account_id, author_account_id, amount, fine_type, reason, due_date):
        self.sequences["fines"] += 1
        row = {
            "id": self.sequences["fines"],
            "account_id": account_id,
            "author_account_id": author_account_id,
            "amount": amount,
            "type": fine_type,
            "reason": reason,
            "due_date": due_date.isoformat(),
            "paid_amount": 0.0,
            "is_paid": False,
            "is_canceled": False,
            "created_at": "2026-03-01T00:00:00+00:00",
        }
        self.tables["fines"].append(row)
        return row

    def get_fine_by_id(self, fine_id):
        for row in self.tables["fines"]:
            if int(row["id"]) == int(fine_id):
                return row
        return None

    def get_user_fines_by_account(self, account_id, active_only=True):
        rows = [row for row in self.tables["fines"] if row.get("account_id") == account_id]
        if not active_only:
            return rows
        return [row for row in rows if not row.get("is_paid") and not row.get("is_canceled")]

    def log_bank_income_by_account(self, account_id, amount, reason):
        if self.fail_log_bank_income:
            return False
        self.tables["bank_history"].append({
            "account_id": account_id,
            "amount": amount,
            "reason": reason,
        })
        self.operations.append({
            "table": "bank_history",
            "action": "insert",
            "account_id": account_id,
            "amount": amount,
            "reason": reason,
        })
        return True


class ModerationServiceTests(unittest.TestCase):
    def setUp(self):
        AccountsService._account_titles_cache = {}
        self.fake_db = _FakeDb()
        self.db_patcher = patch("bot.services.moderation_service.db", self.fake_db)
        self.resolve_patcher = patch("bot.services.moderation_service.AccountsService.resolve_account_id")
        self.titles_patcher = patch("bot.services.moderation_service.AccountsService.get_account_titles")
        self.save_titles_patcher = patch("bot.services.moderation_service.AccountsService.save_account_titles")
        self.mock_resolve = self.resolve_patcher.start()
        self.mock_titles = self.titles_patcher.start()
        self.mock_save_titles = self.save_titles_patcher.start()
        self.db_patcher.start()
        self.mock_titles.side_effect = self._fake_get_account_titles
        self.mock_save_titles.side_effect = self._fake_save_account_titles

    def tearDown(self):
        self.resolve_patcher.stop()
        self.titles_patcher.stop()
        self.save_titles_patcher.stop()
        self.db_patcher.stop()

    def _fake_get_account_titles(self, account_id):
        for row in self.fake_db.tables.get("accounts", []):
            if str(row.get("id")) == str(account_id):
                return list(row.get("titles") or [])
        return []

    def _fake_save_account_titles(self, account_id, titles, source="discord"):
        for row in self.fake_db.tables.get("accounts", []):
            if str(row.get("id")) == str(account_id):
                row["titles"] = list(titles or [])
                row["titles_source"] = source
                return True
        return False

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
        self.assertEqual(self.fake_db.tables["moderation_warn_state_by_violation"][0]["active_warn_count"], 1)
        self.assertEqual(len(self.fake_db.tables["moderation_mutes"]), 1)
        self.assertEqual(
            [row["action_type"] for row in self.fake_db.tables["moderation_actions"]],
            ["warn", "mute", "fine_points", "bank_income"],
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

    def test_prepare_payload_resets_step_for_new_violation_type(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target", "acc-actor", "acc-target"]
        self.fake_db.tables["moderation_violation_types"].append({"id": 2, "code": "bot_flood", "title": "Флуд ботов", "is_active": True})
        self.fake_db.tables["moderation_penalty_rules"].append(
            {"id": 20, "violation_type_id": 2, "escalation_step": 1, "warn_count_before": 0, "apply_warn": True, "mute_minutes": 0, "fine_points": 0, "apply_ban": False, "is_active": True}
        )
        self.fake_db.tables["moderation_warn_state"] = [{"id": 1, "account_id": "acc-target", "active_warn_count": 3}]
        self.fake_db.tables["moderation_warn_state_by_violation"] = [{"id": 1, "account_id": "acc-target", "violation_type_id": 1, "active_warn_count": 1}]

        spam_preview = ModerationService.prepare_moderation_payload("discord", "111", "222", "spam", {"skip_authority": True})
        bot_flood_preview = ModerationService.prepare_moderation_payload("discord", "111", "222", "bot_flood", {"skip_authority": True})

        self.assertTrue(spam_preview["ok"])
        self.assertTrue(bot_flood_preview["ok"])
        self.assertEqual(spam_preview["warn_count_before"], 1)
        self.assertEqual(bot_flood_preview["warn_count_before"], 0)
        self.assertEqual(bot_flood_preview["global_warn_before"], 3)
        self.assertIn("Шаг по текущему нарушению", bot_flood_preview["ui_payload"]["preview_text"])
        self.assertIn("Общие предупреждения", bot_flood_preview["ui_payload"]["preview_text"])

    def test_apply_violation_creates_manual_case_fine_when_rule_requires_payment(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]
        self.fake_db.tables["moderation_penalty_rules"][0]["fine_payment_mode"] = "manual"

        result = ModerationService.apply_violation(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            reason_text="Manual payment path",
            source_platform="discord",
            source_chat_id="987",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(self.fake_db.tables["bank"][0]["total"], 0.0)
        self.assertEqual(len(self.fake_db.tables["moderation_case_fines"]), 1)
        self.assertEqual(self.fake_db.tables["moderation_case_fines"][0]["status"], "pending")
        self.assertEqual(self.fake_db.tables["moderation_case_fines"][0]["payment_mode"], "manual")
        self.assertEqual(self.fake_db.tables["moderation_actions"][-1]["action_type"], "fine_points")
        self.assertIn("payment_mode=manual", self.fake_db.tables["moderation_actions"][-1]["value_text"])
        self.assertIn("ждёт оплаты", result["ui_payload"]["moderator_result_text"])

    def test_apply_violation_manual_fine_falls_back_to_legacy_fine_when_case_fine_table_missing(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]
        self.fake_db.tables["moderation_penalty_rules"][0]["fine_payment_mode"] = "manual"
        self.fake_db.tables.pop("moderation_case_fines")

        result = ModerationService.apply_violation(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            reason_text="Manual payment without case_fines table",
            source_platform="discord",
            source_chat_id="987",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(self.fake_db.tables["bank"][0]["total"], 0.0)
        self.assertEqual(len(self.fake_db.tables["fines"]), 1)
        self.assertEqual(self.fake_db.tables["moderation_actions"][-1]["action_type"], "fine_points")
        self.assertIn("payment_mode=manual", self.fake_db.tables["moderation_actions"][-1]["value_text"])
        self.assertIn("ждёт оплаты", result["ui_payload"]["moderator_result_text"])

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

    def test_apply_violation_uses_clean_record_soft_warn_rule_when_configured(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]
        self.fake_db.tables["moderation_penalty_rules"] = [
            {
                "id": 30,
                "violation_type_id": 1,
                "escalation_step": 0,
                "warn_count_before": 0,
                "warn_increment": 1,
                "warn_ttl_minutes": 14400,
                "mute_minutes": 0,
                "fine_points": 0,
                "ban_minutes": 0,
                "apply_ban": False,
                "only_if_clean_record": True,
                "is_active": True,
                "description_for_admin": "Первый чистый проступок",
                "description_for_user": "Только предупреждение",
            },
            {
                "id": 31,
                "violation_type_id": 1,
                "escalation_step": 1,
                "warn_count_before": 1,
                "warn_increment": 1,
                "warn_ttl_minutes": 14400,
                "mute_minutes": 15,
                "fine_points": 1,
                "ban_minutes": 0,
                "apply_ban": False,
                "is_active": True,
                "description_for_admin": "Повтор после софт-вара",
                "description_for_user": "Дальше будет мут",
            },
        ]

        result = ModerationService.apply_violation(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            reason_text="soft warn",
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["warn_count_before"], 0)
        self.assertEqual(result["warn_count_after"], 1)
        self.assertEqual(
            [row["action_type"] for row in self.fake_db.tables["moderation_actions"]],
            ["warn"],
        )
        self.assertIn("мягкое правило первого чистого проступка", result["ui_payload"]["how_it_works_text"])
        self.assertIsNotNone(self.fake_db.tables["moderation_actions"][0]["ends_at"])

    def test_apply_violation_creates_temporary_ban_when_ban_minutes_configured(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]
        self.fake_db.tables["moderation_penalty_rules"] = [
            {
                "id": 40,
                "violation_type_id": 1,
                "escalation_step": 5,
                "warn_count_before": 4,
                "warn_increment": 1,
                "warn_ttl_minutes": 14400,
                "mute_minutes": 0,
                "fine_points": 0,
                "ban_minutes": 7200,
                "apply_ban": False,
                "is_active": True,
                "description_for_admin": "Временный бан",
                "description_for_user": "Выдан бан на 5 дней",
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
            reason_text="temp ban",
        )

        self.assertIsNotNone(result)
        self.assertTrue(result["ban_applied"])
        self.assertEqual(self.fake_db.tables["moderation_bans"][0]["ends_at"], result["case"]["ban_until"])
        self.assertEqual(
            [row["action_type"] for row in self.fake_db.tables["moderation_actions"]],
            ["warn", "ban"],
        )

    def test_apply_violation_demotes_staff_instead_of_kick(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]
        self.fake_db.tables["accounts"] = [
            {"id": "acc-actor", "titles": ["Админ"]},
            {"id": "acc-target", "titles": ["Ветеран города"]},
        ]
        self.fake_db.tables["moderation_penalty_rules"] = [
            {
                "id": 50,
                "violation_type_id": 1,
                "escalation_step": 4,
                "warn_count_before": 3,
                "warn_increment": 1,
                "warn_ttl_minutes": 14400,
                "mute_minutes": 0,
                "fine_points": 0,
                "apply_kick": True,
                "apply_ban": False,
                "is_active": True,
                "description_for_admin": "Последний пред до кика",
                "description_for_user": "Эскалация",
            },
        ]
        self.fake_db.tables["moderation_warn_state"] = [
            {"id": 1, "account_id": "acc-target", "active_warn_count": 3}
        ]

        result = ModerationService.apply_violation(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            reason_text="override kick to demotion",
        )

        self.assertTrue(result["ok"])
        self.assertFalse(result["ui_payload"]["kick_applied"])
        self.assertTrue(result["ui_payload"]["demotion_applied"])
        self.assertIn("понижение", result["ui_payload"]["selected_action_summary"])
        self.assertEqual(
            [row["action_type"] for row in self.fake_db.tables["moderation_actions"]],
            ["warn", "demotion"],
        )
        self.assertEqual(self.fake_db.tables["accounts"][1]["titles"], ["Участник клубов"])

    def test_apply_violation_bans_staff_on_demotion_floor(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]
        self.fake_db.tables["accounts"] = [
            {"id": "acc-actor", "titles": ["Админ"]},
            {"id": "acc-target", "titles": ["Участник клубов"]},
        ]
        self.fake_db.tables["moderation_penalty_rules"] = [
            {
                "id": 51,
                "violation_type_id": 1,
                "escalation_step": 5,
                "warn_count_before": 4,
                "warn_increment": 1,
                "warn_ttl_minutes": 14400,
                "mute_minutes": 0,
                "fine_points": 0,
                "apply_ban": True,
                "is_active": True,
                "description_for_admin": "Финальная эскалация",
                "description_for_user": "Бан",
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
            reason_text="ban floor",
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["ban_applied"])
        self.assertEqual(
            [row["action_type"] for row in self.fake_db.tables["moderation_actions"]],
            ["warn", "ban"],
        )

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
        self.assertEqual(
            second["message"],
            "Кейс уже был подтверждён ранее. Повторное применение пропущено; ничего дополнительно не применено.",
        )
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
        self.assertEqual(result["rollback_status"], "manual_review_required")

    def test_commit_case_moves_fine_to_bank_and_links_it_to_case_history(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]

        result = ModerationService.commit_case(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            context={"moderation_op_key": "rep:bank-ok", "skip_authority": True, "reason_text": "Flood links"},
        )

        self.assertTrue(result["ok"])
        self.assertEqual(self.fake_db.tables["bank"][0]["total"], 3.0)
        self.assertEqual(len(self.fake_db.tables["bank_history"]), 1)
        self.assertIn("moderation case #", self.fake_db.tables["bank_history"][0]["reason"])
        self.assertIn("op_key=rep:bank-ok", self.fake_db.tables["bank_history"][0]["reason"])
        self.assertEqual(
            [row["action_type"] for row in self.fake_db.tables["moderation_actions"]],
            ["warn", "mute", "fine_points", "bank_income"],
        )
        self.assertEqual(self.fake_db.tables["moderation_actions"][-1]["case_id"], result["case_id"])
        self.assertEqual(self.fake_db.tables["moderation_actions"][-1]["op_key"], "rep:bank-ok")
        self.assertIn("Списан штраф 3 баллов в банк", result["ui_payload"]["moderator_result_text"])

    def test_commit_case_rolls_back_when_bank_income_log_fails(self):
        self.mock_resolve.side_effect = ["acc-actor", "acc-target"]
        self.fake_db.fail_log_bank_income = True

        result = ModerationService.commit_case(
            provider="discord",
            actor="111",
            target="222",
            violation_code="spam",
            context={"moderation_op_key": "rep:bank-log-fail", "skip_authority": True},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error_code"], "bank_income_log_failed")
        self.assertEqual(self.fake_db.tables["bank"][0]["total"], 0.0)
        self.assertEqual(self.fake_db.tables["bank_history"], [])
        self.assertEqual(self.fake_db.point_actions[0]["points"], -3.0)
        self.assertEqual(self.fake_db.point_actions[1]["points"], 3.0)
        self.assertIn(result["rollback_status"], {"rolled_back", "manual_review_required"})


if __name__ == "__main__":
    unittest.main()
