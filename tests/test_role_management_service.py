import unittest
from unittest.mock import patch

from bot.services.role_management_service import (
    DELETE_ROLE_REASON_DISCORD_MANAGED,
    RoleManagementService,
)


class _Resp:
    def __init__(self, data):
        self.data = data


class _TableOp:
    def __init__(self, fake_db, table_name):
        self.fake_db = fake_db
        self.table_name = table_name
        self._filters = []
        self._limit = None
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

    def delete(self):
        self._action = "delete"
        return self

    def execute(self):
        rows = self.fake_db.tables[self.table_name]

        if self._action == "delete":
            kept = []
            deleted = []
            for row in rows:
                if all(str(row.get(k)) == str(v) for k, v in self._filters):
                    deleted.append(dict(row))
                else:
                    kept.append(row)
            self.fake_db.tables[self.table_name] = kept
            return _Resp(deleted)

        selected = []
        for row in rows:
            if all(str(row.get(k)) == str(v) for k, v in self._filters):
                selected.append(dict(row))
        if self._limit is not None:
            selected = selected[: self._limit]
        return _Resp(selected)


class _FakeSupabase:
    def __init__(self, fake_db):
        self.fake_db = fake_db

    def table(self, name):
        return _TableOp(self.fake_db, name)


class _FakeDb:
    def __init__(self):
        self.tables = {
            "roles": [],
            "account_role_assignments": [],
        }
        self.supabase = _FakeSupabase(self)


class RoleManagementServiceDeleteRoleTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = _FakeDb()
        self.patcher = patch("bot.services.role_management_service.db", self.fake_db)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test_delete_role_denies_discord_managed_role_and_keeps_assignments(self):
        self.fake_db.tables["roles"] = [
            {"name": "External", "is_discord_managed": True, "discord_role_id": "12345"},
        ]
        self.fake_db.tables["account_role_assignments"] = [
            {"account_id": "acc-1", "role_name": "External", "source": "discord"},
        ]

        with self.assertLogs("bot.services.role_management_service", level="WARNING") as captured:
            result = RoleManagementService.delete_role(
                "External",
                actor_id="555",
                guild_id="999",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], DELETE_ROLE_REASON_DISCORD_MANAGED)
        self.assertEqual(len(self.fake_db.tables["roles"]), 1)
        self.assertEqual(len(self.fake_db.tables["account_role_assignments"]), 1)
        self.assertTrue(
            any("delete_role denied discord-managed" in message for message in captured.output),
            captured.output,
        )

    def test_delete_role_removes_custom_role_and_assignments(self):
        self.fake_db.tables["roles"] = [
            {"name": "Custom", "is_discord_managed": False, "discord_role_id": None},
        ]
        self.fake_db.tables["account_role_assignments"] = [
            {"account_id": "acc-1", "role_name": "Custom", "source": "custom"},
            {"account_id": "acc-2", "role_name": "Other", "source": "custom"},
        ]

        result = RoleManagementService.delete_role(
            "Custom",
            actor_id="777",
            telegram_user_id="888",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(self.fake_db.tables["roles"], [])
        self.assertEqual(
            self.fake_db.tables["account_role_assignments"],
            [{"account_id": "acc-2", "role_name": "Other", "source": "custom"}],
        )
