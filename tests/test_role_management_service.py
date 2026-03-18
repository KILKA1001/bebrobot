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
        self._payload = None

    def select(self, _fields):
        self._action = "select"
        return self

    def eq(self, key, value):
        self._filters.append((key, value))
        return self

    def is_(self, key, value):
        self._filters.append((key, None if str(value).lower() == "null" else value))
        return self

    def limit(self, n):
        self._limit = n
        return self

    def delete(self):
        self._action = "delete"
        return self

    def update(self, payload):
        self._action = "update"
        self._payload = dict(payload)
        return self

    def upsert(self, payload, **_kwargs):
        self._action = "upsert"
        self._payload = dict(payload)
        return self

    def _matches(self, row):
        return all(row.get(k) == v for k, v in self._filters)

    def execute(self):
        rows = self.fake_db.tables[self.table_name]

        if self._action == "delete":
            kept = []
            deleted = []
            for row in rows:
                if self._matches(row):
                    deleted.append(dict(row))
                else:
                    kept.append(row)
            self.fake_db.tables[self.table_name] = kept
            return _Resp(deleted)

        if self._action == "update":
            updated = []
            for row in rows:
                if self._matches(row):
                    row.update(self._payload)
                    updated.append(dict(row))
            return _Resp(updated)

        if self._action == "upsert":
            if self.table_name == "roles":
                key_fields = ["discord_role_id", "name"]
            elif self.table_name == "role_categories":
                key_fields = ["name"]
            else:
                key_fields = ["name"]
            for row in rows:
                for key_field in key_fields:
                    key_value = self._payload.get(key_field)
                    if key_value and row.get(key_field) == key_value:
                        row.update(self._payload)
                        return _Resp([dict(row)])
            rows.append(dict(self._payload))
            return _Resp([dict(self._payload)])

        selected = []
        for row in rows:
            if self._matches(row):
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
            "external_role_bindings": [],
            "role_categories": [],
        }
        self.supabase = _FakeSupabase(self)


class RoleManagementServiceTests(unittest.TestCase):
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

    def test_list_roles_grouped_auto_upserts_external_bindings_into_catalog(self):
        self.fake_db.tables["external_role_bindings"] = [
            {
                "account_id": "acc-1",
                "source": "discord",
                "external_role_id": "role-1",
                "external_role_name": "External role",
                "deleted_at": None,
            }
        ]

        grouped = RoleManagementService.list_roles_grouped()

        self.assertEqual(grouped[0]["roles"][0]["name"], "External role")
        self.assertEqual(grouped[0]["roles"][0]["discord_role_id"], "role-1")
        self.assertTrue(self.fake_db.tables["roles"])

    def test_move_role_returns_false_when_role_missing_from_catalog(self):
        with self.assertLogs("bot.services.role_management_service", level="WARNING") as captured:
            ok = RoleManagementService.move_role("Missing", "Категория", 1)

        self.assertFalse(ok)
        self.assertTrue(any("move_role denied role missing from canonical catalog" in line for line in captured.output))

    def test_move_role_updates_category_for_external_role(self):
        self.fake_db.tables["roles"] = [
            {
                "name": "External role",
                "category_name": "Discord сервер (auto)",
                "position": 0,
                "is_discord_managed": True,
                "discord_role_id": "role-1",
            }
        ]

        ok = RoleManagementService.move_role("External role", "Новая категория", 3)

        self.assertTrue(ok)
        self.assertEqual(self.fake_db.tables["roles"][0]["category_name"], "Новая категория")
        self.assertEqual(self.fake_db.tables["roles"][0]["position"], 3)


if __name__ == "__main__":
    unittest.main()
