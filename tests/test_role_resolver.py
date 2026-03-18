from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

from bot.services.auth.role_resolver import RoleResolver


class _Resp:
    def __init__(self, data):
        self.data = data


class _TableOp:
    def __init__(self, fake_db, table_name):
        self.fake_db = fake_db
        self.table_name = table_name
        self._filters = []
        self._limit = None

    def select(self, _fields):
        return self

    def eq(self, key, value):
        self._filters.append((key, value))
        return self

    def limit(self, n):
        self._limit = n
        return self

    def is_(self, key, value):
        self._filters.append((key, None if str(value).lower() == "null" else value))
        return self

    def execute(self):
        rows = self.fake_db.tables.get(self.table_name, [])
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
            "account_role_assignments": [],
            "accounts": [],
            "roles": [],
            "role_permissions": [],
            "external_role_bindings": [],
        }
        self.supabase = _FakeSupabase(self)


class RoleResolverTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = _FakeDb()
        self.patcher = patch("bot.services.auth.role_resolver.db", self.fake_db)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test_deny_has_priority_over_allow(self):
        now = datetime.now(timezone.utc)
        self.fake_db.tables["account_role_assignments"] = [
            {
                "account_id": "acc-1",
                "role_name": "moderator",
                "source": "discord",
                "synced_at": now.isoformat(),
            },
            {
                "account_id": "acc-1",
                "role_name": "restricted",
                "source": "telegram",
                "synced_at": now.isoformat(),
            },
        ]
        self.fake_db.tables["roles"] = [{"name": "moderator"}, {"name": "restricted"}]
        self.fake_db.tables["role_permissions"] = [
            {"role_name": "moderator", "permission_name": "tickets.manage", "effect": "allow"},
            {"role_name": "restricted", "permission_name": "tickets.manage", "effect": "deny"},
        ]

        result = RoleResolver.resolve_for_account("acc-1")

        self.assertEqual(result.permissions["deny"], ["tickets.manage"])
        self.assertEqual(result.permissions["allow"], [])

    def test_custom_priority_applies_but_deny_still_wins(self):
        now = datetime.now(timezone.utc)
        self.fake_db.tables["account_role_assignments"] = [
            {
                "account_id": "acc-1",
                "role_name": "external_mod",
                "source": "discord",
                "synced_at": now.isoformat(),
            },
            {
                "account_id": "acc-1",
                "role_name": "manual_override",
                "source": "custom",
                "synced_at": now.isoformat(),
            },
        ]
        self.fake_db.tables["roles"] = [{"name": "external_mod"}, {"name": "manual_override"}]
        self.fake_db.tables["role_permissions"] = [
            {"role_name": "external_mod", "permission_name": "chat.post", "effect": "deny"},
            {"role_name": "manual_override", "permission_name": "chat.post", "effect": "allow"},
        ]

        result = RoleResolver.resolve_for_account("acc-1")

        self.assertEqual(result.permissions["allow"], [])
        self.assertEqual(result.permissions["deny"], ["chat.post"])

    def test_ignores_expired_assignments(self):
        now = datetime.now(timezone.utc)
        self.fake_db.tables["account_role_assignments"] = [
            {
                "account_id": "acc-1",
                "role_name": "legacy",
                "source": "custom",
                "expires_at": (now - timedelta(hours=1)).isoformat(),
                "synced_at": now.isoformat(),
            }
        ]

        result = RoleResolver.resolve_for_account("acc-1")

        self.assertEqual(result.roles, [])


    def test_fallback_to_external_role_bindings_when_assignments_empty(self):
        now = datetime.now(timezone.utc)
        self.fake_db.tables["external_role_bindings"] = [
            {
                "account_id": "acc-1",
                "source": "discord",
                "external_role_id": "123",
                "external_role_name": "Сладкая бебра",
                "last_synced_at": now.isoformat(),
                "deleted_at": None,
            }
        ]
        self.fake_db.tables["roles"] = [
            {
                "name": "Сладкая бебра",
                "category_name": "Клубные роли",
                "discord_role_id": "123",
            }
        ]

        result = RoleResolver.resolve_for_account("acc-1")

        self.assertEqual(len(result.roles), 1)
        self.assertEqual(result.roles[0]["name"], "Сладкая бебра")
        self.assertEqual(result.roles[0]["category"], "Клубные роли")

    def test_external_binding_uses_name_fallback_category_and_logs_id_mismatch(self):
        now = datetime.now(timezone.utc)
        self.fake_db.tables["external_role_bindings"] = [
            {
                "account_id": "acc-1",
                "source": "discord",
                "external_role_id": "999",
                "external_role_name": "Сладкая бебра",
                "last_synced_at": now.isoformat(),
                "deleted_at": None,
            }
        ]
        self.fake_db.tables["roles"] = [
            {
                "name": "Сладкая бебра",
                "category_name": "Клубные роли",
                "discord_role_id": "123",
            }
        ]

        with self.assertLogs("bot.services.auth.role_resolver", level="WARNING") as captured:
            result = RoleResolver.resolve_for_account("acc-1")

        self.assertEqual(result.roles[0]["category"], "Клубные роли")
        self.assertTrue(
            any("catalog name matched but external id mismatched" in message for message in captured.output),
            captured.output,
        )

    def test_external_binding_matches_catalog_by_external_role_id(self):
        now = datetime.now(timezone.utc)
        self.fake_db.tables["external_role_bindings"] = [
            {
                "account_id": "acc-1",
                "source": "telegram",
                "external_role_id": "tg-777",
                "external_role_name": "Телеграм роль",
                "last_synced_at": now.isoformat(),
                "deleted_at": None,
            }
        ]
        self.fake_db.tables["roles"] = [
            {
                "name": "Каталожная телеграм роль",
                "category_name": "Telegram роли",
                "external_role_id": "tg-777",
            }
        ]

        result = RoleResolver.resolve_for_account("acc-1")

        self.assertEqual(result.roles[0]["name"], "Каталожная телеграм роль")
        self.assertEqual(result.roles[0]["category"], "Telegram роли")

    def test_external_binding_logs_multiple_catalog_matches(self):
        now = datetime.now(timezone.utc)
        self.fake_db.tables["external_role_bindings"] = [
            {
                "account_id": "acc-1",
                "source": "discord",
                "external_role_id": "123",
                "external_role_name": "Сладкая бебра",
                "last_synced_at": now.isoformat(),
                "deleted_at": None,
            }
        ]
        self.fake_db.tables["roles"] = [
            {"name": "Сладкая бебра", "category_name": "Клубные роли", "discord_role_id": "123"},
            {"name": "Сладкая бебра 2", "category_name": "Редкие роли", "discord_role_id": "123"},
        ]

        with self.assertLogs("bot.services.auth.role_resolver", level="WARNING") as captured:
            result = RoleResolver.resolve_for_account("acc-1")

        self.assertEqual(result.roles[0]["category"], "Клубные роли")
        self.assertTrue(
            any("multiple catalog matches by external id" in message for message in captured.output),
            captured.output,
        )

    def test_external_binding_logs_when_catalog_role_missing(self):
        now = datetime.now(timezone.utc)
        self.fake_db.tables["external_role_bindings"] = [
            {
                "account_id": "acc-1",
                "source": "discord",
                "external_role_id": "404",
                "external_role_name": "Неизвестная роль",
                "last_synced_at": now.isoformat(),
                "deleted_at": None,
            }
        ]

        with self.assertLogs("bot.services.auth.role_resolver", level="WARNING") as captured:
            result = RoleResolver.resolve_for_account("acc-1")

        self.assertEqual(result.roles[0]["category"], "Внешние роли")
        self.assertTrue(
            any("external role binding not found in catalog" in message for message in captured.output),
            captured.output,
        )
        self.assertTrue(
            any("using fallback external category" in message for message in captured.output),
            captured.output,
        )

    def test_external_binding_uses_updated_catalog_category_after_move(self):
        now = datetime.now(timezone.utc)
        self.fake_db.tables["external_role_bindings"] = [
            {
                "account_id": "acc-1",
                "source": "discord",
                "external_role_id": "123",
                "external_role_name": "Сладкая бебра",
                "last_synced_at": now.isoformat(),
                "deleted_at": None,
            }
        ]
        self.fake_db.tables["roles"] = [
            {
                "name": "Сладкая бебра",
                "category_name": "Новая категория",
                "discord_role_id": "123",
            }
        ]

        result = RoleResolver.resolve_for_account("acc-1")

        self.assertEqual(result.roles[0]["category"], "Новая категория")


if __name__ == "__main__":
    unittest.main()
