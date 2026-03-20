import unittest
from unittest.mock import patch
from types import SimpleNamespace

from bot.services.role_management_service import (
    DELETE_ROLE_REASON_DISCORD_MANAGED,
    DELETE_ROLE_REASON_NOT_FOUND,
    PRIVILEGED_DISCORD_ROLE_MESSAGE,
    ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE,
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

    def test_delete_role_returns_not_found_without_false_success(self):
        with self.assertLogs("bot.services.role_management_service", level="WARNING") as captured:
            result = RoleManagementService.delete_role(
                "Missing",
                actor_id="777",
                telegram_user_id="888",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], DELETE_ROLE_REASON_NOT_FOUND)
        self.assertTrue(any("delete_role skipped role missing" in message for message in captured.output), captured.output)

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
        self.assertEqual(grouped[0]["roles"][0]["description"], "")
        self.assertEqual(grouped[0]["roles"][0]["acquire_hint"], "")
        self.assertTrue(self.fake_db.tables["roles"])

    def test_list_roles_grouped_keeps_backward_compatibility_without_description(self):
        self.fake_db.tables["roles"] = [
            {"name": "Legacy", "category_name": "General", "position": 0},
        ]
        self.fake_db.tables["role_categories"] = [{"name": "General", "position": 0}]

        grouped = RoleManagementService.list_roles_grouped()

        self.assertEqual(grouped[0]["roles"][0]["name"], "Legacy")
        self.assertEqual(grouped[0]["roles"][0]["description"], "")
        self.assertEqual(grouped[0]["roles"][0]["acquire_hint"], "")

    def test_apply_user_role_changes_by_account_logs_each_role_and_keeps_multi_result(self):
        self.fake_db.tables["roles"] = [
            {"name": "Alpha", "category_name": "General", "is_discord_managed": False, "discord_role_id": None},
            {"name": "Beta", "category_name": "General", "is_discord_managed": False, "discord_role_id": None},
            {"name": "Gamma", "category_name": "Events", "is_discord_managed": False, "discord_role_id": None},
        ]
        self.fake_db.tables["account_role_assignments"] = [
            {"account_id": "acc-7", "role_name": "Gamma", "source": "custom"},
        ]

        with self.assertLogs("bot.services.role_management_service", level="INFO") as captured:
            result = RoleManagementService.apply_user_role_changes_by_account(
                "acc-7",
                actor_id="42",
                grant_roles=["Alpha", "Beta", "Alpha"],
                revoke_roles=["Gamma"],
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["grant_success"], ["Alpha", "Beta"])
        self.assertEqual(result["revoke_success"], ["Gamma"])
        assigned_roles = sorted(row["role_name"] for row in self.fake_db.tables["account_role_assignments"])
        self.assertEqual(assigned_roles, ["Alpha", "Beta"])
        self.assertTrue(any("role_name=Alpha action=grant success=True" in message for message in captured.output))
        self.assertTrue(any("role_name=Beta action=grant success=True" in message for message in captured.output))
        self.assertTrue(any("role_name=Gamma action=revoke success=True" in message for message in captured.output))

    def test_list_public_roles_catalog_sorts_categories_and_marks_acquire_methods(self):
        self.fake_db.tables["roles"] = [
            {
                "name": "Куратор",
                "category_name": "Админские",
                "position": 0,
                "description": "Следит за разделом",
                "acquire_hint": "Назначается вручную",
                "is_discord_managed": False,
                "discord_role_id": None,
            },
            {
                "name": "Синхронизируемая",
                "category_name": "Discord",
                "position": 0,
                "description": "Приходит из Discord",
                "acquire_hint": "Поддерживайте роль в Discord",
                "is_discord_managed": True,
                "discord_role_id": "555",
            },
        ]
        self.fake_db.tables["role_categories"] = [
            {"name": "Админские", "position": 2},
            {"name": "Discord", "position": 1},
        ]

        grouped = RoleManagementService.list_public_roles_catalog()

        self.assertEqual([item["category"] for item in grouped], ["Discord", "Админские", "Роли за баллы"])
        self.assertEqual(grouped[0]["roles"][0]["acquire_method_label"], "автоматически синхронизируется с Discord")
        self.assertEqual(grouped[1]["roles"][0]["acquire_method_label"], "выдаёт администратор")
        self.assertEqual(grouped[2]["roles"][0]["acquire_method_label"], "за баллы")
        self.assertEqual(grouped[2]["roles"][0]["points_required"], 10)

    def test_list_public_roles_catalog_backfills_legacy_points_hint_for_canonical_role(self):
        self.fake_db.tables["roles"] = [
            {
                "name": "Новый волонтер",
                "category_name": "Основные",
                "position": 0,
                "description": "",
                "acquire_hint": "",
                "is_discord_managed": False,
                "discord_role_id": "1105906310131744868",
            },
        ]
        self.fake_db.tables["role_categories"] = [{"name": "Основные", "position": 0}]

        grouped = RoleManagementService.list_public_roles_catalog()

        role = grouped[0]["roles"][0]
        self.assertEqual(role["acquire_method_label"], "за баллы")
        self.assertEqual(role["points_required"], 10)
        self.assertEqual(role["acquire_hint"], "Накопить 10 баллов.")

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
        self.assertEqual(self.fake_db.tables["roles"][0]["position"], 0)

    def test_apply_user_role_changes_denies_privileged_discord_role_for_vice(self):
        self.fake_db.tables["roles"] = [
            {
                "name": "Discord Admin",
                "category_name": "Админские",
                "is_discord_managed": True,
                "discord_role_id": "999888",
                "is_privileged_discord_role": True,
            }
        ]

        with (
            patch("bot.services.role_management_service.AuthorityService.is_super_admin", return_value=False),
            patch(
                "bot.services.role_management_service.AuthorityService.resolve_authority",
                return_value=SimpleNamespace(level=80, rank_weight=80, titles=("Вице города",)),
            ),
            self.assertLogs("bot.services.role_management_service", level="WARNING") as captured,
        ):
            result = RoleManagementService.apply_user_role_changes_by_account(
                "acc-7",
                actor_id="42",
                actor_provider="discord",
                actor_user_id="42",
                grant_roles=["Discord Admin"],
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["grant_success"], [])
        self.assertEqual(result["grant_failed"], ["Discord Admin"])
        self.assertEqual(result["grant_denied"][0]["reason"], ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE)
        self.assertEqual(result["grant_denied"][0]["message"], PRIVILEGED_DISCORD_ROLE_MESSAGE)
        self.assertEqual(self.fake_db.tables["account_role_assignments"], [])
        self.assertTrue(any("privileged_discord_role_access_denied" in line for line in captured.output), captured.output)
        self.assertTrue(any("actor_id=42" in line and "discord_role_id=999888" in line for line in captured.output), captured.output)

    def test_get_category_role_positioning_returns_roles_and_end_description(self):
        self.fake_db.tables["roles"] = [
            {"name": "Alpha", "category_name": "General", "position": 0},
            {"name": "Beta", "category_name": "General", "position": 1},
        ]
        self.fake_db.tables["role_categories"] = [{"name": "General", "position": 0}]

        preview = RoleManagementService.get_category_role_positioning("General")

        self.assertEqual(preview["category"], "General")
        self.assertEqual([item["name"] for item in preview["current_roles"]], ["Alpha", "Beta"])
        self.assertEqual(preview["computed_last_position"], 2)
        self.assertEqual(preview["computed_position"], 2)
        self.assertEqual(preview["position_description"], "будет добавлено в конец (#3)")

    def test_create_role_without_position_uses_end_of_category(self):
        self.fake_db.tables["roles"] = [
            {"name": "Alpha", "category_name": "General", "position": 0},
            {"name": "Beta", "category_name": "General", "position": 1},
        ]

        ok = RoleManagementService.create_role("Gamma", "General", description="Описание", position=None)

        self.assertTrue(ok)
        created = next(row for row in self.fake_db.tables["roles"] if row["name"] == "Gamma")
        self.assertEqual(created["position"], 2)
        self.assertEqual(created["description"], "Описание")
        self.assertIsNone(created["acquire_hint"])

    def test_get_role_returns_empty_description_for_legacy_rows(self):
        self.fake_db.tables["roles"] = [
            {"name": "Legacy", "category_name": "General", "position": 0},
        ]

        role = RoleManagementService.get_role("Legacy")

        assert role is not None
        self.assertEqual(role["description"], "")
        self.assertEqual(role["acquire_hint"], "")

    def test_update_role_description_updates_role(self):
        self.fake_db.tables["roles"] = [
            {"name": "Gamma", "category_name": "General", "position": 0, "description": None},
        ]

        ok = RoleManagementService.update_role_description("Gamma", "Новое описание", actor_id="42")

        self.assertTrue(ok)
        self.assertEqual(self.fake_db.tables["roles"][0]["description"], "Новое описание")

    def test_update_role_acquire_hint_updates_role_and_logs_field(self):
        self.fake_db.tables["roles"] = [
            {"name": "Gamma", "category_name": "General", "position": 0, "acquire_hint": None},
        ]

        with self.assertLogs("bot.services.role_management_service", level="INFO") as captured:
            ok = RoleManagementService.update_role_acquire_hint(
                "Gamma",
                "Выдается после турнира",
                actor_id="42",
                operation="role_edit_acquire_hint",
            )

        self.assertTrue(ok)
        self.assertEqual(self.fake_db.tables["roles"][0]["acquire_hint"], "Выдается после турнира")
        self.assertTrue(any("actor_id=42" in line and "field=acquire_hint" in line for line in captured.output), captured.output)


if __name__ == "__main__":
    unittest.main()
