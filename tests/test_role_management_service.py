import unittest
from unittest.mock import patch
from types import SimpleNamespace

from bot.services.role_management_service import (
    DELETE_ROLE_REASON_DISCORD_MANAGED,
    DELETE_ROLE_REASON_NOT_FOUND,
    PRIVILEGED_DISCORD_ROLE_MESSAGE,
    PROTECTED_PROFILE_TITLE_ROLE_MESSAGE,
    ROLE_NAME_CONFLICT_PROFILE_TITLE_MESSAGE,
    ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE,
    ROLE_ASSIGNMENT_REASON_SYNC_ONLY_DISCORD_ROLE,
    SYNC_ONLY_DISCORD_ROLE_MESSAGE,
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

    def insert(self, payload, **_kwargs):
        self._action = "insert"
        self._payload = dict(payload)
        return self

    def _matches(self, row):
        return all(row.get(k) == v for k, v in self._filters)

    def execute(self):
        self.fake_db.operations.append(
            {
                "table": self.table_name,
                "action": self._action,
                "filters": list(self._filters),
            }
        )
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

        if self._action == "insert":
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
            "role_permissions": [],
            "external_role_bindings": [],
            "role_categories": [],
            "account_identities": [],
            "profile_title_roles": [],
            "role_change_audit": [],
        }
        self.operations = []
        self.supabase = _FakeSupabase(self)


class RoleManagementServiceTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = _FakeDb()
        self.patcher = patch("bot.services.role_management_service.db", self.fake_db)
        self.patcher.start()
        RoleManagementService.invalidate_catalog_cache(reason="test_setup")

    def tearDown(self):
        RoleManagementService.invalidate_catalog_cache(reason="test_teardown")
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
        self.fake_db.tables["role_permissions"] = [
            {"role_name": "Custom", "permission_name": "tickets.manage", "effect": "allow"},
            {"role_name": "Other", "permission_name": "chat.post", "effect": "allow"},
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
        self.assertEqual(
            self.fake_db.tables["role_permissions"],
            [{"role_name": "Other", "permission_name": "chat.post", "effect": "allow"}],
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

    def test_list_roles_grouped_preserves_existing_category_position_during_external_sync(self):
        self.fake_db.tables["external_role_bindings"] = [
            {
                "account_id": "acc-1",
                "source": "discord",
                "external_role_id": "role-1",
                "external_role_name": "External role",
                "deleted_at": None,
            }
        ]
        self.fake_db.tables["role_categories"] = [{"name": "Discord сервер (auto)", "position": 0}]

        RoleManagementService.list_roles_grouped()

        self.assertEqual(self.fake_db.tables["role_categories"][0]["position"], 0)

    def test_list_roles_grouped_keeps_backward_compatibility_without_description(self):
        self.fake_db.tables["roles"] = [
            {"name": "Legacy", "category_name": "General", "position": 0},
        ]
        self.fake_db.tables["role_categories"] = [{"name": "General", "position": 0}]

        grouped = RoleManagementService.list_roles_grouped()

        self.assertEqual(grouped[0]["roles"][0]["name"], "Legacy")
        self.assertEqual(grouped[0]["roles"][0]["description"], "")
        self.assertEqual(grouped[0]["roles"][0]["acquire_hint"], "")

    def test_list_roles_grouped_reuses_ttl_cache_for_repeat_reads(self):
        self.fake_db.tables["roles"] = [
            {"name": "Legacy", "category_name": "General", "position": 0},
        ]
        self.fake_db.tables["role_categories"] = [{"name": "General", "position": 0}]

        first = RoleManagementService.list_roles_grouped()
        operation_count_after_first = len(self.fake_db.operations)
        second = RoleManagementService.list_roles_grouped()

        self.assertEqual(first, second)
        self.assertEqual(len(self.fake_db.operations), operation_count_after_first)

    def test_apply_user_role_changes_by_account_logs_each_role_and_keeps_multi_result(self):
        self.fake_db.tables["roles"] = [
            {"name": "Alpha", "category_name": "General", "is_discord_managed": False, "discord_role_id": None},
            {"name": "Beta", "category_name": "General", "is_discord_managed": False, "discord_role_id": None},
            {"name": "Gamma", "category_name": "Events", "is_discord_managed": False, "discord_role_id": None},
        ]
        self.fake_db.tables["account_role_assignments"] = [
            {"account_id": "acc-7", "role_name": "Gamma", "source": "custom"},
        ]
        self.fake_db.tables["account_identities"] = [
            {"account_id": "acc-7", "provider": "telegram", "provider_user_id": "700"},
            {"account_id": "acc-42", "provider": "discord", "provider_user_id": "42"},
        ]

        with self.assertLogs("bot.services.role_management_service", level="INFO") as captured:
            result = RoleManagementService.apply_user_role_changes_by_account(
                "acc-7",
                actor_id="42",
                actor_provider="discord",
                actor_user_id="42",
                grant_roles=["Alpha", "Beta", "Alpha"],
                revoke_roles=["Gamma"],
                source="discord_command",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["grant_success"], ["Alpha", "Beta"])
        self.assertEqual(result["revoke_success"], ["Gamma"])
        assigned_roles = sorted(row["role_name"] for row in self.fake_db.tables["account_role_assignments"])
        self.assertEqual(assigned_roles, ["Alpha", "Beta"])
        self.assertTrue(any("role_name=Alpha action=grant success=True" in message for message in captured.output))
        self.assertTrue(any("role_name=Beta action=grant success=True" in message for message in captured.output))
        self.assertTrue(any("role_name=Gamma action=revoke success=True" in message for message in captured.output))
        audit_actions = [row["action"] for row in self.fake_db.tables["role_change_audit"]]
        self.assertIn("role_grant", audit_actions)
        self.assertIn("role_revoke", audit_actions)
        self.assertIn("role_batch_change", audit_actions)
        batch_row = next(row for row in self.fake_db.tables["role_change_audit"] if row["action"] == "role_batch_change")
        self.assertEqual(batch_row["source"], "discord_command")
        self.assertEqual(batch_row["actor_provider"], "discord")
        self.assertEqual(batch_row["actor_provider_user_id"], "42")
        self.assertEqual(batch_row["target_account_id"], "acc-7")

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

    def test_list_public_roles_catalog_hides_roles_marked_hidden_in_db(self):
        self.fake_db.tables["roles"] = [
            {
                "name": "Видимая",
                "category_name": "Discord",
                "position": 0,
                "description": "Показывается",
                "acquire_hint": "Видна в каталоге",
                "is_discord_managed": True,
                "discord_role_id": "101",
                "show_in_roles_catalog": True,
            },
            {
                "name": "Скрытая",
                "category_name": "Discord",
                "position": 1,
                "description": "Скрыта",
                "acquire_hint": "Не показывается",
                "is_discord_managed": True,
                "discord_role_id": "102",
                "show_in_roles_catalog": False,
            },
        ]
        self.fake_db.tables["role_categories"] = [{"name": "Discord", "position": 0}]

        with self.assertLogs("bot.services.role_management_service", level="INFO") as captured:
            grouped = RoleManagementService.list_public_roles_catalog(log_context="/roles")

        self.assertEqual([role["name"] for role in grouped[0]["roles"]], ["Видимая"])
        self.assertTrue(
            any("filtered hidden role" in message and "role_name=Скрытая" in message for message in captured.output),
            captured.output,
        )

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

    def test_paginate_public_roles_catalog_packs_adjacent_categories_and_splits_large_one(self):
        grouped = [
            {
                "category": "Бета",
                "position": 1,
                "roles": [{"name": f"B{i}"} for i in range(4, 0, -1)],
            },
            {
                "category": "Альфа",
                "position": 1,
                "roles": [{"name": f"A{i}"} for i in range(3, 0, -1)],
            },
            {
                "category": "Пустая",
                "position": 2,
                "roles": [],
            },
            {
                "category": "Гамма",
                "position": 3,
                "roles": [{"name": f"G{i}"} for i in range(9, 0, -1)],
            },
            {
                "category": "",
                "position": 4,
                "roles": [{"name": ""}],
            },
        ]

        with self.assertLogs("bot.systems.roles_catalog_shared", level="INFO") as captured:
            pages = RoleManagementService.paginate_public_roles_catalog(grouped)

        self.assertEqual(len(pages), 4)
        self.assertEqual(pages[0]["page_index"], 0)
        self.assertEqual(pages[0]["total_pages"], 4)
        self.assertEqual([section["category"] for section in pages[0]["sections"]], ["Альфа", "Бета"])
        self.assertEqual([item["name"] for item in pages[0]["sections"][0]["items"]], ["A1", "A2", "A3"])
        self.assertEqual([item["name"] for item in pages[0]["sections"][1]["items"]], ["B1", "B2", "B3", "B4"])
        self.assertEqual(pages[0]["role_count"], 7)
        self.assertEqual(pages[0]["section_count"], 2)
        self.assertEqual([len(section["items"]) for section in pages[1]["sections"]], [8])
        self.assertEqual(pages[1]["sections"][0]["category"], "Гамма")
        self.assertFalse(pages[1]["sections"][0]["is_category_continuation"])
        self.assertTrue(pages[1]["sections"][0]["continues_on_next_page"])
        self.assertEqual([len(section["items"]) for section in pages[2]["sections"]], [1])
        self.assertEqual(pages[2]["sections"][0]["category"], "Гамма")
        self.assertTrue(pages[2]["sections"][0]["is_category_continuation"])
        self.assertFalse(pages[2]["sections"][0]["continues_on_next_page"])
        self.assertEqual([section["category"] for section in pages[3]["sections"]], ["Без категории"])
        self.assertEqual(pages[3]["sections"][0]["items"][0]["name"], "Без названия")
        self.assertTrue(any("roles catalog empty category hidden" in line for line in captured.output))
        self.assertTrue(any("empty category name" in line for line in captured.output))
        self.assertTrue(any("empty role name" in line for line in captured.output))

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

    def test_apply_user_role_changes_denies_hidden_discord_sync_only_role_on_grant(self):
        self.fake_db.tables["roles"] = [
            {
                "name": "Bot Hidden",
                "category_name": "Discord",
                "is_discord_managed": True,
                "discord_role_id": "sync-only-1",
                "show_in_roles_catalog": False,
            }
        ]

        with self.assertLogs("bot.services.role_management_service", level="WARNING") as captured:
            result = RoleManagementService.apply_user_role_changes_by_account(
                "acc-7",
                actor_id="42",
                actor_provider="telegram",
                actor_user_id="42",
                grant_roles=["Bot Hidden"],
                source="telegram_command",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["grant_failed"], ["Bot Hidden"])
        self.assertEqual(result["grant_denied"][0]["reason"], ROLE_ASSIGNMENT_REASON_SYNC_ONLY_DISCORD_ROLE)
        self.assertEqual(result["grant_denied"][0]["message"], SYNC_ONLY_DISCORD_ROLE_MESSAGE)
        self.assertEqual(self.fake_db.tables["account_role_assignments"], [])
        self.assertTrue(any("sync_only_discord_role_access_denied" in line for line in captured.output), captured.output)
        denied_audit = next(row for row in self.fake_db.tables["role_change_audit"] if row["action"] == "role_grant_denied")
        self.assertEqual(denied_audit["error_code"], ROLE_ASSIGNMENT_REASON_SYNC_ONLY_DISCORD_ROLE)

    def test_apply_user_role_changes_denies_hidden_discord_sync_only_role_on_revoke(self):
        self.fake_db.tables["roles"] = [
            {
                "name": "Bot Hidden",
                "category_name": "Discord",
                "is_discord_managed": True,
                "discord_role_id": "sync-only-1",
                "show_in_roles_catalog": False,
            }
        ]
        self.fake_db.tables["account_role_assignments"] = [
            {"account_id": "acc-7", "role_name": "Bot Hidden", "source": "discord"}
        ]

        with self.assertLogs("bot.services.role_management_service", level="WARNING") as captured:
            result = RoleManagementService.apply_user_role_changes_by_account(
                "acc-7",
                actor_id="42",
                actor_provider="discord",
                actor_user_id="42",
                revoke_roles=["Bot Hidden"],
                source="discord_command",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["revoke_failed"], ["Bot Hidden"])
        self.assertEqual(result["revoke_denied"][0]["reason"], ROLE_ASSIGNMENT_REASON_SYNC_ONLY_DISCORD_ROLE)
        self.assertEqual(result["revoke_denied"][0]["message"], SYNC_ONLY_DISCORD_ROLE_MESSAGE)
        self.assertEqual(len(self.fake_db.tables["account_role_assignments"]), 1)
        self.assertTrue(any("sync_only_discord_role_access_denied" in line for line in captured.output), captured.output)
        denied_audit = next(row for row in self.fake_db.tables["role_change_audit"] if row["action"] == "role_revoke_denied")
        self.assertEqual(denied_audit["error_code"], ROLE_ASSIGNMENT_REASON_SYNC_ONLY_DISCORD_ROLE)

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
        denied_audit = next(row for row in self.fake_db.tables["role_change_audit"] if row["action"] == "role_grant_denied")
        self.assertEqual(denied_audit["status"], "denied")
        self.assertEqual(denied_audit["error_code"], ROLE_ASSIGNMENT_REASON_PRIVILEGED_DISCORD_ROLE)
        self.assertEqual(denied_audit["source"], "unknown")

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

    def test_create_role_preserves_existing_category_position(self):
        self.fake_db.tables["role_categories"] = [{"name": "General", "position": 7}]

        ok = RoleManagementService.create_role("Gamma", "General", description="Описание", position=0)

        self.assertTrue(ok)
        created = next(row for row in self.fake_db.tables["roles"] if row["name"] == "Gamma")
        self.assertEqual(self.fake_db.tables["role_categories"][0]["position"], 7)
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

    def test_get_role_uses_role_cache_for_missing_role(self):
        with self.assertLogs("bot.services.role_management_service", level="DEBUG") as captured:
            first = RoleManagementService.get_role("Missing")
            second = RoleManagementService.get_role("Missing")

        self.assertIsNone(first)
        self.assertIsNone(second)
        select_ops = [
            op for op in self.fake_db.operations if op["table"] == "roles" and op["action"] == "select"
        ]
        self.assertEqual(len(select_ops), 12)
        self.assertTrue(any("role cache refreshed role_name=Missing exists=False" in line for line in captured.output), captured.output)
        self.assertTrue(any("role cache hit role_name=Missing exists=false" in line for line in captured.output), captured.output)

    def test_create_role_invalidates_catalog_cache_after_repeat_reads(self):
        self.fake_db.tables["roles"] = [
            {"name": "Alpha", "category_name": "General", "position": 0},
        ]
        self.fake_db.tables["role_categories"] = [{"name": "General", "position": 0}]

        initial = RoleManagementService.list_roles_grouped()
        self.assertEqual([role["name"] for role in initial[0]["roles"]], ["Alpha"])

        ok = RoleManagementService.create_role("Beta", "General", description="Описание")
        self.assertTrue(ok)

        refreshed = RoleManagementService.list_roles_grouped()
        self.assertEqual([role["name"] for role in refreshed[0]["roles"]], ["Alpha", "Beta"])

    def test_update_role_description_updates_role(self):
        self.fake_db.tables["roles"] = [
            {"name": "Gamma", "category_name": "General", "position": 0, "description": None},
        ]

        ok = RoleManagementService.update_role_description("Gamma", "Новое описание", actor_id="42")

        self.assertTrue(ok)
        self.assertEqual(self.fake_db.tables["roles"][0]["description"], "Новое описание")
        audit_row = next(row for row in self.fake_db.tables["role_change_audit"] if row["action"] == "role_edit_description")
        self.assertEqual(audit_row["role_name"], "Gamma")
        self.assertEqual(audit_row["after_value"]["description"], "Новое описание")

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
        audit_row = next(row for row in self.fake_db.tables["role_change_audit"] if row["action"] == "role_edit_acquire_hint")
        self.assertEqual(audit_row["after_value"]["acquire_hint"], "Выдается после турнира")

    def test_apply_user_role_changes_audits_batch_conflict(self):
        result = RoleManagementService.apply_user_role_changes_by_account(
            "acc-1",
            actor_id="42",
            actor_provider="discord",
            actor_user_id="42",
            grant_roles=["Alpha"],
            revoke_roles=["Alpha"],
            source="discord_button",
        )

        self.assertEqual(result["conflicting_roles"], ["Alpha"])
        conflict_row = next(row for row in self.fake_db.tables["role_change_audit"] if row["action"] == "role_batch_conflict")
        self.assertEqual(conflict_row["status"], "conflict")
        self.assertEqual(conflict_row["source"], "discord_button")

    def test_sync_discord_guild_roles_preserves_hidden_public_visibility_flag(self):
        self.fake_db.tables["roles"] = [
            {
                "name": "Bot Hidden",
                "category_name": "Discord сервер (auto)",
                "position": 0,
                "is_discord_managed": True,
                "discord_role_id": "role-1",
                "show_in_roles_catalog": False,
            }
        ]

        result = RoleManagementService.sync_discord_guild_roles([
            {"id": "role-1", "name": "Bot Hidden", "position": 5, "guild_id": "guild-1"}
        ])

        self.assertEqual(result["upserted"], 1)
        self.assertFalse(self.fake_db.tables["roles"][0]["show_in_roles_catalog"])

    def test_sync_discord_guild_roles_removes_dependencies_before_role_row(self):
        self.fake_db.tables["roles"] = [
            {
                "name": "Legacy Discord",
                "category_name": "Discord сервер (auto)",
                "position": 0,
                "is_discord_managed": True,
                "discord_role_id": "old-role",
            }
        ]
        self.fake_db.tables["account_role_assignments"] = [
            {"account_id": "acc-1", "role_name": "Legacy Discord", "source": "discord"}
        ]
        self.fake_db.tables["role_permissions"] = [
            {"role_name": "Legacy Discord", "permission_name": "tickets.manage", "effect": "allow"}
        ]

        result = RoleManagementService.sync_discord_guild_roles([])

        self.assertEqual(result["removed"], 1)
        self.assertEqual(self.fake_db.tables["roles"], [])
        self.assertEqual(self.fake_db.tables["account_role_assignments"], [])
        self.assertEqual(self.fake_db.tables["role_permissions"], [])


    def test_list_roles_grouped_filters_protected_profile_titles_from_catalog(self):
        self.fake_db.tables["roles"] = [
            {"name": "Глава клуба", "category_name": "Админские", "position": 0},
            {"name": "Куратор", "category_name": "Админские", "position": 1},
        ]
        self.fake_db.tables["role_categories"] = [{"name": "Админские", "position": 0}]

        with self.assertLogs("bot.services.role_management_service", level="WARNING") as captured:
            grouped = RoleManagementService.list_roles_grouped(log_context="unit_test")

        self.assertEqual([role["name"] for role in grouped[0]["roles"]], ["Куратор"])
        self.assertTrue(
            any("filtered protected profile title from catalog" in message for message in captured.output),
            captured.output,
        )

    def test_create_role_denies_protected_profile_title(self):
        with self.assertLogs("bot.services.role_management_service", level="WARNING") as captured:
            ok = RoleManagementService.create_role("Глава клуба", "Админские", actor_id="42", source="test")

        self.assertFalse(ok)
        self.assertEqual(self.fake_db.tables["roles"], [])
        self.assertTrue(
            any("create_role denied protected profile title" in message for message in captured.output),
            captured.output,
        )

    def test_create_role_result_denies_name_that_conflicts_with_active_profile_title(self):
        self.fake_db.tables["profile_title_roles"] = [
            {"discord_role_id": "777", "title_name": "Легенда района", "is_active": True},
            {"discord_role_id": "888", "title_name": "Легенда района", "is_active": False},
        ]

        with self.assertLogs("bot.services.role_management_service", level="INFO") as captured:
            result = RoleManagementService.create_role_result(
                "Легенда района",
                "Каталог",
                actor_id="42",
                actor_provider="telegram",
                actor_user_id="42",
                source="telegram_command",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "profile_title_conflict")
        self.assertEqual(result["message"], ROLE_NAME_CONFLICT_PROFILE_TITLE_MESSAGE)
        self.assertEqual(result["discord_role_id"], "777")
        self.assertEqual(self.fake_db.tables["roles"], [])
        self.assertTrue(
            any("create_role denied active profile title conflict" in message and "actor_provider=telegram" in message and "source=telegram_command" in message for message in captured.output),
            captured.output,
        )
        audit_row = next(row for row in self.fake_db.tables["role_change_audit"] if row["action"] == "role_create_denied")
        self.assertEqual(audit_row["error_code"], "profile_title_conflict")
        self.assertEqual(audit_row["error_message"], ROLE_NAME_CONFLICT_PROFILE_TITLE_MESSAGE)

    def test_create_role_result_logs_validation_pass_before_upsert(self):
        with self.assertLogs("bot.services.role_management_service", level="INFO") as captured:
            result = RoleManagementService.create_role_result(
                "Gamma",
                "General",
                description="Описание",
                actor_id="42",
                actor_provider="discord",
                actor_user_id="42",
                source="discord_command",
            )

        self.assertTrue(result["ok"])
        self.assertTrue(
            any("create_role validated profile title uniqueness" in message and "actor_provider=discord" in message and "source=discord_command" in message for message in captured.output),
            captured.output,
        )

    def test_assign_user_role_by_account_denies_protected_profile_title(self):
        with self.assertLogs("bot.services.role_management_service", level="WARNING") as captured:
            result = RoleManagementService.assign_user_role_by_account(
                "acc-1",
                "Глава клуба",
                actor_provider="discord",
                actor_user_id="42",
                target_provider="discord",
                target_user_id="77",
                source="test",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "protected_profile_title")
        self.assertEqual(result["message"], PROTECTED_PROFILE_TITLE_ROLE_MESSAGE)
        self.assertEqual(self.fake_db.tables["account_role_assignments"], [])
        self.assertTrue(
            any("assign_user_role_by_account denied protected profile title" in message for message in captured.output),
            captured.output,
        )
        audit_row = next(row for row in self.fake_db.tables["role_change_audit"] if row["action"] == "role_grant_denied")
        self.assertEqual(audit_row["error_code"], "protected_profile_title")

    def test_revoke_user_role_by_account_denies_protected_profile_title(self):
        self.fake_db.tables["account_role_assignments"] = [{"account_id": "acc-1", "role_name": "Глава клуба", "source": "custom"}]

        with self.assertLogs("bot.services.role_management_service", level="WARNING") as captured:
            result = RoleManagementService.revoke_user_role_by_account(
                "acc-1",
                "Глава клуба",
                actor_provider="discord",
                actor_user_id="42",
                target_provider="discord",
                target_user_id="77",
                source="test",
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "protected_profile_title")
        self.assertEqual(len(self.fake_db.tables["account_role_assignments"]), 1)
        self.assertTrue(
            any("revoke_user_role_by_account denied protected profile title" in message for message in captured.output),
            captured.output,
        )
        audit_row = next(row for row in self.fake_db.tables["role_change_audit"] if row["action"] == "role_revoke_denied")
        self.assertEqual(audit_row["error_code"], "protected_profile_title")


if __name__ == "__main__":
    unittest.main()
