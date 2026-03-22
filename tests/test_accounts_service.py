from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

from bot.services.auth.role_resolver import ResolvedAccess

from bot.services.accounts_service import AccountsService


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

    def is_(self, key, value):
        self._filters.append((key, None if str(value).lower() == "null" else value))
        return self

    def insert(self, payload, **_kwargs):
        self._payload = payload
        self._action = "insert"
        return self

    def upsert(self, payload, **_kwargs):
        self._payload = payload
        self._action = "upsert"
        return self

    def update(self, payload):
        self._payload = payload
        self._action = "update"
        return self

    def delete(self):
        self._action = "delete"
        return self

    def execute(self):
        rows = self.fake_db.tables[self.table_name]
        self.fake_db.operations.append(
            {
                "table": self.table_name,
                "action": self._action,
                "filters": list(self._filters),
            }
        )

        if self._action == "insert":
            if self.table_name == "accounts" and "id" not in self._payload:
                self.fake_db.account_seq += 1
                payload = {"id": f"acc-{self.fake_db.account_seq}"}
            else:
                payload = dict(self._payload)
            rows.append(dict(payload))
            return _Resp([dict(payload)])

        if self._action == "upsert":
            if self.table_name == "account_identities":
                key = (self._payload["provider"], self._payload["provider_user_id"])
                for row in rows:
                    if (row["provider"], row["provider_user_id"]) == key:
                        row.update(self._payload)
                        return _Resp([dict(row)])
            if self.table_name == "scores" and self._payload.get("account_id"):
                key = str(self._payload.get("account_id"))
                for row in rows:
                    if str(row.get("account_id")) == key:
                        row.update(self._payload)
                        return _Resp([dict(row)])
            if self.table_name == "account_links_registry" and self._payload.get("account_id"):
                key = str(self._payload.get("account_id"))
                payload_telegram = self._payload.get("telegram_user_id")
                payload_discord = self._payload.get("discord_user_id")
                for row in rows:
                    if str(row.get("account_id")) == key:
                        row.update(self._payload)
                        for other in rows:
                            if str(other.get("account_id")) == key:
                                continue
                            if payload_telegram and str(other.get("telegram_user_id")) == str(payload_telegram):
                                raise Exception("duplicate key value violates unique constraint telegram_user_id")
                            if payload_discord and str(other.get("discord_user_id")) == str(payload_discord):
                                raise Exception("duplicate key value violates unique constraint discord_user_id")
                        return _Resp([dict(row)])
                for other in rows:
                    if payload_telegram and str(other.get("telegram_user_id")) == str(payload_telegram):
                        raise Exception("duplicate key value violates unique constraint telegram_user_id")
                    if payload_discord and str(other.get("discord_user_id")) == str(payload_discord):
                        raise Exception("duplicate key value violates unique constraint discord_user_id")
            rows.append(dict(self._payload))
            return _Resp([dict(self._payload)])

        if self._action == "update":
            matched = []
            for row in rows:
                if all(str(row.get(k)) == str(v) for k, v in self._filters):
                    row.update(self._payload)
                    matched.append(dict(row))
            return _Resp(matched)

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


class _StrictIdentityTableOp(_TableOp):
    def execute(self):
        if self.table_name == "account_identities" and self._action == "upsert":
            key = (self._payload["provider"], self._payload["provider_user_id"])
            existing = next(
                (
                    row
                    for row in self.fake_db.tables[self.table_name]
                    if (row.get("provider"), row.get("provider_user_id")) == key
                ),
                None,
            )
            if existing is None and not self._payload.get("account_id"):
                error = Exception('null value in column "account_id" of relation "account_identities" violates not-null constraint')
                setattr(error, "code", "23502")
                raise error
        return super().execute()


class _StrictIdentitySupabase(_FakeSupabase):
    def table(self, name):
        if name == "account_identities":
            return _StrictIdentityTableOp(self.fake_db, name)
        return _TableOp(self.fake_db, name)


class _FakeDb:
    def __init__(self):
        self.tables = {
            "accounts": [],
            "account_identities": [],
            "account_link_codes": [],
            "link_tokens": [],
            "scores": [],
            "actions": [],
            "profile_title_roles": [],
            "account_links_registry": [],
            "account_role_assignments": [],
            "roles": [],
            "role_permissions": [],
        }
        self.account_seq = 0
        self.supabase = _FakeSupabase(self)
        self.metrics = []
        self.operations = []

    def _inc_metric(self, name):
        self.metrics.append(name)


class AccountsServiceTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = _FakeDb()
        self.patcher = patch("bot.services.accounts_service.db", self.fake_db)
        self.patcher.start()
        AccountsService._account_titles_cache = {}
        AccountsService._account_id_cache = {}
        AccountsService._title_roles_cache = None

    def tearDown(self):
        self.patcher.stop()

    def test_find_accounts_by_identity_username_matches_telegram_with_or_without_at(self):
        self.fake_db.tables["account_identities"] = [
            {
                "account_id": "acc-1",
                "provider": "telegram",
                "provider_user_id": "222",
                "username": "bebra_admin",
                "display_name": "Bebra Admin",
            }
        ]

        direct = AccountsService.find_accounts_by_identity_username("telegram", "@bebra_admin")
        plain = AccountsService.find_accounts_by_identity_username("telegram", "bebra_admin")

        self.assertEqual(len(direct), 1)
        self.assertEqual(direct[0]["provider_user_id"], "222")
        self.assertEqual(plain[0]["provider_user_id"], "222")

    def test_get_public_identity_context_prefers_custom_nick_over_identity_fields(self):
        self.fake_db.tables["accounts"] = [{"id": "acc-1", "custom_nick": "Капитан Бебра"}]
        self.fake_db.tables["account_identities"] = [
            {
                "account_id": "acc-1",
                "provider": "telegram",
                "provider_user_id": "222",
                "username": "captain_bebra",
                "display_name": "Captain Bebra",
                "global_username": "captain.global",
            }
        ]

        context = AccountsService.get_public_identity_context("telegram", "222")

        self.assertEqual(context["account_id"], "acc-1")
        self.assertEqual(context["custom_nick"], "Капитан Бебра")
        self.assertEqual(context["display_name"], "Captain Bebra")
        self.assertEqual(context["username"], "captain_bebra")
        self.assertEqual(context["global_username"], "captain.global")
        self.assertEqual(context["best_public_name"], "Капитан Бебра")
        self.assertEqual(context["name_source"], "custom_nick")

    def test_get_public_identity_context_falls_back_to_display_name(self):
        self.fake_db.tables["accounts"] = [{"id": "acc-2", "custom_nick": "Игрок"}]
        self.fake_db.tables["account_identities"] = [
            {
                "account_id": "acc-2",
                "provider": "discord",
                "provider_user_id": "333",
                "username": "bebr_user",
                "display_name": "Обычный пользователь",
                "global_username": "bebr.global",
            }
        ]

        context = AccountsService.get_public_identity_context("discord", "333")

        self.assertEqual(context["best_public_name"], "Обычный пользователь")
        self.assertEqual(context["name_source"], "display_name")

    def test_find_accounts_by_identity_username_returns_multiple_candidates(self):
        self.fake_db.tables["account_identities"] = [
            {"account_id": "acc-1", "provider": "telegram", "provider_user_id": "222", "username": "bebra_admin"},
            {"account_id": "acc-2", "provider": "telegram", "provider_user_id": "333", "username": "bebra_admin"},
        ]

        matches = AccountsService.find_accounts_by_identity_username("telegram", "@bebra_admin")

        self.assertEqual(len(matches), 2)
        self.assertEqual({item["provider_user_id"] for item in matches}, {"222", "333"})

    def test_find_accounts_by_identity_username_returns_empty_for_unknown_username(self):
        self.fake_db.tables["account_identities"] = [
            {"account_id": "acc-1", "provider": "telegram", "provider_user_id": "222", "username": "bebra_admin"}
        ]

        matches = AccountsService.find_accounts_by_identity_username("telegram", "@missing_user")

        self.assertEqual(matches, [])

    def test_resolve_user_lookup_prefers_default_provider_but_allows_cross_provider_prefix(self):
        self.fake_db.tables["account_identities"] = [
            {
                "account_id": "acc-1",
                "provider": "telegram",
                "provider_user_id": "222",
                "username": "tg_user",
                "display_name": "Telegram User",
            },
            {
                "account_id": "acc-2",
                "provider": "discord",
                "provider_user_id": "333",
                "username": "ds_user",
                "display_name": "Discord User",
            },
        ]

        telegram_lookup = AccountsService.resolve_user_lookup("@tg_user", default_provider="telegram")
        discord_lookup = AccountsService.resolve_user_lookup("ds:ds_user", default_provider="telegram")

        self.assertEqual(telegram_lookup["status"], "ok")
        self.assertEqual(telegram_lookup["result"]["provider"], "telegram")
        self.assertEqual(telegram_lookup["result"]["provider_user_id"], "222")
        self.assertEqual(discord_lookup["status"], "ok")
        self.assertEqual(discord_lookup["result"]["provider"], "discord")
        self.assertEqual(discord_lookup["result"]["provider_user_id"], "333")

    def test_resolve_user_lookup_supports_account_id_fallback(self):
        account_id = "123e4567-e89b-42d3-a456-426614174000"
        self.fake_db.tables["account_identities"] = [
            {"account_id": account_id, "provider": "telegram", "provider_user_id": "222", "username": "tg_user"},
            {"account_id": account_id, "provider": "discord", "provider_user_id": "333", "username": "ds_user"},
        ]

        lookup = AccountsService.resolve_user_lookup(account_id, default_provider="discord")

        self.assertEqual(lookup["status"], "ok")
        self.assertEqual(lookup["result"]["account_id"], account_id)
        self.assertEqual(lookup["result"]["provider"], "discord")
        self.assertEqual(lookup["result"]["provider_user_id"], "333")

    def test_persist_identity_lookup_fields_updates_existing_identity(self):
        self.fake_db.tables["account_identities"] = [
            {"account_id": "acc-1", "provider": "discord", "provider_user_id": "111"}
        ]

        AccountsService.persist_identity_lookup_fields(
            "discord",
            "111",
            username="bebrobot",
            display_name="Bebra Bot",
            global_username="bebra.global",
        )

        self.assertEqual(self.fake_db.tables["account_identities"][0]["username"], "bebrobot")
        self.assertEqual(self.fake_db.tables["account_identities"][0]["display_name"], "Bebra Bot")
        self.assertEqual(self.fake_db.tables["account_identities"][0]["global_username"], "bebra.global")

    def test_persist_identity_lookup_fields_skips_insert_when_account_id_is_required(self):
        self.fake_db.supabase = _StrictIdentitySupabase(self.fake_db)

        AccountsService.persist_identity_lookup_fields(
            "telegram",
            "222",
            username="lookup_only",
            display_name="Lookup Only",
        )

        self.assertEqual(self.fake_db.tables["account_identities"], [])

    def test_register_creates_account_and_identity(self):
        ok, message = AccountsService.register_identity("discord", "111")
        self.assertTrue(ok)
        self.assertEqual(message, "Регистрация завершена")
        self.assertEqual(len(self.fake_db.tables["accounts"]), 1)
        self.assertEqual(len(self.fake_db.tables["account_identities"]), 1)

    def test_register_repairs_legacy_identity_without_account_id(self):
        self.fake_db.tables["account_identities"].append(
            {"account_id": None, "provider": "telegram", "provider_user_id": "222", "username": "legacy_user"}
        )

        ok, message = AccountsService.register_identity("telegram", "222")

        self.assertTrue(ok)
        self.assertEqual(message, "Регистрация завершена")
        self.assertEqual(len(self.fake_db.tables["accounts"]), 1)
        self.assertEqual(len(self.fake_db.tables["account_identities"]), 1)
        self.assertEqual(self.fake_db.tables["account_identities"][0]["account_id"], self.fake_db.tables["accounts"][0]["id"])

    def test_register_repairs_identity_after_unique_conflict_when_lookup_row_exists(self):
        class _UniqueIdentityInsertTableOp(_TableOp):
            def execute(self):
                if self.table_name == "account_identities" and self._action == "insert":
                    key = (self._payload.get("provider"), self._payload.get("provider_user_id"))
                    for row in self.fake_db.tables[self.table_name]:
                        if (row.get("provider"), row.get("provider_user_id")) == key:
                            raise Exception("duplicate key value violates unique constraint account_identities_provider_user")
                return super().execute()

        class _UniqueIdentityInsertSupabase(_FakeSupabase):
            def table(self, name):
                if name == "account_identities":
                    return _UniqueIdentityInsertTableOp(self.fake_db, name)
                return _TableOp(self.fake_db, name)

        original_load_identity_row = AccountsService._load_identity_row
        load_calls = {"count": 0}

        def fake_load_identity_row(provider, provider_user_id):
            load_calls["count"] += 1
            row = original_load_identity_row(provider, provider_user_id)
            if load_calls["count"] == 1 and row and not row.get("account_id"):
                return None
            return row

        self.fake_db.supabase = _UniqueIdentityInsertSupabase(self.fake_db)
        self.fake_db.tables["account_identities"].append(
            {"account_id": None, "provider": "discord", "provider_user_id": "333", "display_name": "Legacy Discord"}
        )

        with patch.object(AccountsService, "_load_identity_row", side_effect=fake_load_identity_row):
            ok, message = AccountsService.register_identity("discord", "333")

        self.assertTrue(ok)
        self.assertEqual(message, "Регистрация завершена")
        self.assertEqual(len(self.fake_db.tables["accounts"]), 1)
        self.assertEqual(len(self.fake_db.tables["account_identities"]), 1)
        self.assertEqual(self.fake_db.tables["account_identities"][0]["account_id"], self.fake_db.tables["accounts"][0]["id"])

    def test_register_does_not_overwrite_account_id_when_lookup_row_becomes_bound_during_repair(self):
        class _ConcurrentBindTableOp(_TableOp):
            def execute(self):
                if self.table_name == "account_identities" and self._action == "update":
                    for key, value in self._filters:
                        if key == "account_id" and value is None:
                            for row in self.fake_db.tables[self.table_name]:
                                if (
                                    str(row.get("provider")) == "telegram"
                                    and str(row.get("provider_user_id")) == "444"
                                    and row.get("account_id") is None
                                ):
                                    row["account_id"] = "acc-existing"
                    return super().execute()
                return super().execute()

        class _ConcurrentBindSupabase(_FakeSupabase):
            def table(self, name):
                if name == "account_identities":
                    return _ConcurrentBindTableOp(self.fake_db, name)
                return _TableOp(self.fake_db, name)

        self.fake_db.supabase = _ConcurrentBindSupabase(self.fake_db)
        self.fake_db.tables["account_identities"].append(
            {"account_id": None, "provider": "telegram", "provider_user_id": "444", "display_name": "Race User"}
        )

        ok, message = AccountsService.register_identity("telegram", "444")

        self.assertTrue(ok)
        self.assertEqual(message, "Уже зарегистрирован")
        self.assertEqual(self.fake_db.tables["account_identities"][0]["account_id"], "acc-existing")
        self.assertEqual(len(self.fake_db.tables["accounts"]), 1)

    def test_discord_to_telegram_link_flow(self):
        AccountsService.register_identity("discord", "111")

        ok, code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)

        ok, message = AccountsService.consume_telegram_link_code(222, code)
        self.assertTrue(ok)
        self.assertEqual(message, "Аккаунт успешно привязан")
        self.assertTrue(self.fake_db.tables["account_link_codes"][0]["is_used"])
        self.assertEqual(self.fake_db.tables["account_links_registry"][0]["last_link_code_used"], code)
        self.assertTrue(self.fake_db.tables["account_links_registry"][0]["has_used_link_code"])

        discord_account = AccountsService.resolve_account_id("discord", "111")
        telegram_account = AccountsService.resolve_account_id("telegram", "222")
        self.assertEqual(discord_account, telegram_account)

    def test_link_flow_keeps_identity_and_linking_tables_usable(self):
        AccountsService.register_identity("discord", "111")

        ok, code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)
        ok, message = AccountsService.consume_telegram_link_code(222, code)
        self.assertTrue(ok)
        self.assertEqual(message, "Аккаунт успешно привязан")

        identity_rows = self.fake_db.tables["account_identities"]
        registry_row = self.fake_db.tables["account_links_registry"][0]
        code_row = self.fake_db.tables["account_link_codes"][0]

        self.assertEqual({row["provider"] for row in identity_rows}, {"discord", "telegram"})
        self.assertEqual({row["provider_user_id"] for row in identity_rows}, {"111", "222"})
        self.assertEqual(code_row["source_provider_user_id"], "111")
        self.assertEqual(code_row["used_by_provider_user_id"], "222")
        self.assertEqual(registry_row["account_id"], AccountsService.resolve_account_id("discord", "111"))
        self.assertEqual(registry_row["last_link_code_used"], code)
        self.assertTrue(registry_row["has_used_link_code"])

    def test_telegram_to_discord_link_flow(self):
        AccountsService.register_identity("telegram", "333")

        ok, code = AccountsService.issue_telegram_discord_link_code(333)
        self.assertTrue(ok)

        ok, message = AccountsService.consume_discord_link_code(444, code)
        self.assertTrue(ok)
        self.assertEqual(message, "Аккаунт успешно привязан")

        telegram_account = AccountsService.resolve_account_id("telegram", "333")
        discord_account = AccountsService.resolve_account_id("discord", "444")
        self.assertEqual(discord_account, telegram_account)

    def test_issue_link_code_reuses_active_code_instead_of_generating_new_one(self):
        AccountsService.register_identity("discord", "111")

        ok, first_code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)
        self.assertEqual(len(self.fake_db.tables["account_link_codes"]), 1)

        ok, second_code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)
        self.assertEqual(first_code, second_code)
        self.assertEqual(len(self.fake_db.tables["account_link_codes"]), 1)

    def test_issue_link_code_fails_when_target_provider_already_linked(self):
        AccountsService.register_identity("discord", "111")
        ok, code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)
        self.assertTrue(code)
        AccountsService.consume_telegram_link_code(222, code)

        ok, message = AccountsService.issue_discord_telegram_link_code(111)
        self.assertFalse(ok)
        self.assertEqual(message, "Аккаунт уже привязан к telegram")

    def test_issue_link_code_uses_registry_table_for_target_link_check(self):
        AccountsService.register_identity("discord", "111")
        account_id = AccountsService.resolve_account_id("discord", "111")
        self.assertIsNotNone(account_id)
        self.fake_db.tables["account_links_registry"].append(
            {
                "account_id": account_id,
                "telegram_user_id": "222",
                "discord_user_id": "111",
            }
        )

        ok, message = AccountsService.issue_discord_telegram_link_code(111)
        self.assertFalse(ok)
        self.assertEqual(message, "Аккаунт уже привязан к telegram")

    def test_issue_link_code_does_not_block_only_by_used_flag_without_target_binding(self):
        AccountsService.register_identity("discord", "111")
        account_id = AccountsService.resolve_account_id("discord", "111")
        self.assertIsNotNone(account_id)
        self.fake_db.tables["account_links_registry"].append(
            {
                "account_id": account_id,
                "telegram_user_id": None,
                "discord_user_id": "111",
                "has_used_link_code": True,
            }
        )

        ok, code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)
        self.assertTrue(code)

    def test_link_flow_expired_code(self):
        AccountsService.register_identity("discord", "111")
        now = datetime.now(timezone.utc)
        self.fake_db.tables["account_link_codes"].append(
            {
                "code": "EXPIRED1",
                "account_id": "acc-1",
                "target_provider": "telegram",
                "expires_at": (now - timedelta(minutes=1)).isoformat(),
                "is_used": False,
                "attempts": 0,
            }
        )

        ok, message = AccountsService.consume_telegram_link_code(222, "EXPIRED1")
        self.assertFalse(ok)
        self.assertEqual(message, "Срок действия кода истёк")


    def test_consume_link_code_merges_when_target_identity_belongs_to_another_account(self):
        AccountsService.register_identity("discord", "111")
        AccountsService.register_identity("telegram", "222")

        ok, code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)

        ok, message = AccountsService.consume_telegram_link_code(222, code)
        self.assertTrue(ok)
        self.assertEqual(message, "Аккаунт успешно привязан")

        discord_account = AccountsService.resolve_account_id("discord", "111")
        telegram_account = AccountsService.resolve_account_id("telegram", "222")
        self.assertEqual(discord_account, telegram_account)



    def test_merge_registry_rows_deletes_source_before_upsert_to_avoid_unique_conflict(self):
        self.fake_db.tables["account_links_registry"].extend(
            [
                {"account_id": "acc-from", "telegram_user_id": "222", "discord_user_id": None},
                {"account_id": "acc-to", "telegram_user_id": None, "discord_user_id": "111"},
            ]
        )

        AccountsService._merge_registry_rows_for_accounts("acc-from", "acc-to")

        self.assertEqual(
            [row for row in self.fake_db.tables["account_links_registry"] if row.get("account_id") == "acc-from"],
            [],
        )
        target_rows = [row for row in self.fake_db.tables["account_links_registry"] if row.get("account_id") == "acc-to"]
        self.assertEqual(len(target_rows), 1)
        self.assertEqual(target_rows[0].get("telegram_user_id"), "222")
        self.assertEqual(target_rows[0].get("discord_user_id"), "111")

    def test_merge_accounts_merges_registry_rows_before_rebind(self):
        self.fake_db.tables["account_links_registry"].extend(
            [
                {
                    "account_id": "acc-from",
                    "telegram_user_id": "222",
                    "discord_user_id": None,
                    "has_used_link_code": True,
                    "last_link_code_used": "ABC12345",
                },
                {
                    "account_id": "acc-to",
                    "telegram_user_id": None,
                    "discord_user_id": "111",
                    "has_used_link_code": False,
                },
            ]
        )

        AccountsService._merge_accounts("acc-from", "acc-to")

        rows = [row for row in self.fake_db.tables["account_links_registry"] if row.get("account_id") == "acc-to"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("telegram_user_id"), "222")
        self.assertEqual(rows[0].get("discord_user_id"), "111")
        self.assertTrue(rows[0].get("has_used_link_code"))
        self.assertEqual(rows[0].get("last_link_code_used"), "ABC12345")

        source_rows = [row for row in self.fake_db.tables["account_links_registry"] if row.get("account_id") == "acc-from"]
        self.assertEqual(source_rows, [])

    def test_merge_scores_between_accounts_sums_without_user_id_column(self):
        self.fake_db.tables["account_identities"].extend(
            [
                {"account_id": "acc-a", "provider": "discord", "provider_user_id": "111"},
                {"account_id": "acc-b", "provider": "telegram", "provider_user_id": "222"},
            ]
        )
        self.fake_db.tables["scores"].extend(
            [
                {"account_id": "acc-a", "points": 7, "tickets_normal": 1, "tickets_gold": 2},
                {"account_id": "acc-b", "points": 3, "tickets_normal": 2, "tickets_gold": 1},
            ]
        )

        AccountsService._merge_scores_between_accounts("acc-b", "acc-a")

        merged_rows = [row for row in self.fake_db.tables["scores"] if row.get("account_id") == "acc-a"]
        self.assertEqual(len(merged_rows), 1)
        self.assertEqual(float(merged_rows[0].get("points")), 10)
        self.assertEqual(int(merged_rows[0].get("tickets_normal")), 3)
        self.assertEqual(int(merged_rows[0].get("tickets_gold")), 3)

    def test_rebind_account_identities_drops_duplicates_in_target_account(self):
        self.fake_db.tables["account_identities"].extend(
            [
                {"account_id": "acc-a", "provider": "discord", "provider_user_id": "111"},
                {"account_id": "acc-b", "provider": "discord", "provider_user_id": "111"},
                {"account_id": "acc-b", "provider": "telegram", "provider_user_id": "222"},
            ]
        )

        AccountsService._rebind_account_identities("acc-b", "acc-a")

        rows = self.fake_db.tables["account_identities"]
        discord_rows = [r for r in rows if r.get("provider") == "discord" and str(r.get("provider_user_id")) == "111"]
        telegram_rows = [r for r in rows if r.get("provider") == "telegram" and str(r.get("provider_user_id")) == "222"]

        self.assertEqual(len(discord_rows), 1)
        self.assertEqual(str(discord_rows[0].get("account_id")), "acc-a")
        self.assertEqual(len(telegram_rows), 1)
        self.assertEqual(str(telegram_rows[0].get("account_id")), "acc-a")

    def test_consume_link_code_cross_account_merge_sums_scores_and_tickets(self):
        AccountsService.register_identity("discord", "111")
        AccountsService.register_identity("telegram", "222")

        discord_account = AccountsService.resolve_account_id("discord", "111")
        telegram_account = AccountsService.resolve_account_id("telegram", "222")
        self.assertIsNotNone(discord_account)
        self.assertIsNotNone(telegram_account)

        self.fake_db.tables["scores"].append(
            {"account_id": discord_account, "user_id": "111", "points": 10, "tickets_normal": 1, "tickets_gold": 2}
        )
        self.fake_db.tables["scores"].append(
            {"account_id": telegram_account, "user_id": "222", "points": 5.5, "tickets_normal": 3, "tickets_gold": 4}
        )

        ok, code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)

        ok, message = AccountsService.consume_telegram_link_code(222, code)
        self.assertTrue(ok)
        self.assertEqual(message, "Аккаунт успешно привязан")

        final_account = AccountsService.resolve_account_id("telegram", "222")
        self.assertEqual(final_account, discord_account)

        score_rows = [row for row in self.fake_db.tables["scores"] if row.get("account_id") == discord_account]
        self.assertGreaterEqual(len(score_rows), 1)
        merged_points = sum(float(row.get("points", 0) or 0) for row in score_rows)
        merged_tickets_normal = sum(int(row.get("tickets_normal", 0) or 0) for row in score_rows)
        merged_tickets_gold = sum(int(row.get("tickets_gold", 0) or 0) for row in score_rows)
        self.assertEqual(merged_points, 15.5)
        self.assertEqual(merged_tickets_normal, 4)
        self.assertEqual(merged_tickets_gold, 6)

        old_rows = [row for row in self.fake_db.tables["scores"] if row.get("account_id") == telegram_account]
        self.assertEqual(len(old_rows), 0)

    def test_profile_contains_link_status(self):
        AccountsService.register_identity("discord", "111")
        self.fake_db.tables["scores"].append({"user_id": "111", "points": 125})
        profile = AccountsService.get_profile("discord", "111", "Nick")
        self.assertIsNotNone(profile)
        self.assertEqual(profile["link_status"], "Не привязан")
        self.assertEqual(profile["nulls_brawl_id"], "—")
        self.assertEqual(profile["points"], "125")
        self.assertEqual(
            profile["titles_text"],
            "Привяжите Discord и/или подтвердите скрином свое звание администрации клуба для получения звания (временно)",
        )

    def test_save_and_read_account_titles(self):
        AccountsService.register_identity("discord", "111")
        account_id = AccountsService.resolve_account_id("discord", "111")
        self.assertIsNotNone(account_id)

        ok = AccountsService.save_account_titles(account_id, ["Глава клуба", "Главный вице"], source="discord")
        self.assertTrue(ok)

        profile = AccountsService.get_profile("discord", "111", "Nick")
        self.assertIsNotNone(profile)
        self.assertEqual(profile["titles"], ["Глава клуба", "Главный вице"])
        self.assertEqual(profile["titles_text"], "Глава клуба, Главный вице")

    def test_resolve_account_id_uses_ttl_cache_for_repeat_lookup(self):
        AccountsService.register_identity("discord", "111")
        self.fake_db.operations.clear()

        with patch("bot.services.accounts_service.time.monotonic", side_effect=[100.0, 101.0]):
            first = AccountsService.resolve_account_id("discord", "111")
            second = AccountsService.resolve_account_id("discord", "111")

        self.assertEqual(first, second)
        self.assertEqual(first, "acc-1")
        select_operations = [
            op for op in self.fake_db.operations
            if op["table"] == "account_identities" and op["action"] == "select"
        ]
        self.assertEqual(len(select_operations), 1)

    def test_resolve_account_id_refreshes_cache_after_ttl_expiry(self):
        AccountsService.register_identity("discord", "111")
        self.fake_db.operations.clear()

        with patch("bot.services.accounts_service.time.monotonic", side_effect=[100.0, 401.0, 401.0]):
            first = AccountsService.resolve_account_id("discord", "111")
            second = AccountsService.resolve_account_id("discord", "111")

        self.assertEqual(first, "acc-1")
        self.assertEqual(second, "acc-1")
        select_operations = [
            op for op in self.fake_db.operations
            if op["table"] == "account_identities" and op["action"] == "select"
        ]
        self.assertEqual(len(select_operations), 2)


    def test_profile_contains_resolved_roles_payload(self):
        AccountsService.register_identity("discord", "111")
        account_id = AccountsService.resolve_account_id("discord", "111")
        self.assertIsNotNone(account_id)

        self.fake_db.tables["account_role_assignments"].append(
            {
                "account_id": account_id,
                "role_name": "moderator",
                "source": "discord",
                "origin_label": "Discord role",
                "synced_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        self.fake_db.tables["roles"].append({"name": "moderator"})
        self.fake_db.tables["role_permissions"].append(
            {"role_name": "moderator", "permission_name": "tickets.manage", "effect": "allow"}
        )

        with patch(
            "bot.services.accounts_service.RoleResolver.resolve_for_account",
            return_value=ResolvedAccess(
                roles=[
                    {
                        "name": "moderator",
                        "source": "discord",
                        "origin_label": "Discord role",
                        "synced_at": datetime.now(timezone.utc).isoformat(),
                    }
                ],
                permissions={"allow": ["tickets.manage"], "deny": []},
            ),
        ):
            profile = AccountsService.get_profile("discord", "111", "Nick")
        self.assertIsNotNone(profile)
        self.assertEqual(profile["roles"][0]["name"], "moderator")
        self.assertEqual(profile["roles"][0]["source"], "discord")
        self.assertEqual(profile["permissions"]["allow"], ["tickets.manage"])

    def test_get_configured_title_roles_from_db(self):
        self.fake_db.tables["profile_title_roles"].extend(
            [
                {"discord_role_id": "101", "title_name": "Глава клуба", "is_active": True},
                {"discord_role_id": 102, "title_name": "Главный вице", "is_active": True},
                {"discord_role_id": 103, "title_name": "", "is_active": True},
                {"discord_role_id": "bad", "title_name": "Ветеран города", "is_active": True},
                {"discord_role_id": 104, "title_name": "Участник клубов", "is_active": False},
            ]
        )

        AccountsService._title_roles_cache = None
        mapping = AccountsService.get_configured_title_roles()

        self.assertEqual(mapping, {101: "Глава клуба", 102: "Главный вице"})

    def test_profile_points_for_telegram_profile_reads_account_actions(self):
        AccountsService.register_identity("telegram", "222")
        account_id = AccountsService.resolve_account_id("telegram", "222")
        self.assertIsNotNone(account_id)
        self.fake_db.tables["actions"].extend(
            [
                {"account_id": account_id, "points": 10},
                {"account_id": account_id, "points": -3.5},
            ]
        )

        profile = AccountsService.get_profile("telegram", "222", "Nick")
        self.assertIsNotNone(profile)
        self.assertEqual(profile["points"], "6.5")

    def test_link_flow_with_legacy_link_tokens_table(self):
        AccountsService.register_identity("discord", "111")
        self.fake_db.tables.pop("account_link_codes", None)

        ok, code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)
        self.assertEqual(len(self.fake_db.tables["link_tokens"]), 1)

        ok, message = AccountsService.consume_telegram_link_code(222, code)
        self.assertTrue(ok)
        self.assertEqual(message, "Аккаунт успешно привязан")


if __name__ == "__main__":
    unittest.main()
