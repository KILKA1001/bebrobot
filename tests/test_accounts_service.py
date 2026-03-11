from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

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
        }
        self.account_seq = 0
        self.supabase = _FakeSupabase(self)
        self.metrics = []

    def _inc_metric(self, name):
        self.metrics.append(name)


class AccountsServiceTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = _FakeDb()
        self.patcher = patch("bot.services.accounts_service.db", self.fake_db)
        self.patcher.start()
        AccountsService._account_titles_cache = {}
        AccountsService._title_roles_cache = None

    def tearDown(self):
        self.patcher.stop()

    def test_register_creates_account_and_identity(self):
        ok, message = AccountsService.register_identity("discord", "111")
        self.assertTrue(ok)
        self.assertEqual(message, "Регистрация завершена")
        self.assertEqual(len(self.fake_db.tables["accounts"]), 1)
        self.assertEqual(len(self.fake_db.tables["account_identities"]), 1)

    def test_discord_to_telegram_link_flow(self):
        AccountsService.register_identity("discord", "111")

        ok, code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)

        ok, message = AccountsService.consume_telegram_link_code(222, code)
        self.assertTrue(ok)
        self.assertEqual(message, "Аккаунт успешно привязан")
        self.assertTrue(self.fake_db.tables["account_link_codes"][0]["is_used"])

        discord_account = AccountsService.resolve_account_id("discord", "111")
        telegram_account = AccountsService.resolve_account_id("telegram", "222")
        self.assertEqual(discord_account, telegram_account)

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


    def test_consume_link_code_rejects_when_target_identity_belongs_to_another_account(self):
        AccountsService.register_identity("discord", "111")
        AccountsService.register_identity("telegram", "222")

        ok, code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)

        ok, message = AccountsService.consume_telegram_link_code(222, code)
        self.assertFalse(ok)
        self.assertEqual(message, "Этот профиль уже привязан к другому общему аккаунту. Сначала отвяжите его через администратора")

        discord_account = AccountsService.resolve_account_id("discord", "111")
        telegram_account = AccountsService.resolve_account_id("telegram", "222")
        self.assertNotEqual(discord_account, telegram_account)

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
