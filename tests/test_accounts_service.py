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

    def select(self, _fields):
        return self

    def eq(self, key, value):
        self._filters.append((key, value))
        return self

    def limit(self, n):
        self._limit = n
        return self

    def insert(self, payload):
        self._payload = payload
        self._action = "insert"
        return self

    def upsert(self, payload):
        self._payload = payload
        self._action = "upsert"
        return self

    def update(self, payload):
        self._payload = payload
        self._action = "update"
        return self

    def execute(self):
        rows = self.fake_db.tables[self.table_name]

        if getattr(self, "_action", None) == "select":
            pass

        if getattr(self, "_action", None) == "insert":
            rows.append(dict(self._payload))
            return _Resp([dict(self._payload)])

        if getattr(self, "_action", None) == "upsert":
            if self.table_name == "account_identities":
                key = (self._payload["provider"], self._payload["provider_user_id"])
                for row in rows:
                    if (row["provider"], row["provider_user_id"]) == key:
                        row.update(self._payload)
                        return _Resp([dict(row)])
            rows.append(dict(self._payload))
            return _Resp([dict(self._payload)])

        if getattr(self, "_action", None) == "update":
            matched = []
            for row in rows:
                if all(str(row.get(k)) == str(v) for k, v in self._filters):
                    row.update(self._payload)
                    matched.append(dict(row))
            return _Resp(matched)

        selected = []
        for row in rows:
            if all(str(row.get(k)) == str(v) for k, v in self._filters):
                selected.append(dict(row))
        if self._limit is not None:
            selected = selected[: self._limit]
        return _Resp(selected)


class _FakeSupabase:
    def __init__(self):
        self.tables = {
            "account_identities": [],
            "account_link_codes": [],
        }

    def table(self, name):
        op = _TableOp(self, name)
        op._action = "select"
        return op


class _FakeDb:
    def __init__(self):
        self.supabase = _FakeSupabase()
        self.metrics = []

    def _inc_metric(self, name):
        self.metrics.append(name)


class AccountsServiceTests(unittest.TestCase):
    def setUp(self):
        self.fake_db = _FakeDb()
        self.patcher = patch("bot.services.accounts_service.db", self.fake_db)
        self.patcher.start()

        self.fake_db.supabase.tables["account_identities"].append(
            {"account_id": "acc-discord-1", "provider": "discord", "provider_user_id": "111"}
        )

    def tearDown(self):
        self.patcher.stop()

    def test_link_flow_valid_code(self):
        ok, code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)

        ok, message = AccountsService.consume_telegram_link_code(222, code)
        self.assertTrue(ok)
        self.assertEqual(message, "Аккаунт успешно привязан")

        telegram_identity = AccountsService.resolve_telegram_account_id(222)
        self.assertEqual(telegram_identity, "acc-discord-1")

    def test_link_flow_expired_code(self):
        now = datetime.now(timezone.utc)
        self.fake_db.supabase.tables["account_link_codes"].append(
            {
                "code": "EXPIRED1",
                "account_id": "acc-discord-1",
                "expires_at": (now - timedelta(minutes=1)).isoformat(),
                "is_used": False,
                "attempts": 0,
            }
        )

        ok, message = AccountsService.consume_telegram_link_code(222, "EXPIRED1")
        self.assertFalse(ok)
        self.assertEqual(message, "Срок действия кода истёк")

    def test_link_flow_used_code(self):
        now = datetime.now(timezone.utc)
        self.fake_db.supabase.tables["account_link_codes"].append(
            {
                "code": "USEDCODE",
                "account_id": "acc-discord-1",
                "expires_at": (now + timedelta(minutes=5)).isoformat(),
                "is_used": True,
                "attempts": 1,
            }
        )

        ok, message = AccountsService.consume_telegram_link_code(222, "USEDCODE")
        self.assertFalse(ok)
        self.assertEqual(message, "Код уже использован")

    def test_link_flow_relink_overwrites_telegram_identity(self):
        self.fake_db.supabase.tables["account_identities"].append(
            {"account_id": "acc-old", "provider": "telegram", "provider_user_id": "222"}
        )
        ok, code = AccountsService.issue_discord_telegram_link_code(111)
        self.assertTrue(ok)

        ok, message = AccountsService.consume_telegram_link_code(222, code)
        self.assertTrue(ok)
        self.assertEqual(message, "Аккаунт успешно привязан")

        telegram_identity = AccountsService.resolve_telegram_account_id(222)
        self.assertEqual(telegram_identity, "acc-discord-1")

    def test_resolve_account_id_by_discord_and_telegram(self):
        self.fake_db.supabase.tables["account_identities"].extend(
            [
                {"account_id": "acc-discord-1", "provider": "telegram", "provider_user_id": "333"},
                {"account_id": "acc-discord-1", "provider": "discord", "provider_user_id": "444"},
            ]
        )

        self.assertEqual(AccountsService.resolve_account_id("discord", "444"), "acc-discord-1")
        self.assertEqual(AccountsService.resolve_telegram_account_id(333), "acc-discord-1")


if __name__ == "__main__":
    unittest.main()
