import unittest
from unittest.mock import patch

from bot.services.external_roles_sync_service import ExternalRolesSyncService


class _FakeDb:
    supabase = object()


class _BotWithoutGuilds:
    pass


class ExternalRolesSyncServiceTests(unittest.TestCase):
    def test_collect_discord_roles_handles_bot_without_guilds(self):
        bot = _BotWithoutGuilds()
        with self.assertLogs("bot.services.external_roles_sync_service", level="ERROR") as logs:
            result = ExternalRolesSyncService._collect_discord_roles(bot)

        self.assertEqual(result, {})
        self.assertTrue(any("has no guilds attribute" in message for message in logs.output))

    def test_sync_account_by_account_id_returns_false_without_guilds(self):
        class _Resp:
            def __init__(self, data):
                self.data = data

        class _TableOp:
            def select(self, _fields):
                return self

            def eq(self, _key, _value):
                return self

            def limit(self, _n):
                return self

            def execute(self):
                return _Resp([
                    {
                        "account_id": "acc-1",
                        "discord_user_id": "111",
                        "telegram_user_id": "222",
                    }
                ])

        class _Supabase:
            def table(self, _name):
                return _TableOp()

        fake_db = _FakeDb()
        fake_db.supabase = _Supabase()

        with patch("bot.services.external_roles_sync_service.db", fake_db):
            changed = ExternalRolesSyncService.sync_account_by_account_id(_BotWithoutGuilds(), "acc-1")

        self.assertFalse(changed)


if __name__ == "__main__":
    unittest.main()
