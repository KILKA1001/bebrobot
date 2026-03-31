"""
Назначение: модуль "test guiy publish destinations service" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bot.services.guiy_publish_destinations_service import GuiyPublishDestinationsService


class _FakeDiscordChannel:
    def __init__(self, channel_id, name, *, can_view=True, can_send=True, guild=None):
        self.id = channel_id
        self.name = name
        self.guild = guild
        self._permissions = SimpleNamespace(view_channel=can_view, send_messages=can_send, send_messages_in_threads=can_send)

    def permissions_for(self, _member):
        return self._permissions


class _FakeDiscordGuild:
    def __init__(self, guild_id, name, channels, *, me=True):
        self.id = guild_id
        self.name = name
        self.text_channels = channels
        self.threads = []
        self.me = SimpleNamespace(id=999) if me else None
        for channel in channels:
            channel.guild = self

    def get_member(self, _user_id):
        return self.me

    def get_channel(self, channel_id):
        for channel in self.text_channels:
            if channel.id == channel_id:
                return channel
        return None


class GuiyPublishDestinationsServiceTests(unittest.TestCase):
    def test_list_discord_destinations_returns_only_writable_channels(self):
        writable = _FakeDiscordChannel(10, "general", can_view=True, can_send=True)
        hidden = _FakeDiscordChannel(11, "hidden", can_view=False, can_send=True)
        readonly = _FakeDiscordChannel(12, "news", can_view=True, can_send=False)
        guild = _FakeDiscordGuild(1, "Guild", [writable, hidden, readonly])
        bot = SimpleNamespace(guilds=[guild], user=SimpleNamespace(id=999))

        destinations = GuiyPublishDestinationsService.list_discord_destinations(bot)

        self.assertEqual([item.destination_id for item in destinations], ["1:10"])
        self.assertEqual(destinations[0].display_label, "#general — Guild")

    def test_list_telegram_destinations_reads_registry_rows(self):
        fake_query = SimpleNamespace(
            select=lambda *_args, **_kwargs: fake_query,
            eq=lambda *_args, **_kwargs: fake_query,
            in_=lambda *_args, **_kwargs: fake_query,
            order=lambda *_args, **_kwargs: fake_query,
            limit=lambda *_args, **_kwargs: fake_query,
            execute=lambda: SimpleNamespace(
                data=[
                    {
                        "provider": "telegram",
                        "chat_id": "-1001",
                        "chat_title": "Owners",
                        "chat_type": "supergroup",
                        "last_seen_at": "2026-03-20T00:00:00+00:00",
                    }
                ]
            ),
        )
        fake_supabase = SimpleNamespace(table=lambda _name: fake_query)

        with patch("bot.services.guiy_publish_destinations_service.db", SimpleNamespace(supabase=fake_supabase)):
            destinations = GuiyPublishDestinationsService.list_telegram_destinations()

        self.assertEqual(len(destinations), 1)
        self.assertEqual(destinations[0].destination_id, "-1001")
        self.assertEqual(destinations[0].title, "Owners")


if __name__ == "__main__":
    unittest.main()
