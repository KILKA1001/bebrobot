"""
Назначение: модуль "test profile titles logic" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from bot.systems import profile_titles_logic


class ProfileTitlesLogicTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        profile_titles_logic._member_title_role_state_cache.clear()

    async def test_sync_discord_titles_once_skips_unchanged_member_role_state(self):
        leader_role = SimpleNamespace(id=10, name="Глава клуба")
        member = SimpleNamespace(
            id=111,
            bot=False,
            roles=[leader_role],
            guild=SimpleNamespace(id=222),
        )
        bot = SimpleNamespace(guilds=[SimpleNamespace(id=222, members=[member])])

        with (
            patch.object(profile_titles_logic.AccountsService, "get_configured_title_roles", return_value={10: "Глава клуба"}),
            patch.object(profile_titles_logic.AccountsService, "get_configured_title_role_names", return_value={"глава клуба"}),
            patch.object(profile_titles_logic.AccountsService, "resolve_account_id", return_value="acc-1") as resolve_mock,
            patch.object(profile_titles_logic.AccountsService, "get_account_titles", return_value=["Глава клуба"]) as get_titles_mock,
            patch.object(profile_titles_logic.AccountsService, "save_account_titles", return_value=True) as save_mock,
        ):
            await profile_titles_logic.sync_discord_titles_once(bot)
            await profile_titles_logic.sync_discord_titles_once(bot)

        resolve_mock.assert_called_once_with("discord", "111")
        get_titles_mock.assert_called_once_with("acc-1")
        save_mock.assert_not_called()

    async def test_member_update_event_saves_only_when_titles_changed(self):
        before = SimpleNamespace(
            id=111,
            bot=False,
            roles=[SimpleNamespace(id=10, name="Глава клуба")],
            guild=SimpleNamespace(id=222),
        )
        after = SimpleNamespace(
            id=111,
            bot=False,
            roles=[SimpleNamespace(id=20, name="Главный вице")],
            guild=SimpleNamespace(id=222),
        )

        with (
            patch.object(profile_titles_logic.AccountsService, "get_configured_title_roles", return_value={10: "Глава клуба", 20: "Главный вице"}),
            patch.object(profile_titles_logic.AccountsService, "get_configured_title_role_names", return_value={"глава клуба", "главный вице"}),
            patch.object(profile_titles_logic.AccountsService, "resolve_account_id", return_value="acc-1") as resolve_mock,
            patch.object(profile_titles_logic.AccountsService, "get_account_titles", return_value=["Глава клуба"]) as get_titles_mock,
            patch.object(profile_titles_logic.AccountsService, "save_account_titles", return_value=True) as save_mock,
        ):
            await profile_titles_logic.handle_member_update_for_profile_titles(before, after)

        resolve_mock.assert_called_once_with("discord", "111")
        get_titles_mock.assert_called_once_with("acc-1")
        save_mock.assert_called_once_with("acc-1", ["Главный вице"], source="discord")
