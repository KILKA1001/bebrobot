"""
Назначение: модуль "test telegram chat registry router" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from aiogram.dispatcher.event.bases import SkipHandler

from bot.telegram_bot.chat_registry_router import (
    remember_bot_membership,
    remember_group_callback,
    remember_group_edited_message,
    remember_group_message,
    remember_user_membership,
)


class TelegramChatRegistryRouterTests(unittest.IsolatedAsyncioTestCase):
    async def test_group_message_registers_chat_and_skips_for_next_handlers(self):
        message = SimpleNamespace(chat=SimpleNamespace(id=-1001, title="Группа", type="supergroup"))

        with patch("bot.telegram_bot.chat_registry_router.GuiyPublishDestinationsService.register_telegram_chat") as register_mock:
            with self.assertRaises(SkipHandler):
                await remember_group_message(message)

        register_mock.assert_called_once_with(
            chat_id=-1001,
            chat_title="Группа",
            chat_type="supergroup",
            is_active=True,
        )

    async def test_group_edited_message_registers_chat_and_skips_for_next_handlers(self):
        message = SimpleNamespace(chat=SimpleNamespace(id=-1001, title="Группа", type="supergroup"))

        with patch("bot.telegram_bot.chat_registry_router.GuiyPublishDestinationsService.register_telegram_chat") as register_mock:
            with self.assertRaises(SkipHandler):
                await remember_group_edited_message(message)

        register_mock.assert_called_once()

    async def test_group_callback_registers_chat_and_skips_for_next_handlers(self):
        callback = SimpleNamespace(message=SimpleNamespace(chat=SimpleNamespace(id=-1001, title="Группа", type="supergroup")))

        with patch("bot.telegram_bot.chat_registry_router.GuiyPublishDestinationsService.register_telegram_chat") as register_mock:
            with self.assertRaises(SkipHandler):
                await remember_group_callback(callback)

        register_mock.assert_called_once()

    async def test_chat_member_left_triggers_purge_for_regular_user(self):
        update = SimpleNamespace(
            chat=SimpleNamespace(id=-1001, title="Группа", type="supergroup"),
            old_chat_member=SimpleNamespace(status="member"),
            new_chat_member=SimpleNamespace(
                status="left",
                user=SimpleNamespace(id=777, is_bot=False),
            ),
        )

        with (
            patch("bot.telegram_bot.chat_registry_router.GuiyPublishDestinationsService.register_telegram_chat"),
            patch("bot.telegram_bot.chat_registry_router.AccountsService.purge_unlinked_identity", return_value=(True, "purged")) as purge_mock,
        ):
            with self.assertRaises(SkipHandler):
                await remember_user_membership(update)

        purge_mock.assert_called_once_with("telegram", "777")

    async def test_bot_membership_marks_chat_inactive_when_bot_removed(self):
        update = SimpleNamespace(
            chat=SimpleNamespace(id=-1001, title="Группа", type="supergroup"),
            new_chat_member=SimpleNamespace(status="left"),
        )

        with patch("bot.telegram_bot.chat_registry_router.GuiyPublishDestinationsService.register_telegram_chat") as register_mock:
            await remember_bot_membership(update)

        register_mock.assert_called_once_with(
            chat_id=-1001,
            chat_title="Группа",
            chat_type="supergroup",
            is_active=False,
        )

    async def test_chat_member_left_does_not_purge_bot_identity(self):
        update = SimpleNamespace(
            chat=SimpleNamespace(id=-1001, title="Группа", type="supergroup"),
            old_chat_member=SimpleNamespace(status="member"),
            new_chat_member=SimpleNamespace(
                status="left",
                user=SimpleNamespace(id=888, is_bot=True),
            ),
        )

        with (
            patch("bot.telegram_bot.chat_registry_router.GuiyPublishDestinationsService.register_telegram_chat"),
            patch("bot.telegram_bot.chat_registry_router.AccountsService.purge_unlinked_identity") as purge_mock,
        ):
            with self.assertRaises(SkipHandler):
                await remember_user_membership(update)

        purge_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
