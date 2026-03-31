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
    remember_group_callback,
    remember_group_edited_message,
    remember_group_message,
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


if __name__ == "__main__":
    unittest.main()
