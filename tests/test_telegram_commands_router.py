"""
Назначение: модуль "test telegram commands router" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from bot.telegram_bot.commands import get_commands_router
from bot.telegram_bot.commands.linking import roles_catalog_callback, roles_catalog_command
from bot.telegram_bot.main import BOT_COMMANDS


def test_get_commands_router_is_singleton_instance() -> None:
    router_first = get_commands_router()
    router_second = get_commands_router()

    assert router_first is router_second


class TelegramCommandsRouterTests(IsolatedAsyncioTestCase):
    async def test_roles_catalog_command_answers_with_html_and_keyboard(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            chat=SimpleNamespace(id=777),
            answer=AsyncMock(),
        )

        with patch("bot.telegram_bot.commands.linking.persist_telegram_identity_from_user"), patch(
            "bot.telegram_bot.commands.linking.prepare_roles_catalog_pages",
            return_value={
                "status": "ok",
                "message": "",
                "pages": [
                    {
                        "page": 1,
                        "total_pages": 1,
                        "category_count": 1,
                        "role_count": 1,
                        "blocks": [
                            {
                                "category": "Турниры",
                                "roles": [
                                    {
                                        "name": "Чемпион",
                                        "description": "Победитель сезона",
                                        "acquire_method_label": "выдаёт администратор",
                                        "acquire_hint": "Выиграть турнир",
                                    }
                                ],
                            }
                        ],
                    }
                ],
            },
        ):
            await roles_catalog_command(message)

        message.answer.assert_awaited_once()
        _, kwargs = message.answer.await_args
        assert kwargs["parse_mode"] == "HTML"
        assert kwargs["reply_markup"] is not None

    async def test_roles_catalog_callback_edits_existing_message(self) -> None:
        callback_message = SimpleNamespace(
            chat=SimpleNamespace(id=777),
            edit_text=AsyncMock(),
        )
        callback = SimpleNamespace(
            data="roles_catalog:page:2",
            message=callback_message,
            answer=AsyncMock(),
        )

        with patch(
            "bot.telegram_bot.commands.linking.prepare_roles_catalog_pages",
            return_value={
                "status": "ok",
                "message": "",
                "pages": [
                    {
                        "page": 1,
                        "total_pages": 2,
                        "category_count": 1,
                        "role_count": 1,
                        "blocks": [{"category": "Первая", "roles": [{"name": "R1", "description": "", "acquire_method_label": "выдаёт администратор", "acquire_hint": ""}]}],
                    },
                    {
                        "page": 2,
                        "total_pages": 2,
                        "category_count": 1,
                        "role_count": 1,
                        "blocks": [{"category": "Вторая", "roles": [{"name": "R2", "description": "", "acquire_method_label": "за баллы", "acquire_hint": ""}]}],
                    },
                ],
            },
        ):
            await roles_catalog_callback(callback)

        callback_message.edit_text.assert_awaited_once()
        callback.answer.assert_awaited()


def test_bot_commands_include_roles() -> None:
    assert any(command.command == "roles" for command in BOT_COMMANDS)
