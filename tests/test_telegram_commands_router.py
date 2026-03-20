from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from bot.telegram_bot.commands import get_commands_router
from bot.telegram_bot.commands.linking import roles_catalog_command
from bot.telegram_bot.main import BOT_COMMANDS


def test_get_commands_router_is_singleton_instance() -> None:
    router_first = get_commands_router()
    router_second = get_commands_router()

    assert router_first is router_second


class TelegramCommandsRouterTests(IsolatedAsyncioTestCase):
    async def test_roles_catalog_command_answers_with_html(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            answer=AsyncMock(),
        )

        with patch("bot.telegram_bot.commands.linking.persist_telegram_identity_from_user"), patch(
            "bot.telegram_bot.commands.linking.process_roles_catalog_command",
            return_value="<b>Каталог ролей</b>",
        ):
            await roles_catalog_command(message)

        message.answer.assert_awaited_once_with("<b>Каталог ролей</b>", parse_mode="HTML")


def test_bot_commands_include_roles() -> None:
    assert any(command.command == "roles" for command in BOT_COMMANDS)
