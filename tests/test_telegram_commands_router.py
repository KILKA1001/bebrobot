"""
Назначение: модуль "test telegram commands router" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from bot.telegram_bot.commands import get_commands_router
from bot.telegram_bot.commands.linking import (
    link_command,
    link_discord_command,
    profile_command,
    roles_catalog_callback,
    roles_catalog_command,
)
from bot.telegram_bot.main import BOT_COMMANDS
from bot.telegram_bot.commands import proposal as telegram_proposal


def test_get_commands_router_is_singleton_instance() -> None:
    router_first = get_commands_router()
    router_second = get_commands_router()

    assert router_first is router_second


class TelegramCommandsRouterTests(IsolatedAsyncioTestCase):
    async def test_link_command_answers_with_html_parse_mode(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            text="/link",
            chat=SimpleNamespace(type="private"),
            answer=AsyncMock(),
        )

        with (
            patch("bot.telegram_bot.commands.linking.persist_telegram_identity_from_user"),
            patch("bot.telegram_bot.commands.linking.run_blocking_io", return_value="Тест <b>HTML</b>"),
        ):
            await link_command(message)

        message.answer.assert_awaited_once_with("Тест <b>HTML</b>", parse_mode="HTML")

    async def test_link_discord_command_answers_with_html_parse_mode(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            text="/link_discord",
            chat=SimpleNamespace(type="private"),
            answer=AsyncMock(),
        )

        with (
            patch("bot.telegram_bot.commands.linking.persist_telegram_identity_from_user"),
            patch("bot.telegram_bot.commands.linking.run_blocking_io", return_value="Код: <code>123</code>"),
        ):
            await link_discord_command(message)

        message.answer.assert_awaited_once_with("Код: <code>123</code>", parse_mode="HTML")

    async def test_profile_command_resolves_target_from_username_argument(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123, full_name="Caller"),
            text="/profile @target_user",
            reply_to_message=None,
            chat=SimpleNamespace(type="private", id=777),
            answer=AsyncMock(),
            bot=SimpleNamespace(
                get_user_profile_photos=AsyncMock(return_value=SimpleNamespace(total_count=0, photos=[])),
                get_me=AsyncMock(return_value=SimpleNamespace(id=999)),
            ),
        )

        with (
            patch("bot.telegram_bot.commands.linking.persist_telegram_identity_from_user"),
            patch(
                "bot.telegram_bot.commands.linking.run_blocking_io",
                side_effect=[
                    {
                        "status": "ok",
                        "result": {
                            "provider": "telegram",
                            "provider_user_id": "777000",
                            "display_name": "Target User",
                            "username": "target_user",
                        },
                    },
                    "profile text",
                ],
            ) as run_blocking_io_mock,
            patch("bot.telegram_bot.commands.linking._safe_answer", AsyncMock(return_value=True)) as safe_answer_mock,
        ):
            await profile_command(message)

        self.assertEqual(run_blocking_io_mock.await_count, 2)
        self.assertEqual(run_blocking_io_mock.await_args_list[0].args[0], "telegram.profile.resolve_user_lookup")
        self.assertEqual(run_blocking_io_mock.await_args_list[1].args[0], "telegram.profile.process_command")
        self.assertEqual(run_blocking_io_mock.await_args_list[1].kwargs["target_telegram_user_id"], 777000)
        self.assertEqual(run_blocking_io_mock.await_args_list[1].kwargs["target_display_name"], "Target User")
        safe_answer_mock.assert_awaited_once()

    async def test_profile_command_shows_not_found_message_for_unknown_username(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123, full_name="Caller"),
            text="/profile @missing_user",
            reply_to_message=None,
            chat=SimpleNamespace(type="private", id=777),
            answer=AsyncMock(),
        )

        with (
            patch("bot.telegram_bot.commands.linking.persist_telegram_identity_from_user"),
            patch(
                "bot.telegram_bot.commands.linking.run_blocking_io",
                return_value={"status": "not_found", "candidates": []},
            ) as run_blocking_io_mock,
            patch("bot.telegram_bot.commands.linking._safe_answer", AsyncMock(return_value=True)) as safe_answer_mock,
        ):
            await profile_command(message)

        run_blocking_io_mock.assert_awaited_once()
        safe_answer_mock.assert_awaited_once()
        text = safe_answer_mock.await_args.args[1]
        assert "Пользователь не найден" in text

    async def test_proposal_menu_shows_admin_button_only_for_superadmin(self) -> None:
        superadmin_message = SimpleNamespace(
            from_user=SimpleNamespace(id=900),
            answer=AsyncMock(),
        )
        user_message = SimpleNamespace(
            from_user=SimpleNamespace(id=901),
            answer=AsyncMock(),
        )

        with patch("bot.telegram_bot.commands.proposal.AuthorityService.is_super_admin", side_effect=[True, False]):
            await telegram_proposal.proposal_command(superadmin_message)
            await telegram_proposal.proposal_command(user_message)

        superadmin_markup = superadmin_message.answer.await_args_list[0].kwargs["reply_markup"]
        user_markup = user_message.answer.await_args_list[0].kwargs["reply_markup"]
        superadmin_texts = [button.text for row in superadmin_markup.inline_keyboard for button in row]
        user_texts = [button.text for row in user_markup.inline_keyboard for button in row]

        assert "⚙️ Настройки Совета" in superadmin_texts
        assert "⚙️ Настройки Совета" not in user_texts

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

    async def test_proposal_events_choose_confirm_save_updates_channel(self) -> None:
        actor_id = 600
        destination = SimpleNamespace(destination_id="100500", display_label="Совет • Новости")
        telegram_proposal._PENDING_EVENTS_DESTINATION_PICKER[actor_id] = {
            "destinations": [destination],
            "page": 0,
            "selected_destination_id": None,
        }
        callback_message = SimpleNamespace(chat=SimpleNamespace(id=-123), edit_text=AsyncMock())
        choose_callback = SimpleNamespace(
            from_user=SimpleNamespace(id=actor_id),
            data="proposal:events_choose:100500",
            message=callback_message,
            answer=AsyncMock(),
        )
        save_callback = SimpleNamespace(
            from_user=SimpleNamespace(id=actor_id),
            data="proposal:events_save",
            message=callback_message,
            answer=AsyncMock(),
        )

        with (
            patch("bot.telegram_bot.commands.proposal.AuthorityService.is_super_admin", return_value=True),
            patch("bot.telegram_bot.commands.proposal.CouncilSystemEventsService.set_channel", return_value={"ok": True, "message": "✅ Сохранено"}) as set_mock,
        ):
            await telegram_proposal.proposal_callbacks(choose_callback)
            await telegram_proposal.proposal_callbacks(save_callback)

        assert telegram_proposal._PENDING_EVENTS_DESTINATION_PICKER.get(actor_id) is None
        set_mock.assert_called_once_with(provider="telegram", actor_user_id=str(actor_id), destination_id="100500")
        save_text = callback_message.edit_text.await_args_list[-1].args[0]
        assert "✅ Сохранено" in save_text

    async def test_proposal_events_cancel_clears_pending_without_changes(self) -> None:
        actor_id = 601
        telegram_proposal._PENDING_EVENTS_DESTINATION_PICKER[actor_id] = {
            "destinations": [SimpleNamespace(destination_id="x", display_label="X")],
            "page": 0,
            "selected_destination_id": "x",
        }
        callback_message = SimpleNamespace(chat=SimpleNamespace(id=-123), edit_text=AsyncMock())
        cancel_callback = SimpleNamespace(
            from_user=SimpleNamespace(id=actor_id),
            data="proposal:events_cancel",
            message=callback_message,
            answer=AsyncMock(),
        )

        with patch("bot.telegram_bot.commands.proposal.CouncilSystemEventsService.set_channel") as set_mock:
            await telegram_proposal.proposal_callbacks(cancel_callback)

        assert telegram_proposal._PENDING_EVENTS_DESTINATION_PICKER.get(actor_id) is None
        set_mock.assert_not_called()
        cancel_text = callback_message.edit_text.await_args.args[0]
        assert "Изменения не внесены" in cancel_text


def test_bot_commands_include_roles() -> None:
    assert any(command.command == "roles" for command in BOT_COMMANDS)
