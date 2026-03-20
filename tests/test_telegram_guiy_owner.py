import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.telegram_bot.commands import guiy_owner as guiy_owner_module
from bot.telegram_bot.commands.guiy_owner import (
    _PENDING_GUIY_OWNER_ACTIONS,
    _PENDING_GUIY_OWNER_DESTINATIONS,
    _is_pending_guiy_owner_input_message,
    PendingGuiyOwnerAction,
    guiy_owner_destination_callback,
    guiy_owner_action_callback,
    guiy_owner_command,
    guiy_owner_pending_input_handler,
)


class TelegramGuiyOwnerCommandTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self):
        _PENDING_GUIY_OWNER_ACTIONS.clear()
        _PENDING_GUIY_OWNER_DESTINATIONS.clear()
        guiy_owner_module._PENDING_GUIY_OWNER_VISIBLE_ROLES.clear()

    async def test_command_without_args_shows_inline_keyboard(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42, is_bot=False),
            reply_to_message=None,
            bot=SimpleNamespace(get_me=AsyncMock()),
            answer=AsyncMock(),
            chat=SimpleNamespace(id=100),
        )
        command = SimpleNamespace(args=None)

        with (
            patch("bot.telegram_bot.commands.guiy_owner.persist_telegram_identity_from_user"),
            patch("bot.telegram_bot.commands.guiy_owner.time.time", return_value=1_001.0),
        ):
            await guiy_owner_command(message, command)

        self.assertEqual(message.answer.await_count, 1)
        text = message.answer.await_args.args[0]
        keyboard = message.answer.await_args.kwargs["reply_markup"]
        self.assertIn("Owner-управление Гуем", text)
        labels = [button.text for row in keyboard.inline_keyboard for button in row]
        self.assertEqual(
            labels,
            [
                "Написать от Гуя",
                "Ответить от Гуя",
                "Профиль Гуя",
                "Зарегистрировать профиль Гуя",
                "Отмена",
            ],
        )

    async def test_owner_can_send_message_as_guiy_via_text_fallback(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42, is_bot=False),
            reply_to_message=None,
            bot=SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=999))),
            answer=AsyncMock(),
            chat=SimpleNamespace(id=100),
        )
        command = SimpleNamespace(args="say привет от гуя")

        with (
            patch("bot.telegram_bot.commands.guiy_owner.persist_telegram_identity_from_user"),
            patch(
                "bot.telegram_bot.commands.guiy_owner.execute_guiy_owner_flow",
                return_value=SimpleNamespace(ok=True, outbound_text="привет от гуя", guiy_account_id="guiy-acc"),
            ) as execute_mock,
        ):
            await guiy_owner_command(message, command)

        execute_mock.assert_called_once()
        message.answer.assert_awaited_once_with("привет от гуя")

    async def test_text_cancel_clears_pending_owner_state(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42, is_bot=False),
            reply_to_message=None,
            bot=SimpleNamespace(get_me=AsyncMock()),
            answer=AsyncMock(),
            chat=SimpleNamespace(id=100),
        )
        command = SimpleNamespace(args="cancel")
        _PENDING_GUIY_OWNER_ACTIONS[42] = PendingGuiyOwnerAction(
            selected_action="say",
            bot_user_id="999",
            target_message_id=None,
            reply_author_user_id=None,
            created_at=1_000.0,
            target_chat_or_guild="-1001",
            control_chat_id="100",
            target_destination_id="-1001",
        )

        with (
            patch("bot.telegram_bot.commands.guiy_owner.persist_telegram_identity_from_user"),
            patch("bot.telegram_bot.commands.guiy_owner.time.time", return_value=1_001.0),
        ):
            await guiy_owner_command(message, command)

        self.assertNotIn(42, _PENDING_GUIY_OWNER_ACTIONS)
        message.answer.assert_awaited_once()
        self.assertIn("отключён вручную", message.answer.await_args.args[0].lower())


    async def test_profile_menu_auto_bootstraps_and_opens_field_buttons(self):
        callback_message = SimpleNamespace(
            chat=SimpleNamespace(id=100),
            reply_to_message=None,
            answer=AsyncMock(),
            bot=SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=999))),
        )
        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            message=callback_message,
            data="guiy_owner:action:profile",
            answer=AsyncMock(),
        )

        with patch(
            "bot.telegram_bot.commands.guiy_owner.execute_guiy_owner_flow",
            return_value=SimpleNamespace(ok=True, message="✅ Профиль Гуя зарегистрирован.\nТеперь можно открыть редактирование профиля и изменить нужные поля.", guiy_account_id="guiy-acc"),
        ) as execute_mock:
            await guiy_owner_module.guiy_owner_profile_menu_callback(callback)

        execute_mock.assert_called_once()
        callback_message.answer.assert_awaited_once()
        text = callback_message.answer.await_args.args[0]
        keyboard = callback_message.answer.await_args.kwargs["reply_markup"]
        self.assertIn("Теперь можно открыть редактирование профиля", text)
        self.assertEqual([button.text for row in keyboard.inline_keyboard for button in row], [
            "Никнейм",
            "Описание",
            "Null's ID",
            "Отображаемые роли",
            "Отмена",
        ])

    async def test_register_action_success_shows_next_step_buttons(self):
        callback_message = SimpleNamespace(
            chat=SimpleNamespace(id=100),
            reply_to_message=None,
            answer=AsyncMock(),
            bot=SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=999))),
        )
        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            message=callback_message,
            data="guiy_owner:action:register_profile",
            answer=AsyncMock(),
        )

        with patch(
            "bot.telegram_bot.commands.guiy_owner.execute_guiy_owner_flow",
            return_value=SimpleNamespace(ok=True, message="✅ Профиль Гуя уже зарегистрирован.\nТеперь можно открыть редактирование профиля и изменить нужные поля.", guiy_account_id="guiy-acc"),
        ):
            await guiy_owner_action_callback(callback)

        callback_message.answer.assert_awaited_once()
        self.assertIn("Профиль Гуя уже зарегистрирован", callback_message.answer.await_args.args[0])
        self.assertIn("reply_markup", callback_message.answer.await_args.kwargs)

    async def test_register_action_failure_returns_clear_message(self):
        callback_message = SimpleNamespace(
            chat=SimpleNamespace(id=100),
            reply_to_message=None,
            answer=AsyncMock(),
            bot=SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=999))),
        )
        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            message=callback_message,
            data="guiy_owner:action:register_profile",
            answer=AsyncMock(),
        )

        with patch(
            "bot.telegram_bot.commands.guiy_owner.execute_guiy_owner_flow",
            return_value=SimpleNamespace(ok=False, message="❌ Не удалось зарегистрировать профиль Гуя. Причина: База данных недоступна.", guiy_account_id=None),
        ):
            await guiy_owner_action_callback(callback)

        callback_message.answer.assert_awaited_once_with("❌ Не удалось зарегистрировать профиль Гуя. Причина: База данных недоступна.")
    async def test_reply_action_button_without_reply_context_shows_instruction(self):
        callback_message = SimpleNamespace(chat=SimpleNamespace(id=100), reply_to_message=None, answer=AsyncMock(), bot=SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=999))))
        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            message=callback_message,
            data="guiy_owner:action:reply",
            answer=AsyncMock(),
        )

        await guiy_owner_action_callback(callback)

        callback_message.answer.assert_awaited_once()
        self.assertIn("ничего не изменится", callback_message.answer.await_args.args[0].lower())
        self.assertEqual(len(_PENDING_GUIY_OWNER_ACTIONS), 0)

    async def test_pending_profile_input_updates_profile(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42, is_bot=False),
            text="Новый Гуй",
            answer=AsyncMock(),
            chat=SimpleNamespace(id=100),
        )
        _PENDING_GUIY_OWNER_ACTIONS[42] = PendingGuiyOwnerAction(
            selected_action="profile_update",
            bot_user_id="999",
            target_message_id=None,
            reply_author_user_id=None,
            created_at=1_000.0,
            target_chat_or_guild="100",
            control_chat_id="100",
            selected_field="custom_nick",
        )

        with (
            patch("bot.telegram_bot.commands.guiy_owner.persist_telegram_identity_from_user"),
            patch("bot.telegram_bot.commands.guiy_owner.time.time", return_value=1_001.0),
            patch(
                "bot.telegram_bot.commands.guiy_owner.execute_guiy_owner_flow",
                return_value=SimpleNamespace(ok=True, message="✅ Никнейм обновлён", guiy_account_id="guiy-acc"),
            ) as execute_mock,
        ):
            await guiy_owner_pending_input_handler(message)

        execute_mock.assert_called_once()
        message.answer.assert_awaited_once_with("✅ Никнейм обновлён")
        self.assertNotIn(42, _PENDING_GUIY_OWNER_ACTIONS)

    async def test_say_action_without_known_destinations_shows_helpful_message(self):
        callback_message = SimpleNamespace(
            chat=SimpleNamespace(id=100),
            reply_to_message=None,
            answer=AsyncMock(),
            bot=SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(id=999))),
        )
        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            message=callback_message,
            data="guiy_owner:action:say",
            answer=AsyncMock(),
        )

        with patch(
            "bot.telegram_bot.commands.guiy_owner.GuiyPublishDestinationsService.list_telegram_destinations",
            return_value=[],
        ):
            await guiy_owner_action_callback(callback)

        callback_message.answer.assert_awaited_once()
        self.assertIn("нет доступных групп", callback_message.answer.await_args.args[0].lower())

    async def test_destination_confirmation_creates_pending_say_action(self):
        callback_message = SimpleNamespace(
            chat=SimpleNamespace(id=100),
            edit_text=AsyncMock(),
        )
        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            message=callback_message,
            data="guiy_owner_destination:confirm",
            answer=AsyncMock(),
        )
        _PENDING_GUIY_OWNER_DESTINATIONS[42] = {
            "destinations": [
                guiy_owner_module.GuiyPublishDestination(
                    provider="telegram",
                    destination_id="-1001",
                    title="Тестовая группа",
                    subtitle="supergroup",
                    destination_type="supergroup",
                    chat_id="-1001",
                )
            ],
            "page": 0,
            "selected_destination_id": "-1001",
            "bot_user_id": "999",
            "target_message_id": None,
            "reply_author_user_id": None,
            "created_at": 1_000.0,
            "target_chat_or_guild": "100",
        }

        with patch("bot.telegram_bot.commands.guiy_owner.time.time", return_value=1_001.0):
            await guiy_owner_destination_callback(callback)

        pending = _PENDING_GUIY_OWNER_ACTIONS[42]
        self.assertEqual(pending.target_destination_id, "-1001")
        self.assertIn("Тестовая группа", pending.target_destination_label)
        self.assertEqual(pending.control_chat_id, "100")
        callback_message.edit_text.assert_awaited_once()

    async def test_pending_say_sends_to_selected_destination(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42, is_bot=False),
            text="Текст для группы",
            answer=AsyncMock(),
            chat=SimpleNamespace(id=100),
            bot=SimpleNamespace(
                get_me=AsyncMock(return_value=SimpleNamespace(id=999)),
                get_chat_member=AsyncMock(return_value=SimpleNamespace(status="administrator", can_send_messages=True)),
                send_message=AsyncMock(),
            ),
        )
        _PENDING_GUIY_OWNER_ACTIONS[42] = PendingGuiyOwnerAction(
            selected_action="say",
            bot_user_id="999",
            target_message_id=None,
            reply_author_user_id=None,
            created_at=1_000.0,
            target_chat_or_guild="-1001",
            control_chat_id="100",
            target_destination_id="-1001",
            target_destination_label="Тестовая группа — supergroup",
        )

        with (
            patch("bot.telegram_bot.commands.guiy_owner.persist_telegram_identity_from_user"),
            patch("bot.telegram_bot.commands.guiy_owner.time.time", return_value=1_001.0),
            patch(
                "bot.telegram_bot.commands.guiy_owner.execute_guiy_owner_flow",
                return_value=SimpleNamespace(ok=True, outbound_text="Текст для группы", guiy_account_id="guiy-acc"),
            ),
            patch(
                "bot.telegram_bot.commands.guiy_owner.GuiyPublishDestinationsService.get_telegram_destination",
                return_value=SimpleNamespace(destination_id="-1001"),
            ),
        ):
            await guiy_owner_pending_input_handler(message)

        message.bot.send_message.assert_awaited_once_with(-1001, "Текст для группы")
        message.answer.assert_awaited_once()
        self.assertIn("Гуй отправил сообщение сюда", message.answer.await_args.args[0])

    def test_pending_input_filter_ignores_commands(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42, is_bot=False),
            text="/points",
            chat=SimpleNamespace(id=100),
        )
        _PENDING_GUIY_OWNER_ACTIONS[42] = PendingGuiyOwnerAction(
            selected_action="say",
            bot_user_id="999",
            target_message_id=None,
            reply_author_user_id=None,
            created_at=1_000.0,
            target_chat_or_guild="-1001",
            control_chat_id="100",
            target_destination_id="-1001",
        )

        with patch("bot.telegram_bot.commands.guiy_owner.time.time", return_value=1_001.0):
            self.assertFalse(_is_pending_guiy_owner_input_message(message))

    def test_pending_input_filter_ignores_other_chats(self):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42, is_bot=False),
            text="обычный текст",
            chat=SimpleNamespace(id=200),
        )
        _PENDING_GUIY_OWNER_ACTIONS[42] = PendingGuiyOwnerAction(
            selected_action="say",
            bot_user_id="999",
            target_message_id=None,
            reply_author_user_id=None,
            created_at=1_000.0,
            target_chat_or_guild="-1001",
            control_chat_id="100",
            target_destination_id="-1001",
        )

        with patch("bot.telegram_bot.commands.guiy_owner.time.time", return_value=1_001.0):
            self.assertFalse(_is_pending_guiy_owner_input_message(message))


class TelegramGuiyOwnerVisibilityTests(unittest.TestCase):
    def test_guiy_owner_hidden_from_public_help(self):
        from bot.telegram_bot.systems.commands_logic import HELPY_TEXT

        self.assertNotIn("/guiy_owner", HELPY_TEXT)

    def test_guiy_owner_hidden_from_public_command_menu(self):
        from bot.telegram_bot.main import BOT_COMMANDS

        self.assertNotIn("guiy_owner", [item.command for item in BOT_COMMANDS])


if __name__ == "__main__":
    unittest.main()
