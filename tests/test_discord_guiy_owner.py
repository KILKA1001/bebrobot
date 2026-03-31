"""
Назначение: модуль "test discord guiy owner" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


class _FakeBot:
    def command(self, *args, **kwargs):
        def decorator(func):
            return func
        return decorator

    def hybrid_command(self, *args, **kwargs):
        def decorator(func):
            return func
        return decorator


_base_module = types.ModuleType("bot.commands.base")
_base_module.bot = _FakeBot()
sys.modules.setdefault("bot.commands.base", _base_module)

_spec = importlib.util.spec_from_file_location(
    "test_bot_commands_guiy_owner_module",
    Path(__file__).resolve().parents[1] / "bot" / "commands" / "guiy_owner.py",
)
mod = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(mod)


def _button_by_label(view, label: str):
    for child in view.children:
        if getattr(child, "label", None) == label:
            return child
    raise AssertionError(f"button not found: {label}")


class DiscordGuiyOwnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_command_without_args_opens_view(self):
        ctx = SimpleNamespace(
            author=SimpleNamespace(id=42, bot=False, name="owner", display_name="Owner", global_name="OwnerGlobal"),
            bot=SimpleNamespace(user=SimpleNamespace(id=999)),
            message=SimpleNamespace(reference=None),
            guild=SimpleNamespace(id=777),
            channel=SimpleNamespace(id=555),
        )

        with (
            patch.object(mod, "send_temp", AsyncMock()) as send_mock,
            patch.object(mod, "_resolve_reply_message", AsyncMock(return_value=None)),
            patch.object(mod, "_persist_discord_identity"),
        ):
            await mod.guiy_owner(ctx)

        embed = send_mock.await_args.kwargs["embed"]
        view = send_mock.await_args.kwargs["view"]
        self.assertIn("Owner-управление Гуем", embed.title)
        self.assertEqual([child.label for child in view.children], [
            "Написать от Гуя",
            "Ответить от Гуя",
            "Профиль Гуя",
            "Зарегистрировать профиль Гуя",
            "Отмена",
        ])

    async def test_text_fallback_say_uses_shared_flow(self):
        ctx = SimpleNamespace(
            author=SimpleNamespace(id=42, bot=False, name="owner", display_name="Owner", global_name="OwnerGlobal"),
            bot=SimpleNamespace(user=SimpleNamespace(id=999)),
            message=SimpleNamespace(reference=None),
            guild=SimpleNamespace(id=777),
            channel=SimpleNamespace(id=555),
        )

        with (
            patch.object(mod, "send_temp", AsyncMock()) as send_mock,
            patch.object(mod, "_resolve_reply_message", AsyncMock(return_value=None)),
            patch.object(mod, "_persist_discord_identity"),
            patch.object(
                mod,
                "execute_guiy_owner_flow",
                return_value=SimpleNamespace(ok=True, outbound_text="привет из дискорда", guiy_account_id="guiy-acc"),
            ) as execute_mock,
        ):
            await mod.guiy_owner(ctx, "say", payload="привет из дискорда")

        execute_mock.assert_called_once()
        send_mock.assert_awaited_once_with(ctx, "привет из дискорда", delete_after=None)

    async def test_text_cancel_returns_manual_reset_hint(self):
        ctx = SimpleNamespace(
            author=SimpleNamespace(id=42, bot=False, name="owner", display_name="Owner", global_name="OwnerGlobal"),
            bot=SimpleNamespace(user=SimpleNamespace(id=999)),
            message=SimpleNamespace(reference=None, id=321),
            guild=SimpleNamespace(id=777),
            channel=SimpleNamespace(id=555),
        )

        with (
            patch.object(mod, "send_temp", AsyncMock()) as send_mock,
            patch.object(mod, "_persist_discord_identity"),
        ):
            await mod.guiy_owner(ctx, "cancel")

        send_mock.assert_awaited_once()
        self.assertIn("глобального режима ожидания", send_mock.await_args.args[1].lower())

    async def test_profile_button_auto_bootstraps_before_opening_editor(self):
        view = mod.GuiyOwnerActionsView(actor_id=42, bot_user_id="999", target_message_id=None, reply_author_user_id=None)
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=42),
            guild=SimpleNamespace(id=777),
            channel=SimpleNamespace(id=555),
            response=SimpleNamespace(send_message=AsyncMock()),
        )

        with patch.object(
            mod,
            "execute_guiy_owner_flow",
            return_value=SimpleNamespace(ok=True, message="✅ Профиль Гуя зарегистрирован.\nТеперь можно открыть редактирование профиля и изменить нужные поля.", guiy_account_id="guiy-acc"),
        ) as execute_mock:
            await _button_by_label(view, "Профиль Гуя").callback(interaction)

        execute_mock.assert_called_once()
        kwargs = interaction.response.send_message.await_args.kwargs
        self.assertIn("Теперь можно открыть редактирование профиля", kwargs["embed"].description)
        self.assertIsInstance(kwargs["view"], mod.GuiyOwnerProfileView)

    async def test_register_button_success_shows_profile_editor(self):
        view = mod.GuiyOwnerActionsView(actor_id=42, bot_user_id="999", target_message_id=None, reply_author_user_id=None)
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=42),
            guild=SimpleNamespace(id=777),
            channel=SimpleNamespace(id=555),
            response=SimpleNamespace(send_message=AsyncMock()),
        )

        with patch.object(
            mod,
            "execute_guiy_owner_flow",
            return_value=SimpleNamespace(ok=True, message="✅ Профиль Гуя уже зарегистрирован.\nТеперь можно открыть редактирование профиля и изменить нужные поля.", guiy_account_id="guiy-acc"),
        ):
            await _button_by_label(view, "Зарегистрировать профиль Гуя").callback(interaction)

        kwargs = interaction.response.send_message.await_args.kwargs
        self.assertIn("Профиль Гуя уже зарегистрирован", kwargs["embed"].description)
        self.assertIsInstance(kwargs["view"], mod.GuiyOwnerProfileView)

    async def test_register_button_failure_returns_clear_error(self):
        view = mod.GuiyOwnerActionsView(actor_id=42, bot_user_id="999", target_message_id=None, reply_author_user_id=None)
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=42),
            guild=SimpleNamespace(id=777),
            channel=SimpleNamespace(id=555),
            response=SimpleNamespace(send_message=AsyncMock()),
        )

        with patch.object(
            mod,
            "execute_guiy_owner_flow",
            return_value=SimpleNamespace(ok=False, message="❌ Не удалось зарегистрировать профиль Гуя. Причина: База данных недоступна.", guiy_account_id=None),
        ):
            await _button_by_label(view, "Зарегистрировать профиль Гуя").callback(interaction)

        interaction.response.send_message.assert_awaited_once_with(
            "❌ Не удалось зарегистрировать профиль Гуя. Причина: База данных недоступна.",
            ephemeral=True,
        )


if __name__ == "__main__":
    unittest.main()
