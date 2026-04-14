"""
Назначение: модуль "test discord proposal ui" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch


class _FakeBot:
    def hybrid_command(self, *args, **kwargs):
        def decorator(func):
            return func

        return decorator


_base_module = types.ModuleType("bot.commands.base")
_base_module.bot = _FakeBot()
sys.modules.setdefault("bot.commands.base", _base_module)

_SPEC = importlib.util.spec_from_file_location(
    "test_bot_commands_proposal_module",
    Path(__file__).resolve().parents[1] / "bot" / "commands" / "proposal.py",
)
proposal = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(proposal)


class DiscordProposalUiTests(IsolatedAsyncioTestCase):
    @staticmethod
    def _find_button_callback(view, label: str):
        for child in view.children:
            if getattr(child, "label", None) == label:
                return child.callback
        raise AssertionError(f"button not found: {label}")

    async def test_admin_settings_denied_for_regular_user(self) -> None:
        view = proposal.ProposalRootView(actor_id=15)
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=15),
            response=SimpleNamespace(send_message=AsyncMock()),
        )
        callback = self._find_button_callback(view, "⚙️ Настройки Совета")

        with patch.object(proposal.AuthorityService, "is_super_admin", return_value=False):
            await callback(interaction)

        interaction.response.send_message.assert_awaited_once()
        self.assertIn("только суперадмину", interaction.response.send_message.await_args.args[0])

    async def test_admin_settings_opens_for_superadmin(self) -> None:
        view = proposal.ProposalRootView(actor_id=15)
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=15),
            response=SimpleNamespace(send_message=AsyncMock()),
        )
        callback = self._find_button_callback(view, "⚙️ Настройки Совета")

        with patch.object(proposal.AuthorityService, "is_super_admin", return_value=True):
            await callback(interaction)

        kwargs = interaction.response.send_message.await_args.kwargs
        self.assertTrue(kwargs["ephemeral"])
        self.assertIsNotNone(kwargs["view"])

    async def test_events_save_persists_selected_destination(self) -> None:
        view = proposal.ProposalAdminSettingsView(actor_id=22)
        view.events_confirm_active = True
        view.events_selected_destination_id = "777"
        view.events_selected_label = "Совет"
        save_button = proposal._AdminEventsSaveButton(view)
        interaction = SimpleNamespace(
            user=SimpleNamespace(id=22),
            channel=SimpleNamespace(id=555),
            response=SimpleNamespace(edit_message=AsyncMock()),
        )

        with patch.object(
            proposal.CouncilSystemEventsService,
            "set_channel",
            return_value={"ok": True, "message": "✅ Канал сохранён"},
        ) as set_mock:
            await save_button.callback(interaction)

        set_mock.assert_called_once_with(provider="discord", actor_user_id="22", destination_id="777")
        self.assertFalse(view.events_picker_active)
        self.assertFalse(view.events_confirm_active)

    async def test_events_cancel_leaves_channel_unchanged(self) -> None:
        view = proposal.ProposalAdminSettingsView(actor_id=23)
        view.events_picker_active = True
        cancel_button = proposal._AdminEventsCancelButton(view)
        interaction = SimpleNamespace(
            response=SimpleNamespace(edit_message=AsyncMock()),
        )

        await cancel_button.callback(interaction)

        embed = interaction.response.edit_message.await_args.kwargs["embed"]
        self.assertIn("Изменения не внесены", embed.description)
