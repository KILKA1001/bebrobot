"""
Назначение: тесты Discord-меню engagement (видимость кнопок по домену).
"""

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace


class _FakeBot:
    def hybrid_command(self, *args, **kwargs):
        def decorator(func):
            return func

        return decorator


_commands_module = types.ModuleType("bot.commands")
_commands_module.bot = _FakeBot()
sys.modules.setdefault("bot.commands", _commands_module)

_SPEC = importlib.util.spec_from_file_location(
    "test_bot_commands_engagement_module",
    Path(__file__).resolve().parents[1] / "bot" / "commands" / "engagement.py",
)
engagement = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(engagement)


class EngagementMenuViewButtonsTests(unittest.TestCase):
    def _build_view(self, domain: str):
        target = SimpleNamespace(id=321, mention="<@321>")
        return engagement.EngagementMenuView(target=target, actor_id=123, domain=domain)

    def test_sync_buttons_points_domain_keeps_only_points_buttons(self):
        view = self._build_view("points")

        view.sync_buttons()

        labels = [child.label for child in view.children]
        self.assertEqual(labels, ["ℹ️ Что делает команда", "➕ Начислить баллы", "➖ Снять баллы"])
        self.assertTrue(any(child.label == "ℹ️ Что делает команда" for child in view.children))

    def test_sync_buttons_tickets_domain_keeps_only_tickets_buttons(self):
        view = self._build_view("tickets")

        view.sync_buttons()

        labels = [child.label for child in view.children]
        self.assertEqual(
            labels,
            [
                "ℹ️ Что делает команда",
                "🎟️ + Обычные",
                "🎟️ - Обычные",
                "🪙 + Золотые",
                "🪙 - Золотые",
            ],
        )
        self.assertTrue(any(child.label == "ℹ️ Что делает команда" for child in view.children))


if __name__ == "__main__":
    unittest.main()
