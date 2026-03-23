from types import SimpleNamespace
from unittest.mock import patch

from bot.services.moderation_service import ModerationService
from bot.systems.core_logic import get_help_embed
from bot.systems.moderation_rep_ui import REP_FLOW_STEPS, render_rep_preview_text, render_rep_result_text
from bot.telegram_bot.systems.commands_logic import get_helpy_text


class _Resp:
    def __init__(self, data):
        self.data = data


class _TableOp:
    def __init__(self, fake_db, table_name):
        self.fake_db = fake_db
        self.table_name = table_name
        self._filters = []
        self._limit = None

    def select(self, _fields):
        return self

    def eq(self, key, value):
        self._filters.append((key, value))
        return self

    def limit(self, n):
        self._limit = n
        return self

    def execute(self):
        rows = self.fake_db.tables[self.table_name]
        selected = []
        for row in rows:
            if all(str(row.get(k)) == str(v) for k, v in self._filters):
                selected.append(dict(row))
        if self._limit is not None:
            selected = selected[: self._limit]
        return _Resp(selected)


class _FakeSupabase:
    def __init__(self, fake_db):
        self.fake_db = fake_db

    def table(self, name):
        return _TableOp(self.fake_db, name)


class _FakeDb:
    def __init__(self):
        self.tables = {
            "moderation_violation_types": [
                {"id": 1, "code": "spam", "title": "Спам", "is_active": True},
            ],
            "moderation_penalty_rules": [
                {
                    "id": 10,
                    "violation_type_id": 1,
                    "escalation_step": 1,
                    "warn_count_before": 0,
                    "apply_warn": True,
                    "mute_minutes": 360,
                    "fine_points": 10,
                    "apply_ban": False,
                    "is_active": True,
                    "description_for_admin": "Первый спам",
                    "description_for_user": "Не спамьте",
                },
                {
                    "id": 11,
                    "violation_type_id": 1,
                    "escalation_step": 2,
                    "warn_count_before": 1,
                    "apply_warn": True,
                    "mute_minutes": 720,
                    "fine_points": 20,
                    "apply_ban": False,
                    "is_active": True,
                    "description_for_admin": "Повторный спам",
                    "description_for_user": "Следующее наказание сильнее",
                },
            ],
            "moderation_warn_state": [],
        }
        self.supabase = _FakeSupabase(self)


def test_rep_help_visibility_for_telegram_and_discord() -> None:
    with patch(
        "bot.telegram_bot.systems.commands_logic.AuthorityService.resolve_authority",
        return_value=SimpleNamespace(level=30, titles=("Ветеран города",)),
    ), patch(
        "bot.telegram_bot.systems.commands_logic.AuthorityService.has_command_permission",
        return_value=True,
    ):
        telegram_help = get_helpy_text(telegram_user_id=42)

    regular_embed = get_help_embed(
        "admin_fines",
        visibility=SimpleNamespace(level=0, titles=tuple(), is_administrator=False),
    )
    veteran_embed = get_help_embed(
        "admin_fines",
        visibility=SimpleNamespace(level=30, titles=("Ветеран города",), is_administrator=False),
    )

    assert "/rep" in telegram_help
    assert "/rep" not in regular_embed.description
    assert "/rep" in veteran_embed.description


def test_rep_service_keeps_same_escalation_payload_on_both_platforms() -> None:
    fake_db = _FakeDb()

    def _resolve_account_id(provider: str, provider_user_id: str) -> str:
        return f"acc-{provider}-{provider_user_id}"

    authority = SimpleNamespace(allowed=True, message="Разрешено")
    with patch("bot.services.moderation_service.db", fake_db), patch(
        "bot.services.moderation_service.AccountsService.resolve_account_id",
        side_effect=_resolve_account_id,
    ), patch(
        "bot.services.moderation_service.AuthorityService.can_apply_moderation_action",
        return_value=authority,
    ):
        discord_preview = ModerationService.prepare_moderation_payload(
            "discord",
            {"provider": "discord", "provider_user_id": "10", "label": "Mod"},
            {"provider": "discord", "provider_user_id": "20", "label": "Target"},
            "spam",
            {"chat_id": 100},
        )
        telegram_preview = ModerationService.prepare_moderation_payload(
            "telegram",
            {"provider": "telegram", "provider_user_id": "10", "label": "Mod"},
            {"provider": "telegram", "provider_user_id": "20", "label": "Target"},
            "spam",
            {"chat_id": 100},
        )

    assert discord_preview["ok"] is True
    assert telegram_preview["ok"] is True
    discord_ui = discord_preview["ui_payload"]
    telegram_ui = telegram_preview["ui_payload"]

    assert REP_FLOW_STEPS == (
        "Шаг 1: выбрать нарушителя",
        "Шаг 2: выбрать вид нарушения",
        "Шаг 3: увидеть авторасчёт наказания",
        "Шаг 4: подтвердить",
        "Шаг 5: получить итог и объяснение",
    )
    assert discord_ui["warn_count_before"] == telegram_ui["warn_count_before"] == 0
    assert discord_ui["warn_count_after"] == telegram_ui["warn_count_after"] == 1
    assert discord_ui["selected_actions"] == telegram_ui["selected_actions"] == ["warn", "mute", "fine_points"]
    assert discord_ui["selected_action_summary"] == telegram_ui["selected_action_summary"]
    assert discord_ui["next_step_text"] == telegram_ui["next_step_text"]
    assert discord_ui["violator_result_lines"] == telegram_ui["violator_result_lines"]
    assert "мут 6 ч." in discord_ui["selected_action_summary"]
    assert "штраф 10 баллов" in discord_ui["selected_action_summary"]


def test_rep_renderers_include_preview_and_result_explanations() -> None:
    ui_payload = {
        "preview_text": "Кто выбран как цель: Target\nКакой тип нарушения выбран: Спам\nАктивных предупреждений до применения: 1/5\nНаказание сейчас: мут 6 ч. + предупреждение + штраф 10 баллов\nЧто будет дальше: При следующем таком нарушении наказание усилится: бан.",
        "moderator_result_text": "Причина: Спам\nНаказание: мут 6 ч. + предупреждение + штраф 10 баллов\nУ вас теперь 2/5 предупреждений\nПри следующем таком нарушении наказание усилится: бан.",
        "case_id": 501,
    }

    preview_text = render_rep_preview_text(ui_payload)
    result_text = render_rep_result_text(ui_payload)

    assert "Шаг 1: выбрать нарушителя" in preview_text
    assert "Наказание сейчас: мут 6 ч. + предупреждение + штраф 10 баллов" in preview_text
    assert "Причина: Спам" in result_text
    assert "Кейс: #501" in result_text
