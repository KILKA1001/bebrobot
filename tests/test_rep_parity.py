from types import SimpleNamespace
from unittest.mock import patch

from bot.services.moderation_service import ModerationService
from bot.systems.core_logic import get_help_embed
from bot.systems.moderation_rep_ui import (
    REP_FLOW_STEPS,
    render_rep_apply_error_text,
    render_rep_authority_deny_text,
    render_rep_cancelled_text,
    render_rep_duplicate_submit_text,
    render_rep_expired_text,
    render_rep_preview_text,
    render_rep_preview_failed_text,
    render_rep_result_text,
    render_rep_session_status_text,
    render_rep_start_text,
    render_rep_target_not_found_text,
    render_rep_target_prompt_text,
    render_violator_notification_text,
)
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
                {
                    "id": 12,
                    "violation_type_id": 1,
                    "escalation_step": 3,
                    "warn_count_before": 2,
                    "apply_warn": False,
                    "mute_minutes": 0,
                    "fine_points": 0,
                    "apply_ban": True,
                    "is_active": True,
                    "description_for_admin": "Третий спам — бан",
                    "description_for_user": "Дальше будет бан",
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
    assert "reply/@username" in telegram_help
    assert "кнопками" in telegram_help
    assert "preview" in telegram_help
    assert "вручную вводить не нужно" in telegram_help
    assert "Рейтинг должников больше не используется" in telegram_help
    assert "кнопкой «Оплатить legacy-штраф»" in telegram_help
    assert "Рейтинг должников выведен из основного продукта" in regular_embed.description
    assert "/rep" in veteran_embed.description
    assert "reply/mention" in veteran_embed.description
    assert "preview" in veteran_embed.description
    assert "без ручного выбора наказания" in veteran_embed.description
    assert "активные наказания" in veteran_embed.description


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
    assert discord_ui["how_it_works_lines"] == telegram_ui["how_it_works_lines"]
    assert "Наказание выбрано автоматически" in discord_ui["how_it_works_text"]
    assert "мут 6 ч." in discord_ui["selected_action_summary"]
    assert "штраф 10 баллов" in discord_ui["selected_action_summary"]


def test_rep_renderers_include_preview_and_result_explanations() -> None:
    ui_payload = {
        "preview_text": "👤 Нарушитель: Target\n📘 Нарушение: Спам\n⚠️ Предупреждений до применения: 1/5\n🧮 Будет применено сейчас: мут 6 ч. + предупреждение + штраф 10 баллов\n📈 Предупреждений после применения: 2/5\n⏭️ Следующий шаг: При следующем таком нарушении наказание усилится: бан.",
        "how_it_works_text": "• Наказание выбрано автоматически по типу нарушения и числу предупреждений.\n• Изменение вручную в этом сценарии не требуется.\n• Если наказание выглядит неверным — отмените и проверьте историю пользователя.",
        "footer_hint": "Если наказание выглядит неверным — отмените и проверьте историю пользователя.",
        "moderator_result_text": "Кейс #501 создан\nВыдан мут на 6 ч.\nДобавлено предупреждение: 2/5\nСписан штраф 10 баллов в банк\nПри следующем таком нарушении наказание усилится: бан.",
        "violator_result_text": "Нарушение: Спам\nПрименено наказание: мут 6 ч. + предупреждение + штраф 10 баллов\nПредупреждений теперь: 2/5\nМут закончится: 24.03.2026 10:00 UTC\nНаказание выбирается автоматически по типу нарушения и числу предупреждений.\nПри следующем таком нарушении наказание усилится: бан.\nЧтобы избежать следующего усиления, не повторяйте это нарушение и при необходимости запросите у модератора историю кейсов, активные наказания, историю нарушений и текущий счётчик предупреждений.",
        "history_hint": "Историю кейсов, активные наказания, историю нарушений и списания в банк по кейсу смотри в журнале moderation cases и профиле пользователя.",
        "case_id": 501,
    }

    preview_text = render_rep_preview_text(ui_payload)
    compact_preview_text = render_rep_preview_text(ui_payload, compact=True)
    result_text = render_rep_result_text(ui_payload)
    violator_text = render_violator_notification_text(ui_payload)

    assert "Шаг 1: выбрать нарушителя" in preview_text
    assert "Как это работает:" in preview_text
    assert "Будет применено сейчас: мут 6 ч. + предупреждение + штраф 10 баллов" in preview_text
    assert "Если наказание выглядит неверным" in preview_text
    assert "Preview /rep" in compact_preview_text
    assert "Кейс: #501" in result_text
    assert "Историю кейсов" in result_text
    assert "профиле пользователя" in result_text
    assert "Мут закончится" in violator_text
    assert "Чтобы избежать следующего усиления" in violator_text


def test_rep_service_keeps_human_readable_authority_deny_text() -> None:
    fake_db = _FakeDb()

    def _resolve_account_id(provider: str, provider_user_id: str) -> str:
        return f"acc-{provider}-{provider_user_id}"

    authority = SimpleNamespace(allowed=False, message="Вы можете выдавать только мут участникам", deny_reason="action_not_permitted")
    with patch("bot.services.moderation_service.db", fake_db), patch(
        "bot.services.moderation_service.AccountsService.resolve_account_id",
        side_effect=_resolve_account_id,
    ), patch(
        "bot.services.moderation_service.AuthorityService.can_apply_moderation_action",
        return_value=authority,
    ):
        denied = ModerationService.prepare_moderation_payload(
            "telegram",
            {"provider": "telegram", "provider_user_id": "10", "label": "Mod"},
            {"provider": "telegram", "provider_user_id": "20", "label": "Target"},
            "spam",
            {"chat_id": 100},
        )

    assert denied["ok"] is False
    assert denied["error_code"] == "action_not_permitted"
    assert denied["message"] == "Вы можете выдавать только мут участникам"


def test_rep_shared_copy_keeps_same_user_explanations_for_both_platform_entries() -> None:
    start_text = render_rep_start_text(target_selection_hint="reply/mention или @username/id в зависимости от платформы")
    compact_start_text = render_rep_start_text(
        target_selection_hint="reply/@username",
        compact=True,
    )
    target_text = render_rep_target_prompt_text(
        target_selection_hint="reply/mention или @username/id в зависимости от платформы",
        target_label="Target",
    )
    compact_target_text = render_rep_target_prompt_text(
        target_selection_hint="reply/@username",
        target_label="Target",
        compact=True,
    )

    assert "Наказание выбирается автоматически по типу нарушения и числу предупреждений" in start_text
    assert "интерактивная команда" in start_text
    assert "Основа расчёта" in start_text
    assert "До применения всегда будет предпросмотр" in start_text
    assert "Следующий шаг эскалации бот показывает сразу в предпросмотре" in start_text
    assert "Если при применении случится ошибка, кейс не должен считаться подтверждённым" in start_text
    assert "Сейчас выбран: Target" in target_text
    assert "Reply — самый быстрый способ" in target_text
    assert "reply/mention или @username/id" in target_text
    assert "preview" in compact_start_text
    assert "вводить вручную не нужно" in compact_start_text
    assert "Сейчас: Target" in compact_target_text


def test_rep_shared_copy_covers_cancel_duplicate_expired_and_error_states() -> None:
    assert "Никаких действий не применено" in render_rep_cancelled_text()
    assert "Ничего не применено" in render_rep_expired_text()
    assert "Повторное применение пропущено" in render_rep_duplicate_submit_text()
    assert "Ничего не применено" in render_rep_apply_error_text()
    assert "вашей роли" in render_rep_authority_deny_text()
    assert "Не удалось определить нарушителя" in render_rep_target_not_found_text(target_selection_hint="reply")
    assert "Не удалось собрать preview" in render_rep_preview_failed_text()
    assert "Шаг 3/5" in render_rep_session_status_text(current_step=3)
