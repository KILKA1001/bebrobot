from __future__ import annotations

from datetime import datetime
from typing import Any


REP_FLOW_STEPS: tuple[str, ...] = (
    "Шаг 1: выбрать нарушителя",
    "Шаг 2: выбрать вид нарушения",
    "Шаг 3: увидеть авторасчёт наказания",
    "Шаг 4: подтвердить",
    "Шаг 5: получить итог и объяснение",
)
REP_HOW_IT_WORKS_LINES: tuple[str, ...] = (
    "• Наказание выбирается автоматически по типу нарушения и числу предупреждений.",
    "• Следующий шаг эскалации бот показывает сразу в предпросмотре.",
    "• Пока кейс не подтверждён, ничего не применяется.",
    "• Если при применении случится ошибка, кейс не должен считаться подтверждённым.",
)


def render_rep_step_list() -> str:
    return "\n".join(f"• {step}" for step in REP_FLOW_STEPS)


def render_rep_how_it_works_list() -> str:
    return "\n".join(REP_HOW_IT_WORKS_LINES)


def render_rep_start_text(*, target_selection_hint: str) -> str:
    return (
        "🛡️ /rep — единая команда модерации\n\n"
        "Как пользоваться:\n"
        f"{render_rep_step_list()}\n\n"
        "Как это работает:\n"
        f"{render_rep_how_it_works_list()}\n\n"
        f"ℹ️ Как выбрать нарушителя: {target_selection_hint}"
    ).strip()


def render_rep_target_prompt_text(*, target_selection_hint: str, target_label: str | None = None) -> str:
    lines = ["👤 Шаг 1: выберите нарушителя."]
    if target_label:
        lines.append(f"Сейчас выбран: {target_label}")
    lines.extend(
        [
            "",
            "Как это работает:",
            render_rep_how_it_works_list(),
            "",
            f"ℹ️ Как выбрать нарушителя: {target_selection_hint}",
        ]
    )
    return "\n".join(lines).strip()


def render_rep_violation_prompt_text(*, target_label: str) -> str:
    return (
        "📘 Шаг 2: выберите вид нарушения кнопками.\n\n"
        f"Цель: {target_label}\n"
        "После выбора бот сразу покажет авторасчёт наказания, текущий счётчик предупреждений "
        "и следующий шаг эскалации до подтверждения."
    ).strip()


def render_rep_cancelled_text() -> str:
    return "Сценарий /rep отменён. Никаких действий не применено. Запустите /rep заново, если нужно начать сначала."


def render_rep_expired_text() -> str:
    return "Сессия /rep истекла. Ничего не применено. Запустите /rep заново, чтобы начать новый кейс."


def render_rep_duplicate_submit_text() -> str:
    return "Кейс уже был подтверждён ранее. Повторное применение пропущено; ничего дополнительно не применено."


def render_rep_apply_error_text() -> str:
    return "Не удалось завершить /rep. Ничего не применено: обновите экран и попробуйте ещё раз."


def render_rep_foreign_actor_text() -> str:
    return "Эта панель /rep открыта другим модератором."


def _render_history_hint(ui_payload: dict[str, Any]) -> str:
    return str(ui_payload.get("history_hint") or "Историю кейсов, активные наказания, предупреждения и списания в банк смотри через журнал модерации и профиль пользователя.").strip()


def _render_footer_hint(ui_payload: dict[str, Any]) -> str:
    return str(ui_payload.get("footer_hint") or "Если что-то выглядит неверно — отмените сценарий и проверьте историю пользователя.").strip()


def render_rep_preview_text(ui_payload: dict[str, Any]) -> str:
    return (
        "🛡️ /rep — единая команда модерации\n"
        f"{ui_payload.get('preview_text') or ''}\n\n"
        "Как это работает:\n"
        f"{ui_payload.get('how_it_works_text') or ''}\n\n"
        "Дальше:\n"
        f"{render_rep_step_list()}\n\n"
        f"ℹ️ {_render_footer_hint(ui_payload)}"
    ).strip()


def render_rep_result_text(ui_payload: dict[str, Any]) -> str:
    case_id = ui_payload.get("case_id")
    case_line = f"Кейс: #{case_id}\n" if case_id else ""
    return (
        "✅ Модерация применена\n"
        f"{case_line}"
        f"{ui_payload.get('moderator_result_text') or ''}\n\n"
        f"📚 {_render_history_hint(ui_payload)}"
    ).strip()


def render_violator_notification_text(ui_payload: dict[str, Any]) -> str:
    delivered_at = datetime.utcnow().strftime("%d.%m.%Y %H:%M UTC")
    return (
        "🛡️ Решение модератора\n"
        f"{ui_payload.get('violator_result_text') or ''}\n\n"
        f"ℹ️ {_render_history_hint(ui_payload)}\n"
        f"Сообщение отправлено: {delivered_at}"
    ).strip()
