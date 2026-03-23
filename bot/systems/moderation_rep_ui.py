from __future__ import annotations

from typing import Any


REP_FLOW_STEPS: tuple[str, ...] = (
    "Шаг 1: выбрать нарушителя",
    "Шаг 2: выбрать вид нарушения",
    "Шаг 3: увидеть авторасчёт наказания",
    "Шаг 4: подтвердить",
    "Шаг 5: получить итог и объяснение",
)


def render_rep_step_list() -> str:
    return "\n".join(f"• {step}" for step in REP_FLOW_STEPS)


def render_rep_preview_text(ui_payload: dict[str, Any]) -> str:
    return (
        "🛡️ /rep — единая команда модерации\n"
        f"{ui_payload.get('preview_text') or ''}\n\n"
        "Дальше:\n"
        f"{render_rep_step_list()}"
    ).strip()


def render_rep_result_text(ui_payload: dict[str, Any]) -> str:
    case_id = ui_payload.get("case_id")
    case_line = f"Кейс: #{case_id}\n" if case_id else ""
    return (
        "✅ Модерация применена\n"
        f"{case_line}"
        f"{ui_payload.get('moderator_result_text') or ''}"
    ).strip()


def render_violator_notification_text(ui_payload: dict[str, Any]) -> str:
    return (
        "🛡️ Решение модератора\n"
        f"{ui_payload.get('violator_result_text') or ''}"
    ).strip()
