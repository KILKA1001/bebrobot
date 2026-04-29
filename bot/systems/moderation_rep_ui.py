"""
Назначение: модуль "moderation rep ui" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

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
    "• Наказание выбирается автоматически по типу нарушения и числу предупреждений именно по этому нарушению.",
    "• Бот использует сохранённые правила и показывает расчёт до применения.",
    "• Следующий шаг эскалации бот показывает сразу в предпросмотре.",
    "• Пока кейс не подтверждён, ничего не применяется.",
    "• Если расчёт выглядит неверно — отмените сценарий и обратитесь к старшему администратору.",
    "• Если при применении случится ошибка, действие не будет подтверждено.",
)


def render_rep_step_list() -> str:
    return "\n".join(f"• {step}" for step in REP_FLOW_STEPS)


def render_rep_how_it_works_list() -> str:
    return "\n".join(REP_HOW_IT_WORKS_LINES)


def render_rep_start_text(*, target_selection_hint: str, compact: bool = False) -> str:
    if compact:
        return (
            "🛡️ /rep — пошаговая модерация\n\n"
            "Что это: команда для выбора наказания без ручного ввода.\n"
            "Что делать сейчас: выберите нарушителя, затем тип нарушения.\n"
            "Что будет дальше: бот покажет расчёт и попросит подтверждение.\n\n"
            f"ℹ️ Как выбрать нарушителя: {target_selection_hint}"
        ).strip()
    return (
        "🛡️ /rep — единая интерактивная команда модерации\n\n"
        "Как пользоваться:\n"
        f"{render_rep_step_list()}\n\n"
        "Что важно заранее:\n"
        "• Это пошаговый сценарий, а не ручной ввод наказания.\n"
        "• Основа расчёта — выбранный тип нарушения из БД + текущее число предупреждений по этому нарушению.\n"
        "• До применения всегда будет предпросмотр с объяснением и следующим шагом эскалации.\n\n"
        "Как это работает:\n"
        f"{render_rep_how_it_works_list()}\n\n"
        f"ℹ️ Как выбрать нарушителя: {target_selection_hint}"
    ).strip()


def render_rep_target_prompt_text(*, target_selection_hint: str, target_label: str | None = None, compact: bool = False) -> str:
    if compact:
        lines = ["👤 Шаг 1/5: выбери нарушителя."]
        if target_label:
            lines.append(f"Сейчас: {target_label}")
        lines.extend(
            [
                "Reply — самый быстрый способ.",
                "После цели бот даст кнопки с нарушениями.",
                f"ℹ️ {target_selection_hint}",
            ]
        )
        return "\n".join(lines).strip()
    lines = ["👤 Шаг 1: выберите нарушителя."]
    if target_label:
        lines.append(f"Сейчас выбран: {target_label}")
    lines.extend(
        [
            "",
            "Микро-подсказки:",
            "• Reply — самый быстрый способ выбрать нарушителя.",
            "• Нарушение дальше выбирается из списка кнопками.",
            "",
            "Как это работает:",
            render_rep_how_it_works_list(),
            "",
            f"ℹ️ Как выбрать нарушителя: {target_selection_hint}",
        ]
    )
    return "\n".join(lines).strip()


def render_rep_violation_prompt_text(*, target_label: str, compact: bool = False) -> str:
    if compact:
        return (
            "📘 Шаг 2/5: выбери нарушение кнопками.\n"
            f"Цель: {target_label}\n"
            "Дальше бот сразу покажет preview: что было по предупреждениям для этого нарушения, что станет и что будет при повторе."
        ).strip()
    return (
        "📘 Шаг 2: выберите вид нарушения кнопками.\n\n"
        f"Цель: {target_label}\n"
        "После выбора бот сразу покажет авторасчёт наказания, текущий счётчик предупреждений по выбранному нарушению, "
        "следующий шаг эскалации и понятное объяснение результата."
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


def render_rep_authority_deny_text(message: str | None = None) -> str:
    return str(message or "Это действие сейчас недоступно для вашей роли. Проверьте цель, правило и полномочия.").strip()


def render_rep_target_not_found_text(*, target_selection_hint: str) -> str:
    return (
        "Не удалось определить нарушителя. Проверьте, что выбран живой пользователь, и попробуйте ещё раз.\n"
        f"ℹ️ Быстрый способ: {target_selection_hint}"
    ).strip()


def render_rep_preview_failed_text() -> str:
    return (
        "Не удалось собрать preview для /rep. Ничего не применено.\n"
        "Попробуйте открыть сценарий заново; если ошибка повторяется — обратитесь к старшему администратору."
    ).strip()


def render_rep_session_status_text(*, current_step: int, total_steps: int = 5, status_text: str | None = None) -> str:
    step_label = f"Шаг {current_step}/{total_steps}"
    if status_text:
        return f"{step_label} · {status_text}"
    return step_label


def _render_history_hint(ui_payload: dict[str, Any]) -> str:
    return str(
        ui_payload.get("history_hint")
        or "Подробности по кейсу (история, активные наказания, предупреждения и списания в банк) смотри в moderation cases и профиле пользователя."
    ).strip()


def _render_footer_hint(ui_payload: dict[str, Any]) -> str:
    return str(ui_payload.get("footer_hint") or "Если что-то выглядит неверно — отмените сценарий и проверьте историю пользователя.").strip()


def render_rep_preview_text(ui_payload: dict[str, Any], *, compact: bool = False) -> str:
    footer_hint = _render_footer_hint(ui_payload)
    how_it_works = ui_payload.get("how_it_works_text") or ""
    if compact:
        return (
            "🧾 Preview /rep\n"
            f"{ui_payload.get('preview_text') or ''}\n\n"
            "Как это работает:\n"
            f"{how_it_works}\n\n"
            f"ℹ️ {footer_hint}"
        ).strip()
    return (
        "🛡️ /rep — единая команда модерации\n"
        f"{ui_payload.get('preview_text') or ''}\n\n"
        "Как это работает:\n"
        f"{how_it_works}\n\n"
        "Дальше:\n"
        f"{render_rep_step_list()}\n\n"
        f"ℹ️ {footer_hint}"
    ).strip()


def render_rep_result_text(ui_payload: dict[str, Any], *, compact: bool = False) -> str:
    case_id = ui_payload.get("case_id")
    case_line = f"Кейс: #{case_id}\n" if case_id else ""
    if compact:
        return (
            "✅ /rep завершён\n"
            f"{case_line}"
            f"{ui_payload.get('moderator_result_text') or ''}\n\n"
            f"📚 {_render_history_hint(ui_payload)}"
        ).strip()
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
