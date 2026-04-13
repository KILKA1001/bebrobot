"""
Назначение: модуль "proposal ui texts" реализует продуктовый контур в зоне Discord/Telegram/общая логика.
Ответственность: единое формирование шагов, статусов и пояснений для сценария /proposal.
Где используется: команды proposal в Discord и Telegram.
"""

from __future__ import annotations

from typing import Iterable

_STATE_NOW_NEXT: dict[str, tuple[str, str]] = {
    "Ожидает запуска созыва": (
        "Созыв Совета ещё не запущен, поэтому вопрос временно находится в очереди.",
        "Проверьте раздел «Статус» позже: после запуска созыва вопрос автоматически перейдёт к следующему этапу.",
    ),
}

PROPOSAL_MENU_SECTIONS: tuple[str, ...] = (
    "Подать предложение",
    "Статус",
    "Архив решений",
    "Помощь",
)

ARCHIVE_PERIOD_LABELS: dict[str, str] = {
    "30d": "30 дней",
    "90d": "90 дней",
    "365d": "1 год",
    "all": "За всё время",
}
ARCHIVE_STATUS_LABELS: dict[str, str] = {
    "all": "Все статусы",
    "accepted": "Принято",
    "rejected": "Отклонено",
    "pending": "На рассмотрении",
}
ARCHIVE_TYPE_LABELS: dict[str, str] = {
    "all": "Все типы",
    "general": "Общие вопросы",
    "election": "Выборы",
    "other": "Другое",
}

PROPOSAL_HELP_STEPS: tuple[str, ...] = (
    "Нажмите «Подать предложение», чтобы открыть форму нового вопроса в Совет.",
    "Заполните заголовок и текст: заголовок помогает быстро понять суть, а текст фиксирует детали для рассмотрения.",
    "Проверьте экран подтверждения и, если нужно, нажмите «Изменить», чтобы исправить формулировку до отправки.",
    "Нажмите «Отправить», чтобы передать вопрос в очередь Совета.",
    "После отправки откройте «Статус», чтобы увидеть текущий этап, или «Архив решений», чтобы посмотреть предыдущие итоги.",
)


def render_menu_overview() -> str:
    sections = "\n".join(f"• {section}" for section in PROPOSAL_MENU_SECTIONS)
    return (
        "В этом меню одна команда закрывает весь сценарий:\n"
        f"{sections}\n\n"
        "Кнопки ниже ведут по шагам: сначала подайте вопрос, затем проверьте статус, после решения смотрите архив."
    )


def render_confirmation_prompt() -> str:
    return (
        "Проверьте текст перед отправкой в Совет.\n"
        "Если всё верно — нажмите «Отправить».\n"
        "Если нужно уточнение — нажмите «Изменить», затем снова перейдите к подтверждению."
    )


def render_help_text() -> str:
    lines = ["❓ Как пользоваться:"]
    for index, step in enumerate(PROPOSAL_HELP_STEPS, start=1):
        lines.append(f"{index}) {step}")
    return "\n".join(lines)


def render_submit_success_text(*, proposal_id: object, status_label: object) -> str:
    return (
        f"Номер: **#{proposal_id}**\n"
        f"Текущий статус: {status_label}\n\n"
        "Что будет дальше: откройте «Статус», чтобы следить за этапами, и вернитесь в «Архив решений», когда по вопросу появится итог."
    )


def build_submit_success_parts(*, proposal_id: object, status_label: object) -> dict[str, str]:
    return {
        "proposal_number": f"Номер: #{proposal_id}",
        "status": f"Текущий статус: {status_label}",
        "next_step": "Что будет дальше: откройте «Статус», чтобы следить за этапами, и вернитесь в «Архив решений», когда по вопросу появится итог.",
    }


def render_status_text(*, proposal_id: object, title: object, status_label: object, updated_at: object) -> str:
    status_line = _render_state_now_next(str(status_label or ""))
    return (
        f"Предложение: **#{proposal_id} — {title}**\n"
        f"Статус: {status_label}\n"
        f"Последнее обновление: `{updated_at or '—'}`\n\n"
        + status_line
    )


def build_status_parts(*, proposal_id: object, title: object, status_label: object, updated_at: object) -> dict[str, str]:
    status_line = _render_state_now_next(str(status_label or ""))
    return {
        "proposal": f"Предложение: #{proposal_id} — {title}",
        "status": f"Статус: {status_label}",
        "updated_at": f"Последнее обновление: {updated_at or '—'}",
        "next_step": status_line,
    }


def _render_state_now_next(status_label: str) -> str:
    for key, (what_now, what_next) in _STATE_NOW_NEXT.items():
        if key in status_label:
            return f"{what_now} {what_next}"
    return "Если нужен итог по завершённым вопросам, откройте «Архив решений»."


def render_archive_lines(rows: Iterable[dict[str, object]], *, text_limit: int) -> list[str]:
    lines: list[str] = []
    for row in rows:
        final_comment = str(row.get("final_comment") or row.get("decision_text") or "Без комментария")[:text_limit]
        lines.append(
            f"• #{row.get('id')} [{row.get('decision_code') or 'решение'}]\n"
            f"  Итоговый комментарий: {final_comment}"
        )
    return lines


def render_archive_empty_text() -> str:
    return "📚 Архив пока пуст. Когда появятся решения, они будут доступны в этом разделе. Пока можно проверить «Статус» по вашему текущему вопросу."


def render_archive_filters_text(*, period_code: str, status_code: str, question_type_code: str) -> str:
    return (
        "Текущие фильтры:\n"
        f"• Период: {ARCHIVE_PERIOD_LABELS.get(period_code, ARCHIVE_PERIOD_LABELS['90d'])}\n"
        f"• Статус: {ARCHIVE_STATUS_LABELS.get(status_code, ARCHIVE_STATUS_LABELS['all'])}\n"
        f"• Тип вопроса: {ARCHIVE_TYPE_LABELS.get(question_type_code, ARCHIVE_TYPE_LABELS['all'])}\n\n"
        "Следующий шаг: при необходимости меняйте фильтры кнопками ниже, чтобы сузить список решений."
    )
