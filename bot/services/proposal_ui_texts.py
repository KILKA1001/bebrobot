"""
Назначение: модуль "proposal ui texts" реализует продуктовый контур в зоне Discord/Telegram/общая логика.
Ответственность: единое формирование шагов, статусов и пояснений для сценария /proposal.
Где используется: команды proposal в Discord и Telegram.
"""

from __future__ import annotations

from typing import Iterable

PROPOSAL_MENU_SECTIONS: tuple[str, ...] = (
    "Подать предложение",
    "Статус",
    "Архив решений",
    "Помощь",
)

PROPOSAL_HELP_STEPS: tuple[str, ...] = (
    "Нажмите «Подать предложение».",
    "Заполните заголовок и текст предложения.",
    "Проверьте экран подтверждения.",
    "Нажмите «Отправить».",
    "Статус обработки смотрите кнопкой «Статус».",
)


def render_menu_overview() -> str:
    sections = "\n".join(f"• {section}" for section in PROPOSAL_MENU_SECTIONS)
    return (
        "В этом меню одна команда закрывает весь сценарий:\n"
        f"{sections}\n\n"
        "Переходите кнопками ниже — дополнительные команды не нужны."
    )


def render_confirmation_prompt() -> str:
    return "Проверьте текст перед отправкой в Совет. Если нужно, нажмите «Изменить»."


def render_help_text() -> str:
    lines = ["❓ Как пользоваться:"]
    for index, step in enumerate(PROPOSAL_HELP_STEPS, start=1):
        lines.append(f"{index}) {step}")
    return "\n".join(lines)


def render_submit_success_text(*, proposal_id: object, status_label: object) -> str:
    return (
        f"Номер: **#{proposal_id}**\n"
        f"Текущий статус: {status_label}\n\n"
        "Что будет дальше: статус можно открыть кнопкой «Статус» в основном меню команды /proposal."
    )


def build_submit_success_parts(*, proposal_id: object, status_label: object) -> dict[str, str]:
    return {
        "proposal_number": f"Номер: #{proposal_id}",
        "status": f"Текущий статус: {status_label}",
        "next_step": "Что будет дальше: статус можно открыть кнопкой «Статус» в основном меню команды /proposal.",
    }


def render_status_text(*, proposal_id: object, title: object, status_label: object, updated_at: object) -> str:
    return (
        f"Предложение: **#{proposal_id} — {title}**\n"
        f"Статус: {status_label}\n"
        f"Последнее обновление: `{updated_at or '—'}`"
    )


def build_status_parts(*, proposal_id: object, title: object, status_label: object, updated_at: object) -> dict[str, str]:
    return {
        "proposal": f"Предложение: #{proposal_id} — {title}",
        "status": f"Статус: {status_label}",
        "updated_at": f"Последнее обновление: {updated_at or '—'}",
    }


def render_archive_lines(rows: Iterable[dict[str, object]], *, text_limit: int) -> list[str]:
    lines: list[str] = []
    for row in rows:
        lines.append(
            f"• #{row.get('id')} [{row.get('decision_code') or 'решение'}] {str(row.get('decision_text') or 'Без текста')[:text_limit]}"
        )
    return lines


def render_archive_empty_text() -> str:
    return "📚 Архив пока пуст. Когда появятся решения, они будут доступны в этом разделе."
