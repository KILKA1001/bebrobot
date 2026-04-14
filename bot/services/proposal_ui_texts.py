"""
Назначение: модуль "proposal ui texts" реализует продуктовый контур в зоне Discord/Telegram/общая логика.
Ответственность: единое формирование шагов, статусов и пояснений для сценария /proposal.
Где используется: команды proposal в Discord и Telegram.
"""

from __future__ import annotations

from dataclasses import dataclass
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



MENU_ACTION_EXPLANATIONS: tuple[str, ...] = (
    "📝 «Подать предложение» — начать новый вопрос для Совета.",
    "📍 «Статус» — проверить текущий этап по вашему последнему вопросу.",
    "📚 «Архив решений» — открыть уже завершённые решения Совета.",
    "❓ «Помощь» — посмотреть короткую пошаговую инструкцию.",
)


PROPOSAL_ADMIN_SETTINGS_FLOW_STEPS: tuple[str, ...] = (
    "1) Откройте «Настройки Совета».",
    "2) Выберите раздел, затем действие.",
    "3) Если бот запросил подтверждение, проверьте текст и нажмите «Подтвердить».",
    "4) После выполнения действия проверьте блок «Следующий шаг» и выполните его.",
)
PROPOSAL_HELP_STEPS: tuple[str, ...] = (
    "Нажмите «Подать предложение», чтобы открыть форму нового вопроса в Совет.",
    "Заполните заголовок и текст: заголовок помогает быстро понять суть, а текст фиксирует детали для рассмотрения.",
    "Проверьте экран подтверждения и, если нужно, нажмите «Изменить», чтобы исправить формулировку до отправки.",
    "Нажмите «Отправить», чтобы передать вопрос в очередь Совета.",
    "После отправки откройте «Статус», чтобы увидеть текущий этап, или «Архив решений», чтобы посмотреть предыдущие итоги.",
)


@dataclass(frozen=True, slots=True)
class ProposalAdminAction:
    code: str
    title: str
    hint: str
    success_text: str
    next_step: str
    requires_confirmation: bool = False


@dataclass(frozen=True, slots=True)
class ProposalAdminSection:
    code: str
    title: str
    description: str
    actions: tuple[ProposalAdminAction, ...]


PROPOSAL_ADMIN_SECTIONS: tuple[ProposalAdminSection, ...] = (
    ProposalAdminSection(
        code="term",
        title="Созыв",
        description="Проверяйте текущий созыв, запускайте новый и завершайте действующий.",
        actions=(
            ProposalAdminAction(
                code="term_show_current",
                title="Показать текущий созыв",
                hint="Покажет активный созыв и его состояние на сейчас.",
                success_text="Информация о текущем созыве обновлена.",
                next_step="Проверьте, нужен ли запуск нового созыва или завершение текущего.",
            ),
            ProposalAdminAction(
                code="term_start",
                title="Запустить созыв",
                hint="Запустит новый созыв Совета и откроет рабочий цикл.",
                success_text="Созыв запущен.",
                next_step="Следующий шаг: подготовьте выборы и откройте приём кандидатов.",
            ),
            ProposalAdminAction(
                code="term_finish",
                title="Завершить созыв",
                hint="Остановит текущий созыв и завершит его этапы.",
                success_text="Созыв завершён.",
                next_step="Следующий шаг: зафиксируйте итоги и при необходимости начните подготовку нового созыва.",
                requires_confirmation=True,
            ),
        ),
    ),
    ProposalAdminSection(
        code="election",
        title="Выборы",
        description="Создавайте выборы по роли и управляйте этапами кандидатов и голосования.",
        actions=(
            ProposalAdminAction(
                code="election_create_by_role",
                title="Создать по роли",
                hint="Создаст выборы для выбранной роли Совета.",
                success_text="Выборы созданы по роли.",
                next_step="Следующий шаг: откройте приём кандидатов и сообщите участникам о старте.",
            ),
            ProposalAdminAction(
                code="election_open_candidates",
                title="Открыть приём кандидатов",
                hint="Откроет подачу заявок для участия в выборах.",
                success_text="Приём кандидатов открыт.",
                next_step="Следующий шаг: следите за списком кандидатов и подтверждайте заявки.",
            ),
            ProposalAdminAction(
                code="election_close_candidates",
                title="Закрыть приём кандидатов",
                hint="Закроет подачу новых заявок в текущих выборах.",
                success_text="Приём кандидатов закрыт.",
                next_step="Следующий шаг: проверьте финальный список кандидатов и подготовьте запуск голосования.",
            ),
            ProposalAdminAction(
                code="election_start_voting",
                title="Запустить голосование",
                hint="Откроет голосование по текущим выборам.",
                success_text="Голосование запущено.",
                next_step="Следующий шаг: контролируйте участие и затем завершите голосование.",
                requires_confirmation=True,
            ),
            ProposalAdminAction(
                code="election_finish_voting",
                title="Завершить голосование",
                hint="Закроет голосование и перейдёт к этапу итогов.",
                success_text="Голосование завершено.",
                next_step="Следующий шаг: сформируйте предварительные и финальные итоги.",
                requires_confirmation=True,
            ),
        ),
    ),
    ProposalAdminSection(
        code="candidates",
        title="Кандидаты",
        description="Просматривайте список, подтверждайте или отклоняйте заявки и добавляйте кандидатов вручную.",
        actions=(
            ProposalAdminAction(
                code="candidates_list",
                title="Список кандидатов",
                hint="Покажет всех кандидатов в текущих выборах.",
                success_text="Список кандидатов обновлён.",
                next_step="Следующий шаг: подтвердите или отклоните заявки по списку.",
            ),
            ProposalAdminAction(
                code="candidates_approve",
                title="Подтвердить кандидата",
                hint="Подтвердит выбранную заявку кандидата.",
                success_text="Кандидат подтверждён.",
                next_step="Следующий шаг: проверьте, что кандидат появился в подтверждённом списке.",
            ),
            ProposalAdminAction(
                code="candidates_reject",
                title="Отклонить кандидата",
                hint="Отклонит выбранную заявку кандидата.",
                success_text="Заявка кандидата отклонена.",
                next_step="Следующий шаг: при необходимости сообщите кандидату причину и проверьте остальные заявки.",
            ),
            ProposalAdminAction(
                code="candidates_manual_add",
                title="Добавить вручную",
                hint="Добавит кандидата вручную, если текущий статус выборов это позволяет.",
                success_text="Кандидат добавлен вручную.",
                next_step="Следующий шаг: убедитесь, что кандидат есть в списке и может участвовать в выборах.",
            ),
        ),
    ),
    ProposalAdminSection(
        code="results",
        title="Итоги",
        description="Публикуйте предварительные и финальные итоги и фиксируйте итоговое решение.",
        actions=(
            ProposalAdminAction(
                code="results_preliminary",
                title="Предварительные итоги",
                hint="Покажет предварительные итоги по текущему голосованию.",
                success_text="Предварительные итоги подготовлены.",
                next_step="Следующий шаг: проверьте корректность данных перед публикацией финального итога.",
            ),
            ProposalAdminAction(
                code="results_final",
                title="Финальные итоги",
                hint="Подготовит финальные итоги для публикации.",
                success_text="Финальные итоги подготовлены.",
                next_step="Следующий шаг: зафиксируйте решение, чтобы завершить процесс.",
            ),
            ProposalAdminAction(
                code="results_lock_decision",
                title="Зафиксировать решение",
                hint="Зафиксирует итоговое решение и завершит цикл по текущему вопросу.",
                success_text="Решение зафиксировано.",
                next_step="Следующий шаг: уведомьте участников и перейдите к следующему вопросу.",
                requires_confirmation=True,
            ),
        ),
    ),
    ProposalAdminSection(
        code="events",
        title="Системные уведомления",
        description="Настройка канала, куда бот отправляет служебные сообщения Совета.",
        actions=(
            ProposalAdminAction(
                code="events_show_channel",
                title="Показать канал событий",
                hint="Покажет, куда сейчас отправляются служебные сообщения.",
                success_text="Текущий канал уведомлений показан.",
                next_step="Следующий шаг: при необходимости назначьте текущий канал или очистите настройку.",
            ),
            ProposalAdminAction(
                code="events_set_channel_here",
                title="Выбрать из доступных",
                hint="Откроет список чатов и каналов, куда бот может отправлять служебные уведомления Совета.",
                success_text="Канал уведомлений сохранён.",
                next_step="Следующий шаг: выберите чат или канал, затем подтвердите сохранение.",
            ),
            ProposalAdminAction(
                code="events_clear_channel",
                title="Очистить канал",
                hint="Отключит отправку служебных уведомлений в текущий канал.",
                success_text="Канал уведомлений очищен.",
                next_step="Следующий шаг: назначьте новый канал, если уведомления нужно вернуть.",
            ),
        ),
    ),
)

PROPOSAL_ADMIN_SECTION_BY_CODE: dict[str, ProposalAdminSection] = {section.code: section for section in PROPOSAL_ADMIN_SECTIONS}
PROPOSAL_ADMIN_ACTION_BY_CODE: dict[str, ProposalAdminAction] = {
    action.code: action for section in PROPOSAL_ADMIN_SECTIONS for action in section.actions
}


def render_menu_overview() -> str:
    sections = "\n".join(f"• {section}" for section in PROPOSAL_MENU_SECTIONS)
    return (
        "В этом меню одна команда закрывает весь сценарий:\n"
        f"{sections}\n\n"
        "Кнопки ниже ведут по шагам: сначала подайте вопрос, затем проверьте статус, после решения смотрите архив."
    )


def render_menu_action_explanations() -> str:
    return "\n".join(MENU_ACTION_EXPLANATIONS)


def render_submit_form_text() -> str:
    return (
        "📝 <b>Форма подачи</b>\n"
        "Отправьте одним сообщением: заголовок и текст предложения.\n\n"
        "Формат:\n"
        "<code>Заголовок\n\nТекст предложения</code>"
    )


def render_submit_review_text(*, title: str, proposal_text: str) -> str:
    return (
        "📨 <b>Подтверждение отправки</b>\n"
        f"<b>Заголовок:</b> {title}\n"
        f"<b>Текст:</b> {proposal_text}\n\n"
        + render_confirmation_prompt()
    )


def render_admin_settings_flow_text() -> str:
    return "\n".join(PROPOSAL_ADMIN_SETTINGS_FLOW_STEPS)


def render_events_pick_confirmation_text(*, destination_label: str) -> str:
    return (
        f"Вы выбрали: <b>{destination_label}</b>\n"
        "После сохранения системные события Совета будут отправляться сюда."
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


def render_admin_root_text() -> str:
    lines = ["⚙️ <b>Админ-меню Совета</b>", "", "Выберите раздел. В каждом разделе кнопка сразу подсказывает, что произойдёт после нажатия.", "", render_admin_settings_flow_text(), ""]
    for section in PROPOSAL_ADMIN_SECTIONS:
        lines.append(f"• <b>{section.title}</b> — {section.description}")
    return "\n".join(lines)


def render_admin_section_text(section_code: str) -> str:
    section = PROPOSAL_ADMIN_SECTION_BY_CODE.get(section_code)
    if not section:
        return "⚙️ <b>Админ-меню Совета</b>\n\nРаздел не найден. Выберите другой раздел."
    lines = [f"⚙️ <b>{section.title}</b>", "", section.description, "", "Доступные действия:"]
    for action in section.actions:
        lines.append(f"• <b>{action.title}</b> — {action.hint}")
    return "\n".join(lines)


def render_admin_confirm_text(action_code: str) -> str:
    action = PROPOSAL_ADMIN_ACTION_BY_CODE.get(action_code)
    if not action:
        return "❌ Действие не найдено."
    return (
        f"⚠️ <b>Подтверждение действия</b>\n\n"
        f"Вы выбрали: <b>{action.title}</b>.\n"
        f"После нажатия «Подтвердить» бот выполнит действие: {action.hint.lower()}"
    )


def render_admin_action_result(action_code: str, *, custom_result: str | None = None) -> str:
    action = PROPOSAL_ADMIN_ACTION_BY_CODE.get(action_code)
    if not action:
        return "❌ Действие не найдено."
    result_line = custom_result or f"✅ {action.success_text}"
    return f"{result_line}\nСледующий шаг: {action.next_step}"


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
