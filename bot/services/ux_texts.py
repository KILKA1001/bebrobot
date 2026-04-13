"""
Назначение: модуль "ux texts" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
Доменные операции: доменные операции модуля "ux texts".
"""

from __future__ import annotations

from typing import Literal, TypedDict


class StepText(TypedDict):
    what: str
    now: str
    next_step: str
    emoji: str


UxTextKey = Literal[
    "register_identity_missing",
    "register_success",
    "register_failed",
    "link_private_only",
    "link_menu_opened",
    "link_code_issued",
    "link_usage_help",
    "link_code_missing",
]


UX_STEP_TEXTS: dict[UxTextKey, StepText] = {
    "register_identity_missing": {
        "what": "Не получилось определить ваш профиль в текущем чате.",
        "now": "Закройте и снова откройте чат с ботом, затем повторите команду регистрации.",
        "next_step": "После успешной регистрации откроются профиль, роли, магазин и модерационные экраны.",
        "emoji": "❌",
    },
    "register_success": {
        "what": "Профиль создан.",
        "now": "Откройте команду профиля и проверьте, что данные отображаются корректно.",
        "next_step": "Теперь можно пользоваться командами ролей, магазина, модерации и другими экранами.",
        "emoji": "✅",
    },
    "register_failed": {
        "what": "Профиль пока не создан.",
        "now": "Подождите около минуты и повторите команду регистрации.",
        "next_step": "Если ошибка повторяется, передайте администратору текст ошибки из сообщения ниже.",
        "emoji": "❌",
    },
    "link_private_only": {
        "what": "Привязка аккаунтов доступна только в личном чате с ботом.",
        "now": "Откройте личные сообщения с ботом и повторите команду привязки.",
        "next_step": "Бот предложит шаги для получения или ввода кода привязки.",
        "emoji": "❌",
    },
    "link_menu_opened": {
        "what": "Открыто меню привязки Telegram и Discord.",
        "now": "Выберите действие: получить код или ввести код из другой платформы.",
        "next_step": "После подтверждения кода аккаунты будут связаны автоматически.",
        "emoji": "🔗",
    },
    "link_code_issued": {
        "what": "Код привязки готов.",
        "now": "Скопируйте код и отправьте его на другой платформе в команде привязки.",
        "next_step": "После ввода кода вторая платформа подтвердит связку аккаунтов.",
        "emoji": "🔗",
    },
    "link_usage_help": {
        "what": "Здесь настраивается привязка Telegram и Discord.",
        "now": "Выберите один из шагов: получить код для другой платформы или ввести полученный код.",
        "next_step": "После проверки кода связь сохранится, и обе платформы будут работать с одним профилем.",
        "emoji": "ℹ️",
    },
    "link_code_missing": {
        "what": "Код привязки не указан.",
        "now": "Введите команду с кодом из другой платформы или откройте шаг получения кода.",
        "next_step": "После ввода кода бот сразу покажет, успешно ли связаны аккаунты.",
        "emoji": "❌",
    },
}


def get_step_text(key: UxTextKey) -> StepText:
    return UX_STEP_TEXTS[key]


def compose_three_block_message(*, what: str, now: str, next_step: str, emoji: str | None = None) -> str:
    """Build a short 3-block message: what it is, what to do now, what happens next."""
    prefix = f"{emoji} " if emoji else ""
    return (
        f"{prefix}<b>Что это:</b> {what}\n"
        f"<b>Что делать сейчас:</b> {now}\n"
        f"<b>Что будет дальше:</b> {next_step}"
    )


def compose_three_block_plain(*, what: str, now: str, next_step: str, emoji: str | None = None) -> str:
    """Plain-text version for Discord and logs when HTML is not needed."""
    prefix = f"{emoji} " if emoji else ""
    return (
        f"{prefix}Что это: {what}\n"
        f"Что делать сейчас: {now}\n"
        f"Что будет дальше: {next_step}"
    )


def compose_status_message_html(key: UxTextKey) -> str:
    text = get_step_text(key)
    return compose_three_block_message(what=text["what"], now=text["now"], next_step=text["next_step"], emoji=text["emoji"])


def compose_status_message_plain(key: UxTextKey) -> str:
    text = get_step_text(key)
    return compose_three_block_plain(what=text["what"], now=text["now"], next_step=text["next_step"], emoji=text["emoji"])
