"""
Назначение: модуль "test telegram engagement target parse" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from types import SimpleNamespace

from bot.telegram_bot.commands.engagement import _parse_target_arg


def _make_entity(entity_type: str, user_id: int | None = None, url: str | None = None):
    user = SimpleNamespace(id=user_id) if user_id is not None else None
    return SimpleNamespace(type=entity_type, user=user, url=url)


def _make_message(
    text: str,
    *,
    entities=None,
    reply_to_message=None,
    from_user_id: int = 1,
    chat_id: int = 10,
):
    return SimpleNamespace(
        text=text,
        entities=entities or [],
        reply_to_message=reply_to_message,
        from_user=SimpleNamespace(id=from_user_id),
        chat=SimpleNamespace(id=chat_id),
    )


def test_parse_target_from_reply_text_mention_when_reply_author_is_bot():
    reply_message = SimpleNamespace(
        entities=[_make_entity("text_mention", user_id=777)],
        from_user=SimpleNamespace(id=999, is_bot=True),
    )
    message = _make_message("/points", reply_to_message=reply_message)

    assert _parse_target_arg(message) == 777


def test_parse_target_from_reply_text_link_when_reply_author_is_bot():
    reply_message = SimpleNamespace(
        entities=[_make_entity("text_link", url="tg://user?id=888")],
        from_user=SimpleNamespace(id=999, is_bot=True),
    )
    message = _make_message("/tickets", reply_to_message=reply_message)

    assert _parse_target_arg(message) == 888


def test_parse_target_from_reply_author_when_not_bot():
    reply_message = SimpleNamespace(
        entities=[],
        from_user=SimpleNamespace(id=321, is_bot=False),
    )
    message = _make_message("/points", reply_to_message=reply_message)

    assert _parse_target_arg(message) == 321
