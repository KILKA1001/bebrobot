"""
Назначение: модуль "test guiy trigger" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from bot.utils.guiy_trigger import is_guiy_name_trigger


def test_guiy_trigger_matches_name_at_start_middle_and_end() -> None:
    assert is_guiy_name_trigger("Гуй, привет")
    assert is_guiy_name_trigger("привет гуй брат")
    assert is_guiy_name_trigger("как дела, GUiY?")


def test_guiy_trigger_matches_common_russian_cases() -> None:
    assert is_guiy_name_trigger("ответь гую пожалуйста")
    assert is_guiy_name_trigger("я видел гуя вчера")
    assert is_guiy_name_trigger("пишу гуем")


def test_guiy_trigger_ignores_name_inside_longer_words() -> None:
    assert not is_guiy_name_trigger("подгуйка")
    assert not is_guiy_name_trigger("superguiybot")
