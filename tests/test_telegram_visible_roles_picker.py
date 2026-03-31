"""
Назначение: модуль "test telegram visible roles picker" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from bot.telegram_bot.commands import linking


def test_visible_roles_catalog_sorted_by_category_and_role_name():
    catalog = linking._normalize_visible_roles_catalog(
        {
            "Вице": ["Beta", "alpha"],
            "Админ": ["zeta", "Alpha", "zeta"],
        }
    )
    assert [item["category"] for item in catalog[:4]] == ["Админ", "Админ", "Вице", "Вице"]
    assert [item["role"] for item in catalog] == ["Alpha", "zeta", "alpha", "Beta"]


def test_visible_roles_keyboard_has_two_columns_and_pagination():
    catalog = [{"category": "Cat", "role": f"Role {idx}"} for idx in range(11)]
    keyboard = linking._build_visible_roles_keyboard(catalog, selected_roles=["Role 1"], page=0)

    # 10 ролей на первой странице => 5 строк по 2 кнопки + nav + action row
    assert len(keyboard.inline_keyboard) == 7
    assert len(keyboard.inline_keyboard[0]) == 2
    assert keyboard.inline_keyboard[-2][-1].text == "➡️"
