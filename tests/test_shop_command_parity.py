"""
Назначение: модуль "test shop command parity" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from pathlib import Path

from bot.services import shop_service


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_shop_commands_use_shared_service_layer_for_business_logic() -> None:
    telegram_source = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "shop.py").read_text()
    discord_source = (REPO_ROOT / "bot" / "commands" / "shop.py").read_text()

    assert "from bot.services.shop_service import" in telegram_source
    assert "from bot.services.shop_service import" in discord_source


def test_shop_parity_text_constants_are_shared() -> None:
    telegram_source = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "shop.py").read_text()
    discord_source = (REPO_ROOT / "bot" / "commands" / "shop.py").read_text()

    for shared_name in ("SHOP_TEXT_CONFIRM_PURCHASE", "SHOP_TEXT_PROTECTED_FAILURE", "SHOP_PAGE_SIZE"):
        assert shared_name in telegram_source
        assert shared_name in discord_source


def test_shop_parity_matrix_docs_platform_only_differences() -> None:
    doc = (REPO_ROOT / "docs" / "shop_ux_checklist.md").read_text()

    assert "Единая матрица сценариев" in doc
    assert "Только транспортные ограничения платформы" in doc
    assert "Только лимиты раскладки кнопок" in doc
    assert "Только ограничения длины/форматирования текста" in doc


def test_shop_service_keeps_admin_settings_scenario_in_parity_matrix() -> None:
    admin_rows = [row for row in shop_service.SHOP_FLOW_PARITY_MATRIX if row["scenario"] == "admin_settings"]

    assert admin_rows
    assert admin_rows[0]["platform_diff"] == "none"
