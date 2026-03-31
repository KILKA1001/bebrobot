"""
Назначение: модуль "test shop admin entry" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_shop_admin_button_is_not_in_roles_admin_help_anymore() -> None:
    telegram_roles_admin = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "roles_admin.py").read_text()
    discord_roles_admin = (REPO_ROOT / "bot" / "commands" / "roles_admin.py").read_text()

    assert "⚙️ Настройка магазина\", callback_data=f\"roles_admin:{actor_id}:shop_settings" not in telegram_roles_admin
    assert 'name="shop_settings"' not in discord_roles_admin


def test_shop_admin_entry_button_exists_in_shop_and_not_in_roles_admin() -> None:
    telegram_shop = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "shop.py").read_text()
    discord_shop = (REPO_ROOT / "bot" / "commands" / "shop.py").read_text()

    assert "⚙️ Настройка магазина" in telegram_shop
    assert "⚙️ Настройка магазина" in discord_shop


def test_shop_admin_denied_message_for_non_superadmin_direct_callback() -> None:
    telegram_shop = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "shop.py").read_text()

    assert "shop_admin_denied_not_superadmin" in telegram_shop
    assert '"Недостаточно прав"' in telegram_shop
