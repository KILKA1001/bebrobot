"""
Назначение: модуль "test shop admin roles" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from bot.telegram_bot.commands.shop import _shop_admin_roles


def test_shop_admin_roles_reads_name_field_from_grouped_catalog(monkeypatch) -> None:
    monkeypatch.setattr(
        "bot.telegram_bot.commands.shop.RoleManagementService.list_roles_grouped",
        lambda **_: [
            {
                "category": "Категория",
                "roles": [
                    {"name": "SellableByName", "is_sellable": True},
                    {"role": "LegacySellableByRole", "is_sellable": True},
                    {"name": "HiddenFromSellable", "is_sellable": False},
                ],
            }
        ],
    )

    roles = _shop_admin_roles()

    assert roles == ["SellableByName", "LegacySellableByRole"]
