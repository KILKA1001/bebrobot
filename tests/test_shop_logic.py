from unittest.mock import patch

from bot.systems import shop_logic


def test_shop_catalog_items_sorted_and_paged():
    grouped = [
        {
            "category": "B",
            "position": 2,
            "roles": [
                {"name": "Role 3", "position": 3, "description": "d", "acquire_hint": "h"},
                {"name": "Role 1", "position": 1, "description": "", "acquire_hint": ""},
            ],
        },
        {
            "category": "A",
            "position": 1,
            "roles": [
                {"name": "Role 2", "position": 2, "description": "", "acquire_hint": ""},
            ],
        },
    ]
    with patch("bot.systems.shop_logic.RoleManagementService.list_public_roles_catalog", return_value=grouped):
        items = shop_logic.get_shop_catalog_items(log_context="test")

    assert [item.role_name for item in items] == ["Role 2", "Role 1", "Role 3"]
    assert items[0].shop_item_id == "shop_1"
    page = shop_logic.get_shop_page_slice(items, requested_page=0, page_size=2)
    assert page.total_pages == 2
    assert [item.role_name for item in page.items] == ["Role 2", "Role 1"]


def test_find_shop_item_logs_when_missing():
    items = [
        shop_logic.ShopItem(
            shop_item_id="shop_1",
            role_name="Alpha",
            short_name="Alpha",
            category="Cat",
            position=0,
            category_position=0,
            description="",
            acquire_hint="",
        )
    ]
    with patch.object(shop_logic.logger, "error") as error_mock:
        found = shop_logic.find_shop_item(items, "missing")

    assert found is None
    error_mock.assert_called_once()
