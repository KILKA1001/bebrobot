from pathlib import Path
import re
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
    shop_rows = [
        {"role_name": "Role 1", "display_position": 1, "effective_price_points": 11, "base_price_points": 11, "sale_price_points": None, "is_sale_active": False},
        {"role_name": "Role 2", "display_position": 0, "effective_price_points": 10, "base_price_points": 10, "sale_price_points": None, "is_sale_active": False},
        {"role_name": "Role 3", "display_position": 2, "effective_price_points": 12, "base_price_points": 12, "sale_price_points": None, "is_sale_active": False},
    ]
    with patch("bot.systems.shop_logic.RoleManagementService.list_public_roles_catalog", return_value=grouped), patch(
        "bot.systems.shop_logic.RoleManagementService.list_active_shop_role_items", return_value=shop_rows
    ):
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
            price_points=0,
            base_price_points=0,
            sale_price_points=None,
            is_sale_active=False,
        )
    ]
    with patch.object(shop_logic.logger, "error") as error_mock:
        found = shop_logic.find_shop_item(items, "missing")

    assert found is None
    error_mock.assert_called_once()


def test_shop_main_instruction_has_two_clear_lines():
    instruction_lines = [line.strip() for line in shop_logic.SHOP_RENDER_INSTRUCTION.splitlines() if line.strip()]

    assert len(instruction_lines) == 2
    assert "Выберите роль" in instruction_lines[0]
    assert "Проверьте цену" in instruction_lines[1]


def test_shop_texts_avoid_technical_wording_for_users():
    repo_root = Path(__file__).resolve().parents[1]
    telegram_shop_source = (repo_root / "bot" / "telegram_bot" / "commands" / "shop.py").read_text()
    discord_shop_source = (repo_root / "bot" / "commands" / "shop.py").read_text()

    def _extract_constant(source: str, name: str) -> str:
        match = re.search(rf"{name}\s*=\s*\((.*?)\)\n\n", source, flags=re.S)
        if match:
            return match.group(1)
        single_line_match = re.search(rf'{name}\s*=\s*"([^"]+)"', source)
        return single_line_match.group(1) if single_line_match else ""

    user_texts = [
        shop_logic.SHOP_RENDER_INSTRUCTION,
        shop_logic.SHOP_TEXT_CONFIRM_PURCHASE,
        shop_logic.SHOP_TEXT_ITEM_UNAVAILABLE,
        shop_logic.SHOP_TEXT_INSUFFICIENT_POINTS,
        shop_logic.SHOP_TEXT_PAGINATION_ERROR,
        shop_logic.SHOP_TEXT_ITEM_OPEN_ERROR,
        _extract_constant(telegram_shop_source, "SHOP_OPEN_PROMPT_TEXT"),
        _extract_constant(telegram_shop_source, "DM_FALLBACK_TEXT"),
        _extract_constant(discord_shop_source, "SHOP_OPEN_PROMPT_TEXT"),
        _extract_constant(discord_shop_source, "DM_FALLBACK_TEXT"),
    ]
    forbidden_words = ("pagination", "callback", "provider=", "stacktrace", "exception", "traceback")

    for text in user_texts:
        lowered = text.lower()
        for forbidden_word in forbidden_words:
            assert forbidden_word not in lowered


def test_shop_ux_checklist_contains_next_step_for_each_screen():
    assert len(shop_logic.SHOP_UX_CHECKLIST) == 4
    assert "следующий шаг" in shop_logic.SHOP_UX_CHECKLIST[0].lower()
    assert "кнопку «купить»" in shop_logic.SHOP_UX_CHECKLIST[1].lower()
