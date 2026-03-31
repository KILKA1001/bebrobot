from pathlib import Path
import re
from unittest.mock import patch

from bot.services import shop_service as shop_logic


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
    with patch("bot.services.shop_service.RoleManagementService.list_public_roles_catalog", return_value=grouped), patch(
        "bot.services.shop_service.RoleManagementService.list_active_shop_role_items", return_value=shop_rows
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


def test_shop_flow_parity_matrix_covers_required_scenarios():
    expected = {
        "shop_entry",
        "dm_transfer",
        "profile_check",
        "category_selection",
        "pagination",
        "item_card",
        "purchase",
        "back_to_shop",
        "admin_settings",
    }
    matrix_scenarios = {row["scenario"] for row in shop_logic.SHOP_FLOW_PARITY_MATRIX}

    assert expected.issubset(matrix_scenarios)


def test_shop_flow_parity_matrix_limits_platform_differences():
    allowed_differences = {"none", "transport_only", "button_layout_limits", "text_length_limits"}

    for row in shop_logic.SHOP_FLOW_PARITY_MATRIX:
        assert row["platform_diff"] in allowed_differences


def test_purchase_shop_item_requires_previous_volunteer_role():
    item = shop_logic.ShopItem(
        shop_item_id="shop_1",
        role_name="Хороший Помощник Бебр",
        short_name="Хороший Помощник Бебр",
        category="Роли",
        position=0,
        category_position=0,
        description="",
        acquire_hint="",
        price_points=10,
        base_price_points=10,
        sale_price_points=None,
        is_sale_active=False,
    )
    with patch("bot.services.shop_service.get_shop_catalog_items", return_value=[item]), patch(
        "bot.services.shop_service.RoleManagementService.get_role",
        return_value={"is_sellable": True, "discord_role_id": "1105906455233703989"},
    ), patch(
        "bot.services.shop_service.RoleManagementService.get_user_roles_by_account", return_value=[]
    ), patch(
        "bot.services.shop_service.AccountsService.get_profile_by_account", return_value={"points": 200}
    ), patch(
        "bot.services.shop_service.PointsService.remove_points_by_account"
    ) as debit_mock:
        result = shop_logic.purchase_shop_item(
            account_id="acc-1",
            shop_item_id="shop_1",
            actor_provider="telegram",
            actor_user_id="100",
        )

    assert result.ok is False
    assert result.reason == "missing_chain_prerequisite"
    assert "сначала купите роль «Новый волонтер»" in result.message
    debit_mock.assert_not_called()


def test_purchase_shop_item_allows_volunteer_role_when_previous_owned():
    item = shop_logic.ShopItem(
        shop_item_id="shop_1",
        role_name="Хороший Помощник Бебр",
        short_name="Хороший Помощник Бебр",
        category="Роли",
        position=0,
        category_position=0,
        description="",
        acquire_hint="",
        price_points=10,
        base_price_points=10,
        sale_price_points=None,
        is_sale_active=False,
    )
    with patch("bot.services.shop_service.get_shop_catalog_items", return_value=[item]), patch(
        "bot.services.shop_service.RoleManagementService.get_role",
        return_value={"is_sellable": True, "discord_role_id": "1105906455233703989"},
    ), patch(
        "bot.services.shop_service.RoleManagementService.get_user_roles_by_account",
        return_value=[{"name": "Новый волонтер"}],
    ), patch(
        "bot.services.shop_service.AccountsService.get_profile_by_account", return_value={"points": 200}
    ), patch(
        "bot.services.shop_service.PointsService.remove_points_by_account", return_value=True
    ), patch(
        "bot.services.shop_service.RoleManagementService.assign_user_role_by_account", return_value={"ok": True}
    ):
        result = shop_logic.purchase_shop_item(
            account_id="acc-1",
            shop_item_id="shop_1",
            actor_provider="telegram",
            actor_user_id="100",
        )

    assert result.ok is True
    assert result.reason is None
    assert result.spent_points == 10


def test_shop_catalog_hides_locked_volunteer_roles_for_account():
    grouped = [
        {
            "category": "Роли за баллы",
            "position": 1,
            "roles": [
                {"name": "Новый волонтер", "position": 0, "description": "", "acquire_hint": "", "discord_role_id": "1105906310131744868"},
                {"name": "Хороший Помощник Бебр", "position": 1, "description": "", "acquire_hint": "", "discord_role_id": "1105906455233703989"},
            ],
        }
    ]
    shop_rows = [
        {"role_name": "Новый волонтер", "display_position": 0, "effective_price_points": 5, "base_price_points": 5, "sale_price_points": None, "is_sale_active": False},
        {"role_name": "Хороший Помощник Бебр", "display_position": 1, "effective_price_points": 10, "base_price_points": 10, "sale_price_points": None, "is_sale_active": False},
    ]
    with patch("bot.services.shop_service.RoleManagementService.list_public_roles_catalog", return_value=grouped), patch(
        "bot.services.shop_service.RoleManagementService.list_active_shop_role_items", return_value=shop_rows
    ), patch(
        "bot.services.shop_service.RoleManagementService.get_user_roles_by_account", return_value=[]
    ):
        items = shop_logic.get_shop_catalog_items(log_context="test", account_id="acc-1")

    assert [item.role_name for item in items] == ["Новый волонтер"]


def test_shop_catalog_shows_next_volunteer_role_when_previous_owned():
    grouped = [
        {
            "category": "Роли за баллы",
            "position": 1,
            "roles": [
                {"name": "Новый волонтер", "position": 0, "description": "", "acquire_hint": "", "discord_role_id": "1105906310131744868"},
                {"name": "Хороший Помощник Бебр", "position": 1, "description": "", "acquire_hint": "", "discord_role_id": "1105906455233703989"},
            ],
        }
    ]
    shop_rows = [
        {"role_name": "Новый волонтер", "display_position": 0, "effective_price_points": 5, "base_price_points": 5, "sale_price_points": None, "is_sale_active": False},
        {"role_name": "Хороший Помощник Бебр", "display_position": 1, "effective_price_points": 10, "base_price_points": 10, "sale_price_points": None, "is_sale_active": False},
    ]
    with patch("bot.services.shop_service.RoleManagementService.list_public_roles_catalog", return_value=grouped), patch(
        "bot.services.shop_service.RoleManagementService.list_active_shop_role_items", return_value=shop_rows
    ), patch(
        "bot.services.shop_service.RoleManagementService.get_user_roles_by_account",
        return_value=[{"name": "Новый волонтер"}],
    ):
        items = shop_logic.get_shop_catalog_items(log_context="test", account_id="acc-1")

    assert [item.role_name for item in items] == ["Новый волонтер", "Хороший Помощник Бебр"]


def test_shop_catalog_hides_lower_volunteer_roles_after_upgrade():
    grouped = [
        {
            "category": "Роли за баллы",
            "position": 1,
            "roles": [
                {"name": "Новый волонтер", "position": 0, "description": "", "acquire_hint": "", "discord_role_id": "1105906310131744868"},
                {"name": "Хороший Помощник Бебр", "position": 1, "description": "", "acquire_hint": "", "discord_role_id": "1105906455233703989"},
                {"name": "Мастер волонтер", "position": 2, "description": "", "acquire_hint": "", "discord_role_id": "1137775519589466203"},
                {"name": "Легендарный среди волонтеров", "position": 3, "description": "", "acquire_hint": "", "discord_role_id": "1105906637824331788"},
            ],
        }
    ]
    shop_rows = [
        {"role_name": "Новый волонтер", "display_position": 0, "effective_price_points": 5, "base_price_points": 5, "sale_price_points": None, "is_sale_active": False},
        {"role_name": "Хороший Помощник Бебр", "display_position": 1, "effective_price_points": 10, "base_price_points": 10, "sale_price_points": None, "is_sale_active": False},
        {"role_name": "Мастер волонтер", "display_position": 2, "effective_price_points": 20, "base_price_points": 20, "sale_price_points": None, "is_sale_active": False},
        {"role_name": "Легендарный среди волонтеров", "display_position": 3, "effective_price_points": 30, "base_price_points": 30, "sale_price_points": None, "is_sale_active": False},
    ]
    with patch("bot.services.shop_service.RoleManagementService.list_public_roles_catalog", return_value=grouped), patch(
        "bot.services.shop_service.RoleManagementService.list_active_shop_role_items", return_value=shop_rows
    ), patch(
        "bot.services.shop_service.RoleManagementService.get_user_roles_by_account",
        return_value=[{"name": "Мастер волонтер"}],
    ), patch(
        "bot.services.shop_service.RoleManagementService.get_role",
        side_effect=lambda role_name: {
            "Новый волонтер": {"discord_role_id": "1105906310131744868"},
            "Хороший Помощник Бебр": {"discord_role_id": "1105906455233703989"},
            "Мастер волонтер": {"discord_role_id": "1137775519589466203"},
            "Легендарный среди волонтеров": {"discord_role_id": "1105906637824331788"},
        }.get(role_name, {}),
    ):
        items = shop_logic.get_shop_catalog_items(log_context="test", account_id="acc-1")

    assert [item.role_name for item in items] == ["Мастер волонтер", "Легендарный среди волонтеров"]


def test_purchase_shop_item_revoke_lower_volunteer_roles_on_upgrade():
    item = shop_logic.ShopItem(
        shop_item_id="shop_1",
        role_name="Мастер волонтер",
        short_name="Мастер волонтер",
        category="Роли",
        position=0,
        category_position=0,
        description="",
        acquire_hint="",
        price_points=10,
        base_price_points=10,
        sale_price_points=None,
        is_sale_active=False,
    )
    with patch("bot.services.shop_service.get_shop_catalog_items", return_value=[item]), patch(
        "bot.services.shop_service.RoleManagementService.get_role",
        return_value={"is_sellable": True, "discord_role_id": "1137775519589466203"},
    ), patch(
        "bot.services.shop_service.RoleManagementService.get_user_roles_by_account",
        return_value=[{"name": "Хороший Помощник Бебр"}],
    ), patch(
        "bot.services.shop_service.AccountsService.get_profile_by_account", return_value={"points": 200}
    ), patch(
        "bot.services.shop_service.PointsService.remove_points_by_account", return_value=True
    ), patch(
        "bot.services.shop_service.RoleManagementService.assign_user_role_by_account", return_value={"ok": True}
    ), patch(
        "bot.services.shop_service.RoleManagementService.revoke_user_role_by_account", return_value={"ok": True}
    ) as revoke_mock:
        result = shop_logic.purchase_shop_item(
            account_id="acc-1",
            shop_item_id="shop_1",
            actor_provider="telegram",
            actor_user_id="100",
        )

    assert result.ok is True
    revoked_names = [call.args[1] for call in revoke_mock.call_args_list]
    assert revoked_names == ["Новый волонтер", "Хороший Помощник Бебр"]


def test_purchase_shop_item_sends_spent_points_to_bank_on_success():
    item = shop_logic.ShopItem(
        shop_item_id="shop_1",
        role_name="Новый волонтер",
        short_name="Новый волонтер",
        category="Роли",
        position=0,
        category_position=0,
        description="",
        acquire_hint="",
        price_points=15,
        base_price_points=15,
        sale_price_points=None,
        is_sale_active=False,
    )
    with patch("bot.services.shop_service.get_shop_catalog_items", return_value=[item]), patch(
        "bot.services.shop_service.RoleManagementService.get_role",
        return_value={"is_sellable": True, "discord_role_id": "1105906310131744868"},
    ), patch(
        "bot.services.shop_service.RoleManagementService.get_user_roles_by_account", return_value=[]
    ), patch(
        "bot.services.shop_service.AccountsService.get_profile_by_account", return_value={"points": 200}
    ), patch(
        "bot.services.shop_service.PointsService.remove_points_by_account", return_value=True
    ), patch(
        "bot.services.shop_service.RoleManagementService.assign_user_role_by_account", return_value={"ok": True}
    ), patch(
        "bot.services.shop_service.db.add_to_bank", return_value=True
    ) as add_bank_mock, patch(
        "bot.services.shop_service.db.log_bank_income_by_account", return_value=True
    ) as log_income_mock:
        result = shop_logic.purchase_shop_item(
            account_id="acc-1",
            shop_item_id="shop_1",
            actor_provider="telegram",
            actor_user_id="100",
        )

    assert result.ok is True
    add_bank_mock.assert_called_once_with(15.0)
    log_income_mock.assert_called_once_with("acc-1", 15.0, "Покупка роли в магазине: Новый волонтер")
