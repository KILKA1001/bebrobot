"""
Назначение: модуль "test ux text parity" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from pathlib import Path

from bot.services import shop_service
from bot.services import proposal_ui_texts

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_shop_core_texts_include_three_block_structure() -> None:
    texts = (
        shop_service.SHOP_TEXT_CATEGORIES_HINT,
        shop_service.SHOP_TEXT_LIST_HINT,
        shop_service.SHOP_TEXT_CARD_HINT,
        shop_service.SHOP_TEXT_CONFIRM_PURCHASE,
    )
    for text in texts:
        assert "Что это:" in text
        assert "Что делать сейчас:" in text
        assert "Что будет дальше:" in text


def test_shop_ux_logging_events_are_present_for_both_platforms() -> None:
    telegram_source = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "shop.py").read_text()
    discord_source = (REPO_ROOT / "bot" / "commands" / "shop.py").read_text()

    for event in ("ux_screen_open", "ux_action_hint_shown", "ux_fallback_shown", "ux_render_error"):
        assert event in telegram_source
        assert event in discord_source


def test_rep_and_modstatus_have_ux_screen_logging_on_both_platforms() -> None:
    telegram_rep = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "rep.py").read_text()
    discord_rep = (REPO_ROOT / "bot" / "commands" / "rep.py").read_text()
    telegram_modstatus = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "modstatus.py").read_text()
    discord_modstatus = (REPO_ROOT / "bot" / "commands" / "modstatus.py").read_text()

    assert "screen=rep_start" in telegram_rep
    assert "screen=rep_start" in discord_rep
    assert "screen=modstatus" in telegram_modstatus
    assert "screen=modstatus" in discord_modstatus


def test_register_and_linking_prompts_have_actionable_wording_on_both_platforms() -> None:
    telegram_logic_source = (REPO_ROOT / "bot" / "telegram_bot" / "systems" / "commands_logic.py").read_text()
    discord_linking_source = (REPO_ROOT / "bot" / "commands" / "linking.py").read_text()

    assert "compose_three_block_message(" in telegram_logic_source
    assert "compose_three_block_plain(" in discord_linking_source


def test_engagement_foreign_actor_prompt_is_safe_and_actionable() -> None:
    telegram_source = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "engagement.py").read_text()
    discord_source = (REPO_ROOT / "bot" / "commands" / "engagement.py").read_text()

    assert "Чушка" not in telegram_source
    assert "Чушка" not in discord_source
    assert "панель открыта для другого администратора" in telegram_source


def test_admin_commands_have_ux_screen_open_logging_parity() -> None:
    discord_roles_admin = (REPO_ROOT / "bot" / "commands" / "roles_admin.py").read_text()
    telegram_roles_admin = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "roles_admin.py").read_text()
    discord_title = (REPO_ROOT / "bot" / "commands" / "title.py").read_text()
    telegram_title = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "title.py").read_text()
    discord_tournament = (REPO_ROOT / "bot" / "commands" / "tournament.py").read_text()

    assert "screen=roles_admin" in discord_roles_admin
    assert "screen=roles_admin" in telegram_roles_admin
    assert "screen=title_admin" in discord_title
    assert "screen=title_admin" in telegram_title
    assert "screen=tournament_create" in discord_tournament
    assert "screen=tournament_admin" in discord_tournament
    assert "screen=tournament_manage" in discord_tournament


def test_proposal_waiting_launch_status_has_connected_explanation() -> None:
    text = proposal_ui_texts.render_status_text(
        proposal_id=12,
        title="Вопрос",
        status_label="Ожидает запуска созыва",
        updated_at="2026-04-13T10:00:00+00:00",
    )

    assert "Созыв Совета ещё не запущен" in text
    assert "Проверьте раздел «Статус» позже" in text
