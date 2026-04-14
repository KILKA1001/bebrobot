from pathlib import Path


def test_proposal_sections_parity_between_discord_and_telegram() -> None:
    discord_source = Path("bot/commands/proposal.py").read_text(encoding="utf-8")
    telegram_source = Path("bot/telegram_bot/commands/proposal.py").read_text(encoding="utf-8")
    shared_texts_source = Path("bot/services/proposal_ui_texts.py").read_text(encoding="utf-8")

    for text in ["Подать предложение", "Статус", "Архив решений", "Помощь"]:
        assert text in discord_source
        assert text in telegram_source
    assert "Подтверждение отправки" in discord_source
    assert "Подтверждение отправки" in shared_texts_source


def test_proposal_command_registered_on_both_platforms() -> None:
    discord_init = Path("bot/commands/__init__.py").read_text(encoding="utf-8")
    telegram_init = Path("bot/telegram_bot/commands/__init__.py").read_text(encoding="utf-8")

    assert "proposal" in discord_init
    assert "proposal_router" in telegram_init


def test_proposal_channel_settings_are_inside_single_proposal_command() -> None:
    discord_source = Path("bot/commands/proposal.py").read_text(encoding="utf-8")
    telegram_source = Path("bot/telegram_bot/commands/proposal.py").read_text(encoding="utf-8")

    assert "proposal_system_channel" not in discord_source
    assert "proposal_system_channel" not in telegram_source
    assert "Настройки Совета" in discord_source
    assert "Настройки Совета" in telegram_source


def test_proposal_commands_use_shared_status_and_steps_layer() -> None:
    discord_source = Path("bot/commands/proposal.py").read_text(encoding="utf-8")
    telegram_source = Path("bot/telegram_bot/commands/proposal.py").read_text(encoding="utf-8")

    assert "from bot.services.proposal_ui_texts import" in discord_source
    assert "from bot.services.proposal_ui_texts import" in telegram_source
    assert "render_help_text(" in discord_source
    assert "render_help_text(" in telegram_source


def test_release_parity_checklist_exists_for_every_release() -> None:
    checklist = Path("docs/release_parity_checklist.md").read_text(encoding="utf-8")

    assert "каждый релиз" in checklist.lower()
    assert "Различается только UI-слой" in checklist
    assert "Единые статусы и переходы" in checklist


def test_council_settings_flow_parity_for_confirm_and_next_step() -> None:
    discord_source = Path("bot/commands/proposal.py").read_text(encoding="utf-8")
    telegram_source = Path("bot/telegram_bot/commands/proposal.py").read_text(encoding="utf-8")
    shared_texts_source = Path("bot/services/proposal_ui_texts.py").read_text(encoding="utf-8")

    assert "render_admin_confirm_text(" in discord_source
    assert "render_admin_confirm_text(" in telegram_source
    assert "render_admin_action_result(" in discord_source
    assert "render_admin_action_result(" in telegram_source
    assert "proposal:admin_section:{section_code}" in telegram_source
    assert "PROPOSAL_ADMIN_SETTINGS_FLOW_STEPS" in shared_texts_source


def test_council_settings_menu_text_comes_from_shared_module() -> None:
    discord_source = Path("bot/commands/proposal.py").read_text(encoding="utf-8")
    telegram_source = Path("bot/telegram_bot/commands/proposal.py").read_text(encoding="utf-8")

    assert "render_menu_action_explanations(" in discord_source
    assert "render_menu_action_explanations(" in telegram_source
    assert "render_submit_review_text(" in telegram_source


def test_admin_menu_visibility_and_access_rules_are_consistent_between_platforms() -> None:
    discord_source = Path("bot/commands/proposal.py").read_text(encoding="utf-8")
    telegram_source = Path("bot/telegram_bot/commands/proposal.py").read_text(encoding="utf-8")

    assert "Действие доступно только суперадмину" in discord_source
    assert "Доступно только суперадмину." in telegram_source
    assert "⚙️ Настройки Совета" in discord_source
    assert "if is_superadmin" in telegram_source


def test_events_destination_selection_flow_has_choose_confirm_save_and_cancel_on_both_platforms() -> None:
    discord_source = Path("bot/commands/proposal.py").read_text(encoding="utf-8")
    telegram_source = Path("bot/telegram_bot/commands/proposal.py").read_text(encoding="utf-8")
    shared_texts_source = Path("bot/services/proposal_ui_texts.py").read_text(encoding="utf-8")

    assert "render_events_pick_confirmation_text(" in telegram_source
    assert "events_confirm_active" in discord_source
    assert "Вы выбрали: **" in discord_source
    assert "events_save" in discord_source
    assert "events_save" in telegram_source
    assert "_AdminEventsCancelButton" in discord_source
    assert "events_cancel" in telegram_source
    assert "Вы выбрали:" in shared_texts_source
    assert "После сохранения системные события Совета будут отправляться сюда." in shared_texts_source


def test_election_stages_and_candidates_actions_are_exposed_via_shared_catalog() -> None:
    shared_texts_source = Path("bot/services/proposal_ui_texts.py").read_text(encoding="utf-8")

    for action_code in [
        "election_open_candidates",
        "election_close_candidates",
        "election_start_voting",
        "election_finish_voting",
        "candidates_list",
        "candidates_approve",
        "candidates_reject",
        "candidates_manual_add",
    ]:
        assert action_code in shared_texts_source


def test_admin_action_logging_statuses_success_denied_failed_are_present_on_both_platforms() -> None:
    discord_source = Path("bot/commands/proposal.py").read_text(encoding="utf-8")
    telegram_source = Path("bot/telegram_bot/commands/proposal.py").read_text(encoding="utf-8")

    for status in ['status="success"', 'status="denied"', 'status="failed"']:
        assert status in discord_source
        assert status in telegram_source
