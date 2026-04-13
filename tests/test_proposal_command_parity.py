from pathlib import Path


def test_proposal_sections_parity_between_discord_and_telegram() -> None:
    discord_source = Path("bot/commands/proposal.py").read_text(encoding="utf-8")
    telegram_source = Path("bot/telegram_bot/commands/proposal.py").read_text(encoding="utf-8")

    for text in ["Подать предложение", "Статус", "Архив решений", "Помощь", "Подтверждение отправки"]:
        assert text in discord_source
        assert text in telegram_source


def test_proposal_command_registered_on_both_platforms() -> None:
    discord_init = Path("bot/commands/__init__.py").read_text(encoding="utf-8")
    telegram_init = Path("bot/telegram_bot/commands/__init__.py").read_text(encoding="utf-8")

    assert "proposal" in discord_init
    assert "proposal_router" in telegram_init


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
