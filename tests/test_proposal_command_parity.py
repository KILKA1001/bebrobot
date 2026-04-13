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
