from types import SimpleNamespace
from unittest.mock import patch

from bot.telegram_bot.systems.commands_logic import get_helpy_text
from bot.systems.core_logic import HelpView, get_help_embed


def test_telegram_help_text_hides_privileged_and_owner_commands_for_regular_user() -> None:
    with patch("bot.telegram_bot.systems.commands_logic.AuthorityService.resolve_authority", return_value=SimpleNamespace(level=0, titles=tuple())):
        helpy_text = get_helpy_text(telegram_user_id=100)

    assert "/points" not in helpy_text
    assert "/tickets" not in helpy_text
    assert "/roles_admin / /rolesadmin" not in helpy_text
    assert "/guiy_owner" not in helpy_text


def test_telegram_help_text_shows_only_available_privileged_commands() -> None:
    with patch(
        "bot.telegram_bot.systems.commands_logic.AuthorityService.resolve_authority",
        return_value=SimpleNamespace(level=80, titles=("Вице города",)),
    ):
        helpy_text = get_helpy_text(telegram_user_id=200)

    assert "/points [reply|id]" in helpy_text
    assert "/roles_admin / /rolesadmin" in helpy_text
    assert "/tickets [reply|id]" not in helpy_text
    assert "/guiy_owner" not in helpy_text


def test_discord_help_view_hides_mod_panel_for_regular_user() -> None:
    member = SimpleNamespace(id=1, guild_permissions=SimpleNamespace(administrator=False))
    with patch("bot.systems.core_logic.AuthorityService.resolve_authority", return_value=SimpleNamespace(level=0, titles=tuple())):
        view = HelpView(member)

    labels = [child.label for child in view.children]
    assert "🛡️ Доступные мод-команды" not in labels


def test_discord_help_embed_shows_veteran_fine_but_never_owner_command() -> None:
    visibility = SimpleNamespace(level=30, titles=("Ветеран города",), is_administrator=False)
    embed = get_help_embed("fines", visibility=visibility)

    assert "/modstatus" in embed.description
    assert "/rep" in embed.description
    assert "/fine @пользователь сумма тип [причина]" not in embed.description
    assert "/cancel_fine" not in embed.description
    assert "guiy_owner" not in embed.description
