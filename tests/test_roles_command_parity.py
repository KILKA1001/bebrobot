"""
Назначение: модуль "test roles command parity" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bot.telegram_bot.main import BOT_COMMANDS, OWNER_PRIVATE_COMMANDS
from bot.telegram_bot.systems.commands_logic import get_helpy_text
from bot.telegram_bot.commands.roles_admin import _render_help_text as render_telegram_roles_admin_help


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_telegram_command_registry_exposes_roles_parity_commands() -> None:
    public_commands = {command.command for command in BOT_COMMANDS}
    owner_commands = {command.command for command in OWNER_PRIVATE_COMMANDS}

    assert "roles" in public_commands
    assert "roles_admin" in public_commands
    assert "title" in public_commands
    assert "guiy_owner" not in public_commands
    assert "guiy_owner" in owner_commands


def test_telegram_help_text_marks_rolesadmin_alias_and_limits() -> None:
    with patch(
        "bot.telegram_bot.systems.commands_logic.AuthorityService.resolve_authority",
        return_value=SimpleNamespace(level=100, titles=("Главный вице",)),
    ):
        helpy_text = get_helpy_text(telegram_user_id=42)
    roles_admin_help = render_telegram_roles_admin_help()

    assert "/roles_admin / /rolesadmin" in helpy_text
    assert "/title @username" in helpy_text
    assert "/guiy_owner" not in helpy_text
    assert "sync_discord_roles" in roles_admin_help
    assert "текстовый alias <code>/rolesadmin</code>" in roles_admin_help
    assert "пакетный выбор" in roles_admin_help


def test_telegram_roles_admin_source_accepts_rolesadmin_alias() -> None:
    source = (REPO_ROOT / "bot" / "telegram_bot" / "commands" / "roles_admin.py").read_text()

    assert 'Command(commands=["roles_admin", "rolesadmin"])' in source


def test_discord_roles_sources_expose_matching_commands() -> None:
    base_source = (REPO_ROOT / "bot" / "commands" / "base.py").read_text()
    roles_admin_source = (REPO_ROOT / "bot" / "commands" / "roles_admin.py").read_text()
    guiy_owner_source = (REPO_ROOT / "bot" / "commands" / "guiy_owner.py").read_text()
    title_source = (REPO_ROOT / "bot" / "commands" / "title.py").read_text()

    assert 'name="roles"' in base_source
    assert 'name="helpy"' in base_source
    assert '@bot.hybrid_group(name="rolesadmin"' in roles_admin_source
    assert 'name="guiy_owner"' in guiy_owner_source
    assert 'name="title"' in title_source


def test_discord_rolesadmin_help_source_marks_alias_and_batch_limits() -> None:
    source = (REPO_ROOT / "bot" / "commands" / "roles_admin.py").read_text()

    assert "Паритет названий" in source
    assert "sync_discord_roles" in source
    assert "Пакетный режим доступен на обеих платформах" in source
