from pathlib import Path


def _python_files(root: Path):
    for path in root.rglob("*.py"):
        if path.is_file():
            yield path


def test_platform_handlers_do_not_use_council_domain_directly():
    handlers_roots = [Path("bot/commands"), Path("bot/telegram_bot/commands")]
    forbidden = ("bot.domain.council_lifecycle", "from bot.domain import")

    violations: list[str] = []
    for root in handlers_roots:
        for file_path in _python_files(root):
            text = file_path.read_text(encoding="utf-8")
            if any(marker in text for marker in forbidden):
                violations.append(str(file_path))

    assert not violations, (
        "Платформенные хендлеры не должны импортировать council-домен напрямую; "
        "используйте bot.services.council_service: " + ", ".join(sorted(violations))
    )
