from bot.telegram_bot.commands import get_commands_router


def test_get_commands_router_is_singleton_instance() -> None:
    router_first = get_commands_router()
    router_second = get_commands_router()

    assert router_first is router_second
