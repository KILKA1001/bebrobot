import logging

from aiogram import Router

from .engagement import router as engagement_router
from .linking import router as linking_router
from .ai_chat import router as ai_chat_router
from .roles_admin import router as roles_admin_router

logger = logging.getLogger(__name__)


_COMMANDS_ROUTER: Router | None = None


def get_commands_router() -> Router:
    global _COMMANDS_ROUTER
    if _COMMANDS_ROUTER is not None:
        return _COMMANDS_ROUTER

    router = Router()
    child_routers = [
        ("linking", linking_router),
        ("engagement", engagement_router),
        ("roles_admin", roles_admin_router),
        ("ai_chat", ai_chat_router),
    ]
    included = set()
    for name, child_router in child_routers:
        if id(child_router) in included:
            logger.error(
                "telegram commands router duplicate include skipped name=%s router_id=%s",
                name,
                hex(id(child_router)),
            )
            continue
        router.include_router(child_router)
        included.add(id(child_router))

    _COMMANDS_ROUTER = router
    return _COMMANDS_ROUTER
