import logging

from aiogram import Router

from .engagement import router as engagement_router
from .linking import router as linking_router
from .ai_chat import router as ai_chat_router
from .roles_admin import router as roles_admin_router
from .guiy_owner import router as guiy_owner_router


logger = logging.getLogger(__name__)
_COMMANDS_ROUTER: Router | None = None


def get_commands_router() -> Router:
    global _COMMANDS_ROUTER
    if _COMMANDS_ROUTER is not None:
        return _COMMANDS_ROUTER

    router = Router()
    router.include_router(linking_router)
    router.include_router(engagement_router)
    router.include_router(roles_admin_router)
    router.include_router(guiy_owner_router)
    router.include_router(ai_chat_router)
    _COMMANDS_ROUTER = router
    logger.info("telegram commands router initialized")
    return _COMMANDS_ROUTER
