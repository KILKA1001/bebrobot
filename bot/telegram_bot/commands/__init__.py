import logging

from aiogram import Router

from bot.telegram_bot.chat_registry_router import router as chat_registry_router
from .engagement import router as engagement_router
from .linking import router as linking_router
from .ai_chat import router as ai_chat_router
from .roles_admin import router as roles_admin_router
from .guiy_owner import router as guiy_owner_router
from .rep import router as rep_router
from .fines import router as fines_router
from .modstatus import router as modstatus_router
from .title import router as title_router
from .shop import router as shop_router
from .top import router as top_router


logger = logging.getLogger(__name__)
_COMMANDS_ROUTER: Router | None = None


def get_commands_router() -> Router:
    global _COMMANDS_ROUTER
    if _COMMANDS_ROUTER is not None:
        return _COMMANDS_ROUTER

    router = Router()
    router.include_router(chat_registry_router)
    router.include_router(linking_router)
    router.include_router(engagement_router)
    router.include_router(roles_admin_router)
    router.include_router(guiy_owner_router)
    router.include_router(rep_router)
    router.include_router(modstatus_router)
    router.include_router(title_router)
    router.include_router(shop_router)
    router.include_router(top_router)
    router.include_router(fines_router)
    router.include_router(ai_chat_router)
    _COMMANDS_ROUTER = router
    logger.info("telegram commands router initialized")
    return _COMMANDS_ROUTER
