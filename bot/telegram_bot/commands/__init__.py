from aiogram import Router

from .engagement import router as engagement_router
from .linking import router as linking_router
from .ai_chat import router as ai_chat_router
from .roles_admin import router as roles_admin_router


def get_commands_router() -> Router:
    router = Router()
    router.include_router(linking_router)
    router.include_router(engagement_router)
    router.include_router(ai_chat_router)
    router.include_router(roles_admin_router)
    return router
