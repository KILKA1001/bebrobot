from aiogram import Router

from .engagement import router as engagement_router
from .linking import router as linking_router


def get_commands_router() -> Router:
    router = Router()
    router.include_router(linking_router)
    router.include_router(engagement_router)
    return router
