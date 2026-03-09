from aiogram import Router

from .linking import router as linking_router


def get_commands_router() -> Router:
    router = Router()
    router.include_router(linking_router)
    return router
