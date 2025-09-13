from discord.ext import commands
import logging
from discord.errors import HTTPException
from .safe_send import safe_send

# Специальный объект, чтобы понять, передал ли автор `delete_after`
_MISSING = object()


async def send_temp(
    ctx: commands.Context, *args, delay: float = 2.0, **kwargs
):
    """Отправить временное сообщение.

    По умолчанию сообщение удаляется через 5 минут, чтобы не засорять чаты.
    Если явно передать ``delete_after=None`` — сообщение останется навсегда.
    """

    # Забираем значение `delete_after` если оно было передано при вызове
    delete_after = kwargs.pop("delete_after", _MISSING)

    # Если аргумент не передан — используем значение по умолчанию (5 минут)
    if delete_after is _MISSING:
        delete_after = 300

    try:
        return await safe_send(
            ctx,
            *args,
            delete_after=delete_after,  # None сохранит сообщение навсегда
            delay=delay,
            **kwargs,
        )
    except HTTPException as e:
        if e.status == 429:
            logging.warning("send_temp hit rate limit: %s", e.text)
            return None
        raise
