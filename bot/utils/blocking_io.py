import asyncio
import functools
import logging
import time
from collections.abc import Callable
from typing import ParamSpec, TypeVar


P = ParamSpec("P")
R = TypeVar("R")

_DEFAULT_SLOW_THRESHOLD_MS = 250.0


async def run_blocking_io(
    operation: str,
    func: Callable[P, R],
    *args: P.args,
    logger: logging.Logger | None = None,
    slow_threshold_ms: float = _DEFAULT_SLOW_THRESHOLD_MS,
    **kwargs: P.kwargs,
) -> R:
    """Run blocking work in a thread and log slow/error cases.

    This is intended for sync service/database code that is called from async
    Telegram/Discord handlers. It keeps the event loop responsive while still
    providing enough console diagnostics to investigate slow paths.
    """

    bound = functools.partial(func, *args, **kwargs)
    started_at = time.perf_counter()
    try:
        result = await asyncio.to_thread(bound)
    except Exception:
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        if logger is not None:
            logger.exception(
                "blocking io failed operation=%s elapsed_ms=%.1f",
                operation,
                elapsed_ms,
            )
        raise

    elapsed_ms = (time.perf_counter() - started_at) * 1000
    if logger is not None and elapsed_ms >= slow_threshold_ms:
        logger.warning(
            "blocking io slow operation=%s elapsed_ms=%.1f threshold_ms=%.1f",
            operation,
            elapsed_ms,
            slow_threshold_ms,
        )
    return result
