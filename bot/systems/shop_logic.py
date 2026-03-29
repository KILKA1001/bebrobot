"""Backward-compatible shim for shared shop business logic.

Shop business rules live in ``bot.services.shop_service``.
Platform command modules should import from the shared service layer.
"""

from bot.services.shop_service import *  # noqa: F403
