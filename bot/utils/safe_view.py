"""
Назначение: модуль "safe view" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

import discord
import logging

from .safe_interaction import safe_followup_send, safe_response_send

logger = logging.getLogger(__name__)


class SafeView(discord.ui.View):
    """View with global error handler sending errors to user."""

    async def on_error(
        self,
        error: Exception,
        item: discord.ui.Item,
        interaction: discord.Interaction,
    ) -> None:
        try:
            if interaction.response.is_done():
                await safe_followup_send(
                    interaction, f"❌ Ошибка: {error}", ephemeral=True
                )
            else:
                await safe_response_send(
                    interaction, f"❌ Ошибка: {error}", ephemeral=True
                )
        except Exception:
            pass
        import traceback

        logger.error("Interaction error:\n%s", traceback.format_exc())
