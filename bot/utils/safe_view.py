import discord
import logging

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
                await interaction.followup.send(
                    f"❌ Ошибка: {error}", ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    f"❌ Ошибка: {error}", ephemeral=True
                )
        except Exception:
            pass
        import traceback

        logger.error("Interaction error:\n%s", traceback.format_exc())
