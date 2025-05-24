from bot.commands.base import bot, run_monthly_top
from .fines import fine

__all__ = ["bot", "run_monthly_top"]
bot.add_command(fine)
