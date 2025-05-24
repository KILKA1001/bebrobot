from bot.commands.base import bot, run_monthly_top
from .fines import fine
from .fines import myfines

__all__ = ["bot", "run_monthly_top"]
bot.add_command(fine)
bot.add_command(myfines)
