from bot.commands.base import bot, run_monthly_top
# Import modules to register commands automatically
from . import fines
from . import tournament

__all__ = ["bot", "run_monthly_top"]
