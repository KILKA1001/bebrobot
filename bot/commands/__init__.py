from bot.commands.base import bot, run_monthly_top
from .fines import (
fine,
myfines,
all_fines,
finedetails,
editfine,
cancel_fine,
finehistory,
topfines
)

__all__ = ["bot", "run_monthly_top"]
bot.add_command(fine)
bot.add_command(myfines)
bot.add_command(all_fines)
bot.add_command(finedetails)
bot.add_command(editfine)
bot.add_command(cancel_fine)
bot.add_command(finehistory)
bot.add_command(topfines)
