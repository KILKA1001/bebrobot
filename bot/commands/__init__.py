from bot.commands.base import bot, run_monthly_top

# Import all command modules to register them
from . import fines  # noqa: F401
from . import tournament  # noqa: F401
from . import tickets  # noqa: F401
from . import maps  # noqa: F401

# Import specific commands for explicit access if needed
from .fines import (
    fine,
    myfines,
    all_fines,
    finedetails,
    editfine,
    cancel_fine,
    finehistory,
    topfines,
)

from .tournament import (
    createtournament,
    jointournament,
    deletetournament,
    regplayer,
)
from .maps import mapinfo

__all__ = [
    "bot",
    "run_monthly_top",
    "fine",
    "myfines",
    "all_fines",
    "finedetails",
    "editfine",
    "cancel_fine",
    "finehistory",
    "topfines",
    "createtournament",
    "jointournament",
    "deletetournament",
    "regplayer",
    "mapinfo",
]
