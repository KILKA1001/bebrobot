from bot.commands.base import bot, run_monthly_top

# Import all command modules to register them
from . import fines  # noqa: F401
from . import tournament  # noqa: F401
from . import engagement  # noqa: F401
from . import maps  # noqa: F401
from . import linking  # noqa: F401
from . import roles_admin  # noqa: F401
from . import guiy_owner  # noqa: F401
from . import rep  # noqa: F401

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
    tournamentadmin,
)
from .maps import mapinfo
from .linking import link_telegram, link, profile, profile_edit, register_account
from .rep import rep

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
    "tournamentadmin",
    "mapinfo",
    "link_telegram",
    "link",
    "profile",
    "profile_edit",
    "register_account",
    "rep",
]
