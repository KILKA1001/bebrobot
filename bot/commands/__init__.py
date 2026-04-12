from bot.commands.base import bot

# Import all command modules to register them
from . import fines  # noqa: F401
from . import tournament  # noqa: F401
from . import engagement  # noqa: F401
from . import maps  # noqa: F401
from . import linking  # noqa: F401
from . import roles_admin  # noqa: F401
from . import guiy_owner  # noqa: F401
from . import rep as rep_module  # noqa: F401
from . import modstatus as modstatus_module  # noqa: F401
from . import title as title_module  # noqa: F401
from . import shop as shop_module  # noqa: F401

# Import specific commands for explicit access if needed
from .tournament import (
    createtournament,
    jointournament,
    tournamentadmin,
)
from .maps import mapinfo
from .linking import link_telegram, link, profile, profile_edit, register_account
from .rep import rep
from .modstatus import modstatus
from .title import title
from .shop import shop

__all__ = [
    "bot",
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
    "modstatus",
    "title",
    "shop",
]
