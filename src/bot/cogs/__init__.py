"""
Discord Bot Cogs

Organized command groups for the Halo Stats Discord Bot.
"""

from src.bot.cogs.stats import StatsCog
from src.bot.cogs.graph import GraphCog
from src.bot.cogs.graph_commands.display.network.cog import NetworkCog
from src.bot.cogs.graph_commands.display.halonet.cog import HaloNetCog

__all__ = ["StatsCog", "GraphCog", "NetworkCog", "HaloNetCog"]
