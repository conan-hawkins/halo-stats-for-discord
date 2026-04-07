"""Standalone cog for the #network command.

This command is intentionally separated from co-play command ownership to keep
friend-graph concerns isolated.
"""

import discord
from discord.ext import commands

from src.bot.cogs.graph_commands.display.network.runtime import execute_show_network, render_network_graph
from src.database.graph_schema import get_graph_db


class NetworkCog(commands.Cog, name="Network"):
    """Owns the #network command surface and execution flow."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = get_graph_db()

    def _render_network_graph(
        self,
        center_xuid: str,
        center_gamertag: str,
        halo_friends: list,
        center_features: dict | None,
        clustered: bool = False,
        min_group_size: int = 0,
        min_link_strength: float = 1.0,
    ):
        return render_network_graph(
            self.db,
            center_xuid,
            center_gamertag,
            halo_friends,
            center_features,
            clustered=clustered,
            min_group_size=min_group_size,
            min_link_strength=min_link_strength,
        )

    @commands.command(
        name="network",
        help="Show a player network visualization from graph DB data. Usage: #network <gamertag>",
    )
    async def show_network(self, ctx: commands.Context, *inputs):
        await execute_show_network(self, ctx, self.db, inputs)


async def setup(bot: commands.Bot):
    await bot.add_cog(NetworkCog(bot))
