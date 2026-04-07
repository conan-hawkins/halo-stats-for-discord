"""
Graph Commands Cog for Halo Stats Discord Bot.

Maintains Graph compatibility surfaces while delegating command implementations
to focused mixin modules.
"""

import asyncio
import io
from typing import Optional

from discord.ext import commands

from src.api import api_client
from src.bot.cogs.graph_commands import (
    CrawlFriendsCommandMixin,
    CrawlGamesCommandMixin,
    CrawlProgressView,
    GraphStatsCommandMixin,
    HaloGroupsCommandMixin,
    HubsCommandMixin,
    ISSCommandMixin,
)
from src.bot.cogs.graph_commands.display.network.runtime import execute_show_network, render_network_graph
from src.database.graph_schema import get_graph_db


class GraphCog(
    CrawlGamesCommandMixin,
    CrawlFriendsCommandMixin,
    HubsCommandMixin,
    GraphStatsCommandMixin,
    ISSCommandMixin,
    HaloGroupsCommandMixin,
    commands.Cog,
    name="Graph",
):
    """Commands for social graph analysis and network discovery."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = get_graph_db()
        self._crawl_task: Optional[asyncio.Task] = None

    @staticmethod
    def _get_api_client():
        """Provide a single API client access point for command mixins/tests."""
        return api_client

    async def show_network(self, ctx: commands.Context, *inputs):
        """Show a player's Halo-active friend network with a visual graph image."""
        await execute_show_network(self, ctx, self.db, inputs)

    def _render_network_graph(
        self,
        center_xuid: str,
        center_gamertag: str,
        halo_friends: list,
        center_features: Optional[dict],
        clustered: bool = False,
        min_group_size: int = 0,
        min_link_strength: float = 1.0,
    ) -> io.BytesIO:
        """Render the friend network as a PNG and return a BytesIO buffer (sync)."""
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

    @commands.command(name="crawlstop", help="Stop the current background crawl. Admin only.")
    @commands.has_permissions(administrator=True)
    async def stop_crawl(self, ctx: commands.Context):
        """Stop the current background crawl."""
        if self._crawl_task and not self._crawl_task.done():
            self._crawl_task.cancel()
            await ctx.send("Crawl task cancelled. Progress has been saved.")
        else:
            await ctx.send("No crawl is currently running.")


async def setup(bot: commands.Bot):
    """Setup function for loading the cog."""
    await bot.add_cog(GraphCog(bot))
