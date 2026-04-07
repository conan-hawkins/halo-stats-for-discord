"""Implementation for the #crawlfriends command."""

import asyncio
from datetime import datetime
from typing import Awaitable, Callable, Optional

import discord
from discord.ext import commands


class CrawlFriendsCommandMixin:
    @commands.command(name="crawlfriends", help="Start a background Halo-friends crawl from a seed player. Admin only.")
    @commands.has_permissions(administrator=True)
    async def start_crawl(
        self,
        ctx: commands.Context,
        *inputs,
        progress_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
        run_inline: bool = False,
    ):
        """Start a background crawl (admin only)."""
        if not inputs:
            await ctx.send(
                "Usage: `#crawlfriends GAMERTAG [depth]`\n"
                "Example: `#crawlfriends YourGamertag 2`\n"
                "Note: Wrap gamertags with spaces in quotes: `#crawlfriends \"Possibly Tom\" 2`"
            )
            return

        if len(inputs) > 1 and inputs[-1].isdigit():
            gamertag = " ".join(inputs[:-1])
            depth = int(inputs[-1])
        else:
            gamertag = " ".join(inputs)
            depth = 2

        if self._crawl_task and not self._crawl_task.done():
            await ctx.send("A crawl is already running. Wait for it to complete or restart the bot.")
            return

        api = self._get_api_client()

        seed_xuid = await api.resolve_gamertag_to_xuid(gamertag)
        if not seed_xuid:
            await ctx.send(f"Could not resolve **{gamertag}**. Check spelling and try again.")
            return

        seed_player = self.db.get_player(seed_xuid)
        if seed_player and seed_player.get("profile_visibility") == "private":
            await ctx.send(
                f"Cannot crawl **{gamertag}**: profile is marked private (friends list not visible)."
            )
            return

        try:
            seed_friends_probe = await api.get_friends_list(seed_xuid)
        except Exception as e:
            await ctx.send(f"Unable to verify seed profile visibility before crawl: {str(e)}")
            return

        if seed_friends_probe.get("is_private"):
            self.db.insert_or_update_player(
                xuid=seed_xuid,
                gamertag=gamertag,
                profile_visibility="private",
            )
            await ctx.send(
                f"Cannot crawl **{gamertag}**: friends list is private/unavailable."
            )
            return

        if not run_inline:
            await ctx.send(
                f"Starting background friends crawl from **{gamertag}** with depth {depth}...\n"
                "Use `#graphstats` to check progress."
            )

        async def crawl_progress_update(progress):
            if not progress_callback:
                return
            crawled = int(getattr(progress, "nodes_crawled", 0) or 0)
            discovered = int(getattr(progress, "nodes_discovered", 0) or 0)
            denominator = max(crawled + 1, discovered, 1)
            crawl_pct = min(84.0, (float(crawled) / float(denominator)) * 84.0)
            await progress_callback(
                {
                    "stage": "Crawling friends",
                    "percent": crawl_pct,
                    "detail": f"Crawled {crawled} nodes, discovered {discovered}",
                }
            )

        from src.graph.crawler import CrawlConfig, GraphCrawler

        async def run_crawl():
            try:
                config = CrawlConfig(
                    max_depth=depth,
                    collect_stats=True,
                    stats_matches_to_process=25,
                    progress_callback=crawl_progress_update if progress_callback else None,
                )
                crawler = GraphCrawler(api, config, self.db)
                progress = await crawler.crawl_from_seed(seed_gamertag=gamertag)

                if progress_callback:
                    await progress_callback(
                        {
                            "stage": "Finalizing",
                            "percent": 100.0,
                            "detail": "Friends crawl complete",
                        }
                    )

                if not run_inline:
                    channel = ctx.channel
                    embed = discord.Embed(
                        title="Crawl Complete",
                        colour=0x00FF00,
                        timestamp=datetime.now(),
                    )
                    embed.add_field(name="Seed", value=gamertag, inline=True)
                    embed.add_field(name="Depth", value=str(depth), inline=True)
                    embed.add_field(name="Nodes Discovered", value=str(progress.nodes_discovered), inline=True)
                    embed.add_field(name="Halo Players", value=str(progress.halo_players_found), inline=True)
                    embed.add_field(name="Edges", value=str(progress.edges_discovered), inline=True)
                    embed.add_field(name="With Stats", value=str(progress.nodes_with_stats), inline=True)

                    await channel.send(embed=embed)
                return (
                    f"Friends crawl completed for {gamertag}. "
                    f"Discovered {progress.nodes_discovered} players, "
                    f"halo-active {progress.halo_players_found}, "
                    f"stats on {progress.nodes_with_stats}."
                )

            except Exception as e:
                if not run_inline:
                    await ctx.channel.send(f"Crawl error: {str(e)}")
                raise

        self._crawl_task = asyncio.create_task(run_crawl())
        if run_inline:
            return await self._crawl_task
