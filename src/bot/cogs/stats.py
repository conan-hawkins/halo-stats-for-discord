"""
Stats Commands Cog for Halo Stats Discord Bot

Contains all player statistics related commands:
- #full - Get complete stats from player's entire match history
- #ranked - Get stats from ranked matches only
- #casual - Get stats from casual/social matches only
- #cachestatus - Check background caching progress
- #xboxfriends - Get Xbox friends and friends-of-friends network
"""

import os
import json
from pathlib import Path

import discord
from discord.ext import commands

from src.api import api_client
from src.bot.cache_status import load_cache_status_metrics
from src.bot.commands import fetch_and_display_stats
from src.bot.presentation.embeds.cache_status import build_cache_status_embed
from src.bot.presentation.embeds.friends import (
    build_xboxfriends_error_embed,
    build_xboxfriends_loading_embed,
    build_xboxfriends_progress_embed,
    build_xboxfriends_result_embed,
)
from src.bot.presentation.embeds.help import build_command_help_embed, build_stats_help_guide_embed
from src.bot.stats_profiles import (
    CASUAL_STATS_PROFILE,
    FULL_STATS_PROFILE,
    RANKED_STATS_PROFILE,
    STATS_PROFILES,
    StatsProfile,
)
from src.config import CACHE_PROGRESS_FILE, PROJECT_ROOT, XUID_CACHE_FILE
from src.database.graph_schema import get_graph_db


class StatsCog(commands.Cog, name="Stats"):
    """Commands for fetching and displaying Halo Infinite player statistics"""
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def run_stats_profile(self, ctx: commands.Context, profile: StatsProfile, gamertag: str) -> None:
        await fetch_and_display_stats(
            ctx,
            gamertag,
            stat_type=profile.fetch_stat_type,
            matches_to_process=None,
        )

    async def _run_profile_from_inputs(self, ctx: commands.Context, profile: StatsProfile, inputs: tuple[str, ...]) -> None:
        gamertag = ' '.join(inputs).strip()
        await self.run_stats_profile(ctx, profile, gamertag)

    @commands.command(name='help', help='Show detailed instructions for all commands or a single command (example: #help network).')
    async def help_command(self, ctx: commands.Context, command_name: str = None):
        """Custom help with practical command-by-command usage guidance."""
        if command_name:
            cmd = self.bot.get_command(command_name.lower())
            if not cmd:
                await ctx.send(f"Unknown command: {command_name}. Use `#help` to see all commands.")
                return

            embed = build_command_help_embed(cmd)
            await ctx.send(embed=embed)
            return

        embed = build_stats_help_guide_embed(STATS_PROFILES)
        await ctx.send(embed=embed)
    
    @commands.command(name='full', help=FULL_STATS_PROFILE.command_help)
    async def full(self, ctx: commands.Context, *inputs):
        """Get complete stats from player's entire match history"""
        await self._run_profile_from_inputs(ctx, FULL_STATS_PROFILE, inputs)
    
    @commands.command(name='ranked', help=RANKED_STATS_PROFILE.command_help)
    async def ranked(self, ctx: commands.Context, *inputs):
        """Get stats from ranked matches only"""
        await self._run_profile_from_inputs(ctx, RANKED_STATS_PROFILE, inputs)
    
    @commands.command(name='casual', help=CASUAL_STATS_PROFILE.command_help)
    async def casual(self, ctx: commands.Context, *inputs):
        """Get stats from casual/social matches only"""
        await self._run_profile_from_inputs(ctx, CASUAL_STATS_PROFILE, inputs)
    
    @commands.command(name='cachestatus', help='Show progress and estimates for background player caching.')
    async def cache_status(self, ctx: commands.Context):
        """Display the current status of background stats caching"""
        try:
            metrics = load_cache_status_metrics(
                XUID_CACHE_FILE,
                [str(CACHE_PROGRESS_FILE), os.path.join(PROJECT_ROOT, 'cache_progress.json')],
            )
            embed = build_cache_status_embed(metrics)
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"Error checking cache status: {e}")
            print(f"Error in cache_status: {e}")
    
    @commands.command(name='xboxfriends', help='Fetch Xbox friends and friends-of-friends, then apply blacklist checks. Usage: #xboxfriends <gamertag>')
    async def friends_list(self, ctx: commands.Context, *inputs):
        """Get a player's Xbox friends list and friends-of-friends, checking against blacklist"""
        if not inputs:
            await ctx.send("Please provide a gamertag. Example: `#xboxfriends GAMERTAG`")
            return
        
        gamertag = ' '.join(inputs)

        loading_embed = build_xboxfriends_loading_embed(gamertag)
        loading_message = await ctx.send(embed=loading_embed)

        async def safe_update_message(embed: discord.Embed):
            """Update loading message, falling back to a fresh message if edit fails."""
            try:
                await loading_message.edit(embed=embed)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                await ctx.send(embed=embed)
        
        try:
            # Load blacklist
            blacklist_path = Path(__file__).parent.parent.parent.parent / 'data' / 'xuid_gamertag_blacklist.json'
            blacklist = {}
            if blacklist_path.exists():
                try:
                    # utf-8-sig handles files saved with BOM; strip handles accidental empty/whitespace files
                    raw_blacklist = blacklist_path.read_text(encoding='utf-8-sig').strip()
                    blacklist = json.loads(raw_blacklist) if raw_blacklist else {}
                except json.JSONDecodeError as e:
                    print(f"Invalid blacklist JSON at {blacklist_path}: {e}")
                    blacklist = {}
            
            # Progress callback to update the embed
            async def update_progress(current, total, stage, fof_count):
                progress_embed = build_xboxfriends_progress_embed(
                    gamertag,
                    current,
                    total,
                    stage,
                    fof_count,
                )
                await safe_update_message(progress_embed)
            
            # Get friends and friends-of-friends (fetches ALL friends)
            result = await api_client.get_friends_of_friends(
                gamertag,
                max_depth=2,
                progress_callback=update_progress
            )
            
            if result.get('error'):
                error_embed = build_xboxfriends_error_embed(result['error'])
                await safe_update_message(error_embed)
                return
            
            # Get friends and friends-of-friends
            friends = result.get('friends', [])
            fof = result.get('friends_of_friends', [])
            private_friends = result.get('private_friends', [])

            # Persist discovered relationships so graph commands can reuse them.
            try:
                graph_db = get_graph_db()
                target_info = result.get('target') or {}
                target_xuid = target_info.get('xuid')
                target_gt = target_info.get('gamertag') or gamertag

                if target_xuid:
                    graph_db.insert_or_update_player(
                        xuid=target_xuid,
                        gamertag=target_gt,
                        profile_visibility='public',
                        friends_count=len(friends),
                    )

                    # Direct friends: target -> friend
                    direct_edges = []
                    friend_gt_to_xuid = {}
                    for friend in friends:
                        fxuid = friend.get('xuid')
                        fgt = friend.get('gamertag')
                        if not fxuid:
                            continue
                        graph_db.insert_or_update_player(
                            xuid=fxuid,
                            gamertag=fgt,
                            profile_visibility='public',
                        )
                        direct_edges.append((
                            target_xuid,
                            fxuid,
                            bool(friend.get('is_mutual', False)),
                            target_xuid,
                            1,
                        ))
                        if fgt:
                            friend_gt_to_xuid[fgt.lower()] = fxuid

                    if direct_edges:
                        graph_db.insert_friend_edges_batch(direct_edges)

                    # Friends-of-friends: direct_friend -> fof
                    fof_edges = []
                    for second_degree in fof:
                        fof_xuid = second_degree.get('xuid')
                        fof_gt = second_degree.get('gamertag')
                        via_gt = second_degree.get('via')
                        if not fof_xuid:
                            continue

                        graph_db.insert_or_update_player(
                            xuid=fof_xuid,
                            gamertag=fof_gt,
                            profile_visibility='public',
                        )

                        via_xuid = friend_gt_to_xuid.get(via_gt.lower()) if via_gt else None
                        if via_xuid:
                            fof_edges.append((
                                via_xuid,
                                fof_xuid,
                                bool(second_degree.get('is_mutual', False)),
                                target_xuid,
                                2,
                            ))

                    if fof_edges:
                        graph_db.insert_friend_edges_batch(fof_edges)
            except Exception as db_error:
                # Keep command response working even if persistence fails.
                print(f"Failed to persist xboxfriends graph data: {db_error}")

            result_embed = build_xboxfriends_result_embed(
                gamertag,
                friends,
                fof,
                private_friends,
                blacklist,
            )

            await safe_update_message(result_embed)
            
        except Exception as e:
            import traceback
            error_embed = build_xboxfriends_error_embed(f"An error occurred: {e}")
            try:
                await safe_update_message(error_embed)
            except Exception:
                await ctx.send(f"❌ Error in #xboxfriends: {e}")
            print(f"Error in friends_list: {e}")
            traceback.print_exc()


async def setup(bot: commands.Bot):
    """Setup function for loading the cog"""
    await bot.add_cog(StatsCog(bot))
