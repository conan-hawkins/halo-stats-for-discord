"""
Stats Commands Cog for Halo Stats Discord Bot

Contains all player statistics related commands:
- #full - Get complete stats from player's entire match history
- #ranked - Get stats from ranked matches only
- #casual - Get stats from casual/social matches only
- #server - Generate server-wide leaderboard
- #populate - Resolve and cache gamertags from recent matches
- #cachestatus - Check background caching progress
- #xboxfriends - Get Xbox friends and friends-of-friends network
"""

import os
import json
from datetime import datetime
from pathlib import Path

import discord
from discord.ext import commands

from src.api import get_players_from_recent_matches, api_client
from src.bot.commands import fetch_and_display_stats, collect_server_stats
from src.config import CACHE_PROGRESS_FILE, PROJECT_ROOT, XUID_CACHE_FILE
from src.database.graph_schema import get_graph_db


class StatsCog(commands.Cog, name="Stats"):
    """Commands for fetching and displaying Halo Infinite player statistics"""
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.command(name='help', help='Show detailed instructions for all commands or a single command (example: #help network).')
    async def help_command(self, ctx: commands.Context, command_name: str = None):
        """Custom help with practical command-by-command usage guidance."""
        if command_name:
            cmd = self.bot.get_command(command_name.lower())
            if not cmd:
                await ctx.send(f"Unknown command: {command_name}. Use `#help` to see all commands.")
                return

            embed = discord.Embed(
                title=f"Help: #{cmd.name}",
                description=cmd.help or "No additional help is available for this command.",
                colour=0x00BFFF,
                timestamp=datetime.now()
            )
            if cmd.signature:
                embed.add_field(name="Usage", value=f"`#{cmd.name} {cmd.signature}`", inline=False)
            else:
                embed.add_field(name="Usage", value=f"`#{cmd.name}`", inline=False)
            embed.set_footer(text="Tip: Gamertags with spaces should be typed normally, e.g. #stats Player Name")
            await ctx.send(embed=embed)
            return

        embed = discord.Embed(
            title="Halo Bot Command Guide",
            description=(
                "Use `#help <command>` for focused help on one command.\n"
                "Example: `#help xboxfriends`"
            ),
            colour=0x00BFFF,
            timestamp=datetime.now()
        )

        embed.add_field(
            name="Player Stats Commands",
            value=(
                "`#full <gamertag>`: Full lifetime stats from all available matches.\n"
                "`#ranked <gamertag>`: Ranked-only performance summary.\n"
                "`#casual <gamertag>`: Social/casual playlist performance summary.\n"
                "`#populate <gamertag>`: Resolves players from match history and updates XUID cache."
            ),
            inline=False
        )

        embed.add_field(
            name="Social Commands",
            value=(
                "`#xboxfriends <gamertag>`: Live Xbox friends + friends-of-friends scan with blacklist checks.\n"
                "`#network <gamertag>`: Visual graph from data stored in graph database.\n"
                "`#similar <gamertag>`: Finds players with similar stat profiles from graph DB.\n"
                "`#hubs [min_friends]`: Lists players with high Halo-active connectivity."
            ),
            inline=False
        )

        embed.add_field(
            name="Admin and Utility Commands",
            value=(
                "`#server`: Attempts a server-wide leaderboard by matching Discord members to gamertags.\n"
                "`#cachestatus`: Shows progress of background caching jobs.\n"
                "`#graphstats`: Shows social graph database totals and health.\n"
                "`#crawl <gamertag> [depth]` / `#crawlstop`: Start or stop background graph crawling (admin only)."
            ),
            inline=False
        )

        embed.add_field(
            name="Suggested Workflow",
            value=(
                "1) Run `#xboxfriends <gamertag>` to discover social edges quickly.\n"
                "2) Run `#network <gamertag>` to visualize what is already in graph DB.\n"
                "3) Run `#crawl <gamertag> 2` to enrich Halo-active and stat-backed graph quality."
            ),
            inline=False
        )

        embed.set_footer(text="Examples use your own gamertags. No specific player names are required.")
        await ctx.send(embed=embed)
    
    @commands.command(name='full', help='Get complete lifetime stats from all available matches. Usage: #full <gamertag>')
    async def full(self, ctx: commands.Context, *inputs):
        """Get complete stats from player's entire match history"""
        gamertag = ''.join(inputs)
        await fetch_and_display_stats(ctx, gamertag, stat_type="stats", matches_to_process=None)
    
    @commands.command(name='ranked', help='Get ranked-only stats and performance trends. Usage: #ranked <gamertag>')
    async def ranked(self, ctx: commands.Context, *inputs):
        """Get stats from ranked matches only"""
        gamertag = ''.join(inputs)
        await fetch_and_display_stats(ctx, gamertag, stat_type="ranked", matches_to_process=None)
    
    @commands.command(name='casual', help='Get social/casual playlist stats only. Usage: #casual <gamertag>')
    async def casual(self, ctx: commands.Context, *inputs):
        """Get stats from casual/social matches only"""
        gamertag = ''.join(inputs)
        await fetch_and_display_stats(ctx, gamertag, stat_type="social", matches_to_process=None)
    
    @commands.command(name='server', help='Build a server leaderboard by matching Discord members to Halo gamertags.')
    async def server_stats(self, ctx: commands.Context):
        """Generate server-wide leaderboard from all members"""
        await collect_server_stats(ctx, self.bot)
    
    @commands.command(name='populate', help='Resolve and cache players from recent match history. Usage: #populate <gamertag>')
    async def populate_cache(self, ctx: commands.Context, *inputs):
        """Resolve gamertags to XUIDs and populate the XUID cache"""
        if not inputs:
            await ctx.send("Please provide a gamertag. Example: `#populate GAMERTAG`")
            return
        
        gamertag = ' '.join(inputs)
        
        loading_embed = discord.Embed(
            title="Resolving Gamertags...",
            description=f"Finding and caching all players from **{gamertag}**'s match history",
            colour=0xFFA500,
            timestamp=datetime.now()
        )
        loading_message = await ctx.send(embed=loading_embed)

        async def safe_update_message(embed: discord.Embed):
            """Update loading message, falling back to a fresh message if edit fails."""
            try:
                await loading_message.edit(embed=embed)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                await ctx.send(embed=embed)
        
        try:
            print(f"Getting player list for {gamertag}...")
            players = await get_players_from_recent_matches(gamertag, num_matches=999999)
            
            if not players:
                await loading_message.edit(content=f"Could not find any players from {gamertag}'s matches.")
                return
            
            final_embed = discord.Embed(
                title="Gamertag Resolution Complete",
                description=f"Found and cached **{len(players)}** unique players from {gamertag}'s match history\n\n"
                           f"All gamertags → XUIDs are now cached for fast lookup!",
                colour=0x00FF00,
                timestamp=datetime.now()
            )
            final_embed.set_footer(text="Project Goliath")
            await loading_message.edit(embed=final_embed)
            
        except Exception as e:
            await loading_message.edit(content=f"Error: {e}")
            print(f"Error in populate_cache: {e}")
    
    @commands.command(name='cachestatus', help='Show progress and estimates for background player caching.')
    async def cache_status(self, ctx: commands.Context):
        """Display the current status of background stats caching"""
        try:
            with open(XUID_CACHE_FILE, 'r', encoding='utf-8') as f:
                xuid_cache = json.load(f)
            xuid_mappings = len(xuid_cache)

            progress_candidates = [str(CACHE_PROGRESS_FILE), os.path.join(PROJECT_ROOT, 'cache_progress.json')]
            progress_path = next((path for path in progress_candidates if os.path.exists(path)), None)

            progress = {}
            if progress_path:
                with open(progress_path, 'r', encoding='utf-8') as f:
                    progress = json.load(f)

            processed_matches = int(progress.get('processed_matches', progress.get('last_processed_index', 0)) or 0)
            total_matches = int(progress.get('total_matches', 0) or 0)
            unique_players = len(progress.get('unique_players', []))
            resolved_gamertags = len(progress.get('resolved_gamertags', []))
            percent_processed = (processed_matches / total_matches * 100) if total_matches > 0 else 0
            
            embed = discord.Embed(
                title="📊 Background Caching Status",
                colour=0x00BFFF,
                timestamp=datetime.now()
            )
            embed.add_field(
                name="XUID Cache",
                value=f"Total mappings: **{xuid_mappings:,}**",
                inline=False
            )
            embed.add_field(
                name="Match Scan Progress",
                value=(
                    f"Processed: **{processed_matches:,}** / **{total_matches:,}** matches\n"
                    f"Progress: {percent_processed:.1f}%"
                    if total_matches > 0
                    else "No active match scan progress file"
                ),
                inline=False
            )
            embed.add_field(
                name="Discovery Progress",
                value=f"Unique players tracked: **{unique_players:,}**\n"
                      f"Resolved gamertags: **{resolved_gamertags:,}**",
                inline=False
            )
            embed.set_footer(text="Project Goliath")
            
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
        
        loading_embed = discord.Embed(
            title="🔍 Fetching Friends List...",
            description=f"Finding friends and friends-of-friends for **{gamertag}**\n"
                       f"This may take a minute...",
            colour=0xFFA500,
            timestamp=datetime.now()
        )
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
                if stage == 'friends_found':
                    progress_embed = discord.Embed(
                        title="🔍 Fetching Friends of Friends...",
                        description=f"Found **{total}** direct friends for **{gamertag}**\n"
                                   f"Now checking their friends lists...\n\n"
                                   f"Progress: 0/{total} friends checked",
                        colour=0xFFA500,
                        timestamp=datetime.now()
                    )
                else:
                    percent = int((current / total) * 100) if total > 0 else 0
                    bar_filled = int(percent / 5)  # 20 char bar
                    bar = '█' * bar_filled + '░' * (20 - bar_filled)
                    progress_embed = discord.Embed(
                        title="🔍 Fetching Friends of Friends...",
                        description=f"Checking friends lists for **{gamertag}**\n\n"
                                   f"Progress: **{current}/{total}** friends checked\n"
                                   f"`{bar}` {percent}%\n\n"
                                   f"Found **{fof_count}** unique 2nd-degree connections so far",
                        colour=0xFFA500,
                        timestamp=datetime.now()
                    )
                await safe_update_message(progress_embed)
            
            # Get friends and friends-of-friends (fetches ALL friends)
            result = await api_client.get_friends_of_friends(
                gamertag,
                max_depth=2,
                progress_callback=update_progress
            )
            
            if result.get('error'):
                error_embed = discord.Embed(
                    title="❌ Error",
                    description=result['error'],
                    colour=0xFF0000,
                    timestamp=datetime.now()
                )
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
            
            # Check friends against blacklist
            blacklisted_friends = []
            for friend in friends:
                xuid = friend.get('xuid')
                if xuid in blacklist:
                    blacklisted_friends.append(blacklist[xuid])
            
            # Check friends-of-friends against blacklist (count occurrences)
            blacklisted_fof_counts = {}
            for friend in fof:
                xuid = friend.get('xuid')
                if xuid in blacklist:
                    bl_name = blacklist[xuid]
                    blacklisted_fof_counts[bl_name] = blacklisted_fof_counts.get(bl_name, 0) + 1
            
            # Check which private-friends-list users are on blacklist
            private_blacklisted = []
            for pf in private_friends:
                xuid = pf.get('xuid')
                if xuid in blacklist:
                    private_blacklisted.append(blacklist[xuid])
            
            # Format all private friends list
            if private_friends:
                private_names = [str(pf.get('gamertag') or 'Unknown') for pf in private_friends]
                private_text = "\n".join([f"• {name}" for name in private_names])
            else:
                private_text = "N/A"
            
            # Format blacklisted friends text
            if blacklisted_friends:
                bl_friends_text = "\n".join([f"• {name}" for name in blacklisted_friends])
            else:
                bl_friends_text = "N/A"
            
            # Format private list friends text (blacklisted ones)
            if private_blacklisted:
                private_bl_text = "\n".join([f"• {name}" for name in private_blacklisted])
            else:
                private_bl_text = "N/A"
            
            # Format blacklisted friends-of-friends text
            if blacklisted_fof_counts:
                bl_fof_items = []
                for name, count in sorted(blacklisted_fof_counts.items(), key=lambda x: x[1], reverse=True):
                    if count > 1:
                        bl_fof_items.append(f"• {name} x{count}")
                    else:
                        bl_fof_items.append(f"• {name}")
                bl_fof_text = "\n".join(bl_fof_items)
            else:
                bl_fof_text = "N/A"
            
            # Create embed
            result_embed = discord.Embed(
                title=f"👥 Friends Network: {gamertag}",
                colour=0x00FF00,
                timestamp=datetime.now()
            )
            
            # Direct Friends field
            result_embed.add_field(
                name=f"📋 Direct Friends ({len(friends)})",
                value=f"**Blacklisted Friends:**\n{bl_friends_text[:400]}\n\n"
                      f"**Private Friends List ({len(private_friends)}):**\n{private_text[:400]}",
                inline=False
            )
            
            # Friends of Friends field
            result_embed.add_field(
                name=f"🔗 Friends of Friends ({len(fof)})",
                value=f"**Blacklisted Friends:**\n{bl_fof_text[:800]}",
                inline=False
            )
            
            # Summary field
            total_bl_fof = sum(blacklisted_fof_counts.values())
            result_embed.add_field(
                name="📊 Summary",
                value=f"**Direct friends:** {len(friends)}\n"
                      f"**Blacklisted friends:** {len(blacklisted_friends)}\n"
                      f"**Private friends lists:** {len(private_friends)}\n\n"
                      f"**2nd degree friends:** {len(fof)}\n"
                      f"**Blacklisted 2nd degree friends:** {total_bl_fof}",
                inline=False
            )
            result_embed.set_footer(text="Project Goliath • Note: Private friends lists cannot be accessed")
            
            await safe_update_message(result_embed)
            
        except Exception as e:
            import traceback
            error_embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred: {e}",
                colour=0xFF0000,
                timestamp=datetime.now()
            )
            try:
                await safe_update_message(error_embed)
            except Exception:
                await ctx.send(f"❌ Error in #xboxfriends: {e}")
            print(f"Error in friends_list: {e}")
            traceback.print_exc()


async def setup(bot: commands.Bot):
    """Setup function for loading the cog"""
    await bot.add_cog(StatsCog(bot))
