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
from src.config import XUID_CACHE_FILE, CACHE_PROGRESS_FILE


class StatsCog(commands.Cog, name="Stats"):
    """Commands for fetching and displaying Halo Infinite player statistics"""
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
    
    @commands.command(name='full', help='Get stats from ALL match history. Example - #full XxUK D3STROYxX')
    async def full(self, ctx: commands.Context, *inputs):
        """Get complete stats from player's entire match history"""
        gamertag = ''.join(inputs)
        await fetch_and_display_stats(ctx, gamertag, stat_type="stats", matches_to_process=None)
    
    @commands.command(name='ranked', help='Get stats from RANKED matches only. Example - #ranked XxUK D3STROYxX')
    async def ranked(self, ctx: commands.Context, *inputs):
        """Get stats from ranked matches only"""
        gamertag = ''.join(inputs)
        await fetch_and_display_stats(ctx, gamertag, stat_type="ranked", matches_to_process=None)
    
    @commands.command(name='casual', help='Get stats from CASUAL/SOCIAL matches only. Example - #casual XxUK D3STROYxX')
    async def casual(self, ctx: commands.Context, *inputs):
        """Get stats from casual/social matches only"""
        gamertag = ''.join(inputs)
        await fetch_and_display_stats(ctx, gamertag, stat_type="social", matches_to_process=None)
    
    @commands.command(name='server', help='Collect stats from all server members. Bot will try to match Discord names with Halo gamertags.')
    async def server_stats(self, ctx: commands.Context):
        """Generate server-wide leaderboard from all members"""
        await collect_server_stats(ctx, self.bot)
    
    @commands.command(name='populate', help='Resolve and cache gamertags from recent matches. Example - #populate XxUK D3STROYxX')
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
    
    @commands.command(name='cachestatus', help='Check background caching progress')
    async def cache_status(self, ctx: commands.Context):
        """Display the current status of background stats caching"""
        try:
            # Load XUID cache
            with open(XUID_CACHE_FILE, 'r') as f:
                xuid_cache = json.load(f)
            total_players = len(xuid_cache)
            
            # Count cached players
            cached_count = 0
            for xuid in xuid_cache.keys():
                xuid_dir = os.path.join("player_stats_cache", str(xuid))
                if os.path.exists(xuid_dir) and any(f.endswith('.json') for f in os.listdir(xuid_dir)):
                    cached_count += 1
            
            # Load progress
            progress_file = str(CACHE_PROGRESS_FILE)
            current_index = 0
            if os.path.exists(progress_file):
                with open(progress_file, 'r') as f:
                    progress = json.load(f)
                    current_index = progress.get('last_processed_index', 0)
            
            # Calculate percentages
            percent_cached = (cached_count / total_players * 100) if total_players > 0 else 0
            percent_processed = (current_index / total_players * 100) if total_players > 0 else 0
            
            # Estimate remaining time
            remaining = total_players - cached_count
            est_seconds = remaining * 10  # ~10 seconds per player with 5 accounts
            est_hours = est_seconds / 3600
            est_days = est_hours / 24
            
            embed = discord.Embed(
                title="📊 Background Caching Status",
                colour=0x00BFFF,
                timestamp=datetime.now()
            )
            embed.add_field(
                name="Overall Progress",
                value=f"**{cached_count:,}** / **{total_players:,}** players cached\n"
                      f"Progress: {percent_cached:.1f}%",
                inline=False
            )
            embed.add_field(
                name="Current Session",
                value=f"Processing index: {current_index:,} / {total_players:,}\n"
                      f"Session progress: {percent_processed:.1f}%",
                inline=False
            )
            embed.add_field(
                name="Estimated Time Remaining",
                value=f"~{est_hours:.1f} hours ({est_days:.1f} days)\n"
                      f"*Based on 50x parallel processing (5 accounts)*",
                inline=False
            )
            embed.set_footer(text="Project Goliath • Caching runs every 24 hours")
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"Error checking cache status: {e}")
            print(f"Error in cache_status: {e}")
    
    @commands.command(name='xboxfriends', help='Get Xbox friends and friends-of-friends. Example - #friends XxUK D3STROYxX')
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
        
        try:
            # Load blacklist
            blacklist_path = Path(__file__).parent.parent.parent.parent / 'data' / 'xuid_gamertag_blacklist.json'
            blacklist = {}
            if blacklist_path.exists():
                with open(blacklist_path, 'r') as f:
                    blacklist = json.load(f)
            
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
                await loading_message.edit(embed=progress_embed)
            
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
                await loading_message.edit(embed=error_embed)
                return
            
            # Get friends and friends-of-friends
            friends = result.get('friends', [])
            fof = result.get('friends_of_friends', [])
            private_friends = result.get('private_friends', [])
            
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
                private_names = [pf.get('gamertag', 'Unknown') for pf in private_friends]
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
            
            await loading_message.edit(embed=result_embed)
            
        except Exception as e:
            import traceback
            error_embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred: {e}",
                colour=0xFF0000,
                timestamp=datetime.now()
            )
            await loading_message.edit(embed=error_embed)
            print(f"Error in friends_list: {e}")
            traceback.print_exc()


async def setup(bot: commands.Bot):
    """Setup function for loading the cog"""
    await bot.add_cog(StatsCog(bot))
