"""
Halo Infinite Discord Stats Bot
Made by Conan Hawkins
Created: 12/02/2025

Main entry point for the Discord bot
"""

import os
import asyncio
import json
from datetime import datetime

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

from halo_api import StatsFind1, get_players_from_recent_matches, api_client
from commands import fetch_and_display_stats, collect_server_stats

# ============================================================================
# Bot Configuration
# ============================================================================

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

intents = discord.Intents.all() 
intents.members = True

bot = commands.Bot(command_prefix="#", intents=intents)

# ============================================================================
# Discord Commands
# ============================================================================

@bot.command(name='full', help='Get stats from ALL match history. Example - #full XxUK D3STROYxX')
async def full(ctx, *inputs):
    """Get complete stats from player's entire match history"""
    gamertag = ''.join(inputs)
    await fetch_and_display_stats(ctx, gamertag, stat_type="stats", matches_to_process=None)


@bot.command(name='server', help='Collect stats from all server members. Bot will try to match Discord names with Halo gamertags.')
async def server_stats(ctx):
    """Generate server-wide leaderboard from all members"""
    await collect_server_stats(ctx, bot)


@bot.command(name='populate', help='Resolve and cache gamertags from recent matches. Example - #populate XxUK D3STROYxX')
async def populate_cache(ctx, *inputs):
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


@bot.command(name='cachestatus', help='Check background caching progress')
async def cache_status(ctx):
    """Display the current status of background stats caching"""
    try:
        # Load XUID cache
        with open("xuid_gamertag_cache.json", 'r') as f:
            xuid_cache = json.load(f)
        total_players = len(xuid_cache)
        
        # Count cached players
        cached_count = 0
        for xuid in xuid_cache.keys():
            xuid_dir = os.path.join("player_stats_cache", str(xuid))
            if os.path.exists(xuid_dir) and any(f.endswith('.json') for f in os.listdir(xuid_dir)):
                cached_count += 1
        
        # Load progress
        progress_file = "cache_progress.json"
        current_index = 0
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                current_index = progress.get('last_processed_index', 0)
        
        # Calculate percentages
        percent_cached = (cached_count / total_players * 100) if total_players > 0 else 0
        percent_processed = (current_index / total_players * 100) if total_players > 0 else 0
        
        # Estimate remaining time (realistic estimate based on actual performance)
        remaining = total_players - cached_count
        est_seconds = remaining * 10  # With 5 accounts: ~10 seconds per player average
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

# ============================================================================
# Background Tasks & Events
# ============================================================================

@tasks.loop(hours=1)
async def auto_refresh_tokens():
    """Automatically refresh Halo API tokens every hour"""
    print("Checking token validity...")
    
    if await StatsFind1.ensure_valid_tokens():
        print("Tokens are valid")
    else:
        print("Token validation/refresh failed")


@tasks.loop(hours=24)
async def auto_cache_all_players():
    """Background process: Cache full stats for all players in XUID cache if not already cached
    
    Performance optimizations:
    - Parallel processing: Process 20 players concurrently (with 2 accounts = 40 req/10s)
    - Progress tracking: Resume from last position on restart
    - Smart skipping: Pre-filter cached players before processing
    """
    print("Starting background stats caching...")
    
    try:
        with open("xuid_gamertag_cache.json", 'r') as f:
            xuid_cache = json.load(f)
    except Exception as e:
        print(f"Error loading XUID cache: {e}")
        return
    
    # Load progress tracker
    progress_file = "cache_progress.json"
    progress = {"last_processed_index": 0, "completed_xuids": []}
    
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                progress = json.load(f)
            print(f"Resuming from player index {progress['last_processed_index']}")
        except:
            pass
    
    # Convert to list for indexing
    xuid_items = list(xuid_cache.items())
    total = len(xuid_items)
    
    # Pre-filter: Skip players who already have cache
    players_to_process = []
    skipped = 0
    
    start_idx = progress['last_processed_index']
    completed_set = set(progress['completed_xuids'])
    
    for idx in range(start_idx, total):
        xuid, gamertag = xuid_items[idx]
        
        # Skip if already completed in this session
        if xuid in completed_set:
            skipped += 1
            continue
        
        # Skip if cache exists
        xuid_dir = os.path.join("player_stats_cache", str(xuid))
        if os.path.exists(xuid_dir) and any(f.endswith('.json') for f in os.listdir(xuid_dir)):
            skipped += 1
            completed_set.add(xuid)
            continue
        
        players_to_process.append((idx, xuid, gamertag))
    
    print(f"Total: {total}, Already cached: {skipped}, To process: {len(players_to_process)}")
    
    if not players_to_process:
        print("All players already cached!")
        return
    
    # Process in parallel batches
    cached = errors = 0
    batch_size = 50  # Process 50 players concurrently with 5 accounts (10 per account)
    
    async def process_player(idx, xuid, gamertag):
        """Process a single player and return result"""
        try:
            print(f"[{idx+1}/{total}] Caching: {gamertag} (XUID: {xuid})")
            result = await api_client.calculate_comprehensive_stats(xuid, "overall", gamertag, None)
            
            if result and result.get('error') == 0:
                return ('success', idx, xuid)
            else:
                return ('error', idx, xuid)
        except Exception as e:
            print(f"Error caching {gamertag}: {e}")
            return ('error', idx, xuid)
    
    # Process in batches
    for batch_start in range(0, len(players_to_process), batch_size):
        batch = players_to_process[batch_start:batch_start + batch_size]
        
        # Proactively check and refresh tokens before each batch
        # This prevents 401 errors during processing
        try:
            await api_client.ensure_valid_tokens()
        except Exception as e:
            print(f"Warning: Token validation failed before batch: {e}")
        
        # Process batch concurrently
        tasks_list = [process_player(idx, xuid, gamertag) for idx, xuid, gamertag in batch]
        results = await asyncio.gather(*tasks_list, return_exceptions=True)
        
        # Update counters and progress
        for result in results:
            if isinstance(result, Exception):
                errors += 1
            elif result[0] == 'success':
                cached += 1
                completed_set.add(result[2])
            else:
                errors += 1
        
        # Save progress every batch
        last_idx = batch[-1][0]
        progress['last_processed_index'] = last_idx + 1
        progress['completed_xuids'] = list(completed_set)
        
        try:
            with open(progress_file, 'w') as f:
                json.dump(progress, f)
        except:
            pass
        
        # Small delay between batches to avoid overwhelming API
        await asyncio.sleep(1)
        
        # Progress update every 10 batches
        if (batch_start // batch_size) % 10 == 0:
            print(f"Progress: {cached} cached, {errors} errors, {skipped + len(completed_set)} total completed")
    
    print(f"Caching complete: {cached} new, {skipped} skipped, {errors} errors")
    
    # Reset progress file when complete
    try:
        if os.path.exists(progress_file):
            os.remove(progress_file)
    except:
        pass


@bot.event
async def on_ready():
    """Initialize bot and start background tasks"""
    print(f"{bot.user} has connected to Discord!")
    print(f"Bot is in {len(bot.guilds)} server(s)")
    
    if not auto_refresh_tokens.is_running():
        auto_refresh_tokens.start()
        print("Automatic token refresh enabled (checks every hour)")
    
    if not auto_cache_all_players.is_running():
        auto_cache_all_players.start()
        print("Background stats caching enabled (runs every 24 hours)")

# ============================================================================
# Main Entry Point
# ============================================================================

async def main():
    """Initialize authentication and start the Discord bot"""
    print("Validating Halo authentication tokens...")
    
    if not await StatsFind1.ensure_valid_tokens():
        print("Failed to validate tokens. Run: python get_auth_tokens.py")
        return
    
    print("All tokens validated successfully!")
    
    async with bot:
        await bot.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
