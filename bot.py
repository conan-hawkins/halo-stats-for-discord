"""
Halo Infinite Discord Stats Bot
Made by Conan Hawkins
Created: 12/02/2025

Main entry point for the Discord bot
"""

import os
import asyncio
from datetime import datetime

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

from halo_api import StatsFind1, get_players_from_recent_matches
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


@bot.event
async def on_ready():
    """Initialize bot and start background tasks"""
    print(f"{bot.user} has connected to Discord!")
    print(f"Bot is in {len(bot.guilds)} server(s)")
    
    if not auto_refresh_tokens.is_running():
        auto_refresh_tokens.start()
        print("Automatic token refresh enabled (checks every hour)")

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
