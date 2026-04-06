"""
Halo Infinite Discord Stats Bot - Main Module
Made by Conan Hawkins
Created: 12/02/2025

Discord bot setup, configuration, and event handlers.
Commands are organized into Cogs for better maintainability.
"""

import os
import asyncio
from pathlib import Path

import discord
from discord.ext import commands
from dotenv import load_dotenv

# Load environment before importing project modules that read config on import.
load_dotenv()

from src.api import StatsFind1
from src.bot.tasks import auto_refresh_tokens, auto_cache_all_players, proactive_token_refresh


# ============================================================================
# Bot Configuration
# ============================================================================

TOKEN = os.getenv('DISCORD_TOKEN')

intents = discord.Intents.all()
intents.members = True

bot = commands.Bot(command_prefix="#", intents=intents)
bot.remove_command("help")

# Path to cogs directory
COGS_DIR = Path(__file__).parent / "cogs"


# ============================================================================
# Cog Loading
# ============================================================================

async def load_cogs():
    """Load all cog extensions from the cogs directory"""
    # List of cogs to load
    cog_modules = [
        "src.bot.cogs.stats",
        "src.bot.cogs.graph",
        "src.bot.cogs.terminal",
    ]
    
    for cog in cog_modules:
        try:
            await bot.load_extension(cog)
            print(f"✓ Loaded cog: {cog}")
        except Exception as e:
            print(f"✗ Failed to load cog {cog}: {e}")


# ============================================================================
# Event Handlers
# ============================================================================

@bot.event
async def on_ready():
    """Initialize bot and start background tasks"""
    print(f"{bot.user} has connected to Discord!")
    print(f"Bot is in {len(bot.guilds)} server(s)")
    
    # Start background tasks
    if not auto_refresh_tokens.is_running():
        auto_refresh_tokens.start()
        print("✓ Automatic token refresh enabled (checks every hour)")
    
    if not proactive_token_refresh.is_running():
        proactive_token_refresh.start()
        print("✓ Weekly proactive refresh enabled (prevents 90-day token expiration)")
    
    # Uncomment to enable background caching
    # if not auto_cache_all_players.is_running():
    #     auto_cache_all_players.start()
    #     print("✓ Background stats caching enabled (runs every 24 hours)")


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    """Global error handler for bot commands"""
    if isinstance(error, commands.CommandNotFound):
        return  # Silently ignore unknown commands
    
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"Missing required argument: `{error.param.name}`")
        return
    
    if isinstance(error, commands.BadArgument):
        await ctx.send(f"Invalid argument: {error}")
        return
    
    # Log unexpected errors
    print(f"Command error in {ctx.command}: {error}")
    await ctx.send(f"An error occurred: {error}")


# ============================================================================
# Bot Instance Accessors
# ============================================================================

def get_bot() -> commands.Bot:
    """Return the bot instance for external use"""
    return bot


def get_token() -> str:
    """Return the Discord token"""
    return TOKEN


# ============================================================================
# Main Entry Point
# ============================================================================

async def run_bot():
    """Initialize authentication, load cogs, and start the Discord bot"""
    print("=" * 50)
    print("Halo Infinite Discord Stats Bot")
    print("Made by Conan Hawkins")
    print("=" * 50)
    print()
    
    # Validate authentication tokens
    print("Validating Halo authentication tokens...")
    if not await StatsFind1.ensure_valid_tokens():
        print("Failed to validate tokens. Run: python -m src.auth.tokens")
        return
    
    print("All tokens validated successfully!")
    print()
    
    # Check for Discord token
    if not TOKEN:
        print("ERROR: DISCORD_TOKEN not found in environment variables")
        print("Please create a .env file with your Discord bot token:")
        print("  DISCORD_TOKEN=your_token_here")
        return
    
    # Load cogs before starting
    await load_cogs()
    print()
    
    # Start the bot
    async with bot:
        await bot.start(TOKEN)


async def main():
    """Alias for run_bot() for backwards compatibility"""
    await run_bot()


if __name__ == "__main__":
    asyncio.run(run_bot())
