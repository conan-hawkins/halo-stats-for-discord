"""
Discord bot module for Halo Stats

Provides Discord integration with commands, embeds, and background tasks.
"""

from src.bot.commands import fetch_and_display_stats
from src.bot.embeds import format_error_embed, format_stats_embed
from src.bot.main import get_bot, get_token, run_bot, main, load_cogs
from src.bot.tasks import auto_refresh_tokens, auto_cache_all_players

__all__ = [
    # Commands
    "fetch_and_display_stats",
    # Embeds
    "format_error_embed",
    "format_stats_embed",
    # Main
    "get_bot",
    "get_token",
    "run_bot",
    "main",
    "load_cogs",
    # Tasks
    "auto_refresh_tokens",
    "auto_cache_all_players",
]
