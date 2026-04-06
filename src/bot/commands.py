"""
Discord bot commands for Halo Infinite stats
"""

from datetime import datetime
import discord

from src.api.client import StatsFind1
from src.bot.embeds import format_error_embed, format_stats_embed


async def fetch_and_display_stats(ctx, gamertag, stat_type="stats", matches_to_process=None):
    """
    Fetch and display player statistics
    
    Args:
        ctx: Discord context
        gamertag: Xbox gamertag to fetch
        stat_type: Type of stats to fetch
        matches_to_process: Number of matches to process (None = all)
    """
    print(f"[DEBUG] fetch_and_display_stats CALLED for '{gamertag}' at {datetime.now()}")
    print(f"Discord command received: {stat_type} for '{gamertag}' (matches: {'ALL' if matches_to_process is None else matches_to_process})")

    loading_embed = discord.Embed(
        title="Loading Stats...",
        description=f"Fetching stats for **{gamertag}** from {'ALL matches' if matches_to_process is None else f'{matches_to_process} matches'}\nPlease wait...",
        colour=0xFFA500,
        timestamp=datetime.now()
    )
    loading_embed.set_footer(
        text="Project Goliath", 
        icon_url="https://static.wikia.nocookie.net/halo/images/a/a6/H3_Difficulty_LegendaryIcon.png/revision/latest/scale-to-width-down/150?cb=20160930195427"
    )
    print(f"[DEBUG] Sending loading embed...")
    loading_message = await ctx.send(embed=loading_embed)
    print(f"[DEBUG] Loading embed sent, message ID: {loading_message.id}")

    try:
        await StatsFind1.page_getter(gamertag, stat_type, matches_to_process=matches_to_process)
        print(f"API call completed. Error code: {StatsFind1.error_no}")
        
        if StatsFind1.error_no != 0:
            print(f"API returned error {StatsFind1.error_no}")
            print(f"[DEBUG] Deleting loading message and sending error embed...")
            await loading_message.delete()
            error_embed = await format_error_embed(StatsFind1.error_no)
            await ctx.send(embed=error_embed)
            print(f"[DEBUG] Error embed sent")
        else:
            print("API success, formatting Discord message")
            stats_embed = await format_stats_embed(gamertag, StatsFind1.stats_list, stat_type)
            print(f"[DEBUG] Editing loading message {loading_message.id} with stats embed...")
            await loading_message.edit(embed=stats_embed)
            print(f"[DEBUG] Stats embed edit complete")
    except Exception as e:
        import traceback
        print(f"EXCEPTION: {e}")
        print(f"TRACEBACK: {traceback.format_exc()}")
        await loading_message.delete()
        await ctx.send(f"An error occurred: {e}")

__all__ = [
    "fetch_and_display_stats",
]

