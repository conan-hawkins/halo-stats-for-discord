"""Discord bot commands for Halo Infinite stats."""

from datetime import datetime

from src.api.client import StatsFind1
from src.bot.embeds import format_error_embed, format_stats_embed
from src.bot.presentation.embeds.loading import build_stats_loading_embed


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

    loading_embed = build_stats_loading_embed(gamertag, matches_to_process=matches_to_process)
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

