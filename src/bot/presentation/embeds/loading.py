from typing import Optional

import discord

from src.bot.presentation.embed_styles import COLOR_LOADING, apply_footer, current_timestamp


def build_stats_loading_embed(gamertag: str, matches_to_process: Optional[int] = None) -> discord.Embed:
    match_label = "ALL matches" if matches_to_process is None else f"{matches_to_process} matches"
    embed = discord.Embed(
        title="Loading Stats...",
        description=f"Fetching stats for **{gamertag}** from {match_label}\nPlease wait...",
        colour=COLOR_LOADING,
        timestamp=current_timestamp(),
    )
    apply_footer(embed)
    return embed
