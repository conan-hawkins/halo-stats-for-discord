from typing import Optional

import discord

from src.bot.presentation.embed_styles import COLOR_ERROR, COLOR_INFO, COLOR_LOADING, apply_footer, current_timestamp


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


def build_first_run_collecting_embed(gamertag: str) -> discord.Embed:
    """Placeholder shown on a player's first-ever lookup: no numbers yet,
    since the full match history is being collected in the background."""
    embed = discord.Embed(
        title="Collecting Full Match History...",
        description=(
            f"This is the first time **{gamertag}** has been looked up, "
            "so their entire match history is being collected in the "
            "background. This can take a few minutes.\n\n"
            "Run the command again shortly to see stats."
        ),
        colour=COLOR_LOADING,
        timestamp=current_timestamp(),
    )
    apply_footer(embed)
    return embed


def build_first_run_complete_embed(gamertag: str, matches_processed: int) -> discord.Embed:
    """Posted once a first-run background full-collect finishes."""
    embed = discord.Embed(
        title="Full Match History Collected",
        description=(
            f"Finished collecting **{gamertag}**'s full match history "
            f"({matches_processed} matches). Run the command again to see stats."
        ),
        colour=COLOR_INFO,
        timestamp=current_timestamp(),
    )
    apply_footer(embed)
    return embed


def build_first_run_failed_embed(gamertag: str, message: str) -> discord.Embed:
    """Posted if a first-run background full-collect fails."""
    embed = discord.Embed(
        title="Match History Collection Failed",
        description=f"Could not collect match history for **{gamertag}**: {message}",
        colour=COLOR_ERROR,
        timestamp=current_timestamp(),
    )
    apply_footer(embed)
    return embed
