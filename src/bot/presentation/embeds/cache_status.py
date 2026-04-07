import discord

from src.bot.presentation.embed_styles import COLOR_INFO, current_timestamp


def build_cache_status_embed(metrics) -> discord.Embed:
    percent_processed = (
        metrics.processed_matches / metrics.total_matches * 100
        if metrics.total_matches > 0
        else 0
    )

    embed = discord.Embed(
        title="📊 Background Caching Status",
        colour=COLOR_INFO,
        timestamp=current_timestamp(),
    )
    embed.add_field(
        name="XUID Cache",
        value=f"Total mappings: **{metrics.xuid_mappings:,}**",
        inline=False,
    )
    embed.add_field(
        name="Match Scan Progress",
        value=(
            f"Processed: **{metrics.processed_matches:,}** / **{metrics.total_matches:,}** matches\n"
            f"Progress: {percent_processed:.1f}%"
            if metrics.total_matches > 0
            else (
                "No active match scan progress file"
                if metrics.progress_state == "missing"
                else "Progress file unreadable"
            )
        ),
        inline=False,
    )
    embed.add_field(
        name="Gamertag Resolution",
        value=f"Resolved gamertags: **{metrics.resolved_gamertags:,}**",
        inline=False,
    )
    embed.set_footer(text="Project Goliath")
    return embed
