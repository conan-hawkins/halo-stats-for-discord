from collections.abc import Iterable

import discord

from src.bot.presentation.embed_styles import COLOR_INFO, current_timestamp
from src.bot.stats_profiles import StatsProfile


def build_command_help_embed(cmd) -> discord.Embed:
    embed = discord.Embed(
        title=f"Help: #{cmd.name}",
        description=cmd.help or "No additional help is available for this command.",
        colour=COLOR_INFO,
        timestamp=current_timestamp(),
    )
    if cmd.signature:
        embed.add_field(name="Usage", value=f"`#{cmd.name} {cmd.signature}`", inline=False)
    else:
        embed.add_field(name="Usage", value=f"`#{cmd.name}`", inline=False)

    embed.set_footer(text="Tip: Gamertags with spaces should be typed normally, e.g. #stats Player Name")
    return embed


def build_stats_help_guide_embed(stats_profiles: Iterable[StatsProfile]) -> discord.Embed:
    embed = discord.Embed(
        title="Halo Bot Command Guide",
        description=(
            "Use `#help <command>` for focused help on one command.\n"
            "Example: `#help xboxfriends`"
        ),
        colour=COLOR_INFO,
        timestamp=current_timestamp(),
    )

    embed.add_field(
        name="Player Stats Commands",
        value="\n".join(
            f"`#{profile.command_name} <gamertag>`: {profile.guide_description}"
            for profile in stats_profiles
        ),
        inline=False,
    )

    embed.add_field(
        name="Social Commands",
        value=(
            "`#xboxfriends <gamertag>`: Live Xbox friends + friends-of-friends scan with blacklist checks.\n"
            "`#network <gamertag>`: Visual friend graph from data stored in graph database.\n"
            "`#halonet <gamertag>`: Visual co-play graph weighted by shared matches (auto-refreshes missing seed edges).\n"
            "`#hubs [min_friends]`: Lists players with high Halo-active connectivity."
        ),
        inline=False,
    )

    embed.add_field(
        name="Admin and Utility Commands",
        value=(
            "`#cachestatus`: Shows progress of background caching jobs.\n"
            "`#graphstats`: Shows social graph database totals and health.\n"
            "`#crawlfriends <gamertag> [depth]` / `#crawlstop`: Crawl Halo-active friends and update graph DB (admin only, advanced backfill).\n"
            "`#crawlgames <gamertag> [depth] [--global]`: Builds co-play edge weights from shared match history (default focused scope; --global for full sweep)."
        ),
        inline=False,
    )

    embed.add_field(
        name="Suggested Workflow",
        value=(
            "1) Run `#xboxfriends <gamertag>` to discover social edges quickly.\n"
            "2) Run `#halonet <gamertag>` to inspect co-play clusters (the command auto-refreshes missing seed edges).\n"
            "3) Run `#network <gamertag>` for friend-link context around the same player.\n"
            "4) If coverage is still sparse, use admin backfill commands: `#crawlfriends` then `#crawlgames`."
        ),
        inline=False,
    )

    embed.set_footer(text="Examples use your own gamertags. No specific player names are required.")
    return embed
