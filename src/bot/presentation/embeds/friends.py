from datetime import datetime

import discord

from src.bot.presentation.embed_styles import COLOR_ERROR, COLOR_LOADING


def build_xboxfriends_loading_embed(gamertag: str) -> discord.Embed:
    return discord.Embed(
        title="🔍 Fetching Friends List...",
        description=(
            f"Finding friends and friends-of-friends for **{gamertag}**\n"
            "This may take a minute..."
        ),
        colour=COLOR_LOADING,
        timestamp=datetime.now(),
    )


def build_xboxfriends_progress_embed(gamertag: str, current: int, total: int, stage: str, fof_count: int) -> discord.Embed:
    if stage == "friends_found":
        return discord.Embed(
            title="🔍 Fetching Friends of Friends...",
            description=(
                f"Found **{total}** direct friends for **{gamertag}**\n"
                "Now checking their friends lists...\n\n"
                f"Progress: 0/{total} friends checked"
            ),
            colour=COLOR_LOADING,
            timestamp=datetime.now(),
        )

    percent = int((current / total) * 100) if total > 0 else 0
    bar_filled = int(percent / 5)
    bar = "█" * bar_filled + "░" * (20 - bar_filled)

    return discord.Embed(
        title="🔍 Fetching Friends of Friends...",
        description=(
            f"Checking friends lists for **{gamertag}**\n\n"
            f"Progress: **{current}/{total}** friends checked\n"
            f"`{bar}` {percent}%\n\n"
            f"Found **{fof_count}** unique 2nd-degree connections so far"
        ),
        colour=COLOR_LOADING,
        timestamp=datetime.now(),
    )


def build_xboxfriends_error_embed(description: str) -> discord.Embed:
    return discord.Embed(
        title="❌ Error",
        description=str(description),
        colour=COLOR_ERROR,
        timestamp=datetime.now(),
    )


def build_xboxfriends_result_embed(
    gamertag: str,
    friends: list,
    friends_of_friends: list,
    private_friends: list,
    blacklist: dict,
) -> discord.Embed:
    blacklisted_friends = [
        blacklist[friend.get("xuid")]
        for friend in friends
        if friend.get("xuid") in blacklist
    ]

    blacklisted_fof_counts = {}
    for friend in friends_of_friends:
        xuid = friend.get("xuid")
        if xuid in blacklist:
            bl_name = blacklist[xuid]
            blacklisted_fof_counts[bl_name] = blacklisted_fof_counts.get(bl_name, 0) + 1

    if private_friends:
        private_names = [str(pf.get("gamertag") or "Unknown") for pf in private_friends]
        private_text = "\n".join([f"• {name}" for name in private_names])
    else:
        private_text = "N/A"

    if blacklisted_friends:
        bl_friends_text = "\n".join([f"• {name}" for name in blacklisted_friends])
    else:
        bl_friends_text = "N/A"

    if blacklisted_fof_counts:
        bl_fof_items = []
        for name, count in sorted(blacklisted_fof_counts.items(), key=lambda x: x[1], reverse=True):
            if count > 1:
                bl_fof_items.append(f"• {name} x{count}")
            else:
                bl_fof_items.append(f"• {name}")
        bl_fof_text = "\n".join(bl_fof_items)
    else:
        bl_fof_text = "N/A"

    result_embed = discord.Embed(
        title=f"👥 Friends Network: {gamertag}",
        colour=0x00FF00,
        timestamp=datetime.now(),
    )

    result_embed.add_field(
        name=f"📋 Direct Friends ({len(friends)})",
        value=(
            f"**Blacklisted Friends:**\n{bl_friends_text[:400]}\n\n"
            f"**Private Friends List ({len(private_friends)}):**\n{private_text[:400]}"
        ),
        inline=False,
    )

    result_embed.add_field(
        name=f"🔗 Friends of Friends ({len(friends_of_friends)})",
        value=f"**Blacklisted Friends:**\n{bl_fof_text[:800]}",
        inline=False,
    )

    total_bl_fof = sum(blacklisted_fof_counts.values())
    result_embed.add_field(
        name="📊 Summary",
        value=(
            f"**Direct friends:** {len(friends)}\n"
            f"**Blacklisted friends:** {len(blacklisted_friends)}\n"
            f"**Private friends lists:** {len(private_friends)}\n\n"
            f"**2nd degree friends:** {len(friends_of_friends)}\n"
            f"**Blacklisted 2nd degree friends:** {total_bl_fof}"
        ),
        inline=False,
    )
    result_embed.set_footer(text="Project Goliath • Note: Private friends lists cannot be accessed")

    return result_embed
