import discord

from src.bot.presentation.embed_styles import apply_footer, current_timestamp
from src.bot.stats_profiles import get_stats_profile_for_fetch_type


async def format_stats_embed(gamertag, stats_list, stat_type="overall"):
    """
    Format player statistics into a Discord embed.

    Args:
        gamertag: Player's gamertag
        stats_list: List of stats from API [kd_ratio, win_rate, avg_kda, deaths, kills, assists, games_played]
        stat_type: Type of stats ("overall", "ranked", "social")

    Returns:
        Discord Embed object
    """
    print(f"Formatting Discord embed for {gamertag} ({stat_type})")
    print(f"Stats data: {stats_list}")

    profile = get_stats_profile_for_fetch_type(stat_type)

    embed = discord.Embed(
        title=f"{gamertag.upper()} - {profile.display_name}",
        colour=profile.embed_color,
        timestamp=current_timestamp(),
    )

    if len(stats_list) >= 7:
        games_played = stats_list[6]
        embed.description = f"Based on {games_played} matches"

    if not stats_list or len(stats_list) < 6:
        print("ERROR: Invalid stats list")
        embed.add_field(name="Error", value="Invalid stats data received from API", inline=False)
    else:
        try:
            has_matches = any(
                stats_list[i] != "0" and stats_list[i] != "0%"
                for i in range(len(stats_list))
            )

            if not has_matches:
                embed.add_field(
                    name="No Match History Found",
                    value="This player has no recorded matches.",
                    inline=False,
                )
            else:
                kills = f"{int(stats_list[4]):,}" if stats_list[4].isdigit() else stats_list[4]
                deaths = f"{int(stats_list[3]):,}" if stats_list[3].isdigit() else stats_list[3]
                assists = f"{int(stats_list[5]):,}" if stats_list[5].isdigit() else stats_list[5]

                stats_text = f"""```ansi
🏆 𝗪𝗶𝗻 𝗥𝗮𝘁𝗲           📊 𝗞/𝗗 𝗥𝗮𝘁𝗶𝗼
   {stats_list[1]:<18}    {stats_list[0]}

⚔️ 𝗔𝘃𝗴 𝗞𝗗𝗔           💀 𝗞𝗶𝗹𝗹𝘀
   {stats_list[2]:<18}    {kills}

☠️ 𝗗𝗲𝗮𝘁𝗵𝘀             🤝 𝗔𝘀𝘀𝗶𝘀𝘁𝘀
   {deaths:<18}    {assists}
```"""
                embed.add_field(name=" Player Statistics", value=stats_text, inline=False)
                embed.set_image(url="https://gaming-cdn.com/images/products/2674/screenshot/halo-infinite-campaign-pc-xbox-one-game-microsoft-store-wallpaper-2-thumbv2.jpg?v=1732013222")
        except (IndexError, TypeError) as e:
            print(f"ERROR: Failed to format stats: {e}")
            embed.add_field(name="Error", value=f"Could not format stats data: {e}", inline=False)

    apply_footer(embed)
    return embed
