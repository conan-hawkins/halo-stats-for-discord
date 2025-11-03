"""
Discord embed formatting for Halo Infinite statistics
"""

from datetime import datetime
import discord


async def format_error_embed(error_no):
    """
    Create an error embed based on error code
    
    Args:
        error_no: Error number from API
        
    Returns:
        Discord Embed object
    """
    error_messages = {
        1: "ERROR - USE OF UNAUTHORISED CHARACTERS DETECTED.",
        2: "ERROR - PLAYER NOT FOUND. PLEASE CHECK SPELLING.",
        3: "ERROR - PLAYERS PROFILE IS SET TO PRIVATE.",
        4: "ERROR - SOMETHING UNEXPECTED HAPPENED."
    }
    
    title = error_messages.get(error_no, "ERROR - UNKNOWN ERROR OCCURRED.")
    
    embed = discord.Embed(
        title=title,
        colour=0xFF0000,
        timestamp=datetime.now()
    )
    embed.set_footer(
        text="Project Goliath", 
        icon_url='https://www.iconsdb.com/icons/preview/dark-gray/error-4-xxl.png'
    )
    
    return embed


async def format_stats_embed(gamertag, stats_list):
    """
    Format player statistics into a Discord embed
    
    Args:
        gamertag: Player's gamertag
        stats_list: List of stats from API [kd_ratio, win_rate, avg_kda, deaths, kills, assists, games_played]
        
    Returns:
        Discord Embed object
    """
    print(f"Formatting Discord embed for {gamertag}")
    print(f"Stats data: {stats_list}")
    
    title = f"{gamertag.upper()} - OVERALL STATS"
    embed = discord.Embed(
        title=title,
        colour=0x00b0f4,
        timestamp=datetime.now()
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
                stats_list[i] != '0' and stats_list[i] != '0%' 
                for i in range(len(stats_list))
            )
            
            if not has_matches:
                embed.add_field(
                    name="No Match History Found", 
                    value="This player has no recorded matches.", 
                    inline=False
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

    embed.set_footer(
        text="Project Goliath",
        icon_url="https://static.wikia.nocookie.net/halo/images/a/a6/H3_Difficulty_LegendaryIcon.png/revision/latest/scale-to-width-down/150?cb=20160930195427"
    )
    
    return embed


async def format_leaderboard_embed(guild_name, member_stats, successful_fetches, total_members):
    """
    Format server leaderboard into a Discord embed
    
    Args:
        guild_name: Name of the Discord server
        member_stats: List of player stat dictionaries
        successful_fetches: Number of successful API calls
        total_members: Total number of members checked
        
    Returns:
        Discord Embed object
    """
    embed = discord.Embed(
        title=f"🏆 {guild_name} - Halo Stats Leaderboard",
        description=f"Successfully fetched stats for **{successful_fetches}/{total_members}** members",
        colour=0x00b0f4,
        timestamp=datetime.now()
    )
    
    if member_stats:
        leaderboard_text = ""
        for i, player in enumerate(member_stats[:10], 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"**{i}.**"
            leaderboard_text += f"{medal} **{player['gamertag']}**\n"
            leaderboard_text += f"   K/D: {player['kd_ratio']} | Win Rate: {player['win_rate']}% | Avg KDA: {player['avg_kda']}\n"
            leaderboard_text += f"   Games: {player['games_played']} | Kills: {player['kills']:,}\n\n"
        
        embed.add_field(name="📊 Top Players by K/D Ratio", value=leaderboard_text, inline=False)
    else:
        embed.add_field(name="⚠️ No Stats Found", value="Could not fetch stats for any server members.", inline=False)
    
    embed.set_footer(
        text="Project Goliath",
        icon_url="https://static.wikia.nocookie.net/halo/images/a/a6/H3_Difficulty_LegendaryIcon.png/revision/latest/scale-to-width-down/150?cb=20160930195427"
    )
    
    return embed
