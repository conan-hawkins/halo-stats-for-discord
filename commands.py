"""
Discord bot commands for Halo Infinite stats
"""

from datetime import datetime
import discord
from discord.ext import commands

from halo_api import StatsFind1, get_players_from_recent_matches
from discord_utils import get_gamertag_for_member
from embed_formatter import format_error_embed, format_stats_embed, format_leaderboard_embed


async def fetch_and_display_stats(ctx, gamertag, stat_type="stats", matches_to_process=None):
    """
    Fetch and display player statistics
    
    Args:
        ctx: Discord context
        gamertag: Xbox gamertag to fetch
        stat_type: Type of stats to fetch
        matches_to_process: Number of matches to process (None = all)
    """
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
    loading_message = await ctx.send(embed=loading_embed)

    try:
        await StatsFind1.page_getter(gamertag, stat_type, matches_to_process=matches_to_process)
        print(f"API call completed. Error code: {StatsFind1.error_no}")
        
        if StatsFind1.error_no != 0:
            print(f"API returned error {StatsFind1.error_no}")
            await loading_message.delete()
            error_embed = await format_error_embed(StatsFind1.error_no)
            await ctx.send(embed=error_embed)
        else:
            print("API success, formatting Discord message")
            stats_embed = await format_stats_embed(gamertag, StatsFind1.stats_list)
            await loading_message.edit(embed=stats_embed)
    except Exception as e:
        import traceback
        print(f"EXCEPTION: {e}")
        print(f"TRACEBACK: {traceback.format_exc()}")
        await loading_message.delete()
        await ctx.send(f"An error occurred: {e}")


async def collect_server_stats(ctx, bot):
    """
    Collect stats from all server members and create a leaderboard
    
    Args:
        ctx: Discord context
        bot: Discord bot instance
    """
    print(f"Server stats command initiated by {ctx.author}")
    
    loading_embed = discord.Embed(
        title="Collecting Server Stats...",
        description="Scanning all server members and fetching Halo stats...\nThis may take a while!",
        colour=0xFFA500,
        timestamp=datetime.now()
    )
    loading_message = await ctx.send(embed=loading_embed)
    
    members = [member for member in ctx.guild.members if not member.bot]
    print(f"Found {len(members)} non-bot members in server")
    
    member_stats = []
    successful_fetches = 0
    failed_fetches = 0
    
    for member in members:
        try:
            gamertag_attempts = await get_gamertag_for_member(member, bot)
            
            print(f"Attempting to fetch stats for Discord user: {member.name}")
            print(f"   Gamertag attempts (in order): {gamertag_attempts}")
            
            stats_found = False
            for gamertag in gamertag_attempts:
                if stats_found:
                    break
                    
                print(f"   Trying '{gamertag}'...")
                await StatsFind1.page_getter(gamertag, "stats", matches_to_process=None)
            
                if StatsFind1.error_no == 0 and StatsFind1.stats_list and len(StatsFind1.stats_list) >= 7:
                    kd_ratio = float(StatsFind1.stats_list[0]) if StatsFind1.stats_list[0] != 'N/A' else 0
                    win_rate = float(StatsFind1.stats_list[1].rstrip('%')) if '%' in StatsFind1.stats_list[1] else 0
                    avg_kda = float(StatsFind1.stats_list[2]) if StatsFind1.stats_list[2] != 'N/A' else 0
                    kills = int(StatsFind1.stats_list[4]) if StatsFind1.stats_list[4].isdigit() else 0
                    games_played = int(StatsFind1.stats_list[6]) if StatsFind1.stats_list[6].isdigit() else 0
                    
                    member_stats.append({
                        'discord_name': member.display_name,
                        'gamertag': gamertag,
                        'kd_ratio': kd_ratio,
                        'win_rate': win_rate,
                        'avg_kda': avg_kda,
                        'kills': kills,
                        'games_played': games_played
                    })
                    successful_fetches += 1
                    stats_found = True
                    print(f"Successfully fetched stats for {gamertag}")
                else:
                    print(f"No stats found for '{gamertag}', trying next option...")
            
            if not stats_found:
                failed_fetches += 1
                print(f"Could not find stats for {member.name} with any gamertag variation")
                
        except Exception as e:
            failed_fetches += 1
            print(f"Error fetching stats for {member.display_name}: {e}")
    
    member_stats.sort(key=lambda x: x['kd_ratio'], reverse=True)
    
    leaderboard_embed = await format_leaderboard_embed(
        ctx.guild.name, 
        member_stats, 
        successful_fetches, 
        len(members)
    )
    
    await loading_message.edit(embed=leaderboard_embed)
    print(f"Server stats command completed: {successful_fetches} successful, {failed_fetches} failed")


async def populate_player_cache(ctx):
    """
    Cache all players from a gamertag's entire match history
    
    Args:
        ctx: Discord context
    """
    # This will be called from bot.py with proper input handling
    pass
