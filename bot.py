import os
import discord
from discord.ext import commands
from dotenv import load_dotenv
from main import StatsFind1
from datetime import datetime
import asyncio

# Made by XxUK D3STROYxX 
# 12/02/2025
#============================================================================#
#                                                                            #
# Sets up a discord bot and some basic commands, the commands are mostly     #
# executed in attached files they are only initiated and returned here       #
#                                                                            #
#============================================================================#

#==========================================================
# Sets up dotenv to hide the discord token
#==========================================================
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

#==========================================================
# Sets intents for the bot = needed for a bot to function
#==========================================================
intents = discord.Intents.all() 
intents.members = True
# set a prefix
bot = commands.Bot(command_prefix="#", intents=intents) 

#==========================================================#==========================================================
           
                                #==========================================================
                                # Halo stat tracker commands
                                #==========================================================

#==========================================================#==========================================================
#==========================================================
# makes a stats function that passes the stats stat_type
# marker into ranked_and_stats function
#==========================================================
# sets up command name
@bot.command(name='stats', help='Use command prefix followed by stats or ranked and gamertag to search. Example - #stats XxUK D3STROYxX') 
async def stats(ctx, *inputs):
    stat_type = "stats"
    await ranked_and_stats(ctx, stat_type, *inputs)

#==========================================================
# makes a ranked function that passes the ranked stat_type
# marker into the ranked_and_stats function
#==========================================================
# sets up command name
@bot.command(name='ranked')
async def ranked(ctx, *inputs):
    stat_type = "ranked"
    await ranked_and_stats(ctx, stat_type, *inputs)

#==========================================================#==========================================================
           
                                #==========================================================
                                # Halo stat tracker commands
                                #==========================================================

#==========================================================#==========================================================
#==========================================================
# makes a ranked_and_stats function that can run the ranked 
# or stats commands using the stat_type marker
#==========================================================
async def ranked_and_stats(ctx, stat_type, *inputs):
    # Merge inputs for gamertags with spaces in them
    if len(inputs) == 1:
        pass
    elif len(inputs) == 2:
        inputs = inputs[0] + " " + inputs[1]
    elif len(inputs) == 3:
        inputs = inputs[0] + " " + inputs[1] + " " + inputs[2]
    elif len(inputs) == 4:
        inputs = inputs[0] + " " + inputs[1] + " " + inputs[2] + " " + inputs[3]
    # Exit program and send error message if input merging catches and error
    else:
        print("ERROR: Number of inputs are invalid")
        exit()
    
    gamertag = inputs

    print("Test point 1", gamertag, stat_type) # !!!==== For testing to be removed later ====!!!

# opens the main file and run page_getter function   
    StatsFind1.page_getter(gamertag, stat_type)
    
    # Makes template for stats command
    if stat_type == "stats":
        title = ((gamertag+" - overall stats").upper())
        embed = discord.Embed(title=title,
                            colour=0x00b0f4,
                            timestamp=datetime.now())
    
    # Makes template for ranked command
    else:    
        title = ((gamertag+" - ranked stats").upper())
        embed = discord.Embed(title=title,
                            colour=0xFF0000,
                            timestamp=datetime.now())
    print("Test point 4", stat_type) # !!!==== For testing to be removed later ====!!! 
    # Adds all information retreived in the main file to the template
    embed.add_field(name="WIN RATE",
                    value= StatsFind1.stats_list[1],
                    inline=True)
    embed.add_field(name="KD RATIO",
                    value= StatsFind1.stats_list[0],
                    inline=True)
    embed.add_field(name="AVG KDA",
                    value= StatsFind1.stats_list[2],
                    inline=True)
    embed.add_field(name="KILLS",
                    value= StatsFind1.stats_list[4],
                    inline=True)
    embed.add_field(name="DEATHS",
                    value= StatsFind1.stats_list[6],
                    inline=True)
    embed.add_field(name="ASSISTS",
                    value= StatsFind1.stats_list[5],
                    inline=True)
    
    print("Test point 5", stat_type) # !!!==== For testing to be removed later ====!!! 
    
    # Adds large image to the template
    embed.set_image(url="https://gaming-cdn.com/images/products/2674/screenshot/halo-infinite-campaign-pc-xbox-one-game-microsoft-store-wallpaper-2-thumbv2.jpg?v=1732013222")

    # Adds footer with icon to template
    embed.set_footer(text="Project Goliath",
                    icon_url="https://www.freeiconspng.com/img/36668")
    
    print("Test final before send") # !!!==== For testing to be removed later ====!!!
    
    # Sends finished template to discord
    await ctx.send(embed=embed)

async def main():
    async with bot:
        await bot.start(TOKEN)

asyncio.run(main())

"""
Needs to be implemented in the future!!!

#==========================================================
# event executed when a valid command catches an error
#==========================================================
#async def on_command_error(self, context: Context, error) -> None:
    
    The code in this event is executed every time a normal valid command catches an error.

    :param context: The context of the normal command that failed executing.
    :param error: The error that has been faced.
"""

bot.run(TOKEN)