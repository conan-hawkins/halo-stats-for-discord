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
#==========================================================
# Sets global variables to avoid circular import issues 
#==========================================================
global gamertag
global stat_type

#==========================================================#==========================================================
           
                                #==========================================================
                                # Halo stat tracker commands Initialisation 
                                #==========================================================

#==========================================================#==========================================================
#==========================================================
# makes a stats and a ranked function that passes the 
# correct stat_type marker into ranked_and_stats function
#==========================================================
# sets up stats and help command
@bot.command(name='stats', help='Use command prefix followed by stats or ranked and gamertag to search. Example - #stats XxUK D3STROYxX') 
async def stats(ctx, *inputs):
    stat_type = "stats" # This is used for path selection later. If stat_type = "stats" do this ect.
    await ranked_and_stats(ctx, stat_type, *inputs)

# sets up ranked command
@bot.command(name='ranked')
async def ranked(ctx, *inputs):
    stat_type = "ranked" # This is used for path selection later. If stat_type = "ranked" do this ect.
    await ranked_and_stats(ctx, stat_type, *inputs)

#==========================================================
# event executed when a command catches an error
#==========================================================
async def on_command_error(ctx, error_no):
    # The code in this event is executed every time a normal valid or invalid command catches an error.
    print("Test final before error send") # !!!==== For testing to be removed later ====!!!
    if error_no == 1: # The use of unauthorised characters detected in input
         title = (("Error - Use of unauthorised characters detected.").upper())
    elif error_no == 2: # Player was not found
        print(404,"was found in main file") # !!!==== For testing to be removed later ====!!!
        title = (("Error - Player not found. Please check spelling.").upper())
    elif error_no == 3: # Players profile was set to private
        print(404,"was found in main file") # !!!==== For testing to be removed later ====!!!
        title = (("Error - Players profile is set to private.").upper())
    elif error_no == 4: # Unexpected error in main file
        print(404,"was found in main file") # !!!==== For testing to be removed later ====!!!
        title = (("Error - Something unexpected happened.").upper())
    else:
        pass
    
    embed = discord.Embed(title=title,
                                colour=0xFF0000,
                                timestamp=datetime.now())
    # Adds footer with icon to template
    embed.set_footer(text="Project Goliath", 
                     icon_url = 'https://www.iconsdb.com/icons/preview/dark-gray/error-4-xxl.png')
    
    print("Test final before error send2") # !!!==== For testing to be removed later ====!!!
    
    # Sends finished error message to discord
    await ctx.send(embed=embed)

#==========================================================#==========================================================
           
                                #==========================================================
                                # Main command functions  
                                #==========================================================

#==========================================================#==========================================================
#==========================================================
# makes a ranked_and_stats function that can run the ranked 
# or stats commands using the stat_type marker
#==========================================================
async def ranked_and_stats(ctx, stat_type, *inputs): 
    inputs = ( ''.join(inputs) )
    
    '''
    # Basic input security needs to be updated before wider test hosting.
    for i in inputs:
        print(i)
        if i == '"' or "'" or "\\" or "" or "(" or ")" or "[" or "]" or "{" or "}" or ",":
            error_no = 1
            on_command_error(ctx, error_no) # Opens error function
        else:
            pass
    
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
        error_no = 1
        on_command_error(ctx, error_no) # Opens error function
    '''
    
    gamertag = inputs 
    print("Test point 1", gamertag, stat_type) # !!!==== For testing to be removed later ====!!!

    # opens the main file and run page_getter function   
    StatsFind1.page_getter(gamertag, stat_type)
    
    # Checks if main file returns error
    if StatsFind1.error_no != 0:
        await on_command_error(ctx, StatsFind1.error_no)
    else:
        await format_and_send(ctx, stat_type, gamertag)

async def format_and_send(ctx, stat_type, gamertag): 
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
    embed.add_field(name="WIN RATE - "+StatsFind1.stats_list[1]+"           KD RATIO - "+StatsFind1.stats_list[0],
                    value="",
                    inline=False)
    embed.add_field(name="AVG KDA  - "+StatsFind1.stats_list[2]+"               KILLS    - "+StatsFind1.stats_list[4],
                    value="",
                    inline=False)
    embed.add_field(name="DEATHS   - "+StatsFind1.stats_list[2]+"                ASSISTS  - "+StatsFind1.stats_list[4],
                    value="",
                    inline=False)
    #embed.add_field(name="KD RATIO",
    #                value= StatsFind1.stats_list[0],
    #                inline=True)
    #embed.add_field(name="AVG KDA",
    #                value= StatsFind1.stats_list[2],
    #                inline=True)
    #embed.add_field(name="KILLS",
    #                value= StatsFind1.stats_list[4],
    #                inline=True)
    #embed.add_field(name="DEATHS",
    #                value= StatsFind1.stats_list[6],
    #                inline=True)
    #embed.add_field(name="ASSISTS",
    #                value= StatsFind1.stats_list[5],
    #                inline=True)
    
    print("Test point 5", stat_type) # !!!==== For testing to be removed later ====!!! 
    
    # Adds large image to the template
    embed.set_image(url="https://gaming-cdn.com/images/products/2674/screenshot/halo-infinite-campaign-pc-xbox-one-game-microsoft-store-wallpaper-2-thumbv2.jpg?v=1732013222")

    # Adds footer with icon to template
    embed.set_footer(text="Project Goliath",
                    icon_url="https://static.wikia.nocookie.net/halo/images/a/a6/H3_Difficulty_LegendaryIcon.png/revision/latest/scale-to-width-down/150?cb=20160930195427")
    
    print("Test final before send") # !!!==== For testing to be removed later ====!!!
    
    # Sends finished template to discord
    await ctx.send(embed=embed)

async def main():
    async with bot:
        await bot.start(TOKEN)

asyncio.run(main())



bot.run(TOKEN)