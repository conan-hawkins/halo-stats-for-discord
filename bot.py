import os
import discord
from discord.ext import commands
from dotenv import load_dotenv
from main import ranked_com
from main import stats_com

# Made by XxUK D3STROYxX 
# 12/02/2025
#============================================================================#
#                                                                            #
# Sets up a discord bot and some basic commands, the commands are mostly     #
# executed in attached files they are only initiated here                    #
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
# makes a stats function that can take multiple inputs
#==========================================================
# sets up command name
@bot.command(name='stats', help='Use command prefix followed by stats or ranked and gamertag to search. Example - #stats XxUK D3STROYxX')
# takes one or multiple inputs and combines them into one gamertag format
async def stats(ctx, *inputs):
    if len(inputs) == 1:
        pass
    elif len(inputs) == 2:
        inputs = inputs[0] + " " + inputs[1]
    elif len(inputs) == 3:
        inputs = inputs[0] + " " + inputs[1] + " " + inputs[2]
    elif len(inputs) == 4:
        inputs = inputs[0] + " " + inputs[1] + " " + inputs[2] + " " + inputs[3]
    gamertag = inputs

# opens the main file and run stats_com function
    stats_com(gamertag)
# sends the gamertag and output of the stats_com function
    await ctx.send(gamertag, file=discord.File("cropped_example.png"))


#==========================================================
# makes a ranked function that can take multiple inputs
#==========================================================
# sets up command name
@bot.command(name='ranked')
# takes one or multiple inputs and combines them into one gamertag format
async def ranked(ctx, *inputs):
    if len(inputs) == 1:
        pass
    elif len(inputs) == 2:
        inputs = inputs[0] + " " + inputs[1]
    elif len(inputs) == 3:
        inputs = inputs[0] + " " + inputs[1] + " " + inputs[2]
    elif len(inputs) == 4:
        inputs = inputs[0] + " " + inputs[1] + " " + inputs[2] + " " + inputs[3]
    gamertag = inputs

# opens the main file and run ranked_com function
    ranked_com(gamertag)
# sends the gamertag and output of the ranked_com function to the discord guild
    await ctx.send(gamertag, file=discord.File("cropped_example.png"))

"""
#==========================================================
# event executed when a valid command catches an error
#==========================================================
#async def on_command_error(self, context: Context, error) -> None:
    
    The code in this event is executed every time a normal valid command catches an error.

    :param context: The context of the normal command that failed executing.
    :param error: The error that has been faced.
"""

bot.run(TOKEN)