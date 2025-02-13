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
# Sets intents for the bot = This is needed for a bot to function
#==========================================================
intents = discord.Intents.all() 
intents.members = True

# "Import" the intents and set a prefix that's used to interact with the bot 
bot = commands.Bot(command_prefix="!", intents=intents) 

#==========================================================#==========================================================
           
                                #==========================================================
                                # Halo stat tracker commands
                                #==========================================================

#==========================================================#==========================================================
# sets up a command names
@bot.command(name='stats', name='ranked', help='Use command prefix followed by stats or ranked and gamertag to search. Example - !stats XxUK D3STROYxX')

#==========================================================
# makes a stats function that can take multiple inputs
#==========================================================
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

# opens the main file and run botbot function
    stats_com(gamertag)
# sends the gamertag and output of the botbot function
    await ctx.send(gamertag, file=discord.File("cropped_example.png"))

#==========================================================
# makes a ranked function that can take multiple inputs
#==========================================================
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

# opens the main file and run botbot function
    ranked_com(gamertag)
# sends the gamertag and output of the botbot function to the discord guild
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