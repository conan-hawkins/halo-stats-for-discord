from datetime import datetime

import discord
from discord.ext import commands

from .render import build_terminal_message_payload
from .state import TerminalState
from .views import TerminalView


class TerminalCog(commands.Cog, name="Terminal"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.command(name="terminal", help="Open the interactive terminal controller (prefix-only).")
    async def terminal(self, ctx: commands.Context):
        state = TerminalState(
            requester_id=ctx.author.id,
            last_output=f"Session opened by {ctx.author.display_name} at {datetime.now().strftime('%H:%M:%S')}",
        )

        view = TerminalView(self.bot, ctx, state)
        embed, file = build_terminal_message_payload(state)
        if file:
            msg = await ctx.send(embed=embed, file=file, view=view)
        else:
            msg = await ctx.send(embed=embed, view=view)
        view.message = msg
