from .cog import TerminalCog


async def setup(bot):
    await bot.add_cog(TerminalCog(bot))


__all__ = ["TerminalCog", "setup"]
