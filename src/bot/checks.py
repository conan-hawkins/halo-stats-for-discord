"""
Command access checks for the Halo Stats Discord Bot.
"""

from discord.ext import commands

from src.config.settings import ADMIN_USER_IDS

# Commands anyone can run without being on the admin allowlist.
PUBLIC_COMMANDS = {"help", "full", "ranked", "casual"}


def is_admin(ctx: commands.Context) -> bool:
    """Return True if the invoking user is on the admin allowlist."""
    return ctx.author.id in ADMIN_USER_IDS


def admin_only():
    """Command check decorator restricting a command to allowlisted admins."""
    return commands.check(is_admin)
