import discord

from src.bot.presentation.embed_styles import (
    COLOR_ERROR,
    ERROR_FOOTER_ICON_URL,
    PROJECT_FOOTER_TEXT,
    apply_footer,
    current_timestamp,
)


async def format_error_embed(error_no):
    """
    Create an error embed based on error code.

    Args:
        error_no: Error number from API

    Returns:
        Discord Embed object
    """
    error_messages = {
        1: "ERROR - USE OF UNAUTHORISED CHARACTERS DETECTED.",
        2: "ERROR - PLAYER NOT FOUND. PLEASE CHECK SPELLING.",
        3: "ERROR - PLAYERS PROFILE IS SET TO PRIVATE.",
        4: "ERROR - SOMETHING UNEXPECTED HAPPENED.",
    }

    title = error_messages.get(error_no, "ERROR - UNKNOWN ERROR OCCURRED.")

    embed = discord.Embed(
        title=title,
        colour=COLOR_ERROR,
        timestamp=current_timestamp(),
    )
    apply_footer(embed, text=PROJECT_FOOTER_TEXT, icon_url=ERROR_FOOTER_ICON_URL)

    return embed
