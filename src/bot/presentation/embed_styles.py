from datetime import datetime

import discord


PROJECT_FOOTER_TEXT = "Project Goliath"
PROJECT_FOOTER_ICON_URL = "https://static.wikia.nocookie.net/halo/images/a/a6/H3_Difficulty_LegendaryIcon.png/revision/latest/scale-to-width-down/150?cb=20160930195427"
ERROR_FOOTER_ICON_URL = "https://www.iconsdb.com/icons/preview/dark-gray/error-4-xxl.png"

COLOR_ERROR = 0xFF0000
COLOR_LOADING = 0xFFA500
COLOR_INFO = 0x00BFFF


def current_timestamp() -> datetime:
    return datetime.now()


def apply_footer(
    embed: discord.Embed,
    text: str = PROJECT_FOOTER_TEXT,
    icon_url: str = PROJECT_FOOTER_ICON_URL,
) -> discord.Embed:
    embed.set_footer(text=text, icon_url=icon_url)
    return embed
