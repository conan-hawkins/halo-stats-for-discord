from src.bot.presentation.embeds.cache_status import build_cache_status_embed
from src.bot.presentation.embeds.error import format_error_embed
from src.bot.presentation.embeds.friends import (
    build_xboxfriends_error_embed,
    build_xboxfriends_loading_embed,
    build_xboxfriends_progress_embed,
    build_xboxfriends_result_embed,
)
from src.bot.presentation.embeds.help import build_command_help_embed, build_stats_help_guide_embed
from src.bot.presentation.embeds.loading import build_stats_loading_embed
from src.bot.presentation.embeds.stats import format_stats_embed

__all__ = [
    "build_cache_status_embed",
    "build_command_help_embed",
    "build_stats_loading_embed",
    "build_stats_help_guide_embed",
    "build_xboxfriends_error_embed",
    "build_xboxfriends_loading_embed",
    "build_xboxfriends_progress_embed",
    "build_xboxfriends_result_embed",
    "format_error_embed",
    "format_stats_embed",
]
