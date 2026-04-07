"""Compatibility facade for embed builders.

This module preserves existing import paths while delegating implementations
to the presentation layer modules.
"""

from src.bot.presentation.embeds.error import format_error_embed
from src.bot.presentation.embeds.stats import format_stats_embed


__all__ = [
    "format_error_embed",
    "format_stats_embed",
]
