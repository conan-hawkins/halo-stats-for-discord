"""
Halo Infinite API module

Provides client and rate limiting for Halo Waypoint API access.
"""

# Import from refactored modules
from src.api.rate_limiters import (
    XboxProfileRateLimiter,
    HaloStatsRateLimiter,
    xbox_profile_rate_limiter,
    halo_stats_rate_limiter,
)
from src.api.utils import (
    safe_read_json,
    safe_write_json,
    is_token_valid,
)
from src.api.xuid_cache import (
    load_xuid_cache,
    save_xuid_cache,
    load_xuid_cache_full,
    get_gamertag_history,
)

# Import from main client module
from src.api.client import (
    HaloAPIClient,
    StatsFind,
    StatsFind1,
    api_client,
    get_players_from_recent_matches,
)

__all__ = [
    # Client
    "HaloAPIClient",
    "StatsFind",
    "StatsFind1",
    "api_client",
    "get_players_from_recent_matches",
    # Rate limiters
    "XboxProfileRateLimiter",
    "HaloStatsRateLimiter",
    "xbox_profile_rate_limiter",
    "halo_stats_rate_limiter",
    # Utilities
    "safe_read_json",
    "safe_write_json",
    "is_token_valid",
    # XUID cache
    "load_xuid_cache",
    "save_xuid_cache",
    "load_xuid_cache_full",
    "get_gamertag_history",
]
