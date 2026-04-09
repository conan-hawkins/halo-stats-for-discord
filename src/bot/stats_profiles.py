from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class StatsProfile:
    command_name: str
    terminal_action: str
    fetch_stat_type: str
    api_stat_type: str
    display_name: str
    embed_color: int
    command_help: str
    guide_description: str
    force_full_fetch: bool = False


FULL_STATS_PROFILE = StatsProfile(
    command_name="full",
    terminal_action="cmd_full",
    fetch_stat_type="stats",
    api_stat_type="overall",
    display_name="OVERALL STATS",
    embed_color=0x00B0F4,
    command_help="Get complete lifetime stats from all available matches. Usage: #full <gamertag>",
    guide_description="Full lifetime stats from all available matches.",
)

RANKED_STATS_PROFILE = StatsProfile(
    command_name="ranked",
    terminal_action="cmd_ranked",
    fetch_stat_type="ranked",
    api_stat_type="ranked",
    display_name="RANKED STATS",
    embed_color=0xFFD700,
    command_help="Get ranked-only stats and performance trends. Usage: #ranked <gamertag>",
    guide_description="Ranked-only performance summary.",
)

CASUAL_STATS_PROFILE = StatsProfile(
    command_name="casual",
    terminal_action="cmd_casual",
    fetch_stat_type="social",
    api_stat_type="social",
    display_name="CASUAL STATS",
    embed_color=0x00FF00,
    command_help="Get social/casual playlist stats only. Usage: #casual <gamertag>",
    guide_description="Social/casual playlist performance summary.",
)

STATS_PROFILES: Tuple[StatsProfile, ...] = (
    FULL_STATS_PROFILE,
    RANKED_STATS_PROFILE,
    CASUAL_STATS_PROFILE,
)

STATS_PROFILE_BY_TERMINAL_ACTION: Dict[str, StatsProfile] = {
    profile.terminal_action: profile for profile in STATS_PROFILES
}

STATS_PROFILE_BY_FETCH_TYPE: Dict[str, StatsProfile] = {
    FULL_STATS_PROFILE.fetch_stat_type: FULL_STATS_PROFILE,
    FULL_STATS_PROFILE.api_stat_type: FULL_STATS_PROFILE,
    RANKED_STATS_PROFILE.fetch_stat_type: RANKED_STATS_PROFILE,
    CASUAL_STATS_PROFILE.fetch_stat_type: CASUAL_STATS_PROFILE,
}


def get_stats_profile_for_terminal_action(action: str) -> Optional[StatsProfile]:
    return STATS_PROFILE_BY_TERMINAL_ACTION.get((action or "").strip())


def get_stats_profile_for_fetch_type(stat_type: str) -> StatsProfile:
    normalized = (stat_type or "").strip().lower()
    return STATS_PROFILE_BY_FETCH_TYPE.get(normalized, FULL_STATS_PROFILE)
