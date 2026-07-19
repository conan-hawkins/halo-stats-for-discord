"""
Configuration constants for the Halo Stats Discord Bot.

Centralizes paths and settings that may need to change together.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env values before configuration constants are evaluated.
load_dotenv()

# =============================================================================
# BASE PATHS
# =============================================================================

# Project root directory (parent of src/)
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Data directory for cache and database files
DATA_DIR = PROJECT_ROOT / "data"

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

# Base directory for token cache files
TOKEN_CACHE_DIR = PROJECT_ROOT / "data" / "auth"

# Token cache file paths
TOKEN_CACHE_FILE = TOKEN_CACHE_DIR / "token_cache.json"
TOKEN_CACHE_ACCOUNT2 = TOKEN_CACHE_DIR / "token_cache_account2.json"
TOKEN_CACHE_ACCOUNT3 = TOKEN_CACHE_DIR / "token_cache_account3.json"
TOKEN_CACHE_ACCOUNT4 = TOKEN_CACHE_DIR / "token_cache_account4.json"
TOKEN_CACHE_ACCOUNT5 = TOKEN_CACHE_DIR / "token_cache_account5.json"
TOKEN_SWAP_MARKER_FILE = TOKEN_CACHE_DIR / "token_refresh_swap.json"


def get_token_cache_path(account_num: int = 1) -> Path:
    """
    Get the token cache file path for a specific account.
    
    Args:
        account_num: Account number (1-5)
    
    Returns:
        Path to the token cache file
    """
    if account_num == 1:
        return TOKEN_CACHE_FILE
    return TOKEN_CACHE_DIR / f"token_cache_account{account_num}.json"


# XUID/Gamertag cache
XUID_CACHE_FILE = DATA_DIR / "xuid_gamertag_cache.json"

# Database file
DATABASE_FILE = DATA_DIR / "halo_stats_v2.db"

# Progress tracking file
CACHE_PROGRESS_FILE = DATA_DIR / "cache_progress.json"

# Medal icon cache (sprite sheet + metadata fetched from the Halo CDN)
MEDAL_ICON_CACHE_DIR = DATA_DIR / "medal_icons"
MEDAL_SHEET_CACHE_FILE = MEDAL_ICON_CACHE_DIR / "medal_sheet_xl.png"
MEDAL_METADATA_CACHE_FILE = MEDAL_ICON_CACHE_DIR / "metadata.json"

# =============================================================================
# API CONFIGURATION
# =============================================================================

# Rate limiting settings
REQUESTS_PER_SECOND_PER_ACCOUNT = 8
MAX_ACCOUNTS = 5

# =============================================================================
# SPAM PROTECTION / FRESHNESS
# =============================================================================

# Per-user cooldown on stat commands (#full/#ranked/#coreranked/#rotationalranked/#casual)
STATS_USER_COOLDOWN_SECONDS = int(os.getenv("STATS_USER_COOLDOWN_SECONDS", "8"))

# If a player's match history was API-checked within this window, serve straight
# from cache with zero API calls. force_full_fetch bypasses this.
STATS_HISTORY_FRESHNESS_TTL_SECONDS = int(
    os.getenv("STATS_HISTORY_FRESHNESS_TTL_SECONDS", "90")
)

# =============================================================================
# STATS CLASSIFICATION
# =============================================================================

# The permanent "core" ranked playlists (current Ranked Arena, Ranked Doubles,
# Ranked Slayer). #coreranked aggregates only these, which matches
# halotracker.com's ranked lifetime overview; every other CSR playlist
# (retired launch-era Ranked Arena queues, rotational playlists like Ranked
# Snipers / Tactical / FFA / 1v1 Showdown, and any future rotation entries)
# falls into #rotationalranked automatically because rotational is defined as
# "is_ranked and not core". Compare against a lowercased playlist_id.
# Lives here (not client.py) so src/database can import it without a circular
# import through src.api.
CORE_RANKED_PLAYLIST_IDS = frozenset({
    "edfef3ac-9cbe-4fa2-b949-8f29deafd483",  # Ranked Arena
    "fa5aa2a3-2428-4912-a023-e1eeea7b877c",  # Ranked Doubles
    "dcb2e24e-05fb-4390-8076-32a0cdb4326e",  # Ranked Slayer
})

# =============================================================================
# TERMINAL AUTH CONFIGURATION
# =============================================================================


def get_terminal_admin_password() -> str:
    """Return the current terminal admin password from environment variables."""
    return os.getenv("TERMINAL_ADMIN_PASSWORD", "").strip()

# Password required to enter admin terminal mode.
# Leave unset/empty to disable admin terminal login.
TERMINAL_ADMIN_PASSWORD = get_terminal_admin_password()


def get_admin_user_ids() -> set[int]:
    """Return the set of Discord user IDs allowed to run admin-only commands."""
    raw = os.getenv("ADMIN_USER_IDS", "")
    return {int(part) for part in raw.split(",") if part.strip().isdigit()}

# Discord user IDs allowed to run admin-only bot commands.
ADMIN_USER_IDS = get_admin_user_ids()

# =============================================================================
# INITIALIZATION
# =============================================================================

def ensure_data_directories():
    """Ensure all required data directories exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TOKEN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    MEDAL_ICON_CACHE_DIR.mkdir(parents=True, exist_ok=True)


# Create directories on import
ensure_data_directories()
