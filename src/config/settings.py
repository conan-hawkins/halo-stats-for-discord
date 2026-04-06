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

# =============================================================================
# API CONFIGURATION  
# =============================================================================

# Rate limiting settings
REQUESTS_PER_SECOND_PER_ACCOUNT = 8
MAX_ACCOUNTS = 5

# =============================================================================
# TERMINAL AUTH CONFIGURATION
# =============================================================================


def get_terminal_admin_password() -> str:
    """Return the current terminal admin password from environment variables."""
    return os.getenv("TERMINAL_ADMIN_PASSWORD", "").strip()

# Password required to enter admin terminal mode.
# Leave unset/empty to disable admin terminal login.
TERMINAL_ADMIN_PASSWORD = get_terminal_admin_password()

# =============================================================================
# INITIALIZATION
# =============================================================================

def ensure_data_directories():
    """Ensure all required data directories exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TOKEN_CACHE_DIR.mkdir(parents=True, exist_ok=True)


# Create directories on import
ensure_data_directories()
