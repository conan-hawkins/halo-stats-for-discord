"""
Configuration module for Halo Stats Discord Bot
"""

from src.config.settings import (
    # Path configuration
    PROJECT_ROOT,
    DATA_DIR,
    TOKEN_CACHE_DIR,
    TOKEN_CACHE_FILE,
    TOKEN_CACHE_ACCOUNT2,
    TOKEN_CACHE_ACCOUNT3,
    TOKEN_CACHE_ACCOUNT4,
    TOKEN_CACHE_ACCOUNT5,
    get_token_cache_path,
    XUID_CACHE_FILE,
    DATABASE_FILE,
    CACHE_PROGRESS_FILE,
    # API configuration
    REQUESTS_PER_SECOND_PER_ACCOUNT,
    MAX_ACCOUNTS,
    # Utility functions
    ensure_data_directories,
)

__all__ = [
    "PROJECT_ROOT",
    "DATA_DIR",
    "TOKEN_CACHE_DIR",
    "TOKEN_CACHE_FILE",
    "TOKEN_CACHE_ACCOUNT2",
    "TOKEN_CACHE_ACCOUNT3",
    "TOKEN_CACHE_ACCOUNT4",
    "TOKEN_CACHE_ACCOUNT5",
    "get_token_cache_path",
    "XUID_CACHE_FILE",
    "DATABASE_FILE",
    "CACHE_PROGRESS_FILE",
    "REQUESTS_PER_SECOND_PER_ACCOUNT",
    "MAX_ACCOUNTS",
    "ensure_data_directories",
]
