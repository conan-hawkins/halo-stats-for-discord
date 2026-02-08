"""
XUID Cache Management for Halo Infinite API

Provides persistent caching of XUID to Gamertag mappings to minimize
API calls for gamertag resolution.
"""

import traceback
from typing import Dict

from src.api.utils import safe_read_json, safe_write_json
from src.config import XUID_CACHE_FILE


def load_xuid_cache() -> Dict[str, str]:
    """
    Load the persistent XUID -> Gamertag cache.
    
    Returns:
        Dictionary mapping XUIDs to gamertags
    """
    return safe_read_json(XUID_CACHE_FILE, default={})


def save_xuid_cache(cache: Dict[str, str]) -> None:
    """
    Save the XUID -> Gamertag cache.
    
    Args:
        cache: Dictionary mapping XUIDs to gamertags
    """
    try:
        safe_write_json(XUID_CACHE_FILE, cache)
    except Exception as e:
        print(f"Failed to save XUID cache: {e}")
        traceback.print_exc()
