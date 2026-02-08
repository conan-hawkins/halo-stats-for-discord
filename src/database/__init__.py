"""
Database module for Halo Stats Discord Bot

Provides SQLite-based caching and schema management.
Includes social graph database for network analysis.
"""

from src.database.schema import HaloStatsDBv2, MEDAL_NAME_MAPPING, MEDAL_ID_BY_NAME
from src.database.cache import get_cache, PlayerStatsCacheV2
from src.database.graph_schema import (
    HaloSocialGraphDB,
    get_graph_db,
    GRAPH_DATABASE_FILE,
)

__all__ = [
    # Stats cache
    "get_cache",
    "PlayerStatsCacheV2",
    "HaloStatsDBv2",
    "MEDAL_NAME_MAPPING",
    "MEDAL_ID_BY_NAME",
    
    # Social graph
    "HaloSocialGraphDB",
    "get_graph_db",
    "GRAPH_DATABASE_FILE",
]
