"""
SQLite-based player stats cache v2 for Halo Infinite Discord Bot
Uses normalized schema from schema.py
Replaces JSON file caching with direct database operations
"""

import sqlite3
import os
from typing import Optional, Dict, List
from datetime import datetime
import threading

from src.database.schema import HaloStatsDBv2, MEDAL_NAME_MAPPING
from src.config import DATABASE_FILE


class PlayerStatsCacheV2:
    """
    Thread-safe cache interface for player stats using normalized SQLite v2 schema
    Provides same interface as old JSON-based cache for compatibility
    """
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DATABASE_FILE)
        self.db = HaloStatsDBv2(self.db_path)
    
    def save_player_stats(self, xuid: str, stat_type: str, stats_data: Dict, gamertag: str = None) -> bool:
        """
        Save player stats to database - processes match data into normalized tables
        
        Args:
            xuid: Player XUID
            stat_type: "overall", "ranked", or "social" (stored per-match via is_ranked)
            stats_data: Full stats dictionary with 'processed_matches', etc.
            gamertag: Player gamertag
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Insert/update player
            last_update = stats_data.get('last_update', datetime.now().isoformat())
            self.db.insert_or_update_player(xuid, gamertag, last_update)
            
            # Process each match
            matches_to_save = stats_data.get('processed_matches', [])
            print(f"[CACHE] Saving {len(matches_to_save)} matches for {gamertag or xuid}")
            
            matches_saved = 0
            for match_data in matches_to_save:
                match_id = match_data.get('match_id')
                if not match_id:
                    continue
                
                # Insert match metadata
                self.db.insert_match(match_data)

                # Persist full roster when participant payload is available.
                participants = match_data.get('all_participants') or []
                if participants:
                    self.db.insert_match_participants(match_id, participants)
                
                # Insert player's performance in this match
                if self.db.insert_player_match(xuid, match_data):
                    matches_saved += 1
            
            print(f"[CACHE] Successfully saved {matches_saved}/{len(matches_to_save)} matches for {gamertag or xuid}")
            return True
            
        except Exception as e:
            print(f"Error saving stats for {xuid}: {e}")
            return False
    
    def load_player_stats(self, xuid: str, stat_type: str, gamertag: str = None) -> Optional[Dict]:
        """
        Load player stats from database
        Reconstructs the format expected by existing code
        
        Args:
            xuid: Player XUID
            stat_type: "overall", "ranked", or "social"
            gamertag: Player gamertag (optional, for fallback lookup)
        
        Returns:
            Stats dictionary or None if not found
        """
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor()
            
            print(f"[CACHE] Loading stats for xuid={xuid}, gamertag={gamertag}, stat_type={stat_type}")
            
            # Find player by XUID or gamertag
            player_xuid = xuid
            cursor.execute("SELECT xuid, gamertag, last_processed_at FROM players WHERE xuid = ?", (xuid,))
            player = cursor.fetchone()
            
            if not player and gamertag:
                cursor.execute("SELECT xuid, gamertag, last_processed_at FROM players WHERE gamertag = ?", (gamertag,))
                player = cursor.fetchone()
                if player:
                    player_xuid = player['xuid']
            
            if not player:
                print(f"[CACHE] Player not found in database: xuid={xuid}, gamertag={gamertag}")
                return None
            
            print(f"[CACHE] Found player: {player['gamertag']}, last_processed={player['last_processed_at']}")
            
            # Get processed matches for this player (for "overall", get ALL matches regardless of ranked status)
            matches = self.get_player_processed_matches(player_xuid, "overall")  # Always get all matches first
            
            if not matches:
                print(f"[CACHE] No matches found for player {player['gamertag']}")
                return None
            
            print(f"[CACHE] Found {len(matches)} total matches for {player['gamertag']}")
            
            # Reconstruct the expected format with ALL matches
            # (stats filtering happens in _calculate_stats_from_matches based on stat_type)
            stats_data = {
                'xuid': player_xuid,
                'gamertag': player['gamertag'],
                'stat_type': stat_type,
                'last_update': player['last_processed_at'],
                'incomplete_data': False,
                'failed_match_count': 0,
                'processed_matches': matches
            }
            
            return stats_data
            
        except Exception as e:
            print(f"Error loading stats for {xuid}: {e}")
            return None
    
    def get_player_processed_matches(self, xuid: str, stat_type: str = "overall", 
                                      limit: int = None) -> List[Dict]:
        """
        Get list of processed match IDs for a player
        Used for incremental cache updates
        """
        conn = self.db._get_connection()
        cursor = conn.cursor()
        
        ranked_filter = ""
        if stat_type == "ranked":
            ranked_filter = "AND m.is_ranked = 1"
        elif stat_type == "social":
            ranked_filter = "AND m.is_ranked = 0"
        
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        cursor.execute(f"""
            SELECT 
                pm.match_id,
                pm.kills,
                pm.deaths,
                pm.assists,
                pm.outcome,
                pm.medal_set_id,
                m.duration,
                m.start_time,
                m.is_ranked,
                m.playlist_id,
                m.match_category,
                m.category_source,
                m.map_id,
                m.map_version
            FROM player_match pm
            JOIN matches m ON pm.match_id = m.match_id
            WHERE pm.xuid = ? {ranked_filter}
            ORDER BY m.start_time DESC
            {limit_clause}
        """, (xuid,))
        
        matches = []
        for row in cursor.fetchall():
            match_dict = {
                'match_id': row['match_id'],
                'kills': row['kills'],
                'deaths': row['deaths'],
                'assists': row['assists'],
                'outcome': row['outcome'],
                'duration': row['duration'],
                'start_time': row['start_time'],
                'is_ranked': bool(row['is_ranked']),
                'playlist_id': row['playlist_id'],
                'match_category': row['match_category'] or 'unknown',
                'category_source': row['category_source'],
                'map_id': row['map_id'],
                'map_version': row['map_version'],
                'medals': []  # Will be populated if needed
            }
            
            # Optionally get medals (expensive, only if needed)
            if row['medal_set_id']:
                match_dict['medals'] = self._get_medals_for_set(row['medal_set_id'])
            
            matches.append(match_dict)
        
        return matches
    
    def _get_medals_for_set(self, medal_set_id: int) -> List[Dict]:
        """Get medal list from medal_set_id"""
        conn = self.db._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM medal_sets WHERE medal_set_id = ?", (medal_set_id,))
        row = cursor.fetchone()
        
        if not row:
            return []
        
        medals = []
        for key in row.keys():
            if key.startswith('medal_') and key != 'medal_set_id' and key != 'medal_hash':
                count = row[key]
                if count and count > 0:
                    medal_id = int(key.replace('medal_', ''))
                    medals.append({
                        'NameId': medal_id,
                        'Count': count,
                        'TotalPersonalScoreAwarded': 0
                    })
        
        return medals
    
    def get_cached_match_ids(self, xuid: str, stat_type: str = "overall") -> set:
        """
        Get set of match IDs already cached for a player
        Used for incremental fetching
        """
        conn = self.db._get_connection()
        cursor = conn.cursor()
        
        ranked_filter = ""
        if stat_type == "ranked":
            ranked_filter = "AND m.is_ranked = 1"
        elif stat_type == "social":
            ranked_filter = "AND m.is_ranked = 0"
        
        cursor.execute(f"""
            SELECT pm.match_id 
            FROM player_match pm
            JOIN matches m ON pm.match_id = m.match_id
            WHERE pm.xuid = ? {ranked_filter}
        """, (xuid,))
        
        return {row['match_id'] for row in cursor.fetchall()}
    
    def check_player_cached(self, xuid: str, stat_type: str = "overall", gamertag: str = None) -> bool:
        """
        Check if player has cached data
        
        Args:
            xuid: Player XUID
            stat_type: "overall", "ranked", or "social"
            gamertag: Player gamertag (optional)
        
        Returns:
            True if player has cached data
        """
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor()
            
            # Check by XUID
            cursor.execute("""
                SELECT COUNT(*) as match_count FROM player_match WHERE xuid = ?
            """, (xuid,))
            
            row = cursor.fetchone()
            if row and row['match_count'] > 0:
                return True
            
            # Fallback to gamertag
            if gamertag:
                cursor.execute("""
                    SELECT p.xuid FROM players p
                    JOIN player_match pm ON p.xuid = pm.xuid
                    WHERE p.gamertag = ?
                    LIMIT 1
                """, (gamertag,))
                
                return cursor.fetchone() is not None
            
            return False
            
        except Exception as e:
            print(f"Error checking cache for {xuid}: {e}")
            return False
    
    def get_cached_player_count(self) -> int:
        """Get total number of cached players"""
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(DISTINCT xuid) FROM player_match")
            return cursor.fetchone()[0]
        except:
            return 0
    
    def get_stats(self) -> Dict:
        """Get database statistics"""
        return self.db.get_stats_summary()
    
    def resolve_xuid_by_gamertag(self, gamertag: str) -> Optional[str]:
        """Look up XUID by gamertag in database"""
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT xuid FROM players WHERE gamertag = ?", (gamertag,))
            row = cursor.fetchone()
            return row['xuid'] if row else None
        except:
            return None
    
    def close(self):
        """Close database connection"""
        self.db.close()


# Global instance for easy access
_cache_instance = None

def get_cache() -> PlayerStatsCacheV2:
    """Get global cache instance"""
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = PlayerStatsCacheV2()
    return _cache_instance
