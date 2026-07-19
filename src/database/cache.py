"""
SQLite-based player stats cache v2 for Halo Infinite Discord Bot
Uses normalized schema from schema.py
Replaces JSON file caching with direct database operations
"""

import os
from typing import Optional, Dict, List, Tuple
from datetime import datetime

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
        conn = self.db._get_connection()
        try:
            # The whole per-player batch is written under ONE transaction and
            # committed once at the end (one fsync-class write instead of ~4 per
            # match - the dominant cost on HDD-backed storage; composes with
            # synchronous=NORMAL). To keep atomicity per-match despite the single
            # commit, each match is wrapped in its own SAVEPOINT: a single bad
            # match rolls back only itself and the batch continues ("114 of 115",
            # never "0 of 115"), and no partial player_mode_stats delta or
            # ALTER TABLE from a failed match survives the commit.
            last_update = stats_data.get('last_update', datetime.now().isoformat())
            self.db.insert_or_update_player(
                xuid, gamertag, last_update, commit=False,
                incomplete_data=bool(stats_data.get('incomplete_data')),
                failed_match_count=int(stats_data.get('failed_match_count') or 0),
            )

            # Process each match
            matches_to_save = stats_data.get('processed_matches', [])
            print(f"[CACHE] Saving {len(matches_to_save)} matches for {gamertag or xuid}")

            matches_saved = 0
            for match_data in matches_to_save:
                match_id = match_data.get('match_id')
                if not match_id:
                    continue

                # Primary unit: match metadata + this player's performance, kept
                # atomic so the player_mode_stats summary can never desync from
                # player_match (insert_player_match applies the summary delta).
                conn.execute("SAVEPOINT player_match_write")
                try:
                    saved_ok = self.db.insert_match(match_data, commit=False)
                    if saved_ok:
                        saved_ok = self.db.insert_player_match(xuid, match_data, commit=False)
                except Exception as match_err:
                    saved_ok = False
                    print(f"[CACHE] Skipping match {match_id}: {match_err}")

                if saved_ok:
                    conn.execute("RELEASE SAVEPOINT player_match_write")
                    matches_saved += 1
                else:
                    conn.execute("ROLLBACK TO SAVEPOINT player_match_write")
                    conn.execute("RELEASE SAVEPOINT player_match_write")
                    # A rolled-back ALTER TABLE may have removed a medal column
                    # we recorded in the in-memory cache.
                    self.db._reset_medal_set_columns_cache()
                    continue

                # Supplementary roster data: best-effort and isolated in its own
                # savepoint, so a participants failure never rolls back the
                # player's own match stats saved above.
                participants = match_data.get('all_participants') or []
                if participants:
                    conn.execute("SAVEPOINT participants_write")
                    try:
                        part_ok = self.db.insert_match_participants(match_id, participants, commit=False)
                    except Exception as part_err:
                        part_ok = False
                        print(f"[CACHE] Participants for {match_id} failed: {part_err}")
                    if part_ok:
                        conn.execute("RELEASE SAVEPOINT participants_write")
                    else:
                        conn.execute("ROLLBACK TO SAVEPOINT participants_write")
                        conn.execute("RELEASE SAVEPOINT participants_write")

            conn.commit()
            print(f"[CACHE] Successfully saved {matches_saved}/{len(matches_to_save)} matches for {gamertag or xuid}")
            return True

        except Exception as e:
            # Batch-level failure (e.g. the player upsert or the final commit).
            # Undo everything and drop the medal-column cache, since a rolled-back
            # ALTER TABLE may have removed a column we recorded.
            conn.rollback()
            self.db._reset_medal_set_columns_cache()
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
            cursor.execute(
                "SELECT xuid, gamertag, last_processed_at, incomplete_data, failed_match_count FROM players WHERE xuid = ?",
                (xuid,)
            )
            player = cursor.fetchone()

            if not player and gamertag:
                cursor.execute(
                    "SELECT xuid, gamertag, last_processed_at, incomplete_data, failed_match_count FROM players WHERE gamertag = ?",
                    (gamertag,)
                )
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
                'incomplete_data': bool(player['incomplete_data']),
                'failed_match_count': player['failed_match_count'] or 0,
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

        rows = cursor.fetchall()
        medal_set_ids = {row['medal_set_id'] for row in rows if row['medal_set_id']}
        medals_by_set_id = self._get_medals_for_sets(medal_set_ids)

        matches = []
        for row in rows:
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
                'medals': medals_by_set_id.get(row['medal_set_id'], []) if row['medal_set_id'] else []
            }

            matches.append(match_dict)

        return matches
    
    def get_player_mode_summary(self, xuid: str, stat_type: str = "overall") -> Optional[Dict]:
        """
        Get precomputed per-player, per-game-mode stats from player_mode_stats.

        Returns None if no summary row exists yet (e.g. player added before
        the table was backfilled, or before their first match was inserted) -
        callers should fall back to on-demand calculation in that case.
        """
        conn = self.db._get_connection()
        cursor = conn.cursor()

        game_mode = (stat_type if stat_type in ("ranked", "social", "core_ranked", "rotational_ranked")
                     else "overall")

        cursor.execute(
            "SELECT * FROM player_mode_stats WHERE xuid = ? AND game_mode = ?",
            (xuid, game_mode)
        )
        row = cursor.fetchone()
        if not row:
            return None

        total_kills = row['total_kills']
        total_deaths = row['total_deaths']
        total_assists = row['total_assists']
        games_played = row['games_played']
        wins = row['wins']

        kd_ratio = round(total_kills / total_deaths if total_deaths > 0 else total_kills, 2)
        kda = round((total_kills + (total_assists / 3)) - total_deaths, 2)
        avg_kda = round(kda / games_played if games_played > 0 else 0, 2)
        win_rate = f"{round(wins / games_played * 100 if games_played > 0 else 0, 1)}%"

        return {
            'total_kills': total_kills,
            'total_deaths': total_deaths,
            'total_assists': total_assists,
            'wins': wins,
            'losses': row['losses'],
            'ties': row['draws'],
            'dnf': row['dnf'],
            'games_played': games_played,
            'kd_ratio': kd_ratio,
            'kda': kda,
            'avg_kda': avg_kda,
            'win_rate': win_rate
        }

    def get_player_medal_summary(self, xuid: str, stat_type: str = "overall") -> Optional[List[Dict]]:
        """
        Get precomputed per-medal counts for a player/mode from player_medal_totals:
        one {medal_name_id, medal_name, count} entry per medal type earned in
        that mode. stat_type "overall" returns the combined total across modes.

        Returns None if no rows exist yet (e.g. player added before the
        medal-totals backfill ran, or before their first match with medals was
        inserted) - callers should fall back to on-demand calculation.
        """
        conn = self.db._get_connection()
        cursor = conn.cursor()

        game_mode = stat_type if stat_type in ("ranked", "social") else "overall"

        cursor.execute("""
            SELECT pmt.medal_name_id, mt.medal_name, pmt.count
            FROM player_medal_totals pmt
            LEFT JOIN medal_types mt ON mt.medal_name_id = pmt.medal_name_id
            WHERE pmt.xuid = ? AND pmt.game_mode = ? AND pmt.count > 0
            ORDER BY pmt.count DESC
        """, (xuid, game_mode))
        rows = cursor.fetchall()
        if not rows:
            return None

        return [
            {
                'medal_name_id': row['medal_name_id'],
                'medal_name': row['medal_name'] or f"Unknown Medal {row['medal_name_id']}",
                'count': row['count'],
            }
            for row in rows
        ]

    _MEDAL_SET_ID_BATCH_SIZE = 500

    @staticmethod
    def _medals_from_row(row) -> List[Dict]:
        """Convert a medal_sets row into a list of {NameId, Count, ...} medals"""
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

    def _get_medals_for_sets(self, medal_set_ids) -> Dict[int, List[Dict]]:
        """Batch-fetch medal lists for many medal_set_ids in a few IN (...) queries"""
        medal_set_ids = list(medal_set_ids)
        if not medal_set_ids:
            return {}

        conn = self.db._get_connection()
        cursor = conn.cursor()

        medals_by_set_id: Dict[int, List[Dict]] = {}
        batch_size = self._MEDAL_SET_ID_BATCH_SIZE
        for i in range(0, len(medal_set_ids), batch_size):
            chunk = medal_set_ids[i:i + batch_size]
            placeholders = ",".join("?" * len(chunk))
            cursor.execute(f"SELECT * FROM medal_sets WHERE medal_set_id IN ({placeholders})", chunk)
            for row in cursor.fetchall():
                medals_by_set_id[row['medal_set_id']] = self._medals_from_row(row)

        return medals_by_set_id
    
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

    def get_seed_verified_match_ids(self, seed_xuid: str, limit_matches: int = None) -> List[str]:
        """Get verified seed match IDs from normalized match history tables."""
        return self.db.get_seed_verified_match_ids(seed_xuid, limit_matches=limit_matches)

    def get_participant_coverage_for_matches(self, match_ids: List[str], seed_xuid: str) -> Dict[str, Dict]:
        """Get participant-count coverage for a supplied set of match IDs."""
        return self.db.get_participant_coverage_for_matches(match_ids, seed_xuid)

    def get_pair_match_category_counts(self, scope_xuids: List[str]) -> Dict[Tuple[str, str], Dict[str, int]]:
        """Get ranked/social/custom/unknown match counts for each scoped player pair."""
        return self.db.get_pair_match_category_counts(scope_xuids)
    
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
