"""
Social Graph Database Schema for Halo Infinite Player Network Analysis
======================================================================

Implements the data model for:
- Player identity (Node Table)
- Friend graph (Edge Table)
- Halo Infinite player features (Feature Store)
- Co-play graph (Optional weighted edges)

This schema supports:
- Connected components analysis
- Hub & spoke detection
- KNN player similarity
- Community detection
- Influence modeling

Author: Graph Analysis Extension
"""

import sqlite3
import os
from typing import Optional, Dict, List, Tuple, Set
from datetime import datetime, timedelta
import threading
from pathlib import Path

from src.config import DATA_DIR


# Graph database file
GRAPH_DATABASE_FILE = DATA_DIR / "halo_social_graph.db"


class HaloSocialGraphDB:
    """
    Thread-safe SQLite database for social graph analysis.
    
    Implements the four core datasets:
    - Players (Node Table)
    - Friends (Edge Table) 
    - Halo Features (Feature Store)
    - Co-play (Weighted Social Edges)
    """
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(GRAPH_DATABASE_FILE)
        self.local = threading.local()
        self._init_db()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection"""
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.local.conn.row_factory = sqlite3.Row
            # Enable WAL mode for better concurrent access
            self.local.conn.execute("PRAGMA journal_mode=WAL")
            # Enable foreign keys
            self.local.conn.execute("PRAGMA foreign_keys=ON")
        return self.local.conn
    
    def _init_db(self):
        """Initialize the social graph database schema"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # ============================================================
        # Table 1: Players (Node Table)
        # One row per Xbox user in the graph
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS graph_players (
                xuid TEXT PRIMARY KEY,
                gamertag TEXT,
                profile_visibility TEXT DEFAULT 'unknown',
                account_tier TEXT DEFAULT 'unknown',
                region TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                halo_active INTEGER DEFAULT 0,
                last_crawled TIMESTAMP,
                crawl_depth INTEGER DEFAULT 0,
                friends_count INTEGER DEFAULT 0,
                is_seed INTEGER DEFAULT 0
            )
        """)
        
        # ============================================================
        # Table 2: Friends (Edge Table)
        # One row per friendship edge (store both directions)
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS graph_friends (
                src_xuid TEXT NOT NULL,
                dst_xuid TEXT NOT NULL,
                edge_type TEXT DEFAULT 'friend',
                is_mutual INTEGER DEFAULT 0,
                discovered_from TEXT,
                depth INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_verified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (src_xuid, dst_xuid),
                FOREIGN KEY (src_xuid) REFERENCES graph_players(xuid),
                FOREIGN KEY (dst_xuid) REFERENCES graph_players(xuid)
            )
        """)
        
        # ============================================================
        # Table 3: Halo Features (Feature Store)
        # One row per player with Halo Infinite statistics
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS halo_features (
                xuid TEXT PRIMARY KEY,
                gamertag TEXT,
                csr REAL DEFAULT 0,
                csr_tier TEXT,
                kd_ratio REAL DEFAULT 0,
                win_rate REAL DEFAULT 0,
                matches_played INTEGER DEFAULT 0,
                matches_week REAL DEFAULT 0,
                ranked_matches INTEGER DEFAULT 0,
                social_matches INTEGER DEFAULT 0,
                ranked_ratio REAL DEFAULT 0,
                arena_ratio REAL DEFAULT 0,
                btb_ratio REAL DEFAULT 0,
                total_kills INTEGER DEFAULT 0,
                total_deaths INTEGER DEFAULT 0,
                total_assists INTEGER DEFAULT 0,
                avg_kills REAL DEFAULT 0,
                avg_deaths REAL DEFAULT 0,
                headshot_rate REAL DEFAULT 0,
                last_match TIMESTAMP,
                first_match TIMESTAMP,
                stats_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (xuid) REFERENCES graph_players(xuid)
            )
        """)
        
        # ============================================================
        # Table 4: Co-play Graph (Optional but powerful)
        # One row per player pair that played together
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS graph_coplay (
                src_xuid TEXT NOT NULL,
                dst_xuid TEXT NOT NULL,
                matches_together INTEGER DEFAULT 0,
                wins_together INTEGER DEFAULT 0,
                last_played TIMESTAMP,
                first_played TIMESTAMP,
                total_minutes INTEGER DEFAULT 0,
                avg_csr_diff REAL DEFAULT 0,
                same_team_count INTEGER DEFAULT 0,
                opposing_team_count INTEGER DEFAULT 0,
                PRIMARY KEY (src_xuid, dst_xuid),
                FOREIGN KEY (src_xuid) REFERENCES graph_players(xuid),
                FOREIGN KEY (dst_xuid) REFERENCES graph_players(xuid)
            )
        """)
        
        # ============================================================
        # Table 5: Crawl Queue
        # Track XUIDs to crawl and their status
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crawl_queue (
                xuid TEXT PRIMARY KEY,
                priority INTEGER DEFAULT 0,
                depth INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message TEXT,
                retry_count INTEGER DEFAULT 0
            )
        """)
        
        # ============================================================
        # Table 6: Crawl Progress
        # Track overall crawl statistics
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crawl_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                crawl_name TEXT,
                seed_xuid TEXT,
                max_depth INTEGER,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                nodes_discovered INTEGER DEFAULT 0,
                edges_discovered INTEGER DEFAULT 0,
                halo_players_found INTEGER DEFAULT 0,
                nodes_crawled INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running'
            )
        """)
        
        # ============================================================
        # Indexes for performance
        # ============================================================
        
        # Player indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_graph_players_gamertag ON graph_players(gamertag)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_graph_players_halo_active ON graph_players(halo_active)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_graph_players_last_seen ON graph_players(last_seen)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_graph_players_crawl_depth ON graph_players(crawl_depth)")
        
        # Friend edge indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_graph_friends_src ON graph_friends(src_xuid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_graph_friends_dst ON graph_friends(dst_xuid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_graph_friends_depth ON graph_friends(depth)")
        
        # Halo features indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_halo_features_csr ON halo_features(csr)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_halo_features_kd ON halo_features(kd_ratio)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_halo_features_matches ON halo_features(matches_played)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_halo_features_last_match ON halo_features(last_match)")
        
        # Co-play indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_graph_coplay_src ON graph_coplay(src_xuid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_graph_coplay_dst ON graph_coplay(dst_xuid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_graph_coplay_matches ON graph_coplay(matches_together)")
        
        # Crawl queue indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_crawl_queue_status ON crawl_queue(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_crawl_queue_priority ON crawl_queue(priority DESC)")
        
        conn.commit()
        print(f"[GRAPH DB] Initialized social graph database at {self.db_path}")
    
    # =========================================================================
    # PLAYER NODE OPERATIONS
    # =========================================================================
    
    def insert_or_update_player(
        self,
        xuid: str,
        gamertag: str = None,
        halo_active: bool = False,
        profile_visibility: str = 'unknown',
        region: str = None,
        crawl_depth: int = 0,
        friends_count: int = 0,
        is_seed: bool = False
    ) -> bool:
        """Insert or update a player node in the graph"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        try:
            cursor.execute("""
                INSERT INTO graph_players 
                (xuid, gamertag, halo_active, profile_visibility, region, 
                 first_seen, last_seen, crawl_depth, friends_count, is_seed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(xuid) DO UPDATE SET
                    gamertag = COALESCE(excluded.gamertag, graph_players.gamertag),
                    halo_active = CASE 
                        WHEN excluded.halo_active = 1 THEN 1 
                        ELSE graph_players.halo_active 
                    END,
                    profile_visibility = CASE 
                        WHEN excluded.profile_visibility != 'unknown' 
                        THEN excluded.profile_visibility 
                        ELSE graph_players.profile_visibility 
                    END,
                    region = COALESCE(excluded.region, graph_players.region),
                    last_seen = excluded.last_seen,
                    crawl_depth = MIN(graph_players.crawl_depth, excluded.crawl_depth),
                    friends_count = CASE 
                        WHEN excluded.friends_count > 0 
                        THEN excluded.friends_count 
                        ELSE graph_players.friends_count 
                    END,
                    is_seed = CASE 
                        WHEN excluded.is_seed = 1 THEN 1 
                        ELSE graph_players.is_seed 
                    END
            """, (xuid, gamertag, int(halo_active), profile_visibility, region,
                  now, now, crawl_depth, friends_count, int(is_seed)))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting player {xuid}: {e}")
            return False
    
    def get_player(self, xuid: str) -> Optional[Dict]:
        """Get a player by XUID"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM graph_players WHERE xuid = ?", (xuid,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_player_by_gamertag(self, gamertag: str) -> Optional[Dict]:
        """Get a player by gamertag"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM graph_players WHERE gamertag = ? COLLATE NOCASE", (gamertag,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def mark_player_crawled(self, xuid: str) -> bool:
        """Mark a player as crawled"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE graph_players 
                SET last_crawled = ? 
                WHERE xuid = ?
            """, (datetime.now().isoformat(), xuid))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error marking player crawled: {e}")
            return False
    
    def get_players_to_crawl(
        self, 
        max_depth: int = 3, 
        limit: int = 100,
        halo_only: bool = True,
        min_age_hours: int = 24
    ) -> List[Dict]:
        """Get players that need to be crawled"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = (datetime.now() - timedelta(hours=min_age_hours)).isoformat()
        
        halo_filter = "AND halo_active = 1" if halo_only else ""
        
        cursor.execute(f"""
            SELECT * FROM graph_players
            WHERE crawl_depth <= ?
            AND (last_crawled IS NULL OR last_crawled < ?)
            {halo_filter}
            ORDER BY crawl_depth ASC, last_seen DESC
            LIMIT ?
        """, (max_depth, cutoff, limit))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def get_halo_active_players(self, since_days: int = 30, limit: int = 1000) -> List[Dict]:
        """Get players who are active in Halo Infinite"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cutoff = (datetime.now() - timedelta(days=since_days)).isoformat()
        
        cursor.execute("""
            SELECT gp.*, hf.csr, hf.kd_ratio, hf.matches_played, hf.last_match
            FROM graph_players gp
            LEFT JOIN halo_features hf ON gp.xuid = hf.xuid
            WHERE gp.halo_active = 1
            AND (hf.last_match IS NULL OR hf.last_match > ?)
            ORDER BY hf.last_match DESC NULLS LAST
            LIMIT ?
        """, (cutoff, limit))
        
        return [dict(row) for row in cursor.fetchall()]
    
    # =========================================================================
    # FRIEND EDGE OPERATIONS
    # =========================================================================
    
    def insert_friend_edge(
        self,
        src_xuid: str,
        dst_xuid: str,
        is_mutual: bool = False,
        discovered_from: str = None,
        depth: int = 0
    ) -> bool:
        """Insert a friend edge into the graph"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        try:
            cursor.execute("""
                INSERT INTO graph_friends 
                (src_xuid, dst_xuid, is_mutual, discovered_from, depth, created_at, last_verified)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(src_xuid, dst_xuid) DO UPDATE SET
                    is_mutual = CASE 
                        WHEN excluded.is_mutual = 1 THEN 1 
                        ELSE graph_friends.is_mutual 
                    END,
                    last_verified = excluded.last_verified,
                    depth = MIN(graph_friends.depth, excluded.depth)
            """, (src_xuid, dst_xuid, int(is_mutual), discovered_from, depth, now, now))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting friend edge {src_xuid} -> {dst_xuid}: {e}")
            return False
    
    def insert_friend_edges_batch(self, edges: List[Tuple]) -> int:
        """
        Batch insert friend edges.
        
        Args:
            edges: List of tuples (src_xuid, dst_xuid, is_mutual, discovered_from, depth)
        
        Returns:
            Number of edges inserted
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        count = 0
        
        try:
            for edge in edges:
                src_xuid, dst_xuid, is_mutual, discovered_from, depth = edge
                cursor.execute("""
                    INSERT INTO graph_friends 
                    (src_xuid, dst_xuid, is_mutual, discovered_from, depth, created_at, last_verified)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(src_xuid, dst_xuid) DO UPDATE SET
                        is_mutual = CASE 
                            WHEN excluded.is_mutual = 1 THEN 1 
                            ELSE graph_friends.is_mutual 
                        END,
                        last_verified = excluded.last_verified,
                        depth = MIN(graph_friends.depth, excluded.depth)
                """, (src_xuid, dst_xuid, int(is_mutual), discovered_from, depth, now, now))
                count += 1
            
            conn.commit()
            return count
        except Exception as e:
            print(f"Error batch inserting edges: {e}")
            conn.rollback()
            return count
    
    def get_friends(self, xuid: str, mutual_only: bool = False) -> List[Dict]:
        """Get all friends of a player"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        mutual_filter = "AND is_mutual = 1" if mutual_only else ""
        
        cursor.execute(f"""
            SELECT gf.*, gp.gamertag, gp.halo_active
            FROM graph_friends gf
            LEFT JOIN graph_players gp ON gf.dst_xuid = gp.xuid
            WHERE gf.src_xuid = ?
            {mutual_filter}
        """, (xuid,))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def get_friend_count(self, xuid: str) -> int:
        """Get count of friends for a player"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM graph_friends WHERE src_xuid = ?", (xuid,))
        return cursor.fetchone()[0]

    def get_halo_friend_count(self, xuid: str) -> int:
        """Get count of friends currently marked as Halo-active."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM graph_friends gf
            JOIN graph_players gp ON gf.dst_xuid = gp.xuid
            WHERE gf.src_xuid = ?
            AND gp.halo_active = 1
        """, (xuid,))
        return cursor.fetchone()[0]

    def get_verified_halo_friend_count(self, xuid: str) -> int:
        """Get count of friends verified Halo-active via recorded Halo matches."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM graph_friends gf
            JOIN graph_players gp ON gf.dst_xuid = gp.xuid
            JOIN halo_features hf ON gf.dst_xuid = hf.xuid
            WHERE gf.src_xuid = ?
            AND gp.halo_active = 1
            AND COALESCE(hf.matches_played, 0) > 0
        """, (xuid,))
        return cursor.fetchone()[0]

    def get_verified_halo_incoming_friend_count(self, xuid: str) -> int:
        """Get count of verified Halo-active players who list this xuid as a friend."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM graph_friends gf
            JOIN graph_players gp ON gf.src_xuid = gp.xuid
            JOIN halo_features hf ON gf.src_xuid = hf.xuid
            WHERE gf.dst_xuid = ?
            AND gp.halo_active = 1
            AND COALESCE(hf.matches_played, 0) > 0
        """, (xuid,))
        return cursor.fetchone()[0]

    def get_verified_halo_incoming_friends(self, xuid: str) -> List[Dict]:
        """Get verified Halo-active players who list this xuid as a friend."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT gf.src_xuid, gp.gamertag, hf.kd_ratio, hf.matches_played
            FROM graph_friends gf
            JOIN graph_players gp ON gf.src_xuid = gp.xuid
            JOIN halo_features hf ON gf.src_xuid = hf.xuid
            WHERE gf.dst_xuid = ?
            AND gp.halo_active = 1
            AND COALESCE(hf.matches_played, 0) > 0
            ORDER BY COALESCE(hf.matches_played, 0) DESC, gp.gamertag ASC
        """, (xuid,))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_halo_friends(self, xuid: str) -> List[Dict]:
        """Get friends who are also Halo active"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT gf.*, gp.gamertag, gp.halo_active, hf.csr, hf.kd_ratio, hf.matches_played
            FROM graph_friends gf
            JOIN graph_players gp ON gf.dst_xuid = gp.xuid
            LEFT JOIN halo_features hf ON gf.dst_xuid = hf.xuid
            WHERE gf.src_xuid = ?
            AND gp.halo_active = 1
        """, (xuid,))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def edge_exists(self, src_xuid: str, dst_xuid: str) -> bool:
        """Check if an edge exists between two players"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM graph_friends WHERE src_xuid = ? AND dst_xuid = ?",
            (src_xuid, dst_xuid)
        )
        return cursor.fetchone() is not None

    def get_edges_within_set(self, xuids: list) -> List[Dict]:
        """Return all graph_friends edges where both endpoints are in the given XUID set."""
        if len(xuids) < 2:
            return []
        conn = self._get_connection()
        cursor = conn.cursor()
        placeholders = ','.join('?' * len(xuids))
        cursor.execute(
            f"SELECT src_xuid, dst_xuid FROM graph_friends "
            f"WHERE src_xuid IN ({placeholders}) AND dst_xuid IN ({placeholders})",
            xuids + xuids
        )
        return [dict(row) for row in cursor.fetchall()]

    # =========================================================================
    # HALO FEATURES OPERATIONS
    # =========================================================================
    
    def insert_or_update_halo_features(
        self,
        xuid: str,
        gamertag: str = None,
        csr: float = 0,
        csr_tier: str = None,
        kd_ratio: float = 0,
        win_rate: float = 0,
        matches_played: int = 0,
        matches_week: float = 0,
        ranked_matches: int = 0,
        social_matches: int = 0,
        ranked_ratio: float = 0,
        arena_ratio: float = 0,
        btb_ratio: float = 0,
        total_kills: int = 0,
        total_deaths: int = 0,
        total_assists: int = 0,
        avg_kills: float = 0,
        avg_deaths: float = 0,
        headshot_rate: float = 0,
        last_match: str = None,
        first_match: str = None
    ) -> bool:
        """Insert or update Halo features for a player"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        
        try:
            cursor.execute("""
                INSERT INTO halo_features 
                (xuid, gamertag, csr, csr_tier, kd_ratio, win_rate, matches_played,
                 matches_week, ranked_matches, social_matches, ranked_ratio,
                 arena_ratio, btb_ratio, total_kills, total_deaths, total_assists,
                 avg_kills, avg_deaths, headshot_rate, last_match, first_match, stats_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(xuid) DO UPDATE SET
                    gamertag = COALESCE(excluded.gamertag, halo_features.gamertag),
                    csr = excluded.csr,
                    csr_tier = excluded.csr_tier,
                    kd_ratio = excluded.kd_ratio,
                    win_rate = excluded.win_rate,
                    matches_played = excluded.matches_played,
                    matches_week = excluded.matches_week,
                    ranked_matches = excluded.ranked_matches,
                    social_matches = excluded.social_matches,
                    ranked_ratio = excluded.ranked_ratio,
                    arena_ratio = excluded.arena_ratio,
                    btb_ratio = excluded.btb_ratio,
                    total_kills = excluded.total_kills,
                    total_deaths = excluded.total_deaths,
                    total_assists = excluded.total_assists,
                    avg_kills = excluded.avg_kills,
                    avg_deaths = excluded.avg_deaths,
                    headshot_rate = excluded.headshot_rate,
                    last_match = excluded.last_match,
                    first_match = COALESCE(halo_features.first_match, excluded.first_match),
                    stats_updated = excluded.stats_updated
            """, (xuid, gamertag, csr, csr_tier, kd_ratio, win_rate, matches_played,
                  matches_week, ranked_matches, social_matches, ranked_ratio,
                  arena_ratio, btb_ratio, total_kills, total_deaths, total_assists,
                  avg_kills, avg_deaths, headshot_rate, last_match, first_match, now))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting halo features for {xuid}: {e}")
            return False
    
    def get_halo_features(self, xuid: str) -> Optional[Dict]:
        """Get Halo features for a player"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM halo_features WHERE xuid = ?", (xuid,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_similar_players_knn(
        self,
        xuid: str,
        k: int = 3,
        csr_weight: float = 1.0,
        kd_weight: float = 1.0,
        winrate_weight: float = 0.5
    ) -> List[Dict]:
        """
        Find K nearest neighbors based on Halo features.
        Uses simple euclidean distance on normalized features.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Get target player features
        cursor.execute("SELECT csr, kd_ratio, win_rate FROM halo_features WHERE xuid = ?", (xuid,))
        target = cursor.fetchone()
        
        if not target:
            return []
        
        target_csr, target_kd, target_wr = target
        
        # Find similar players (simple SQL distance calculation)
        cursor.execute("""
            SELECT 
                hf.*,
                gp.gamertag,
                (
                    ? * ABS(COALESCE(hf.csr, 0) - ?) / 1000.0 +
                    ? * ABS(COALESCE(hf.kd_ratio, 0) - ?) +
                    ? * ABS(COALESCE(hf.win_rate, 0) - ?) / 100.0
                ) as distance
            FROM halo_features hf
            JOIN graph_players gp ON hf.xuid = gp.xuid
            WHERE hf.xuid != ?
            AND hf.matches_played > 10
            ORDER BY distance ASC
            LIMIT ?
        """, (csr_weight, target_csr or 0, 
              kd_weight, target_kd or 0,
              winrate_weight, target_wr or 0,
              xuid, k))
        
        return [dict(row) for row in cursor.fetchall()]
    
    # =========================================================================
    # CO-PLAY GRAPH OPERATIONS
    # =========================================================================
    
    def insert_or_update_coplay(
        self,
        src_xuid: str,
        dst_xuid: str,
        matches_together: int = 1,
        wins_together: int = 0,
        last_played: str = None,
        first_played: str = None,
        total_minutes: int = 0,
        same_team: bool = True
    ) -> bool:
        """Insert or update a co-play edge"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO graph_coplay 
                (src_xuid, dst_xuid, matches_together, wins_together, 
                 last_played, first_played, total_minutes, same_team_count, opposing_team_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(src_xuid, dst_xuid) DO UPDATE SET
                    matches_together = graph_coplay.matches_together + excluded.matches_together,
                    wins_together = graph_coplay.wins_together + excluded.wins_together,
                    last_played = MAX(graph_coplay.last_played, excluded.last_played),
                    first_played = MIN(graph_coplay.first_played, excluded.first_played),
                    total_minutes = graph_coplay.total_minutes + excluded.total_minutes,
                    same_team_count = graph_coplay.same_team_count + excluded.same_team_count,
                    opposing_team_count = graph_coplay.opposing_team_count + excluded.opposing_team_count
            """, (src_xuid, dst_xuid, matches_together, wins_together,
                  last_played, first_played, total_minutes,
                  1 if same_team else 0, 0 if same_team else 1))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting coplay edge: {e}")
            return False
    
    def get_coplay_partners(self, xuid: str, min_matches: int = 2) -> List[Dict]:
        """Get players who have played together with the given player"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT gc.*, gp.gamertag, gp.halo_active
            FROM graph_coplay gc
            LEFT JOIN graph_players gp ON gc.dst_xuid = gp.xuid
            WHERE gc.src_xuid = ?
            AND gc.matches_together >= ?
            ORDER BY gc.matches_together DESC
        """, (xuid, min_matches))
        
        return [dict(row) for row in cursor.fetchall()]
    
    # =========================================================================
    # CRAWL QUEUE OPERATIONS
    # =========================================================================
    
    def add_to_crawl_queue(
        self,
        xuid: str,
        priority: int = 0,
        depth: int = 0,
        force_pending: bool = False
    ) -> bool:
        """Add a player to the crawl queue
        
        Args:
            xuid: Player XUID
            priority: Queue priority (higher = processed first)
            depth: Crawl depth
            force_pending: If True, reset status to pending even if previously completed
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if force_pending:
                # Force reset to pending status (used when starting new crawl from seed)
                cursor.execute("""
                    INSERT INTO crawl_queue (xuid, priority, depth, status, added_at)
                    VALUES (?, ?, ?, 'pending', ?)
                    ON CONFLICT(xuid) DO UPDATE SET
                        priority = MAX(crawl_queue.priority, excluded.priority),
                        depth = MIN(crawl_queue.depth, excluded.depth),
                        status = 'pending',
                        started_at = NULL,
                        completed_at = NULL,
                        error_message = NULL
                """, (xuid, priority, depth, datetime.now().isoformat()))
            else:
                # Only update priority/depth, keep existing status
                cursor.execute("""
                    INSERT INTO crawl_queue (xuid, priority, depth, status, added_at)
                    VALUES (?, ?, ?, 'pending', ?)
                    ON CONFLICT(xuid) DO UPDATE SET
                        priority = MAX(crawl_queue.priority, excluded.priority),
                        depth = MIN(crawl_queue.depth, excluded.depth)
                """, (xuid, priority, depth, datetime.now().isoformat()))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error adding to crawl queue: {e}")
            return False
    
    def add_to_crawl_queue_batch(self, items: List[Tuple[str, int, int]]) -> int:
        """
        Batch add items to crawl queue.
        
        Args:
            items: List of (xuid, priority, depth) tuples
        
        Returns:
            Number of items added
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        count = 0
        
        try:
            for xuid, priority, depth in items:
                cursor.execute("""
                    INSERT INTO crawl_queue (xuid, priority, depth, status, added_at)
                    VALUES (?, ?, ?, 'pending', ?)
                    ON CONFLICT(xuid) DO UPDATE SET
                        priority = MAX(crawl_queue.priority, excluded.priority),
                        depth = MIN(crawl_queue.depth, excluded.depth)
                """, (xuid, priority, depth, now))
                count += 1
            
            conn.commit()
            return count
        except Exception as e:
            print(f"Error batch adding to crawl queue: {e}")
            conn.rollback()
            return count
    
    def get_next_from_queue(self, batch_size: int = 10) -> List[Dict]:
        """Get next items from crawl queue"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM crawl_queue
            WHERE status = 'pending'
            ORDER BY priority DESC, depth ASC, added_at ASC
            LIMIT ?
        """, (batch_size,))
        
        items = [dict(row) for row in cursor.fetchall()]
        
        # Mark as in-progress
        if items:
            xuids = [item['xuid'] for item in items]
            placeholders = ','.join('?' * len(xuids))
            cursor.execute(f"""
                UPDATE crawl_queue 
                SET status = 'in_progress', started_at = ?
                WHERE xuid IN ({placeholders})
            """, [datetime.now().isoformat()] + xuids)
            conn.commit()
        
        return items
    
    def mark_queue_item_complete(self, xuid: str, error: str = None) -> bool:
        """Mark a crawl queue item as complete"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        status = 'failed' if error else 'completed'
        
        try:
            cursor.execute("""
                UPDATE crawl_queue 
                SET status = ?, completed_at = ?, error_message = ?
                WHERE xuid = ?
            """, (status, datetime.now().isoformat(), error, xuid))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error marking queue item complete: {e}")
            return False
    
    def get_queue_stats(self) -> Dict:
        """Get crawl queue statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        cursor.execute("SELECT status, COUNT(*) as cnt FROM crawl_queue GROUP BY status")
        for row in cursor.fetchall():
            stats[row['status']] = row['cnt']
        
        cursor.execute("SELECT COUNT(*) FROM crawl_queue")
        stats['total'] = cursor.fetchone()[0]
        
        return stats
    
    # =========================================================================
    # GRAPH ANALYTICS
    # =========================================================================
    
    def get_graph_stats(self) -> Dict:
        """Get overall graph statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        # Node counts
        cursor.execute("SELECT COUNT(*) FROM graph_players")
        stats['total_players'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM graph_players WHERE halo_active = 1")
        stats['halo_active_players'] = cursor.fetchone()[0]
        
        # Edge counts
        cursor.execute("SELECT COUNT(*) FROM graph_friends")
        stats['total_friend_edges'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM graph_coplay")
        stats['total_coplay_edges'] = cursor.fetchone()[0]
        
        # Features
        cursor.execute("SELECT COUNT(*) FROM halo_features WHERE matches_played > 0")
        stats['players_with_stats'] = cursor.fetchone()[0]
        
        # Average degree
        cursor.execute("""
            SELECT AVG(cnt) FROM (
                SELECT COUNT(*) as cnt FROM graph_friends GROUP BY src_xuid
            )
        """)
        avg_degree = cursor.fetchone()[0]
        stats['avg_friend_degree'] = round(avg_degree, 2) if avg_degree else 0
        
        # Halo-only average degree
        cursor.execute("""
            SELECT AVG(cnt) FROM (
                SELECT COUNT(*) as cnt 
                FROM graph_friends gf
                JOIN graph_players gp ON gf.dst_xuid = gp.xuid
                WHERE gp.halo_active = 1
                GROUP BY gf.src_xuid
            )
        """)
        halo_degree = cursor.fetchone()[0]
        stats['avg_halo_friend_degree'] = round(halo_degree, 2) if halo_degree else 0
        
        # Depth distribution
        cursor.execute("""
            SELECT crawl_depth, COUNT(*) as cnt 
            FROM graph_players 
            GROUP BY crawl_depth 
            ORDER BY crawl_depth
        """)
        stats['depth_distribution'] = {row['crawl_depth']: row['cnt'] for row in cursor.fetchall()}
        
        # Database size
        if os.path.exists(self.db_path):
            stats['db_size_mb'] = round(os.path.getsize(self.db_path) / (1024 * 1024), 2)
        
        return stats
    
    def find_hubs(self, min_degree: int = 50, halo_only: bool = True) -> List[Dict]:
        """Find hub players with high friend counts"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if halo_only:
            cursor.execute("""
                SELECT 
                    gf.src_xuid as xuid,
                    gp.gamertag,
                    COUNT(*) as friend_count,
                    hf.csr,
                    hf.kd_ratio
                FROM graph_friends gf
                JOIN graph_players gp ON gf.src_xuid = gp.xuid
                LEFT JOIN halo_features hf ON gf.src_xuid = hf.xuid
                WHERE gp.halo_active = 1
                GROUP BY gf.src_xuid
                HAVING COUNT(*) >= ?
                ORDER BY friend_count DESC
            """, (min_degree,))
        else:
            cursor.execute("""
                SELECT 
                    gf.src_xuid as xuid,
                    gp.gamertag,
                    COUNT(*) as friend_count,
                    hf.csr,
                    hf.kd_ratio
                FROM graph_friends gf
                JOIN graph_players gp ON gf.src_xuid = gp.xuid
                LEFT JOIN halo_features hf ON gf.src_xuid = hf.xuid
                GROUP BY gf.src_xuid
                HAVING COUNT(*) >= ?
                ORDER BY friend_count DESC
            """, (min_degree,))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def get_connected_component(self, start_xuid: str, max_size: int = 1000) -> Set[str]:
        """Get all XUIDs in the connected component containing start_xuid"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        visited = set()
        queue = [start_xuid]
        
        while queue and len(visited) < max_size:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            
            # Get neighbors
            cursor.execute("""
                SELECT dst_xuid FROM graph_friends WHERE src_xuid = ?
                UNION
                SELECT src_xuid FROM graph_friends WHERE dst_xuid = ?
            """, (current, current))
            
            for row in cursor.fetchall():
                neighbor = row[0]
                if neighbor not in visited:
                    queue.append(neighbor)
        
        return visited
    
    def close(self):
        """Close database connection"""
        if hasattr(self.local, 'conn'):
            self.local.conn.close()
            del self.local.conn


# Singleton instance
_graph_db_instance = None


def get_graph_db() -> HaloSocialGraphDB:
    """Get the singleton graph database instance"""
    global _graph_db_instance
    if _graph_db_instance is None:
        _graph_db_instance = HaloSocialGraphDB()
    return _graph_db_instance
