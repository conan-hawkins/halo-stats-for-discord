"""
SQLite-based normalized database schema for Halo Infinite Discord Bot v2
Normalized structure: Matches, Players, Player_Match, Medal_Sets, Medal_Types
"""

import sqlite3
import json
import os
import hashlib
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import threading

from src.config import DATABASE_FILE

# Medal name mappings from Halo Infinite API
# Source: Halo Infinite medal metadata
MEDAL_NAME_MAPPING = {
    # Multi-kill medals
    622331684: "Double Kill",
    2780740615: "Triple Kill",
    2063152177: "Overkill",
    835814121: "Killtacular",
    2123530881: "Killtrocity",
    # Note: Higher multi-kills use different IDs
    
    # Killing Spree medals
    2758320809: "Killing Spree",      # 5 kills without dying
    1169390319: "Killing Frenzy",     # 10 kills without dying
    3934547153: "Running Riot",       # 15 kills without dying
    1512363953: "Rampage",            # 20 kills without dying
    3655682764: "Nightmare",          # 25 kills without dying
    1176569867: "Boogeyman",          # 30 kills without dying
    265478668: "Grim Reaper",         # 35 kills without dying
    4261842076: "Demon",              # 40 kills without dying
    
    # Weapon-specific medals
    3233952928: "Headshot",           # Precision weapon headshot kill
    548533137: "Perfect",             # Kill with perfect accuracy (no missed shots)
    1734214473: "Sniper Kill",        # Kill with sniper rifle
    3091261182: "No Scope",           # Sniper kill without scoping
    2625820422: "Snapshot",           # Quick scope sniper kill
    269174970: "Reversal",            # Kill enemy who damaged you first
    1169571763: "Ninja",              # Back smack/assassination
    1172766553: "Grenade Kill",       # Kill with grenade
    2852571933: "Melee Kill",         # Kill with melee
    2861418269: "Beatdown",           # Melee kill from behind
    1146876011: "Bulltrue",           # Kill enemy while they're sword lunging
    2418616582: "Hail Mary",          # Long distance grenade kill
    3488248720: "Sticky",             # Kill with plasma grenade stick
    1210678802: "Remote Detonation",  # Kill with remote detonation
    3905838030: "Skewer Kill",        # Kill with Skewer
    1880789493: "Pancake",            # Kill with Repulsor (push off map)
    4229934157: "Return to Sender",   # Kill with deflected projectile
    1283796619: "Tag & Bag",          # Kill recently marked enemy
    
    # Objective medals
    1284032216: "Flag Kill",          # Kill while holding flag
    3334154676: "Ball Kill",          # Kill while holding oddball
    3732790338: "Carrier Kill",       # Kill the flag/ball carrier
    2602963073: "Goal Line Stand",    # Kill flag carrier near your base
    1472686630: "Interception",       # Grab enemy flag mid-air
    
    # Vehicle medals  
    # (Add more as discovered)
    
    # Assist medals
    # (Add more as discovered)
}

# Reverse mapping for lookups
MEDAL_ID_BY_NAME = {v: k for k, v in MEDAL_NAME_MAPPING.items()}


class HaloStatsDBv2:
    """Thread-safe SQLite database with normalized schema for Halo stats"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DATABASE_FILE)
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
        """Initialize normalized database schema"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # ============================================================
        # Table 1: Matches - Match metadata
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id TEXT PRIMARY KEY,
                duration TEXT,
                start_time TEXT NOT NULL,
                is_ranked INTEGER NOT NULL DEFAULT 0,
                playlist_id TEXT,
                match_category TEXT NOT NULL DEFAULT 'unknown',
                category_source TEXT,
                map_id TEXT,
                map_version TEXT
            )
        """)

        # Migration-safe columns for existing DBs created before category support.
        self._ensure_column_exists("matches", "match_category", "TEXT NOT NULL DEFAULT 'unknown'")
        self._ensure_column_exists("matches", "category_source", "TEXT")
        
        # ============================================================
        # Table 2: Players - Player information
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS players (
                xuid TEXT PRIMARY KEY,
                gamertag TEXT,
                last_processed_at TEXT,
                date_added TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # ============================================================
        # Table 3: Medal Types - Reference table for medal IDs to names
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS medal_types (
                medal_name_id INTEGER PRIMARY KEY,
                medal_name TEXT NOT NULL,
                medal_category TEXT
            )
        """)
        
        # ============================================================
        # Table 4: Medal Sets - Unique combinations of medals
        # Each row represents a unique set of medals earned in a match
        # ============================================================
        # Build dynamic columns for known medals
        medal_columns = self._get_medal_columns_sql()
        
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS medal_sets (
                medal_set_id INTEGER PRIMARY KEY AUTOINCREMENT,
                medal_hash TEXT UNIQUE NOT NULL,
                {medal_columns}
            )
        """)
        
        # ============================================================
        # Table 5: Player_Match - Junction table with player stats per match
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_match (
                xuid TEXT NOT NULL,
                match_id TEXT NOT NULL,
                kills INTEGER NOT NULL DEFAULT 0,
                deaths INTEGER NOT NULL DEFAULT 0,
                assists INTEGER NOT NULL DEFAULT 0,
                outcome INTEGER NOT NULL DEFAULT 0,
                medal_set_id INTEGER,
                PRIMARY KEY (xuid, match_id),
                FOREIGN KEY (xuid) REFERENCES players(xuid),
                FOREIGN KEY (match_id) REFERENCES matches(match_id),
                FOREIGN KEY (medal_set_id) REFERENCES medal_sets(medal_set_id)
            )
        """)

        # ============================================================
        # Table 6: Match Participants - Full match rosters with team attribution
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS match_participants (
                match_id TEXT NOT NULL,
                xuid TEXT NOT NULL,
                outcome INTEGER NOT NULL DEFAULT 0,
                team_id TEXT,
                inferred_team_id TEXT,
                kills INTEGER NOT NULL DEFAULT 0,
                deaths INTEGER NOT NULL DEFAULT 0,
                assists INTEGER NOT NULL DEFAULT 0,
                csr INTEGER,
                csr_tier TEXT,
                PRIMARY KEY (match_id, xuid),
                FOREIGN KEY (match_id) REFERENCES matches(match_id),
                FOREIGN KEY (xuid) REFERENCES players(xuid)
            )
        """)
        
        # ============================================================
        # Indexes for performance
        # ============================================================
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_start_time ON matches(start_time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_playlist ON matches(playlist_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_ranked ON matches(is_ranked)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_category ON matches(match_category)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_map ON matches(map_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_gamertag ON players(gamertag)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_player_match_xuid ON player_match(xuid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_player_match_match ON player_match(match_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_match_participants_match ON match_participants(match_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_match_participants_xuid ON match_participants(xuid)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_match_participants_team ON match_participants(team_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_match_participants_inferred_team ON match_participants(inferred_team_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_medal_sets_hash ON medal_sets(medal_hash)")
        
        # Populate medal_types reference table
        self._populate_medal_types(cursor)
        
        conn.commit()

    def _ensure_column_exists(self, table_name: str, column_name: str, column_sql: str) -> None:
        """Add a column to an existing table if it is missing."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing = {row['name'] for row in cursor.fetchall()}
        if column_name in existing:
            return
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")
        conn.commit()
    
    def _get_medal_columns_sql(self) -> str:
        """Generate SQL column definitions for all known medals"""
        columns = []
        for medal_id, medal_name in MEDAL_NAME_MAPPING.items():
            # Sanitize column name: replace spaces with underscores, lowercase
            col_name = f"medal_{medal_id}"
            columns.append(f"{col_name} INTEGER NOT NULL DEFAULT 0")
        return ",\n                ".join(columns)
    
    def _populate_medal_types(self, cursor):
        """Populate the medal_types reference table"""
        for medal_id, medal_name in MEDAL_NAME_MAPPING.items():
            # Determine category based on medal name
            category = self._get_medal_category(medal_name)
            cursor.execute("""
                INSERT OR IGNORE INTO medal_types (medal_name_id, medal_name, medal_category)
                VALUES (?, ?, ?)
            """, (medal_id, medal_name, category))
    
    def _get_medal_category(self, medal_name: str) -> str:
        """Categorize medal by name"""
        multi_kill = ["Double Kill", "Triple Kill", "Overkill", "Killtacular", "Killtrocity"]
        spree = ["Killing Spree", "Killing Frenzy", "Running Riot", "Rampage", 
                 "Nightmare", "Boogeyman", "Grim Reaper", "Demon"]
        objective = ["Flag Kill", "Ball Kill", "Carrier Kill", "Goal Line Stand", "Interception"]
        
        if medal_name in multi_kill:
            return "Multi-Kill"
        elif medal_name in spree:
            return "Spree"
        elif medal_name in objective:
            return "Objective"
        else:
            return "Skill"
    
    def _generate_medal_hash(self, medals: List[Dict]) -> str:
        """Generate a unique hash for a medal combination"""
        # Sort medals by ID and create a consistent string
        medal_dict = {}
        for medal in medals:
            name_id = medal.get('NameId')
            count = medal.get('Count', 0)
            if name_id and count > 0:
                medal_dict[name_id] = count
        
        # Sort by medal ID for consistency
        sorted_medals = sorted(medal_dict.items())
        medal_str = json.dumps(sorted_medals, sort_keys=True)
        return hashlib.md5(medal_str.encode()).hexdigest()
    
    def get_or_create_medal_set(self, medals: List[Dict]) -> Optional[int]:
        """Get existing medal_set_id or create new one for the medal combination"""
        if not medals:
            return None
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Generate hash for this medal combination
        medal_hash = self._generate_medal_hash(medals)
        
        # Check if this combination already exists
        cursor.execute("SELECT medal_set_id FROM medal_sets WHERE medal_hash = ?", (medal_hash,))
        row = cursor.fetchone()
        
        if row:
            return row['medal_set_id']
        
        # Create new medal set
        # Build column names and values
        columns = ["medal_hash"]
        values = [medal_hash]
        placeholders = ["?"]
        
        for medal in medals:
            name_id = medal.get('NameId')
            count = medal.get('Count', 0)
            if name_id and count > 0:
                col_name = f"medal_{name_id}"
                # Check if column exists (for unknown medals)
                cursor.execute(f"PRAGMA table_info(medal_sets)")
                existing_cols = {row['name'] for row in cursor.fetchall()}
                
                if col_name not in existing_cols:
                    # Add column for new medal type
                    cursor.execute(f"ALTER TABLE medal_sets ADD COLUMN {col_name} INTEGER NOT NULL DEFAULT 0")
                    # Also add to medal_types if not exists
                    cursor.execute("""
                        INSERT OR IGNORE INTO medal_types (medal_name_id, medal_name, medal_category)
                        VALUES (?, ?, ?)
                    """, (name_id, f"Unknown Medal {name_id}", "Unknown"))
                
                columns.append(col_name)
                values.append(count)
                placeholders.append("?")
        
        # Insert new medal set
        sql = f"INSERT INTO medal_sets ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        cursor.execute(sql, values)
        conn.commit()
        
        return cursor.lastrowid
    
    def insert_match(self, match_data: Dict) -> bool:
        """Insert match metadata"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO matches 
                (match_id, duration, start_time, is_ranked, playlist_id, match_category, category_source, map_id, map_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                match_data.get('match_id'),
                match_data.get('duration'),
                match_data.get('start_time'),
                1 if match_data.get('is_ranked') else 0,
                match_data.get('playlist_id'),
                match_data.get('match_category', 'unknown') or 'unknown',
                match_data.get('category_source'),
                match_data.get('map_id'),
                match_data.get('map_version')
            ))

            cursor.execute(
                """
                UPDATE matches
                SET
                    match_category = COALESCE(?, match_category),
                    category_source = COALESCE(?, category_source)
                WHERE match_id = ?
                """,
                (
                    match_data.get('match_category'),
                    match_data.get('category_source'),
                    match_data.get('match_id'),
                ),
            )
            conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting match: {e}")
            return False
    
    def insert_or_update_player(self, xuid: str, gamertag: str = None, 
                                 last_processed_at: str = None) -> bool:
        """Insert or update player information"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Check if player exists
            cursor.execute("SELECT xuid FROM players WHERE xuid = ?", (xuid,))
            exists = cursor.fetchone()
            
            if exists:
                # Update existing player
                updates = []
                values = []
                if gamertag:
                    updates.append("gamertag = ?")
                    values.append(gamertag)
                if last_processed_at:
                    updates.append("last_processed_at = ?")
                    values.append(last_processed_at)
                
                if updates:
                    values.append(xuid)
                    cursor.execute(f"""
                        UPDATE players SET {', '.join(updates)} WHERE xuid = ?
                    """, values)
            else:
                # Insert new player
                cursor.execute("""
                    INSERT INTO players (xuid, gamertag, last_processed_at, date_added)
                    VALUES (?, ?, ?, ?)
                """, (xuid, gamertag, last_processed_at, datetime.now().isoformat()))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting/updating player: {e}")
            return False
    
    def insert_player_match(self, xuid: str, match_data: Dict) -> bool:
        """Insert player's performance for a specific match"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Get or create medal set
            medals = match_data.get('medals', [])
            medal_set_id = self.get_or_create_medal_set(medals) if medals else None
            
            cursor.execute("""
                INSERT OR REPLACE INTO player_match 
                (xuid, match_id, kills, deaths, assists, outcome, medal_set_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                xuid,
                match_data.get('match_id'),
                match_data.get('kills', 0),
                match_data.get('deaths', 0),
                match_data.get('assists', 0),
                match_data.get('outcome', 0),
                medal_set_id
            ))
            conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting player_match: {e}")
            return False

    def insert_match_participants(self, match_id: str, participants: List[Dict]) -> bool:
        """Insert or update all participants for a match."""
        if not match_id or not participants:
            return True

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            for participant in participants:
                participant_xuid = str(participant.get('xuid') or '').strip()
                if not participant_xuid:
                    continue

                participant_gamertag = participant.get('gamertag')
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO players (xuid, gamertag, date_added)
                    VALUES (?, ?, ?)
                    """,
                    (participant_xuid, participant_gamertag, datetime.now().isoformat()),
                )

                if participant_gamertag:
                    cursor.execute(
                        """
                        UPDATE players
                        SET gamertag = COALESCE(gamertag, ?)
                        WHERE xuid = ?
                        """,
                        (participant_gamertag, participant_xuid),
                    )

                cursor.execute(
                    """
                    INSERT OR REPLACE INTO match_participants
                    (match_id, xuid, outcome, team_id, inferred_team_id, kills, deaths, assists, csr, csr_tier)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        match_id,
                        participant_xuid,
                        int(participant.get('outcome', 0) or 0),
                        participant.get('team_id'),
                        participant.get('inferred_team_id'),
                        int(participant.get('kills', 0) or 0),
                        int(participant.get('deaths', 0) or 0),
                        int(participant.get('assists', 0) or 0),
                        participant.get('csr'),
                        participant.get('csr_tier'),
                    ),
                )

            conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting match participants for {match_id}: {e}")
            return False

    def get_match_participants(self, match_id: str) -> List[Dict]:
        """Get all persisted participants for a single match."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                mp.match_id,
                mp.xuid,
                mp.outcome,
                mp.team_id,
                mp.inferred_team_id,
                mp.kills,
                mp.deaths,
                mp.assists,
                mp.csr,
                mp.csr_tier,
                p.gamertag,
                m.start_time
            FROM match_participants mp
            LEFT JOIN players p ON p.xuid = mp.xuid
            LEFT JOIN matches m ON m.match_id = mp.match_id
            WHERE mp.match_id = ?
            ORDER BY mp.xuid
            """,
            (match_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_scope_match_participants(self, scope_xuids: List[str]) -> Dict[str, List[Dict]]:
        """Get participants for matches where any scope player appears, filtered to scope players."""
        normalized_scope = [str(x).strip() for x in scope_xuids if str(x).strip()]
        if not normalized_scope:
            return {}

        conn = self._get_connection()
        cursor = conn.cursor()

        placeholders = ",".join(["?"] * len(normalized_scope))
        params = normalized_scope + normalized_scope
        cursor.execute(
            f"""
            WITH scope_matches AS (
                SELECT DISTINCT match_id
                FROM match_participants
                WHERE xuid IN ({placeholders})
            )
            SELECT
                mp.match_id,
                mp.xuid,
                mp.outcome,
                mp.team_id,
                mp.inferred_team_id,
                m.start_time
            FROM match_participants mp
            JOIN scope_matches sm ON sm.match_id = mp.match_id
            LEFT JOIN matches m ON m.match_id = mp.match_id
            WHERE mp.xuid IN ({placeholders})
            ORDER BY mp.match_id
            """,
            params,
        )

        grouped: Dict[str, List[Dict]] = {}
        for row in cursor.fetchall():
            row_dict = dict(row)
            grouped.setdefault(row_dict['match_id'], []).append(row_dict)

        return grouped

    def get_all_match_participants(self, limit_matches: Optional[int] = None) -> Dict[str, List[Dict]]:
        """Get participants for all matches, optionally limited by newest match start_time."""
        conn = self._get_connection()
        cursor = conn.cursor()

        limit_clause = ""
        params: List[int] = []
        if limit_matches is not None and int(limit_matches) > 0:
            limit_clause = "LIMIT ?"
            params.append(int(limit_matches))

        cursor.execute(
            f"""
            WITH selected_matches AS (
                SELECT m.match_id
                FROM matches m
                ORDER BY COALESCE(m.start_time, '') DESC, m.match_id ASC
                {limit_clause}
            )
            SELECT
                mp.match_id,
                mp.xuid,
                mp.outcome,
                mp.team_id,
                mp.inferred_team_id,
                m.start_time
            FROM match_participants mp
            JOIN selected_matches sm ON sm.match_id = mp.match_id
            LEFT JOIN matches m ON m.match_id = mp.match_id
            ORDER BY COALESCE(m.start_time, '') DESC, mp.match_id ASC
            """,
            params,
        )

        grouped: Dict[str, List[Dict]] = {}
        for row in cursor.fetchall():
            row_dict = dict(row)
            grouped.setdefault(row_dict['match_id'], []).append(row_dict)

        return grouped
    
    def get_player_stats(self, xuid: str, stat_type: str = "overall") -> Optional[Dict]:
        """Get aggregated player stats from normalized data"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Build query based on stat_type
        ranked_filter = ""
        if stat_type == "ranked":
            ranked_filter = "AND m.is_ranked = 1"
        elif stat_type == "social":
            ranked_filter = "AND m.is_ranked = 0"
        
        # Get basic stats
        cursor.execute(f"""
            SELECT 
                p.xuid,
                p.gamertag,
                COUNT(pm.match_id) as games_played,
                SUM(pm.kills) as total_kills,
                SUM(pm.deaths) as total_deaths,
                SUM(pm.assists) as total_assists,
                SUM(CASE WHEN pm.outcome = 2 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pm.outcome = 3 THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN pm.outcome = 1 THEN 1 ELSE 0 END) as draws
            FROM players p
            LEFT JOIN player_match pm ON p.xuid = pm.xuid
            LEFT JOIN matches m ON pm.match_id = m.match_id
            WHERE p.xuid = ? {ranked_filter}
            GROUP BY p.xuid
        """, (xuid,))
        
        row = cursor.fetchone()
        if not row:
            return None
        
        stats = dict(row)
        
        # Calculate derived stats
        if stats['total_deaths'] > 0:
            stats['kd_ratio'] = round(stats['total_kills'] / stats['total_deaths'], 2)
        else:
            stats['kd_ratio'] = stats['total_kills']
        
        if stats['games_played'] > 0:
            stats['win_rate'] = round((stats['wins'] / stats['games_played']) * 100, 1)
            stats['avg_kills'] = round(stats['total_kills'] / stats['games_played'], 1)
            stats['avg_deaths'] = round(stats['total_deaths'] / stats['games_played'], 1)
        else:
            stats['win_rate'] = 0
            stats['avg_kills'] = 0
            stats['avg_deaths'] = 0
        
        return stats
    
    def get_player_matches(self, xuid: str, limit: int = 100, 
                           stat_type: str = "overall") -> List[Dict]:
        """Get player's match history"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        ranked_filter = ""
        if stat_type == "ranked":
            ranked_filter = "AND m.is_ranked = 1"
        elif stat_type == "social":
            ranked_filter = "AND m.is_ranked = 0"
        
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
            LIMIT ?
        """, (xuid, limit))
        
        return [dict(row) for row in cursor.fetchall()]
    
    def get_player_medal_totals(self, xuid: str, stat_type: str = "overall") -> Dict[str, int]:
        """Get total medals earned by player"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        ranked_filter = ""
        if stat_type == "ranked":
            ranked_filter = "AND m.is_ranked = 1"
        elif stat_type == "social":
            ranked_filter = "AND m.is_ranked = 0"
        
        # Get all medal columns
        cursor.execute("PRAGMA table_info(medal_sets)")
        columns = []
        for row in cursor.fetchall():
            col = row['name']
            if not col.startswith('medal_'):
                continue
            medal_suffix = col.replace('medal_', '', 1)
            if medal_suffix.isdigit():
                columns.append(col)
        
        if not columns:
            return {}
        
        # Build sum query for each medal column
        sums = ", ".join([f"SUM(ms.{col}) as {col}" for col in columns])
        
        cursor.execute(f"""
            SELECT {sums}
            FROM player_match pm
            JOIN matches m ON pm.match_id = m.match_id
            JOIN medal_sets ms ON pm.medal_set_id = ms.medal_set_id
            WHERE pm.xuid = ? {ranked_filter}
        """, (xuid,))
        
        row = cursor.fetchone()
        if not row:
            return {}
        
        # Convert to medal name -> count
        result = {}
        for col in columns:
            medal_id = int(col.replace('medal_', ''))
            count = row[col] or 0
            if count > 0:
                medal_name = MEDAL_NAME_MAPPING.get(medal_id, f"Unknown ({medal_id})")
                result[medal_name] = count
        
        return result
    
    def get_stats_summary(self) -> Dict:
        """Get database statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        cursor.execute("SELECT COUNT(*) FROM players")
        stats['total_players'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM matches")
        stats['total_matches'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM player_match")
        stats['total_player_matches'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM medal_sets")
        stats['unique_medal_sets'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM medal_types")
        stats['medal_types_known'] = cursor.fetchone()[0]
        
        # Database file size
        if os.path.exists(self.db_path):
            stats['db_size_mb'] = round(os.path.getsize(self.db_path) / (1024 * 1024), 2)
        
        return stats
    
    def close(self):
        """Close database connection"""
        if hasattr(self.local, 'conn'):
            self.local.conn.close()
            del self.local.conn


# Export medal mappings for external use
def get_medal_name(medal_id: int) -> str:
    """Get medal name from ID"""
    return MEDAL_NAME_MAPPING.get(medal_id, f"Unknown Medal ({medal_id})")

def get_medal_id(medal_name: str) -> Optional[int]:
    """Get medal ID from name"""
    return MEDAL_ID_BY_NAME.get(medal_name)
