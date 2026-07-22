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

from src.config import CORE_RANKED_PLAYLIST_IDS, DATABASE_FILE

# Medal name mappings from Halo Infinite API.
# Source: the official medal metadata endpoint
# (gamecms-hacs.svc.halowaypoint.com/hi/Waypoint/file/medals/metadata.json),
# fetched and verified 2026-07-17 - the previous hand-curated version of this
# table had every entry except Double Kill paired with the wrong medal name.
MEDAL_NAME_MAPPING = {
    # Spree medals
    3233952928: "Killjoy",
    2780740615: "Killing Spree",
    3169118333: "Driving Spree",
    4261842076: "Killing Frenzy",
    2848470465: "Death Cabbie",
    1739996188: "Immortal Chauffeur",
    418532952: "Running Riot",
    1486797009: "Rampage",
    710323196: "Nightmare",
    1720896992: "Boogeyman",
    2567026752: "Grim Reaper",
    2875941471: "Demon",

    # Mode medals
    3488248720: "Stopped Short",
    976049027: "Flag Joust",
    4247875860: "Duelist",
    1472686630: "Always Rotating",
    2717755703: "Sole Survivor",
    4285712605: "Hang Up",
    2964157454: "Call Blocked",
    3227840152: "Goal Line Stand",
    2623698509: "Lone Wolf",
    3732790338: "Fumble",
    3630529364: "Clock Stop",
    1376646881: "Great Journey",
    1025827095: "Culling",
    88914608: "Blight",
    557309779: "Zombie Slayer",
    1090931685: "Monopoly",
    580478179: "Hill Guardian",
    394349536: "Clear Reception",
    2426456555: "Secure Line",
    3931425309: "Signal Block",
    1680000231: "Flawless Victory",
    1169390319: "Steaktacular",
    3011158621: "Necromancer",
    3120600565: "Immortal",
    521420212: "Ace",
    781229683: "Straight Balling",
    3528500956: "All That Juice",
    629165579: "Power Outage",
    3467301935: "Purge",
    1155542859: "Disease",
    1447057920: "Undead Hunter",
    1064731598: "Untainted",
    865763896: "Perfection",
    4100966367: "Extermination",
    217730222: "Hell's Janitor",
    17866865: "The Sickness",
    1765213446: "Cleansing",
    3786134933: "Plague",
    1719203329: "Pestilence",
    496411737: "Purification",
    3520382976: "Scourge",
    2164872967: "Divine Intervention",
    3653884673: "Apocalypse",

    # Multikill medals
    622331684: "Double Kill",
    2063152177: "Triple Kill",
    835814121: "Overkill",
    2137071619: "Killtacular",
    1430343434: "Killtrocity",
    3835606176: "Killamanjaro",
    2242633421: "Killtastrophe",
    3352648716: "Killpocalypse",
    3233051772: "Killionaire",

    # Proficiency medals
    2477555653: "Spotter",
    1685043466: "Treasure Hunter",
    20397755: "Saboteur",
    1284032216: "Wingman",
    2926348688: "Wheelman",
    3783455472: "Gunner",
    3027762381: "Driver",
    2593226288: "Pilot",
    2278023431: "Tanker",
    2852571933: "Rifleman",
    1146876011: "Bomber",
    2648272972: "Grenadier",
    269174970: "Boxer",
    1210678802: "Warrior",
    1172766553: "Gunslinger",
    3347922939: "Scattergunner",
    4277328263: "Sharpshooter",
    2758320809: "Marksman",
    4086138034: "Heavy",
    555849395: "Bodyguard",
    2750622016: "Breacher",

    # Skill medals
    548533137: "Back Smack",
    1229018603: "Dogfight",
    2418616582: "Harpoon",
    87172902: "Odin's Raven",
    731054446: "Skyjack",
    3655682764: "Stick",
    3546244406: "Kong",
    2123530881: "Reversal",
    4229934157: "Snipe",
    3334154676: "Guardian Angel",
    1969067783: "Chain Reaction",
    221693153: "Splatter",
    3114137341: "Bulltrue",
    3905838030: "Cluster Luck",
    1880789493: "Mind the Gap",
    3876426273: "Pancake",
    656245292: "Rideshare",
    1841872491: "Tag & Bag",
    1734214473: "Whiplash",
    2827657131: "Windshield Wiper",
    3934547153: "Hail Mary",
    265478668: "Nade Shot",
    1512363953: "Perfect",
    2414983178: "Bank Shot",
    988255960: "Fire & Forget",
    4215552487: "Ballista",
    4132863117: "Pull",
    2602963073: "No Scope",
    677323068: "Death Race",
    1477806194: "Counter-snipe",
    2253222811: "Nuclear Football",
    524758914: "Boom Block",
    3059799290: "Return to Sender",
    1623236079: "Autopilot Engaged",
    670606868: "Sneak King",
    3217141618: "Achilles Spine",
    1646928910: "Grand Slam",
    651256911: "Interlinked",
    3085856613: "Ninja",
    1312042926: "Quigley",
    3160646854: "Remote Detonation",

    # Style medals
    3739610597: "Flyin' High",
    2625820422: "From the Grave",
    3091261182: "Last Shot",
    1065136443: "Mount Up",
    2861418269: "Quick Draw",
    1445036152: "Reclaimer",
    275666139: "Special Delivery",
    3588869844: "From the Void",
    690125105: "Grapple-jack",
    175594566: "Hold This",
    3475540930: "Lawnmower",
    1283796619: "Off the Rack",
    3583966655: "Party's Over",
    2019283350: "Pineapple Express",
    1298835518: "Ramming Speed",
    1169571763: "Shot Caller",
    1176569867: "Yard Sale",
    1331361851: "Mounted & Loaded",
    1427176344: "360",
    641726424: "Combat Evolved",
    2396845048: "Deadly Catch",
    197913196: "Driveby",
    2967011722: "Street Sweeper",
    4007438389: "Blind Fire",
    1211820913: "Fastball",
}

# Reverse mapping for lookups
MEDAL_ID_BY_NAME = {v: k for k, v in MEDAL_NAME_MAPPING.items()}


class HaloStatsDBv2:
    """Thread-safe SQLite database with normalized schema for Halo stats"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(DATABASE_FILE)
        self.local = threading.local()
        # Cache of medal_sets column names, populated lazily from the live
        # schema. Avoids re-running PRAGMA table_info on the hot insert path.
        self._medal_set_columns: Optional[set] = None
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
            # Block-and-retry instead of failing immediately under write contention
            self.local.conn.execute("PRAGMA busy_timeout=5000")
            # Safe with WAL: skips the per-commit fsync (only a checkpoint fsync
            # could be lost on an OS/power crash, never on a process crash, and
            # never corruption). Biggest single write-latency win on an HDD.
            self.local.conn.execute("PRAGMA synchronous=NORMAL")
            # Keep ORDER BY / GROUP BY / DISTINCT spill b-trees in RAM, off the HDD.
            self.local.conn.execute("PRAGMA temp_store=MEMORY")
            # ~128MB page cache. Access to this (large, HDD-backed) DB is
            # concentrated on the asyncio event-loop thread, so connection count
            # is low and this does not multiply badly across threads.
            self.local.conn.execute("PRAGMA cache_size=-131072")
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

        # Migration-safe columns for existing DBs created before completeness
        # tracking was persisted (previously computed per-request and discarded).
        self._ensure_column_exists("players", "incomplete_data", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column_exists("players", "failed_match_count", "INTEGER NOT NULL DEFAULT 0")

        # ============================================================
        # Table 2b: Player Failed Matches - specific match IDs whose detail
        # fetch failed, so the next history check can retry exactly those
        # matches instead of re-crawling the player's entire history.
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_failed_matches (
                xuid TEXT NOT NULL,
                match_id TEXT NOT NULL,
                PRIMARY KEY (xuid, match_id),
                FOREIGN KEY (xuid) REFERENCES players(xuid)
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
        # Table 7: Player Mode Stats - Precomputed per-player, per-game-mode
        # aggregates, maintained incrementally by insert_player_match to
        # avoid on-demand full match-history scans. game_mode is 'overall'/
        # 'ranked'/'social' plus the ranked split 'core_ranked'/
        # 'rotational_ranked' (see CORE_RANKED_PLAYLIST_IDS).
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_mode_stats (
                xuid TEXT NOT NULL,
                game_mode TEXT NOT NULL,
                games_played INTEGER NOT NULL DEFAULT 0,
                total_kills INTEGER NOT NULL DEFAULT 0,
                total_deaths INTEGER NOT NULL DEFAULT 0,
                total_assists INTEGER NOT NULL DEFAULT 0,
                wins INTEGER NOT NULL DEFAULT 0,
                losses INTEGER NOT NULL DEFAULT 0,
                draws INTEGER NOT NULL DEFAULT 0,
                dnf INTEGER NOT NULL DEFAULT 0,
                last_updated TEXT,
                PRIMARY KEY (xuid, game_mode),
                FOREIGN KEY (xuid) REFERENCES players(xuid)
            )
        """)

        # ============================================================
        # Table 8: Player Medal Totals - Precomputed per-player, per-game-mode,
        # per-medal counts, maintained incrementally by insert_player_match
        # (mirrors player_mode_stats above). game_mode is 'overall'/'ranked'/
        # 'social' plus the ranked split 'core_ranked'/'rotational_ranked'
        # (see CORE_RANKED_PLAYLIST_IDS); 'overall' is the combined total
        # across ranked + social.
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_medal_totals (
                xuid TEXT NOT NULL,
                game_mode TEXT NOT NULL,
                medal_name_id INTEGER NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                last_updated TEXT,
                PRIMARY KEY (xuid, game_mode, medal_name_id),
                FOREIGN KEY (xuid) REFERENCES players(xuid),
                FOREIGN KEY (medal_name_id) REFERENCES medal_types(medal_name_id)
            )
        """)

        # ============================================================
        # Table 9: Playlist Metadata - cache of discovery-infiniteugc
        # playlist lookups, keyed by playlist_asset_id (not asset+version -
        # ranked status is a property of the playlist's persistent identity;
        # a reworked playlist gets a new asset id, not a new version of the
        # old one). Populated lazily at ingest time
        # (HaloAPIClient._lookup_or_resolve_playlist_ranked) and proactively
        # by reclassify_playlists_backfill.py, so _classify_match_category
        # can detect ranked playlists that rotated in after
        # RANKED_PLAYLIST_IDS was last hand-updated.
        # ============================================================
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS playlist_metadata (
                playlist_asset_id TEXT PRIMARY KEY,
                public_name TEXT,
                is_ranked INTEGER NOT NULL DEFAULT 0,
                resolution_status TEXT NOT NULL DEFAULT 'unresolved',
                last_checked_at TEXT NOT NULL,
                last_version_id TEXT
            )
        """)

        # ============================================================
        # Indexes for performance
        # ============================================================
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_start_time ON matches(start_time)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_playlist ON matches(playlist_id)")
        # Lets reclassify_playlists_backfill's "most recent match per
        # playlist" sample query (WHERE playlist_id=? ORDER BY start_time
        # DESC LIMIT 1) do an index-only seek instead of sorting every
        # matching row - decisive on popular playlists with millions of rows.
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_playlist_start ON matches(playlist_id, start_time)")
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
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_player_medal_totals_medal_name_id ON player_medal_totals(medal_name_id)")
        
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
        """Populate/refresh the medal_types reference table so a corrected
        MEDAL_NAME_MAPPING self-heals any previously mislabeled rows on the
        next startup, rather than leaving stale names stuck in an existing
        database. Uses INSERT OR IGNORE + UPDATE (not INSERT OR REPLACE):
        REPLACE deletes-then-reinserts on a PK conflict, and since
        player_medal_totals has a foreign key on medal_name_id, every delete
        forces SQLite to scan that (multi-million-row) table to verify no
        child rows reference it - here we only ever touch non-key columns, so
        no delete/FK check ever fires."""
        for medal_id, medal_name in MEDAL_NAME_MAPPING.items():
            # Determine category based on medal name
            category = self._get_medal_category(medal_name)
            cursor.execute(
                "INSERT OR IGNORE INTO medal_types (medal_name_id, medal_name, medal_category) VALUES (?, ?, ?)",
                (medal_id, medal_name, category)
            )
            cursor.execute(
                "UPDATE medal_types SET medal_name = ?, medal_category = ? WHERE medal_name_id = ?",
                (medal_name, category, medal_id)
            )
    
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
    
    def _load_medal_set_columns(self, cursor: sqlite3.Cursor) -> set:
        """Return the (cached) set of medal_sets column names, populating it
        from the live schema on first use. Cheap once warm - avoids running
        PRAGMA table_info on every medal insert."""
        if self._medal_set_columns is None:
            cursor.execute("PRAGMA table_info(medal_sets)")
            self._medal_set_columns = {row['name'] for row in cursor.fetchall()}
        return self._medal_set_columns

    def _reset_medal_set_columns_cache(self) -> None:
        """Drop the cached medal_sets column set. Call after a rolled-back
        transaction that may have undone an ALTER TABLE ADD COLUMN, so the next
        call re-reads the real schema instead of trusting a stale cache."""
        self._medal_set_columns = None

    def get_or_create_medal_set(self, medals: List[Dict], commit: bool = True) -> Optional[int]:
        """Get existing medal_set_id or create new one for the medal combination.

        Pass commit=False to take part in a caller-managed transaction (the row
        is still written, just not committed here)."""
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

        # New combination. Resolve the current column set once (cached across
        # calls), instead of re-scanning PRAGMA table_info once per medal.
        existing_cols = self._load_medal_set_columns(cursor)

        # Create new medal set - build column names and values
        columns = ["medal_hash"]
        values = [medal_hash]
        placeholders = ["?"]

        for medal in medals:
            name_id = medal.get('NameId')
            count = medal.get('Count', 0)
            if name_id and count > 0:
                col_name = f"medal_{name_id}"
                if col_name not in existing_cols:
                    # Add column for new medal type. Guard against the race
                    # where a concurrent writer already added it (the ALTER
                    # then raises "duplicate column name", which is benign).
                    try:
                        cursor.execute(f"ALTER TABLE medal_sets ADD COLUMN {col_name} INTEGER NOT NULL DEFAULT 0")
                    except sqlite3.OperationalError:
                        pass
                    existing_cols.add(col_name)
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
        if commit:
            conn.commit()

        return cursor.lastrowid
    
    def insert_match(self, match_data: Dict, commit: bool = True) -> bool:
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
            if commit:
                conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting match: {e}")
            return False

    def get_playlist_metadata(self, playlist_asset_id: str) -> Optional[sqlite3.Row]:
        """Point lookup by asset id. Cheap indexed PK read - called inline
        from classification code, same convention as get_player_mode_summary."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM playlist_metadata WHERE playlist_asset_id = ?",
            (playlist_asset_id,)
        )
        return cursor.fetchone()

    def upsert_playlist_metadata(self, playlist_asset_id: str, public_name: Optional[str],
                                  is_ranked: bool, resolution_status: str,
                                  version_id: Optional[str] = None, commit: bool = True) -> None:
        """No FK children reference playlist_metadata, so INSERT OR REPLACE is safe here."""
        conn = self._get_connection()
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        cursor.execute("""
            INSERT OR REPLACE INTO playlist_metadata
                (playlist_asset_id, public_name, is_ranked, resolution_status, last_checked_at, last_version_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (playlist_asset_id, public_name, 1 if is_ranked else 0, resolution_status, now, version_id))
        if commit:
            conn.commit()

    def insert_or_update_player(self, xuid: str, gamertag: str = None,
                                 last_processed_at: str = None, commit: bool = True,
                                 incomplete_data: Optional[bool] = None,
                                 failed_match_count: Optional[int] = None) -> bool:
        """Insert or update player information.

        incomplete_data/failed_match_count reflect whether the most recent
        fetch that populated this player had any failed match lookups -
        persisted (not just computed per-request and discarded) so the
        history-sync completeness checks can act on it on the next request."""
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
                if incomplete_data is not None:
                    updates.append("incomplete_data = ?")
                    values.append(1 if incomplete_data else 0)
                if failed_match_count is not None:
                    updates.append("failed_match_count = ?")
                    values.append(failed_match_count)

                if updates:
                    values.append(xuid)
                    cursor.execute(f"""
                        UPDATE players SET {', '.join(updates)} WHERE xuid = ?
                    """, values)
            else:
                # Insert new player
                cursor.execute("""
                    INSERT INTO players (xuid, gamertag, last_processed_at, date_added, incomplete_data, failed_match_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    xuid, gamertag, last_processed_at, datetime.now().isoformat(),
                    1 if incomplete_data else 0, failed_match_count or 0
                ))

            if commit:
                conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting/updating player: {e}")
            return False
    
    @staticmethod
    def _outcome_bucket(outcome: int) -> Optional[str]:
        """Map a player_match outcome code to its player_mode_stats column."""
        return {1: 'draws', 2: 'wins', 3: 'losses', 4: 'dnf'}.get(outcome)

    def _apply_player_mode_stats_delta(self, cursor: sqlite3.Cursor, xuid: str,
                                        match_id: str, match_data: Dict) -> None:
        """Update the precomputed player_mode_stats row(s) for xuid to reflect
        this match insert/replace, using a signed delta so reprocessing an
        already-seen match (INSERT OR REPLACE) doesn't double-count."""
        cursor.execute(
            "SELECT kills, deaths, assists, outcome FROM player_match WHERE xuid = ? AND match_id = ?",
            (xuid, match_id)
        )
        old_row = cursor.fetchone()

        new_kills = match_data.get('kills', 0)
        new_deaths = match_data.get('deaths', 0)
        new_assists = match_data.get('assists', 0)
        new_outcome = match_data.get('outcome', 0)

        kills_delta = new_kills - (old_row['kills'] if old_row else 0)
        deaths_delta = new_deaths - (old_row['deaths'] if old_row else 0)
        assists_delta = new_assists - (old_row['assists'] if old_row else 0)
        games_delta = 0 if old_row else 1

        old_bucket = self._outcome_bucket(old_row['outcome']) if old_row else None
        new_bucket = self._outcome_bucket(new_outcome)
        bucket_deltas = {'wins': 0, 'losses': 0, 'draws': 0, 'dnf': 0}
        if old_bucket:
            bucket_deltas[old_bucket] -= 1
        if new_bucket:
            bucket_deltas[new_bucket] += 1

        # Buckets mirror HaloClient._calculate_stats_from_matches' stat_type
        # filter: is_ranked picks 'ranked' vs 'social', but
        # match_category='custom' (private/forge/local lobbies) is excluded
        # from every bucket, including 'overall' - customs stay in
        # matches/player_match (never deleted) but never contribute to
        # precomputed aggregates. 'unknown' (not yet classified) still
        # counts, same as before. Ranked matches additionally land in exactly
        # one of 'core_ranked' (core playlists incl. launch-era Ranked Arena) or
        # 'rotational_ranked' (every other CSR playlist), so
        # ranked == core_ranked + rotational_ranked always holds.
        game_mode = 'ranked' if match_data.get('is_ranked') else 'social'
        is_custom = match_data.get('match_category') == 'custom'
        now = datetime.now().isoformat()

        modes = set() if is_custom else {game_mode, 'overall'}
        if not is_custom and match_data.get('is_ranked'):
            playlist_id = (match_data.get('playlist_id') or '').strip().lower()
            modes.add('core_ranked' if playlist_id in CORE_RANKED_PLAYLIST_IDS
                      else 'rotational_ranked')
        for mode in modes:
            cursor.execute(
                "INSERT OR IGNORE INTO player_mode_stats (xuid, game_mode) VALUES (?, ?)",
                (xuid, mode)
            )
            cursor.execute("""
                UPDATE player_mode_stats
                SET games_played = games_played + ?,
                    total_kills = total_kills + ?,
                    total_deaths = total_deaths + ?,
                    total_assists = total_assists + ?,
                    wins = wins + ?,
                    losses = losses + ?,
                    draws = draws + ?,
                    dnf = dnf + ?,
                    last_updated = ?
                WHERE xuid = ? AND game_mode = ?
            """, (
                games_delta, kills_delta, deaths_delta, assists_delta,
                bucket_deltas['wins'], bucket_deltas['losses'],
                bucket_deltas['draws'], bucket_deltas['dnf'],
                now, xuid, mode
            ))

    def _medal_counts_from_set_id(self, cursor: sqlite3.Cursor, medal_set_id: Optional[int]) -> Dict[int, int]:
        """Return {medal_name_id: count} for a medal_sets row, or {} if unset."""
        if not medal_set_id:
            return {}
        cursor.execute("SELECT * FROM medal_sets WHERE medal_set_id = ?", (medal_set_id,))
        row = cursor.fetchone()
        if not row:
            return {}
        counts = {}
        for key in row.keys():
            if key.startswith('medal_') and key != 'medal_set_id' and key != 'medal_hash':
                count = row[key]
                if count:
                    counts[int(key.replace('medal_', ''))] = count
        return counts

    def _apply_player_medal_totals_delta(self, cursor: sqlite3.Cursor, xuid: str,
                                          match_id: str, match_data: Dict,
                                          new_medal_set_id: Optional[int]) -> None:
        """Update the precomputed player_medal_totals rows for xuid to reflect
        this match insert/replace, using a signed per-medal delta so
        reprocessing an already-seen match doesn't double-count (mirrors
        _apply_player_mode_stats_delta above)."""
        cursor.execute(
            "SELECT medal_set_id FROM player_match WHERE xuid = ? AND match_id = ?",
            (xuid, match_id)
        )
        old_row = cursor.fetchone()
        old_medal_set_id = old_row['medal_set_id'] if old_row else None

        if old_medal_set_id == new_medal_set_id:
            return

        old_counts = self._medal_counts_from_set_id(cursor, old_medal_set_id)
        new_counts = self._medal_counts_from_set_id(cursor, new_medal_set_id)
        medal_ids = set(old_counts) | set(new_counts)
        if not medal_ids:
            return

        game_mode = 'ranked' if match_data.get('is_ranked') else 'social'
        is_custom = match_data.get('match_category') == 'custom'
        now = datetime.now().isoformat()

        modes = set() if is_custom else {game_mode, 'overall'}
        if not is_custom and match_data.get('is_ranked'):
            playlist_id = (match_data.get('playlist_id') or '').strip().lower()
            modes.add('core_ranked' if playlist_id in CORE_RANKED_PLAYLIST_IDS
                      else 'rotational_ranked')
        for mode in modes:
            for medal_id in medal_ids:
                delta = new_counts.get(medal_id, 0) - old_counts.get(medal_id, 0)
                if delta == 0:
                    continue
                cursor.execute(
                    "INSERT OR IGNORE INTO player_medal_totals (xuid, game_mode, medal_name_id, count) "
                    "VALUES (?, ?, ?, 0)",
                    (xuid, mode, medal_id)
                )
                cursor.execute("""
                    UPDATE player_medal_totals
                    SET count = count + ?, last_updated = ?
                    WHERE xuid = ? AND game_mode = ? AND medal_name_id = ?
                """, (delta, now, xuid, mode, medal_id))

    def insert_player_match(self, xuid: str, match_data: Dict, commit: bool = True) -> bool:
        """Insert player's performance for a specific match"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Get or create medal set
            medals = match_data.get('medals', [])
            medal_set_id = self.get_or_create_medal_set(medals, commit=commit) if medals else None

            match_id = match_data.get('match_id')
            self._apply_player_mode_stats_delta(cursor, xuid, match_id, match_data)
            self._apply_player_medal_totals_delta(cursor, xuid, match_id, match_data, medal_set_id)

            cursor.execute("""
                INSERT OR REPLACE INTO player_match
                (xuid, match_id, kills, deaths, assists, outcome, medal_set_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                xuid,
                match_id,
                match_data.get('kills', 0),
                match_data.get('deaths', 0),
                match_data.get('assists', 0),
                match_data.get('outcome', 0),
                medal_set_id
            ))
            if commit:
                conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting player_match: {e}")
            return False

    def insert_match_participants(self, match_id: str, participants: List[Dict], commit: bool = True) -> bool:
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

            if commit:
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

    def get_pair_match_category_counts(self, scope_xuids: List[str]) -> Dict[Tuple[str, str], Dict[str, int]]:
        """Get per-pair match-category counts for players inside a supplied scope set."""
        normalized_scope: List[str] = []
        seen_scope = set()
        for raw_xuid in scope_xuids or []:
            normalized_xuid = str(raw_xuid or "").strip()
            if not normalized_xuid or normalized_xuid in seen_scope:
                continue
            seen_scope.add(normalized_xuid)
            normalized_scope.append(normalized_xuid)

        if len(normalized_scope) < 2:
            return {}

        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS temp_pair_scope_xuids (
                xuid TEXT PRIMARY KEY
            )
            """
        )
        cursor.execute("DELETE FROM temp_pair_scope_xuids")

        try:
            ranked_playlist_ids = (
                "6e4e9372-5d49-4f87-b0a7-4489b5e96a0b",
                "edfef3ac-9cbe-4fa2-b949-8f29deafd483",
            )
            ranked_playlist_sql = ", ".join(f"'{playlist_id}'" for playlist_id in ranked_playlist_ids)

            ranked_expr = (
                "(raw_match_category = 'ranked' "
                f"OR (raw_match_category = 'unknown' AND playlist_id_lower IN ({ranked_playlist_sql})))"
            )
            custom_expr = (
                "(raw_match_category = 'custom' "
                "OR (raw_match_category = 'unknown' AND "
                "(playlist_id_lower = '' OR playlist_id_lower LIKE '%custom%')))"
            )
            social_expr = (
                "(raw_match_category = 'social' "
                "OR (raw_match_category = 'unknown' AND playlist_id_lower <> '' "
                f"AND playlist_id_lower NOT IN ({ranked_playlist_sql}) "
                "AND playlist_id_lower NOT LIKE '%custom%'))"
            )

            cursor.executemany(
                "INSERT OR IGNORE INTO temp_pair_scope_xuids (xuid) VALUES (?)",
                [(xuid,) for xuid in normalized_scope],
            )

            cursor.execute(
                f"""
                WITH scope_matches AS (
                    SELECT DISTINCT mp.match_id
                    FROM match_participants mp
                    JOIN temp_pair_scope_xuids scope ON scope.xuid = mp.xuid
                ),
                scoped_participants AS (
                    SELECT DISTINCT mp.match_id, mp.xuid
                    FROM match_participants mp
                    JOIN scope_matches sm ON sm.match_id = mp.match_id
                    JOIN temp_pair_scope_xuids scope ON scope.xuid = mp.xuid
                ),
                pair_rows AS (
                    SELECT
                        CASE WHEN sp1.xuid < sp2.xuid THEN sp1.xuid ELSE sp2.xuid END AS src_xuid,
                        CASE WHEN sp1.xuid < sp2.xuid THEN sp2.xuid ELSE sp1.xuid END AS dst_xuid,
                        LOWER(COALESCE(m.match_category, 'unknown')) AS raw_match_category,
                        LOWER(COALESCE(m.playlist_id, '')) AS playlist_id_lower
                    FROM scoped_participants sp1
                    JOIN scoped_participants sp2
                      ON sp1.match_id = sp2.match_id
                     AND sp1.xuid < sp2.xuid
                    LEFT JOIN matches m ON m.match_id = sp1.match_id
                )
                SELECT
                    src_xuid,
                    dst_xuid,
                    SUM(CASE WHEN {ranked_expr} THEN 1 ELSE 0 END) AS ranked_count,
                    SUM(CASE WHEN {social_expr} THEN 1 ELSE 0 END) AS social_count,
                    SUM(CASE WHEN {custom_expr} THEN 1 ELSE 0 END) AS custom_count,
                    SUM(CASE WHEN NOT ({ranked_expr} OR {social_expr} OR {custom_expr}) THEN 1 ELSE 0 END) AS unknown_count
                FROM pair_rows
                GROUP BY src_xuid, dst_xuid
                """
            )

            pair_counts: Dict[Tuple[str, str], Dict[str, int]] = {}
            for row in cursor.fetchall():
                src_xuid = str(row['src_xuid'] or '').strip()
                dst_xuid = str(row['dst_xuid'] or '').strip()
                if not src_xuid or not dst_xuid or src_xuid == dst_xuid:
                    continue
                pair_counts[(src_xuid, dst_xuid)] = {
                    'ranked': int(row['ranked_count'] or 0),
                    'social': int(row['social_count'] or 0),
                    'custom': int(row['custom_count'] or 0),
                    'unknown': int(row['unknown_count'] or 0),
                }

            return pair_counts
        finally:
            cursor.execute("DELETE FROM temp_pair_scope_xuids")

    def get_seed_match_participants(self, seed_xuid: str, limit_matches: Optional[int] = None) -> Dict[str, List[Dict]]:
        """Get full match rosters for matches where the seed player participated."""
        normalized_seed = str(seed_xuid or '').strip()
        if not normalized_seed:
            return {}

        conn = self._get_connection()
        cursor = conn.cursor()

        limit_clause = ""
        params: List[object] = [normalized_seed]
        if limit_matches is not None and int(limit_matches) > 0:
            limit_clause = "LIMIT ?"
            params.append(int(limit_matches))

        cursor.execute(
            f"""
            WITH seed_matches AS (
                SELECT m.match_id
                FROM matches m
                JOIN match_participants smp ON smp.match_id = m.match_id
                WHERE smp.xuid = ?
                ORDER BY COALESCE(m.start_time, '') DESC, m.match_id ASC
                {limit_clause}
            )
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
            JOIN seed_matches sm ON sm.match_id = mp.match_id
            LEFT JOIN players p ON p.xuid = mp.xuid
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

    def get_seed_verified_match_ids(self, seed_xuid: str, limit_matches: Optional[int] = None) -> List[str]:
        """Get match IDs from verified seed history in player_match/matches."""
        normalized_seed = str(seed_xuid or '').strip()
        if not normalized_seed:
            return []

        conn = self._get_connection()
        cursor = conn.cursor()

        limit_clause = ""
        params: List[object] = [normalized_seed]
        if limit_matches is not None and int(limit_matches) > 0:
            limit_clause = "LIMIT ?"
            params.append(int(limit_matches))

        cursor.execute(
            f"""
            SELECT m.match_id
            FROM player_match pm
            JOIN matches m ON m.match_id = pm.match_id
            WHERE pm.xuid = ?
            ORDER BY COALESCE(m.start_time, '') DESC, m.match_id ASC
            {limit_clause}
            """,
            params,
        )

        return [str(row['match_id']) for row in cursor.fetchall() if row['match_id']]

    def get_participant_coverage_for_matches(self, match_ids: List[str], seed_xuid: str) -> Dict[str, Dict[str, object]]:
        """Get participant count and seed presence for each supplied match ID."""
        normalized_seed = str(seed_xuid or '').strip()
        if not normalized_seed:
            return {}

        normalized_match_ids: List[str] = []
        seen_match_ids = set()
        for match_id in match_ids or []:
            normalized_match_id = str(match_id or '').strip()
            if not normalized_match_id or normalized_match_id in seen_match_ids:
                continue
            seen_match_ids.add(normalized_match_id)
            normalized_match_ids.append(normalized_match_id)

        if not normalized_match_ids:
            return {}

        conn = self._get_connection()
        cursor = conn.cursor()

        # Use a temp table instead of a massive IN(...) list to avoid SQL variable limits.
        cursor.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS temp_match_scope (
                match_id TEXT PRIMARY KEY
            )
            """
        )
        cursor.execute("DELETE FROM temp_match_scope")

        try:
            cursor.executemany(
                "INSERT OR IGNORE INTO temp_match_scope (match_id) VALUES (?)",
                [(match_id,) for match_id in normalized_match_ids],
            )

            cursor.execute(
                """
                SELECT
                    scope.match_id,
                    COUNT(mp.xuid) AS participant_count,
                    MAX(CASE WHEN mp.xuid = ? THEN 1 ELSE 0 END) AS seed_present
                FROM temp_match_scope scope
                LEFT JOIN match_participants mp ON mp.match_id = scope.match_id
                GROUP BY scope.match_id
                ORDER BY scope.match_id
                """,
                (normalized_seed,),
            )

            coverage: Dict[str, Dict[str, object]] = {}
            for row in cursor.fetchall():
                match_id = str(row['match_id'])
                coverage[match_id] = {
                    'participant_count': int(row['participant_count'] or 0),
                    'seed_present': bool(row['seed_present'] or 0),
                }
            return coverage
        finally:
            cursor.execute("DELETE FROM temp_match_scope")

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
    
    def _sum_medal_columns_by_id(self, xuid: str, stat_type: str = "overall") -> Dict[int, int]:
        """On-demand SUM over medal_sets, keyed by medal_id. Shared by
        get_player_medal_totals (name-keyed) and get_player_medal_totals_by_id."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Custom/private matches never count toward "social" or "overall",
        # same as HaloClient._calculate_stats_from_matches / the
        # player_mode_stats and player_medal_totals backfills.
        filter_params: tuple = ()
        if stat_type == "ranked":
            ranked_filter = "AND m.is_ranked = 1"
        elif stat_type == "social":
            ranked_filter = "AND m.is_ranked = 0 AND m.match_category != 'custom'"
        elif stat_type in ("core_ranked", "rotational_ranked"):
            # Mirrors _mode_where_clause in player_mode_stats_backfill.py:
            # COALESCE matters so NULL-playlist ranked rows route to
            # rotational rather than dropping out of both sub-buckets.
            core_placeholders = ",".join("?" for _ in CORE_RANKED_PLAYLIST_IDS)
            in_clause = "IN" if stat_type == "core_ranked" else "NOT IN"
            ranked_filter = (
                f"AND m.is_ranked = 1 AND LOWER(COALESCE(m.playlist_id, '')) "
                f"{in_clause} ({core_placeholders})"
            )
            filter_params = tuple(sorted(CORE_RANKED_PLAYLIST_IDS))
        else:
            ranked_filter = "AND m.match_category != 'custom'"

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
        """, (xuid, *filter_params))

        row = cursor.fetchone()
        if not row:
            return {}

        result = {}
        for col in columns:
            medal_id = int(col.replace('medal_', ''))
            count = row[col] or 0
            if count > 0:
                result[medal_id] = count

        return result

    def get_player_medal_totals(self, xuid: str, stat_type: str = "overall") -> Dict[str, int]:
        """Get total medals earned by player, keyed by medal name"""
        return {
            MEDAL_NAME_MAPPING.get(medal_id, f"Unknown ({medal_id})"): count
            for medal_id, count in self._sum_medal_columns_by_id(xuid, stat_type).items()
        }

    def get_player_medal_totals_by_id(self, xuid: str, stat_type: str = "overall") -> Dict[int, int]:
        """Get total medals earned by player, keyed by medal_id (for icon lookup)"""
        return self._sum_medal_columns_by_id(xuid, stat_type)
    
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
