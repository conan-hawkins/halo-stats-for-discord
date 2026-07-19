"""
One-time backfill for the player_medal_totals summary table from existing
player_match/medal_sets history. Safe to re-run: each mode's rows are fully
recomputed and written with INSERT OR REPLACE, not incremented.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from src.database.cache import get_cache, PlayerStatsCacheV2


@dataclass
class BackfillResult:
    modes_processed: int = 0
    rows_written: int = 0


def backfill_player_medal_totals(db_path: Optional[str] = None) -> BackfillResult:
    """Recompute player_medal_totals for every player, game mode, and medal type."""
    cache = PlayerStatsCacheV2(db_path) if db_path else get_cache()
    db = cache.db
    conn = db._get_connection()
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(medal_sets)")
    medal_columns = [
        row["name"] for row in cursor.fetchall()
        if row["name"].startswith("medal_") and row["name"] not in ("medal_set_id", "medal_hash")
    ]

    result = BackfillResult()
    if not medal_columns:
        return result

    now = datetime.now().isoformat()
    sum_clauses = ", ".join(f"COALESCE(SUM(ms.{col}), 0) as {col}" for col in medal_columns)

    # Buckets mirror HaloClient._calculate_stats_from_matches' stat_type
    # filter: is_ranked picks 'ranked' vs 'social', but match_category='custom'
    # (private/forge/local lobbies) is excluded from 'social' and 'overall' too.
    modes = ["overall", "ranked", "social"]

    for mode in modes:
        if mode == "overall":
            where_clause = "WHERE m.match_category != 'custom'"
        elif mode == "ranked":
            where_clause = "WHERE m.is_ranked = 1"
        else:
            where_clause = "WHERE m.is_ranked = 0 AND m.match_category != 'custom'"

        cursor.execute(f"""
            SELECT pm.xuid, {sum_clauses}
            FROM player_match pm
            JOIN matches m ON pm.match_id = m.match_id
            JOIN medal_sets ms ON pm.medal_set_id = ms.medal_set_id
            {where_clause}
            GROUP BY pm.xuid
        """)
        rows = cursor.fetchall()

        params = []
        for row in rows:
            for col in medal_columns:
                count = row[col]
                if count:
                    medal_id = int(col.replace("medal_", ""))
                    params.append((row["xuid"], mode, medal_id, count, now))

        cursor.executemany("""
            INSERT OR REPLACE INTO player_medal_totals
            (xuid, game_mode, medal_name_id, count, last_updated)
            VALUES (?, ?, ?, ?, ?)
        """, params)

        result.modes_processed += 1
        result.rows_written += len(params)

    conn.commit()
    return result


if __name__ == "__main__":
    outcome = backfill_player_medal_totals()
    print(f"Backfilled {outcome.rows_written} player_medal_totals rows across {outcome.modes_processed} modes")
