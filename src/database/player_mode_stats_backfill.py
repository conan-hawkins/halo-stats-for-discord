"""
One-time backfill for the player_mode_stats summary table from existing
player_match/matches history. Safe to re-run: each mode's rows are fully
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


def backfill_player_mode_stats(db_path: Optional[str] = None) -> BackfillResult:
    """Recompute player_mode_stats for every player and game mode."""
    cache = PlayerStatsCacheV2(db_path) if db_path else get_cache()
    db = cache.db
    conn = db._get_connection()
    cursor = conn.cursor()

    # Buckets mirror HaloClient._calculate_stats_from_matches' stat_type
    # filter, which splits on is_ranked, not match_category.
    modes = ["overall", "ranked", "social"]

    result = BackfillResult()
    now = datetime.now().isoformat()

    for mode in modes:
        if mode == "overall":
            where_clause = ""
            params: tuple = ()
        elif mode == "ranked":
            where_clause = "WHERE m.is_ranked = 1"
            params = ()
        else:
            where_clause = "WHERE m.is_ranked = 0"
            params = ()

        cursor.execute(f"""
            SELECT
                pm.xuid,
                COUNT(pm.match_id) as games_played,
                COALESCE(SUM(pm.kills), 0) as total_kills,
                COALESCE(SUM(pm.deaths), 0) as total_deaths,
                COALESCE(SUM(pm.assists), 0) as total_assists,
                SUM(CASE WHEN pm.outcome = 2 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pm.outcome = 3 THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN pm.outcome = 1 THEN 1 ELSE 0 END) as draws,
                SUM(CASE WHEN pm.outcome = 4 THEN 1 ELSE 0 END) as dnf
            FROM player_match pm
            JOIN matches m ON pm.match_id = m.match_id
            {where_clause}
            GROUP BY pm.xuid
        """, params)
        rows = cursor.fetchall()

        cursor.executemany("""
            INSERT OR REPLACE INTO player_mode_stats
            (xuid, game_mode, games_played, total_kills, total_deaths, total_assists,
             wins, losses, draws, dnf, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (row["xuid"], mode, row["games_played"], row["total_kills"], row["total_deaths"],
             row["total_assists"], row["wins"], row["losses"], row["draws"], row["dnf"], now)
            for row in rows
        ])

        result.modes_processed += 1
        result.rows_written += len(rows)

    conn.commit()
    return result


if __name__ == "__main__":
    outcome = backfill_player_mode_stats()
    print(f"Backfilled {outcome.rows_written} player_mode_stats rows across {outcome.modes_processed} modes")
