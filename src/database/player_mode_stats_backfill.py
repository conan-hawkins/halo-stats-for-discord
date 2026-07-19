"""
One-time backfill for the player_mode_stats summary table from existing
player_match/matches history. Safe to re-run: each mode's rows are fully
recomputed and written with INSERT OR REPLACE, not incremented.

Run all modes:    python -m src.database.player_mode_stats_backfill
Specific modes:   python -m src.database.player_mode_stats_backfill core_ranked rotational_ranked
(the mode filter avoids rescanning player_match x matches for buckets that
are already correct - each mode is a full-table scan, slow on the prod HDD)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

from src.config import CORE_RANKED_PLAYLIST_IDS
from src.database.cache import get_cache, PlayerStatsCacheV2

ALL_MODES = ("overall", "ranked", "core_ranked", "rotational_ranked", "social")


@dataclass
class BackfillResult:
    modes_processed: int = 0
    rows_written: int = 0


def _mode_where_clause(mode: str) -> tuple[str, tuple]:
    """Return (WHERE clause, params) selecting the matches in a game_mode
    bucket, mirroring _apply_player_mode_stats_delta's bucketing."""
    core_placeholders = ",".join("?" for _ in CORE_RANKED_PLAYLIST_IDS)
    core_params = tuple(sorted(CORE_RANKED_PLAYLIST_IDS))

    if mode == "overall":
        return "WHERE m.match_category != 'custom'", ()
    if mode == "ranked":
        return "WHERE m.is_ranked = 1", ()
    if mode == "core_ranked":
        # COALESCE matters: LOWER(NULL) IN (...) is NULL, which would drop
        # NULL-playlist ranked rows from BOTH ranked sub-buckets and break the
        # ranked == core + rotational invariant. Coalescing to '' routes them
        # to rotational, matching the Python delta.
        return (
            f"WHERE m.is_ranked = 1 AND LOWER(COALESCE(m.playlist_id, '')) IN ({core_placeholders})",
            core_params,
        )
    if mode == "rotational_ranked":
        return (
            f"WHERE m.is_ranked = 1 AND LOWER(COALESCE(m.playlist_id, '')) NOT IN ({core_placeholders})",
            core_params,
        )
    if mode == "social":
        return "WHERE m.is_ranked = 0 AND m.match_category != 'custom'", ()
    raise ValueError(f"Unknown game mode: {mode}")


def backfill_player_mode_stats(db_path: Optional[str] = None,
                               modes: Optional[Sequence[str]] = None) -> BackfillResult:
    """Recompute player_mode_stats for every player, for the given game modes
    (default: all of them)."""
    cache = PlayerStatsCacheV2(db_path) if db_path else get_cache()
    db = cache.db
    conn = db._get_connection()
    cursor = conn.cursor()

    # Buckets mirror HaloClient._calculate_stats_from_matches' stat_type
    # filter: is_ranked picks 'ranked' vs 'social', but match_category='custom'
    # (private/forge/local lobbies) is excluded from 'social' and 'overall' too.
    # 'core_ranked'/'rotational_ranked' split 'ranked' by playlist_id against
    # CORE_RANKED_PLAYLIST_IDS.
    selected_modes = tuple(modes) if modes else ALL_MODES
    unknown = set(selected_modes) - set(ALL_MODES)
    if unknown:
        raise ValueError(f"Unknown game modes: {sorted(unknown)} (valid: {ALL_MODES})")

    result = BackfillResult()
    now = datetime.now().isoformat()

    for mode in selected_modes:
        where_clause, params = _mode_where_clause(mode)

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
    requested_modes = sys.argv[1:] or None
    outcome = backfill_player_mode_stats(modes=requested_modes)
    print(f"Backfilled {outcome.rows_written} player_mode_stats rows across {outcome.modes_processed} modes")
