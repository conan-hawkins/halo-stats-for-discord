"""
One-time backfill for the player_medal_totals summary table from existing
player_match/medal_sets history. Safe to re-run: each mode's rows are fully
recomputed and written with INSERT OR REPLACE, not incremented.

Run all modes:    python -m src.database.player_medal_totals_backfill
Specific modes:   python -m src.database.player_medal_totals_backfill core_ranked rotational_ranked
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

from src.database.cache import get_cache, PlayerStatsCacheV2
from src.database.player_mode_stats_backfill import ALL_MODES, _mode_where_clause


@dataclass
class BackfillResult:
    modes_processed: int = 0
    rows_written: int = 0


def backfill_player_medal_totals(db_path: Optional[str] = None,
                                  modes: Optional[Sequence[str]] = None) -> BackfillResult:
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
    # filter (and player_mode_stats_backfill's _mode_where_clause, reused
    # here so the two summary tables can never drift out of sync again):
    # is_ranked picks 'ranked' vs 'social', match_category='custom' (private/
    # forge/local lobbies) is excluded from every bucket but 'ranked', and
    # ranked additionally splits into 'core_ranked'/'rotational_ranked' by
    # playlist_id against CORE_RANKED_PLAYLIST_IDS.
    selected_modes = tuple(modes) if modes else ALL_MODES
    unknown = set(selected_modes) - set(ALL_MODES)
    if unknown:
        raise ValueError(f"Unknown game modes: {sorted(unknown)} (valid: {ALL_MODES})")

    for mode in selected_modes:
        where_clause, where_params = _mode_where_clause(mode)

        cursor.execute(f"""
            SELECT pm.xuid, {sum_clauses}
            FROM player_match pm
            JOIN matches m ON pm.match_id = m.match_id
            JOIN medal_sets ms ON pm.medal_set_id = ms.medal_set_id
            {where_clause}
            GROUP BY pm.xuid
        """, where_params)
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
    requested_modes = sys.argv[1:] or None
    outcome = backfill_player_medal_totals(modes=requested_modes)
    print(f"Backfilled {outcome.rows_written} player_medal_totals rows across {outcome.modes_processed} modes")
