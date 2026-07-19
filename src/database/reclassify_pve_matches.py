"""
One-time (re-runnable) backfill: reclassify existing PvE / Firefight matches
out of the PvP aggregates.

The classifier historically filed Firefight (co-op vs AI) playlists as
'social' - they carry a real matchmade playlist_id and aren't ranked or
flagged custom - so they counted in every "overall"/"social" K/D, KDA and
medal total. A single Firefight game (80-180 kills for ~0 deaths) badly
skews lifetime PvP stats.

This moves them into the existing 'custom' bucket (which every overall/social
filter already excludes) tagged category_source='pve_firefight', so they stay
in the DB and fully identifiable but stop contaminating PvP stats. Reusing
'custom' means zero query changes anywhere - the exclusion is automatic.

PvE playlists are identified by a PVE_PLAYLIST_NAME_HINTS substring match on
their resolved playlist_metadata.public_name (same signal the live ingest
path now uses via HaloAPIClient._public_name_is_pve). Pure DB, no network.

Ordering:
  1. python -m src.database.reclassify_playlists_backfill   (resolves names)
  2. python -m src.database.reclassify_pve_matches          (this script)
  3. python -m src.database.player_mode_stats_backfill      (recompute)
  4. python -m src.database.player_medal_totals_backfill    (recompute)

Steps 3-4 need no code change: they INSERT OR REPLACE a full recompute and
already exclude match_category='custom' from overall/social, and Firefight is
is_ranked=0, so it drops out of all three modes automatically. Changing
matches here does NOT retroactively fix the precomputed player_mode_stats /
player_medal_totals tables - those are only updated incrementally on insert.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from src.api.client import HaloAPIClient
from src.database.cache import get_cache, PlayerStatsCacheV2


@dataclass
class PveReclassifyResult:
    pve_playlists: int = 0
    matches_reclassified: int = 0
    affected_players: int = 0
    per_playlist_counts: Dict[str, int] = field(default_factory=dict)


def _pve_playlist_ids(cursor) -> List[str]:
    """Resolved playlist_asset_ids whose PublicName marks them PvE/Firefight."""
    cursor.execute(
        "SELECT playlist_asset_id, public_name FROM playlist_metadata "
        "WHERE resolution_status = 'resolved'"
    )
    return [
        row["playlist_asset_id"]
        for row in cursor.fetchall()
        if HaloAPIClient._public_name_is_pve(row["public_name"])
    ]


def reclassify_pve_matches(db_path: Optional[str] = None, dry_run: bool = False) -> PveReclassifyResult:
    cache = PlayerStatsCacheV2(db_path) if db_path else get_cache()
    conn = cache.db._get_connection()
    cursor = conn.cursor()
    result = PveReclassifyResult()

    playlist_ids = _pve_playlist_ids(cursor)
    result.pve_playlists = len(playlist_ids)
    if not playlist_ids:
        return result

    for asset_id in playlist_ids:
        if dry_run:
            # Count-only preview - same predicate as the UPDATE's WHERE.
            cursor.execute(
                "SELECT COUNT(*) AS cnt FROM matches "
                "WHERE playlist_id = ? AND match_category != 'custom'",
                (asset_id,),
            )
            changed = cursor.fetchone()["cnt"]
        else:
            # Per-playlist so each statement is an index-bounded seek on
            # idx_matches_playlist rather than one 1.6M-row sweep. WHERE
            # match_category != 'custom' makes re-runs idempotent (already
            # reclassified rows are skipped) and never disturbs real customs.
            cursor.execute(
                "UPDATE matches "
                "SET match_category = 'custom', category_source = 'pve_firefight', is_ranked = 0 "
                "WHERE playlist_id = ? AND match_category != 'custom'",
                (asset_id,),
            )
            changed = cursor.rowcount

        if changed:
            result.per_playlist_counts[asset_id] = changed
            result.matches_reclassified += changed

    if not dry_run:
        conn.commit()

    # Distinct players with at least one match on a PvE playlist. Keyed on
    # playlist_id (not the post-update category_source tag) so it's meaningful
    # in --dry-run and on re-runs alike.
    placeholders = ",".join("?" * len(playlist_ids))
    cursor.execute(
        f"""
        SELECT COUNT(DISTINCT pm.xuid) AS players
        FROM player_match pm
        JOIN matches m ON m.match_id = pm.match_id
        WHERE m.playlist_id IN ({placeholders})
        """,
        playlist_ids,
    )
    result.affected_players = cursor.fetchone()["players"] or 0
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report how many matches WOULD be reclassified without writing.",
    )
    args = parser.parse_args()

    outcome = reclassify_pve_matches(dry_run=args.dry_run)
    verb = "would reclassify" if args.dry_run else "reclassified"
    print(
        f"Found {outcome.pve_playlists} PvE/Firefight playlist(s); "
        f"{verb} {outcome.matches_reclassified} matches "
        f"across {outcome.affected_players} players."
    )
    if outcome.per_playlist_counts:
        print("Per playlist (asset_id: matches):")
        for pid, cnt in sorted(outcome.per_playlist_counts.items(), key=lambda kv: -kv[1]):
            print(f"  {pid}: {cnt}")
    if not args.dry_run:
        print("Now re-run:")
        print("  python -m src.database.player_mode_stats_backfill")
        print("  python -m src.database.player_medal_totals_backfill")
