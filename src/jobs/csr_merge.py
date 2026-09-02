"""
Merge a csr_backfill.db into the live database.

    python -m src.jobs.csr_merge
    python -m src.jobs.csr_merge --dry-run
    python -m src.jobs.csr_merge --in ~/csr_backfill.db

**Stop the bot first.** Only the bot may write the live database, and it holds
a single write connection for the life of the process. Racing it would mean
gambling on busy_timeout for no benefit; stopping it costs a couple of minutes:

    docker compose stop bot
    docker compose exec ... (or run this on the host against the live path)
    docker compose up -d bot

Idempotent. Both target tables are keyed by their natural composite key and
written with INSERT OR REPLACE, so re-running produces the same state. Neither
table has FK children, which is what makes REPLACE safe here (see the warning
on player_medal_totals in schema.py for the case where it is not).

Rows whose xuid is absent from `players` are skipped rather than inserted:
both tables declare a foreign key to players(xuid) and the live connection runs
with PRAGMA foreign_keys=ON, so inserting an orphan would fail the whole
transaction. In practice the backfill only ever reads xuids out of the live
player_mode_stats, so this should report zero.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

TABLES = ("player_playlist_csr", "player_csr_season")

# HaloAPIClient.RANKED_PLAYLIST_IDS are recognised as ranked without a lookup,
# which means _lookup_or_resolve_playlist_ranked short-circuits them and they
# NEVER get a playlist_metadata row. That was harmless while nothing displayed
# playlist names; now it is not. edfef3ac is Ranked Arena and is the single
# most-played ranked playlist, so without this the busiest playlist on the site
# renders as a raw GUID.
HARDCODED_PLAYLIST_NAMES = {
    "6e4e9372-5d49-4f87-b0a7-4489b5e96a0b": "Ranked Arena",
    "edfef3ac-9cbe-4fa2-b949-8f29deafd483": "Ranked Arena",
}


@dataclass
class MergeResult:
    playlist_csr_rows: int = 0
    season_rows: int = 0
    orphans_skipped: int = 0
    playlist_names_seeded: int = 0
    dry_run: bool = False


def _seed_hardcoded_playlist_names(conn) -> int:
    """Give the short-circuited ranked playlists a metadata row.

    Only inserts where no row exists, so a name the resolver has since
    discovered for itself always wins over this fallback.
    """
    seeded = 0
    now = datetime.now().isoformat()
    for asset_id, name in HARDCODED_PLAYLIST_NAMES.items():
        cur = conn.execute(
            """INSERT OR IGNORE INTO playlist_metadata
                   (playlist_asset_id, public_name, is_ranked, resolution_status,
                    last_checked_at, last_version_id)
               VALUES (?, ?, 1, 'resolved', ?, NULL)""",
            (asset_id, name, now))
        seeded += cur.rowcount
    return seeded


def merge_csr(db_path: Optional[str] = None, in_path: Optional[str] = None,
              dry_run: bool = False) -> MergeResult:
    from src.config import DATA_DIR

    db_path = db_path or str(Path(DATA_DIR) / "halo_stats_v2.db")
    in_path = in_path or str(Path.home() / "csr_backfill.db")
    if not Path(in_path).exists():
        raise FileNotFoundError(f"No backfill file at {in_path}")

    result = MergeResult(dry_run=dry_run)
    conn = sqlite3.connect(db_path, timeout=120)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("ATTACH DATABASE ? AS src", (in_path,))

    src_counts = {t: conn.execute(f"SELECT COUNT(*) FROM src.{t}").fetchone()[0]
                  for t in TABLES}
    print(f"[CSR-MERGE] source: {in_path}")
    for t in TABLES:
        print(f"[CSR-MERGE]   {t}: {src_counts[t]:,} rows")

    # Orphans would abort the whole transaction under foreign_keys=ON, so count
    # them up front and exclude them rather than discovering it half way in.
    for t in TABLES:
        n = conn.execute(
            f"SELECT COUNT(*) FROM src.{t} s "
            f"WHERE NOT EXISTS (SELECT 1 FROM main.players p WHERE p.xuid = s.xuid)"
        ).fetchone()[0]
        result.orphans_skipped += n
        if n:
            print(f"[CSR-MERGE]   WARNING {n:,} {t} rows have no players row; skipping them")

    if dry_run:
        print("[CSR-MERGE] dry run - nothing written")
        conn.execute("DETACH DATABASE src")
        conn.close()
        result.playlist_csr_rows = src_counts["player_playlist_csr"]
        result.season_rows = src_counts["player_csr_season"]
        return result

    try:
        with conn:
            result.playlist_names_seeded = _seed_hardcoded_playlist_names(conn)
            cur = conn.execute("""
                INSERT OR REPLACE INTO main.player_playlist_csr
                    (xuid, playlist_asset_id, current_csr, current_tier,
                     current_sub_tier, all_time_max, last_updated)
                SELECT s.xuid, s.playlist_asset_id, s.current_csr, s.current_tier,
                       s.current_sub_tier, s.all_time_max, s.last_updated
                FROM src.player_playlist_csr s
                WHERE EXISTS (SELECT 1 FROM main.players p WHERE p.xuid = s.xuid)
            """)
            result.playlist_csr_rows = cur.rowcount
            cur = conn.execute("""
                INSERT OR REPLACE INTO main.player_csr_season
                    (xuid, playlist_asset_id, season_id, csr, tier, sub_tier,
                     season_max, last_updated)
                SELECT s.xuid, s.playlist_asset_id, s.season_id, s.csr, s.tier,
                       s.sub_tier, s.season_max, s.last_updated
                FROM src.player_csr_season s
                WHERE EXISTS (SELECT 1 FROM main.players p WHERE p.xuid = s.xuid)
            """)
            result.season_rows = cur.rowcount
    finally:
        conn.execute("DETACH DATABASE src")
        conn.close()

    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db-path", help="live halo_stats_v2.db")
    ap.add_argument("--in", dest="in_path", help="backfill file (default ~/csr_backfill.db)")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would be written and exit")
    args = ap.parse_args()

    r = merge_csr(db_path=args.db_path, in_path=args.in_path, dry_run=args.dry_run)
    print("\n" + "=" * 56)
    print(f"  player_playlist_csr : {r.playlist_csr_rows:,} rows"
          f"{' (would write)' if r.dry_run else ' written'}")
    print(f"  player_csr_season   : {r.season_rows:,} rows"
          f"{' (would write)' if r.dry_run else ' written'}")
    if r.playlist_names_seeded:
        print(f"  playlist names seeded: {r.playlist_names_seeded}")
    if r.orphans_skipped:
        print(f"  orphans skipped     : {r.orphans_skipped:,}")
    print("=" * 56)
    return 0


if __name__ == "__main__":
    sys.exit(main())
