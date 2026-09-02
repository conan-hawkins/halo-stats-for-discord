#!/usr/bin/env python3
"""Seed the career rank table and backfill players' current ranks.

    docker exec -i halo-bot python - --seed-only  < tools/backfill_career_ranks.py
    docker exec -i halo-bot python - --limit 5000 < tools/backfill_career_ranks.py

Two separate things:

  --seed-only   store the 272 static rank definitions (titles, XP, icon paths)
                from gamecms. Idempotent, and only needs re-running when 343
                extends the track.

  default       resolve current rank for players who have none, 32 at a time.

Ongoing freshness is NOT this tool's job. Career XP moves only when a player
plays, so the bot refreshes a rank when a page-load refresh actually brought
new matches in (see src/api/progression.py). This exists to seed the back
catalogue once and to top up players the site has never shown.

The endpoint omits players it has nothing for - plenty of xuids in the DB are
opponents who never played again - so absence is left as NULL rather than
written down as rank 0.
"""
import argparse
import asyncio
import sqlite3
import sys
from pathlib import Path

from src.api import progression
from src.api.client import HaloAPIClient
from src.api.rate_limiters import halo_stats_rate_limiter
from src.jobs.csr_backfill import _load_cached_spartan_accounts

BATCH = HaloAPIClient.CAREER_RANK_BATCH_MAX      # 32; 33+ truncates silently


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path")
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--seed-only", action="store_true")
    ap.add_argument("--refresh-all", action="store_true")
    args = ap.parse_args()

    from src.config import DATA_DIR
    db_path = args.db_path or str(Path(DATA_DIR) / "halo_stats_v2.db")

    client = HaloAPIClient()
    client.spartan_accounts = _load_cached_spartan_accounts()
    if not client.spartan_accounts:
        print("no cached Spartan tokens - start the bot once, or re-auth")
        return 2
    halo_stats_rate_limiter.set_num_accounts(len(client.spartan_accounts))

    db = sqlite3.connect(db_path, timeout=60)
    db.execute("PRAGMA busy_timeout=30000")

    defs = await client.get_career_rank_definitions()
    if defs:
        n = progression.seed_rank_definitions(db, defs)
        print(f"[CAREER] seeded {n} rank definitions")
    else:
        print("[CAREER] could not fetch rank definitions")
        if args.seed_only:
            return 1
    if args.seed_only:
        return 0

    where = "" if args.refresh_all else "WHERE career_rank_updated_at IS NULL"
    todo = [str(r[0]) for r in db.execute(
        f"SELECT xuid FROM players {where}"
        f" ORDER BY last_processed_at DESC LIMIT {int(args.limit)}")]
    print(f"[CAREER] {len(todo):,} players to resolve (batch {BATCH})")

    resolved = failed = 0
    for i in range(0, len(todo), BATCH):
        chunk = todo[i:i + BATCH]
        try:
            resolved += await progression.refresh_career_ranks(client, db, chunk)
        except Exception as e:
            # Nothing is written for a failed chunk, so those players keep a
            # NULL timestamp and the next run picks them up. Never marked done.
            failed += 1
            print(f"[CAREER] chunk at {i} FAILED, left for a later run - {e}")
        if (i // BATCH) % 20 == 0 or i + BATCH >= len(todo):
            print(f"[CAREER] {min(i + BATCH, len(todo)):,}/{len(todo):,}"
                  f"  resolved={resolved:,}  failed_chunks={failed}")
        await asyncio.sleep(0.15)

    have = db.execute(
        "SELECT COUNT(*) FROM players WHERE career_rank IS NOT NULL").fetchone()[0]
    print(f"\n[CAREER] resolved {resolved:,} this run; {have:,} players now have a rank")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
