#!/usr/bin/env python3
"""Populate players.gamerpic from profile.svc.

    docker exec -i halo-bot python - --limit 5000 < tools/backfill_gamerpics.py

profile.svc/users?xuids= already returns {xuid, gamertag, gamerpic} and the
client keeps only the gamertag, so this is recovering something we were always
being handed. Batched at the endpoint's measured ceiling of exactly 100 ids
(101 returns 400).

WHY A SEPARATE TOOL rather than a column filled on the read path: the API
service reads the database and holds no Halo credentials, so it cannot resolve
a gamerpic itself. The bot has to write them down.

Only players missing a gamerpic are fetched, so re-running is cheap and this
doubles as the incremental top-up for newly-seen players.
"""
import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import aiohttp

from src.api.client import HaloAPIClient
from src.api.rate_limiters import halo_stats_rate_limiter

BATCH = HaloAPIClient.PROFILE_BATCH_MAX          # exactly 100; 101 -> 400


async def fetch_batch(client: HaloAPIClient, session: aiohttp.ClientSession,
                      xuids: List[str]) -> Dict[str, str]:
    token = client.get_next_spartan_token()
    if isinstance(token, dict):
        token = token.get("token")
    url = f"{client.PROFILE_BATCH_URL}?" + "&".join(f"xuids={x}" for x in xuids)
    headers = {"x-343-authorization-spartan": token, "User-Agent": client.USER_AGENT,
               "Accept": "application/json"}
    async with session.get(url, headers=headers) as r:
        if r.status != 200:
            # One nonexistent id 400s the WHOLE batch rather than being omitted,
            # so a failure here is not evidence about any particular player.
            print(f"  batch of {len(xuids)} -> HTTP {r.status}")
            return {}
        out = {}
        for e in json.loads(await r.text()) or []:
            pic = (e.get("gamerpic") or {})
            # 'medium' is 208px - enough for a retina avatar without being the
            # 424px 'large' for every row of a leaderboard.
            url_ = pic.get("medium") or pic.get("small")
            if e.get("xuid") and url_:
                out[str(e["xuid"])] = url_
        return out


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path")
    ap.add_argument("--limit", type=int, default=2000,
                    help="how many players to resolve this run")
    ap.add_argument("--refresh-all", action="store_true",
                    help="also re-resolve players that already have one")
    args = ap.parse_args()

    import sqlite3
    from src.config import DATA_DIR
    db_path = args.db_path or str(Path(DATA_DIR) / "halo_stats_v2.db")

    client = HaloAPIClient()
    if not await client.ensure_valid_tokens():
        print("no valid tokens")
        return 2
    halo_stats_rate_limiter.set_num_accounts(len(client.spartan_accounts or [1]))

    db = sqlite3.connect(db_path, timeout=60)
    db.execute("PRAGMA busy_timeout=30000")
    where = "" if args.refresh_all else "WHERE gamerpic IS NULL"
    todo = [r[0] for r in db.execute(
        f"SELECT xuid FROM players {where} ORDER BY last_processed_at DESC NULLS LAST "
        f"LIMIT {int(args.limit)}")]
    print(f"[PIC] {len(todo):,} players to resolve (batch {BATCH})")

    now = datetime.now().isoformat()
    done = 0
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(todo), BATCH):
            chunk = todo[i:i + BATCH]
            got = await fetch_batch(client, session, chunk)
            if got:
                with db:
                    db.executemany(
                        "UPDATE players SET gamerpic=?, gamerpic_updated_at=? WHERE xuid=?",
                        [(u, now, x) for x, u in got.items()])
                done += len(got)
            if (i // BATCH) % 10 == 0 or i + BATCH >= len(todo):
                print(f"[PIC] {min(i + BATCH, len(todo)):,}/{len(todo):,} resolved={done:,}")
            await asyncio.sleep(0.2)

    have = db.execute("SELECT COUNT(*) FROM players WHERE gamerpic IS NOT NULL").fetchone()[0]
    total = db.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    print(f"\n[PIC] resolved {done:,} this run; {have:,}/{total:,} players now have one")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
