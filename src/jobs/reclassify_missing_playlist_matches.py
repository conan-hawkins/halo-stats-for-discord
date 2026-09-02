"""
One-time backfill: re-fetch and reclassify matches stuck with the legacy
category_source='missing_playlist' value (written by an older, simpler
version of _classify_match_category, before it had the ranked-name text
heuristic and other signals). That exact category_source string is never
written by current code, so any row carrying it has never been reprocessed
since.

insert_match() is NOT reusable for this: its UPDATE only refreshes
match_category/category_source via COALESCE and never touches is_ranked or
playlist_id on an existing row (by design - a match's ranked status normally
never changes after the fact). This script performs its own explicit UPDATE
when reclassification changes the outcome.

After running this, re-run player_mode_stats_backfill.py and
player_medal_totals_backfill.py - the precomputed per-mode/medal totals for
affected players were built from the OLD is_ranked value at ingestion time
and won't self-correct just because matches.is_ranked changed.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import List, Optional

import aiohttp

from src.api.client import HaloAPIClient
from src.config import TOKEN_CACHE_FILE
from src.database.cache import get_cache, PlayerStatsCacheV2

LEGACY_CATEGORY_SOURCE = "missing_playlist"
CONCURRENCY = 5


@dataclass
class ReclassifyResult:
    total_matches: int = 0
    reclassified_to_ranked: int = 0
    reclassified_other: int = 0
    unchanged: int = 0
    fetch_failed: int = 0
    failed_match_ids: List[str] = field(default_factory=list)


def _load_spartan_token() -> str:
    with open(TOKEN_CACHE_FILE) as f:
        cache = json.load(f)
    return cache["spartan"]["token"]


def _get_matches_to_reclassify(db_path: Optional[str] = None):
    cache = PlayerStatsCacheV2(db_path) if db_path else get_cache()
    conn = cache.db._get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT m.match_id, MIN(pm.xuid) as xuid, m.is_ranked, m.match_category, m.category_source
        FROM matches m
        JOIN player_match pm ON pm.match_id = m.match_id
        WHERE m.category_source = ?
        GROUP BY m.match_id
    """, (LEGACY_CATEGORY_SOURCE,))
    return cache, cursor.fetchall()


async def reclassify_missing_playlist_matches(db_path: Optional[str] = None) -> ReclassifyResult:
    cache, rows = _get_matches_to_reclassify(db_path)
    conn = cache.db._get_connection()
    result = ReclassifyResult(total_matches=len(rows))

    if not rows:
        return result

    client = HaloAPIClient()
    client.spartan_token = _load_spartan_token()

    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def process_one(session: aiohttp.ClientSession, row) -> None:
        match_id = row["match_id"]
        xuid = row["xuid"]
        async with semaphore:
            try:
                fresh = await client.get_match_stats_for_match(match_id, xuid, session)
            except Exception as e:
                print(f"[RECLASSIFY] Error fetching {match_id}: {e}")
                fresh = None

        if not fresh:
            result.fetch_failed += 1
            result.failed_match_ids.append(match_id)
            return

        new_is_ranked = 1 if fresh.get("is_ranked") else 0
        new_category = fresh.get("match_category") or "unknown"
        new_source = fresh.get("category_source")

        if (new_is_ranked == row["is_ranked"]
                and new_category == row["match_category"]
                and new_source == row["category_source"]):
            result.unchanged += 1
            return

        conn.execute("""
            UPDATE matches
            SET is_ranked = ?, match_category = ?, category_source = ?,
                playlist_id = COALESCE(?, playlist_id),
                map_id = COALESCE(?, map_id),
                map_version = COALESCE(?, map_version)
            WHERE match_id = ?
        """, (
            new_is_ranked, new_category, new_source,
            fresh.get("playlist_id"),
            fresh.get("map_id"), fresh.get("map_version"),
            match_id,
        ))

        if new_is_ranked:
            result.reclassified_to_ranked += 1
        else:
            result.reclassified_other += 1

    connector = aiohttp.TCPConnector(limit=CONCURRENCY * 2)
    timeout = aiohttp.ClientTimeout(total=60, connect=15, sock_connect=15, sock_read=45)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [asyncio.create_task(process_one(session, row)) for row in rows]
        done_count = 0
        for task in asyncio.as_completed(tasks):
            await task
            done_count += 1
            if done_count % 250 == 0:
                print(f"[RECLASSIFY] Progress: {done_count}/{len(rows)}")

    conn.commit()
    return result


if __name__ == "__main__":
    outcome = asyncio.run(reclassify_missing_playlist_matches())
    print(
        f"Reclassified {outcome.total_matches} legacy 'missing_playlist' matches: "
        f"{outcome.reclassified_to_ranked} -> ranked, "
        f"{outcome.reclassified_other} -> other category, "
        f"{outcome.unchanged} unchanged, "
        f"{outcome.fetch_failed} failed to fetch"
    )
    if outcome.failed_match_ids:
        print(f"Failed match IDs (first 20): {outcome.failed_match_ids[:20]}")
