"""
Re-runnable backfill: resolve distinct matches.map_id values against the
discovery-infiniteugc map endpoint, caching each map's PublicName in
map_metadata and its artwork under MAP_IMAGE_CACHE_DIR.

matches has stored a bare MapVariant asset GUID since day one and nothing
else, so no match has ever been able to say which map it was played on. The
ingest path now resolves each new asset id lazily
(HaloAPIClient._lookup_or_resolve_map); this fills in the history that was
written before that existed, so the website is not waiting on people to
replay every map before it can name them.

Ordered by match count, most-played first, because the distribution is
severely top-heavy: the 25 most-played asset ids cover roughly 80% of all
matches ever ingested, and the ~22k-long tail is Forge variants that a
handful of matches each reference. --limit therefore buys most of the value
in its first few dozen calls, and the default stops well short of the tail.

Unlike playlists, matches DOES store map_version, so this reads a real
version id straight out of the table rather than sampling a live match to
obtain one. The resolver still falls back to the unversioned URL, which is
what rescues maps whose stored version has since been retired.

Safe to re-run: asset ids already cached 'resolved' or 'not_found' are
skipped with zero network calls unless --retry-unresolved is passed. Nothing
in matches is rewritten - map names are joined at read time, so there is no
64M-row UPDATE here and no precomputed table to rebuild afterwards.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import aiohttp

from src.api.client import HaloAPIClient
# Deliberately imported rather than copied a third time: this is the same
# "use whatever cached tokens exist, never open a browser" rule every offline
# job needs, and three divergent copies of it is how one of them ends up
# quietly attempting an interactive login on a headless box.
from src.jobs.reclassify_playlists_backfill import _load_cached_spartan_accounts
from src.database.cache import get_cache, PlayerStatsCacheV2

CONCURRENCY = 5
DEFAULT_LIMIT = 500


@dataclass
class MapBackfillResult:
    maps_checked: int = 0
    maps_named: int = 0
    maps_with_artwork: int = 0
    maps_unresolved: int = 0
    matches_covered: int = 0


async def backfill_map_metadata(
    db_path: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
    retry_unresolved: bool = False,
) -> MapBackfillResult:
    cache = PlayerStatsCacheV2(db_path) if db_path else get_cache()
    conn = cache.db._get_connection()
    cursor = conn.cursor()
    result = MapBackfillResult()

    # GROUP BY over the whole matches table, served by idx_matches_map. Runs in
    # under ten seconds on a 64M-row database because it is an index-only scan;
    # the ORDER BY is over the ~22k distinct groups, not the rows.
    cursor.execute(
        """
        SELECT m.map_id AS map_asset_id, COUNT(*) AS match_count
        FROM matches m
        LEFT JOIN map_metadata mm ON mm.map_asset_id = m.map_id
        WHERE m.map_id IS NOT NULL
          AND (mm.map_asset_id IS NULL
               OR (? = 1 AND mm.resolution_status != 'resolved'))
        GROUP BY m.map_id
        ORDER BY match_count DESC
        LIMIT ?
        """,
        (1 if retry_unresolved else 0, limit),
    )
    targets: List[Tuple[str, int]] = [
        (row["map_asset_id"], row["match_count"]) for row in cursor.fetchall()
    ]
    result.maps_checked = len(targets)
    result.matches_covered = sum(count for _, count in targets)

    if not targets:
        return result

    # _lookup_or_resolve_map short-circuits on anything already cached
    # 'not_found', which is exactly what --retry-unresolved just asked to
    # re-attempt. Clear those rows so the lookups below actually go out.
    if retry_unresolved:
        cursor.executemany(
            "DELETE FROM map_metadata WHERE map_asset_id = ?",
            [(asset_id,) for asset_id, _ in targets],
        )
        conn.commit()

    # One stored version id per asset. No ORDER BY: any version is a fine first
    # attempt (the resolver falls back to the unversioned URL regardless), and
    # sorting 6M rows for the newest one on a popular map would cost far more
    # than the fallback it saves.
    versions: Dict[str, Optional[str]] = {}
    for asset_id, _ in targets:
        version_cursor = conn.cursor()
        version_cursor.execute(
            "SELECT map_version FROM matches WHERE map_id = ? AND map_version IS NOT NULL LIMIT 1",
            (asset_id,),
        )
        row = version_cursor.fetchone()
        versions[asset_id] = row["map_version"] if row else None

    client = HaloAPIClient()
    client.spartan_accounts = _load_cached_spartan_accounts()
    if not client.spartan_accounts:
        raise RuntimeError(
            "No valid cached Spartan tokens found in data/auth/token_cache*.json - "
            "run `python get_auth_tokens.py` (account 1) or "
            "`python -m src.auth.setup_account <n>` (accounts 2-5) first. "
            "This offline batch job intentionally does not attempt an interactive refresh."
        )
    await client.get_clearance_token()

    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def resolve_one(session: aiohttp.ClientSession, asset_id: str) -> None:
        async with semaphore:
            # Writes map_metadata and caches the artwork as a side effect; the
            # returned name is re-read from the row below so that a resolve
            # which found a name but no picture is still counted honestly.
            await client._lookup_or_resolve_map(asset_id, versions.get(asset_id), session)

        row = cache.db.get_map_metadata(asset_id)
        if row and row["resolution_status"] == "resolved":
            if row["public_name"]:
                result.maps_named += 1
            else:
                result.maps_unresolved += 1
            from src.api.map_images import cached_image_path

            if cached_image_path(asset_id):
                result.maps_with_artwork += 1
        else:
            result.maps_unresolved += 1

    connector = aiohttp.TCPConnector(limit=CONCURRENCY * 2)
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_connect=10, sock_read=20)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [asyncio.create_task(resolve_one(session, asset_id)) for asset_id, _ in targets]
        done = 0
        for task in asyncio.as_completed(tasks):
            await task
            done += 1
            if done % 25 == 0:
                print(f"[MAP-BACKFILL] Resolved {done}/{len(targets)} maps")

    conn.commit()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit", type=int, default=DEFAULT_LIMIT,
        help=f"How many distinct map asset ids to resolve, most-played first (default {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--retry-unresolved", action="store_true",
        help="Also re-attempt maps previously cached as not_found/error",
    )
    args = parser.parse_args()

    outcome = asyncio.run(
        backfill_map_metadata(limit=args.limit, retry_unresolved=args.retry_unresolved)
    )
    print(
        f"Checked {outcome.maps_checked} maps covering {outcome.matches_covered:,} matches: "
        f"{outcome.maps_named} named, {outcome.maps_with_artwork} with artwork cached, "
        f"{outcome.maps_unresolved} unresolved."
    )
