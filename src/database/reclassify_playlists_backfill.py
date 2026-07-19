"""
One-time (re-runnable) backfill: resolve every distinct matches.playlist_id
against the discovery-infiniteugc playlist-metadata endpoint, cache results
in playlist_metadata, and reclassify any matches whose playlist turns out to
be ranked. RANKED_PLAYLIST_IDS (src/api/client.py) is a static, known-stale
whitelist - playlist ids rotate across Halo Infinite seasons, and this
recovers matches on playlists that rotated in after that whitelist was last
hand-updated.

The discovery-infiniteugc endpoint requires a version id - the unversioned
URL form always 404s, even for a real, live playlist (confirmed against the
actual API). matches has no stored playlist-version column, so for each
distinct playlist_id this samples one real match_id/xuid pair already in the
DB and calls HaloAPIClient.get_match_stats_for_match for it - the same
ingest-time path every live match fetch already uses, which transparently
resolves and caches the playlist via a fresh, live VersionId as a side
effect (HaloAPIClient._lookup_or_resolve_playlist_ranked). This does not
re-derive or overwrite that sample match's own row.

Once playlist_asset_id is known ranked, every matches row referencing it is
updated in one UPDATE ... WHERE playlist_id = ? statement - not one write
per match. Never touches match_category='custom' rows - custom matches are
never reclassified into ranked.

Safe to re-run: playlist_metadata rows already resolution_status='resolved'
or 'not_found' are skipped (zero network calls) unless --retry-unresolved is
passed, and the matches UPDATE only touches rows not already
match_category IN ('ranked', 'custom').

After running this, re-run player_mode_stats_backfill.py and
player_medal_totals_backfill.py - changing matches.is_ranked/match_category
here does not retroactively correct the precomputed player_mode_stats /
player_medal_totals tables (those are only updated incrementally on insert,
via _apply_player_mode_stats_delta / _apply_player_medal_totals_delta).
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import aiohttp

from src.api.client import HaloAPIClient
from src.api.utils import is_token_valid, safe_read_json
from src.config import get_token_cache_path
from src.database.cache import get_cache, PlayerStatsCacheV2

CONCURRENCY = 5


def _load_cached_spartan_accounts() -> List[Dict]:
    """
    Load whatever already-valid Spartan tokens exist in the account 1-5
    cache files, without attempting any refresh or interactive OAuth login.

    This is an offline batch job - it must never block on a browser popup.
    If a cached token is expired, that account is simply skipped rather than
    refreshed; run `python get_auth_tokens.py` (account 1) or
    `python -m src.auth.setup_account <n>` (accounts 2-5) first if none are
    valid.
    """
    accounts = []
    for i in range(1, 6):
        cache = safe_read_json(get_token_cache_path(i), default={})
        spartan_info = cache.get("spartan") if cache else None
        if spartan_info and is_token_valid(spartan_info):
            accounts.append({
                'id': f'account{i}',
                'token': spartan_info.get("token"),
                'name': f'Account {i}',
            })
    return accounts


@dataclass
class PlaylistBackfillResult:
    playlists_checked: int = 0
    playlists_confirmed_ranked: int = 0
    playlists_confirmed_not_ranked: int = 0
    playlists_unresolved: int = 0
    matches_reclassified_to_ranked: int = 0
    unresolved_playlist_match_counts: Dict[str, int] = field(default_factory=dict)


async def backfill_playlist_reclassification(
    db_path: Optional[str] = None,
    retry_unresolved: bool = False,
) -> PlaylistBackfillResult:
    cache = PlayerStatsCacheV2(db_path) if db_path else get_cache()
    conn = cache.db._get_connection()
    cursor = conn.cursor()
    result = PlaylistBackfillResult()

    cursor.execute("""
        SELECT DISTINCT m.playlist_id AS playlist_asset_id
        FROM matches m
        LEFT JOIN playlist_metadata plm ON plm.playlist_asset_id = m.playlist_id
        WHERE m.playlist_id IS NOT NULL
          AND (plm.playlist_asset_id IS NULL
               OR (? = 1 AND plm.resolution_status != 'resolved'))
    """, (1 if retry_unresolved else 0,))
    playlist_ids: List[str] = [row["playlist_asset_id"] for row in cursor.fetchall()]
    result.playlists_checked = len(playlist_ids)

    if playlist_ids:
        # HaloAPIClient._lookup_or_resolve_playlist_ranked (invoked below via
        # get_match_stats_for_match) has its own playlist_metadata cache
        # check and will silently refuse to re-attempt anything already
        # cached 'not_found' - which is exactly every playlist this query
        # just selected as needing (re)resolution. Clear those rows first so
        # the sample-match fetch below actually re-resolves them instead of
        # short-circuiting on stale state from a prior run.
        cursor.executemany(
            "DELETE FROM playlist_metadata WHERE playlist_asset_id = ?",
            [(pid,) for pid in playlist_ids],
        )
        conn.commit()

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

        # One representative (match_id, xuid) per distinct playlist_id, used
        # to obtain a live VersionId - a fresh cursor per lookup since these
        # interleave with concurrent awaits below. Playlist *versions* rotate
        # independently of the asset id (confirmed live: an old match's
        # VersionId 404s against discovery-infiniteugc even though the
        # asset is still active/resolvable via its current version), so this
        # samples the most recent match for the playlist to maximize the
        # chance its version hasn't been retired from the service yet.
        sample_by_playlist: Dict[str, tuple] = {}
        for asset_id in playlist_ids:
            sample_cursor = conn.cursor()
            # Two separate indexed lookups (idx_matches_playlist_start, then
            # idx_player_match_match) rather than one joined query - keeps
            # the ORDER BY ... LIMIT 1 a clean index-only seek on `matches`
            # instead of risking the planner joining before sorting.
            sample_cursor.execute(
                "SELECT match_id FROM matches WHERE playlist_id = ? ORDER BY start_time DESC LIMIT 1",
                (asset_id,),
            )
            match_row = sample_cursor.fetchone()
            row = None
            if match_row:
                sample_cursor.execute(
                    "SELECT xuid FROM player_match WHERE match_id = ? LIMIT 1",
                    (match_row["match_id"],),
                )
                xuid_row = sample_cursor.fetchone()
                if xuid_row:
                    row = {"match_id": match_row["match_id"], "xuid": xuid_row["xuid"]}
            if row:
                sample_by_playlist[asset_id] = (row["match_id"], row["xuid"])

        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def resolve_one(session: aiohttp.ClientSession, asset_id: str) -> None:
            sample = sample_by_playlist.get(asset_id)
            if not sample:
                cache.db.upsert_playlist_metadata(
                    asset_id, None, False, "not_found", version_id=None, commit=False,
                )
                result.playlists_unresolved += 1
                return

            match_id, xuid = sample
            async with semaphore:
                # Resolves+caches playlist_metadata as a side effect via
                # _lookup_or_resolve_playlist_ranked; the match_data return
                # value itself is unused here.
                await client.get_match_stats_for_match(match_id, xuid, session)

            row = cache.db.get_playlist_metadata(asset_id)
            if row and row["resolution_status"] == "resolved":
                if row["is_ranked"]:
                    result.playlists_confirmed_ranked += 1
                else:
                    result.playlists_confirmed_not_ranked += 1
            else:
                result.playlists_unresolved += 1

        connector = aiohttp.TCPConnector(limit=CONCURRENCY * 2)
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_connect=10, sock_read=20)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = [asyncio.create_task(resolve_one(session, pid)) for pid in playlist_ids]
            done = 0
            for task in asyncio.as_completed(tasks):
                await task
                done += 1
                if done % 25 == 0:
                    print(f"[PLAYLIST-BACKFILL] Resolved {done}/{len(playlist_ids)} playlists")

        conn.commit()

    cursor.execute(
        "SELECT playlist_asset_id FROM playlist_metadata "
        "WHERE resolution_status = 'resolved' AND is_ranked = 1"
    )
    ranked_playlist_ids = {row["playlist_asset_id"] for row in cursor.fetchall()}
    ranked_playlist_ids |= HaloAPIClient.RANKED_PLAYLIST_IDS

    for asset_id in ranked_playlist_ids:
        cursor.execute("""
            UPDATE matches
            SET is_ranked = 1, match_category = 'ranked', category_source = 'playlist_metadata'
            WHERE playlist_id = ? AND match_category NOT IN ('ranked', 'custom')
        """, (asset_id,))
        result.matches_reclassified_to_ranked += cursor.rowcount

    cursor.execute("""
        SELECT m.playlist_id, COUNT(*) as cnt
        FROM matches m
        JOIN playlist_metadata plm ON plm.playlist_asset_id = m.playlist_id
        WHERE plm.resolution_status != 'resolved'
        GROUP BY m.playlist_id
    """)
    result.unresolved_playlist_match_counts = {row["playlist_id"]: row["cnt"] for row in cursor.fetchall()}

    conn.commit()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retry-unresolved", action="store_true",
                         help="Also re-attempt playlists previously cached as not_found/error")
    args = parser.parse_args()

    outcome = asyncio.run(backfill_playlist_reclassification(retry_unresolved=args.retry_unresolved))
    print(
        f"Checked {outcome.playlists_checked} playlists: "
        f"{outcome.playlists_confirmed_ranked} confirmed ranked, "
        f"{outcome.playlists_confirmed_not_ranked} confirmed not ranked, "
        f"{outcome.playlists_unresolved} still unresolved. "
        f"{outcome.matches_reclassified_to_ranked} matches reclassified to ranked."
    )
    if outcome.unresolved_playlist_match_counts:
        print("Unresolved playlists (asset_id: match_count) - investigate/manually classify if needed:")
        for pid, cnt in sorted(outcome.unresolved_playlist_match_counts.items(), key=lambda kv: -kv[1]):
            print(f"  {pid}: {cnt}")
    print("Now re-run:")
    print("  python -m src.database.player_mode_stats_backfill")
    print("  python -m src.database.player_medal_totals_backfill")
