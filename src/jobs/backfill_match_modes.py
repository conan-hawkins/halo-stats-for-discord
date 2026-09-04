"""
Re-runnable backfill: fill matches.game_variant_category (Slayer, Oddball,
CTF...) on rows written before that column existed.

Every match ingested from now on records its own game variant category - the
ingest path was already parsing GameVariantCategory off MatchInfo as a
custom-vs-matchmade signal and discarding it. What that does NOT do is reach
backwards: the ~64M rows already in the table have NULL there forever unless
something goes and looks them up.

This does, and it does it the cheap way. The per-match stats endpoint costs
one request per match; the player match-LIST endpoint returns 25 matches per
request and its Results entries carry the whole MatchInfo block - the game
variant category and the MapVariant included. So a player's last 50 matches,
which is exactly what the website's match history shows, cost two requests
rather than fifty.

It is therefore scoped to recent history per player, not to the whole table.
Filling all 64M rows would be ~2.6M rate-limited requests for matches nobody
is looking at; filling the visible window for the players people actually
open is a few thousand. Players are visited most-recently-refreshed first,
which is the closest signal the bot holds to "someone is looking at this
profile" - the website's own auto-refresh is what updates that column.

Rows this never reaches keep a NULL category, and the website renders that as
an unknown mode rather than as a guess.

Safe to re-run and safe to interrupt: every write is COALESCE-guarded and
scoped to rows that are still NULL, players whose visible window is already
complete are skipped before any request is made, and each player commits as
it finishes.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import aiohttp

from src.api.client import HaloAPIClient
from src.api.rate_limiters import BUCKET_MATCH_LIST, halo_stats_rate_limiter
# Shared rather than re-copied - see the same import in resolve_maps_backfill.
from src.jobs.reclassify_playlists_backfill import _load_cached_spartan_accounts
from src.database.cache import get_cache, PlayerStatsCacheV2

PAGE_SIZE = 25
DEFAULT_MATCHES_PER_PLAYER = 50
DEFAULT_PLAYER_LIMIT = 200

# Retries per page, for a transient 429 or socket error. Deliberately small:
# this walks sequentially, one player and one page at a time, because the gain
# here is per-REQUEST (25 matches each) rather than per-connection, and it
# shares a global Halo rate budget with the live bot and the website's refresh
# endpoint - both of which somebody is waiting on. It yields to them rather
# than competing, and a page it cannot get is a page the next run picks up.
MAX_PAGE_ATTEMPTS = 3


@dataclass
class ModeBackfillResult:
    players_visited: int = 0
    players_skipped_complete: int = 0
    pages_fetched: int = 0
    matches_seen: int = 0
    matches_updated: int = 0
    maps_resolved: int = 0
    variants_resolved: int = 0


async def _fetch_match_page(
    client: HaloAPIClient,
    session: aiohttp.ClientSession,
    xuid: str,
    start: int,
) -> Optional[List[Dict]]:
    """One page of a player's match list, or None if it could not be fetched.

    None is deliberately not the same as an empty list: an empty list means
    "no more matches", which ends the walk, while None means "we do not know",
    which must not be read as end-of-history.
    """
    url = (
        f"https://halostats.svc.halowaypoint.com/hi/players/xuid({xuid})/matches"
        f"?start={start}&count={PAGE_SIZE}"
    )
    for attempt in range(MAX_PAGE_ATTEMPTS):
        try:
            async with halo_stats_rate_limiter.slot(bucket=BUCKET_MATCH_LIST) as account_index:
                spartan_token = client.get_next_spartan_token(account_index)
                if isinstance(spartan_token, dict) and "token" in spartan_token:
                    spartan_token = spartan_token["token"]

                headers = {
                    "Authorization": f"Spartan {spartan_token}",
                    "x-343-authorization-spartan": spartan_token,
                    "User-Agent": client.user_agent,
                    "Accept": "application/json",
                }
                if client.clearance_token and client.clearance_token != "skip":
                    headers["x-343-authorization-clearance"] = client.clearance_token

                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        halo_stats_rate_limiter.note_result(BUCKET_MATCH_LIST, rate_limited=False)
                        payload = await response.json()
                        return payload.get("Results", []) or []
                    if response.status == 429:
                        # Honour the server's own hint, same as the live crawl.
                        halo_stats_rate_limiter.note_result(BUCKET_MATCH_LIST, rate_limited=True)
                        retry_after = response.headers.get("Retry-After")
                        wait = int(retry_after) if retry_after and retry_after.isdigit() else 2
                        wait = min(wait * (2 ** attempt), 30)
                        halo_stats_rate_limiter.set_backoff(seconds=wait, account_index=account_index)
                        await asyncio.sleep(wait)
                        continue
                    if response.status in (401, 403):
                        # Token trouble is not per-page and will not fix itself
                        # by retrying; let the caller stop cleanly.
                        print(f"[MODE-BACKFILL] HTTP {response.status} for {xuid} - token rejected")
                        return None
                    print(f"[MODE-BACKFILL] HTTP {response.status} for {xuid} start={start}")
        except Exception as e:
            print(f"[MODE-BACKFILL] Error fetching {xuid} start={start}: {e}")
        await asyncio.sleep(1 + attempt)
    return None


def _select_players(conn, player_limit: int) -> List[Tuple[str, str]]:
    """Players worth visiting, most-recently-refreshed first.

    last_processed_at is bumped by every stats refresh, and the website's own
    auto-refresh drives most of those, so this orders by the best available
    proxy for "somebody is reading this profile".
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT xuid, gamertag
        FROM players
        WHERE last_processed_at IS NOT NULL
        ORDER BY last_processed_at DESC
        LIMIT ?
        """,
        (player_limit,),
    )
    return [(row["xuid"], row["gamertag"] or row["xuid"]) for row in cursor.fetchall()]


def _missing_in_window(conn, xuid: str, matches_per_player: int) -> int:
    """How many of this player's most recent matches still lack a category.

    Answered from the database before a single request goes out, so a player
    who was backfilled on a previous run costs nothing this run.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) AS missing FROM (
            SELECT m.game_variant_category AS category
            FROM player_match pm
            JOIN matches m ON m.match_id = pm.match_id
            WHERE pm.xuid = ?
            ORDER BY m.start_time DESC
            LIMIT ?
        )
        WHERE category IS NULL
        """,
        (xuid, matches_per_player),
    )
    row = cursor.fetchone()
    return row["missing"] if row else 0


async def backfill_match_modes(
    db_path: Optional[str] = None,
    player_limit: int = DEFAULT_PLAYER_LIMIT,
    matches_per_player: int = DEFAULT_MATCHES_PER_PLAYER,
    resolve_assets: bool = True,
) -> ModeBackfillResult:
    cache = PlayerStatsCacheV2(db_path) if db_path else get_cache()
    conn = cache.db._get_connection()
    result = ModeBackfillResult()

    players = _select_players(conn, player_limit)
    if not players:
        return result

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

    seen_maps: set = set()
    seen_variants: set = set()
    timeout = aiohttp.ClientTimeout(total=45, connect=10, sock_connect=10, sock_read=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for xuid, gamertag in players:
            if _missing_in_window(conn, xuid, matches_per_player) == 0:
                result.players_skipped_complete += 1
                continue

            result.players_visited += 1
            cursor = conn.cursor()
            # Assets this player's matches referenced, resolved once the row
            # updates are committed and the write lock is released.
            pending_maps: Dict[str, Optional[str]] = {}
            pending_variants: Dict[str, Optional[str]] = {}
            collected = 0
            start = 0
            while collected < matches_per_player:
                page = await _fetch_match_page(client, session, xuid, start)
                if page is None:
                    break
                result.pages_fetched += 1
                if not page:
                    break

                for entry in page:
                    match_id = entry.get("MatchId")
                    match_info = entry.get("MatchInfo")
                    if not match_id or not isinstance(match_info, dict):
                        continue
                    collected += 1
                    result.matches_seen += 1

                    # Read exactly as the ingest path reads it, so a row filled
                    # here is indistinguishable from one written live.
                    category = client._coerce_intish(match_info.get("GameVariantCategory"))
                    variant_asset_id = None
                    variant_version_id = None
                    variant_name = None
                    for variant_key in ("UgcGameVariant", "GameVariant"):
                        variant = match_info.get(variant_key)
                        if not isinstance(variant, dict):
                            continue
                        if variant_asset_id is None and variant.get("AssetId"):
                            variant_asset_id = variant.get("AssetId")
                            variant_version_id = variant.get("VersionId")
                        if variant_name is None:
                            name = str(variant.get("Name") or "").strip()
                            if name:
                                variant_name = name

                    map_asset_id = None
                    map_version_id = None
                    map_info = match_info.get("MapVariant")
                    if isinstance(map_info, dict):
                        map_asset_id = map_info.get("AssetId")
                        map_version_id = map_info.get("VersionId")

                    if category is None and variant_asset_id is None and map_asset_id is None:
                        continue

                    # COALESCE on every column and no INSERT: this job enriches
                    # rows the ingest path owns, and must never invent a match
                    # row that no stats fetch has ever validated.
                    cursor.execute(
                        """
                        UPDATE matches
                        SET
                            game_variant_id = COALESCE(game_variant_id, ?),
                            game_variant_version = COALESCE(game_variant_version, ?),
                            game_variant_category = COALESCE(game_variant_category, ?),
                            game_variant_name = COALESCE(game_variant_name, ?),
                            map_id = COALESCE(map_id, ?),
                            map_version = COALESCE(map_version, ?)
                        WHERE match_id = ?
                        """,
                        (
                            variant_asset_id, variant_version_id, category, variant_name,
                            map_asset_id, map_version_id, match_id,
                        ),
                    )
                    result.matches_updated += cursor.rowcount

                    # Assets are only NOTED here, and resolved after the commit
                    # below. Resolving inline deadlocks: this loop holds an open
                    # write transaction on matches, while the resolver writes its
                    # metadata row from a thread-pool thread - which, because the
                    # bot's connections are thread-local, is a SECOND connection
                    # to the same file. It waits on a write lock this loop is
                    # holding and dies with "database is locked".
                    if resolve_assets:
                        if map_asset_id and map_asset_id not in seen_maps:
                            seen_maps.add(map_asset_id)
                            pending_maps[map_asset_id] = map_version_id
                        if variant_asset_id and variant_asset_id not in seen_variants:
                            seen_variants.add(variant_asset_id)
                            pending_variants[variant_asset_id] = variant_version_id

                if len(page) < PAGE_SIZE:
                    break
                start += PAGE_SIZE

            # Per player, so an interrupted run keeps everything it has done -
            # and, critically, BEFORE the resolves below, which write to the
            # same database from another thread and would otherwise block on
            # the write lock this transaction holds.
            conn.commit()

            # Names are joined from the metadata tables at read time, so
            # resolving an asset once here names every match that ever used it -
            # including matches this job will never visit.
            for asset_id, version_id in pending_maps.items():
                if cache.db.get_map_metadata(asset_id) is None:
                    await client._lookup_or_resolve_map(asset_id, version_id, session)
                    result.maps_resolved += 1
            for asset_id, version_id in pending_variants.items():
                if cache.db.get_game_variant_metadata(asset_id) is None:
                    await client._lookup_or_resolve_game_variant(asset_id, version_id, session)
                    result.variants_resolved += 1
            print(
                f"[MODE-BACKFILL] {gamertag}: {result.matches_updated} matches enriched so far "
                f"({result.players_visited} players visited, {result.players_skipped_complete} already complete)"
            )

    conn.commit()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--players", type=int, default=DEFAULT_PLAYER_LIMIT,
        help=f"How many players to visit, most-recently-refreshed first (default {DEFAULT_PLAYER_LIMIT})",
    )
    parser.add_argument(
        "--matches-per-player", type=int, default=DEFAULT_MATCHES_PER_PLAYER,
        help=f"How far back to fill per player (default {DEFAULT_MATCHES_PER_PLAYER}, the site's window)",
    )
    parser.add_argument(
        "--no-assets", action="store_true",
        help="Skip resolving map and game-variant names for assets seen along the way",
    )
    args = parser.parse_args()

    outcome = asyncio.run(
        backfill_match_modes(
            player_limit=args.players,
            matches_per_player=args.matches_per_player,
            resolve_assets=not args.no_assets,
        )
    )
    print(
        f"Visited {outcome.players_visited} players "
        f"({outcome.players_skipped_complete} already complete), "
        f"{outcome.pages_fetched} pages, {outcome.matches_seen} matches seen, "
        f"{outcome.matches_updated} rows enriched, {outcome.maps_resolved} new maps and "
        f"{outcome.variants_resolved} new game variants resolved."
    )
