"""
Re-runnable backfill: put a NAME on every player who appears on a match
scoreboard, so the website stops rendering "Unknown player".

match_participants stores the whole lobby by xuid, but `players` only holds
people somebody has actually looked up - so most of a scoreboard is anonymous.
Roughly 327k of the 363k distinct participant xuids have no gamertag there.

The names are written to `xuid_gamertags`, NOT to `players`. Those tables mean
different things: `players` is "somebody we track", and it is what
/api/players/search scans on every keystroke, so folding roster names into it
would flood search with profiles carrying no games and more than double the
scan. A row in xuid_gamertags is a name and nothing more.

Cheap, because HaloAPIClient.resolve_xuids_batch is cache-first and resolves
100 ids per request rather than one:

  - anything already in xuid_gamertag_cache.json costs NO network call at all
    (~28k of the unknowns on first run), and
  - the rest go out 100 at a time.

--limit bounds a run. Safe to re-run and safe to interrupt: each batch is
written as it completes, and ids already named are excluded by the query, so
a second run picks up exactly where the first stopped.

Ids Xbox itself will not resolve - deleted or banned accounts - simply stay
absent. That is an answer, not a failure, and the website must render it as
such rather than inventing a name.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import List, Optional

from src.api.client import HaloAPIClient
# Shared rather than re-copied - see the same import in resolve_maps_backfill.
from src.jobs.reclassify_playlists_backfill import _load_cached_spartan_accounts
from src.database.cache import get_cache, PlayerStatsCacheV2

# How many are handed to resolve_xuids_batch at once.
#
# Sized against DISK, not the network. That call already chunks to
# PROFILE_BATCH_MAX (100) per request, so the batch size does not change how
# many requests go out - but it is also cache-THROUGH, and one call rewrites
# both of the bot's JSON caches in full, whatever it learned:
# xuid_gamertag_cache.json (~31MB) and its history sidecar (~133MB), each
# re-read, re-serialised and fsynced. That is ~359MB of I/O per call.
#
# At 500 this cost ~360MB per 500 names - about 130GB to name the remaining
# roster. At 10000 the same work costs ~7GB. The requests are identical either
# way; only the number of full-file rewrites changes.
#
# The batch is also the DB commit granularity, so an interrupted run re-does at
# most this many - and re-doing them is nearly free, because the names are
# already in the JSON cache by then and resolve_xuids_batch is cache-first.
BATCH = 10000
DEFAULT_LIMIT = 5000


@dataclass
class ParticipantBackfillResult:
    unnamed_before: int = 0
    attempted: int = 0
    resolved: int = 0
    unresolvable: int = 0


def _unnamed_participants(conn, limit: int) -> List[str]:
    """Participant xuids with no name in either table.

    Ordered by how many scoreboards they appear on, so a bounded run buys the
    most visible names first - the same "most-played first" reasoning as the
    map backfill.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT mp.xuid, COUNT(*) AS appearances
        FROM match_participants mp
        LEFT JOIN players p ON p.xuid = mp.xuid
        LEFT JOIN xuid_gamertags xg ON xg.xuid = mp.xuid
        WHERE (p.xuid IS NULL OR p.gamertag IS NULL)
          AND xg.xuid IS NULL
        GROUP BY mp.xuid
        ORDER BY appearances DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [row["xuid"] for row in cursor.fetchall()]


async def backfill_participant_gamertags(
    db_path: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
) -> ParticipantBackfillResult:
    cache = PlayerStatsCacheV2(db_path) if db_path else get_cache()
    conn = cache.db._get_connection()
    result = ParticipantBackfillResult()

    targets = _unnamed_participants(conn, limit)
    result.unnamed_before = len(targets)
    if not targets:
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

    for i in range(0, len(targets), BATCH):
        chunk = targets[i : i + BATCH]
        result.attempted += len(chunk)

        mapping = await client.resolve_xuids_batch(chunk)
        # Written per batch, so an interrupted run keeps everything it learned.
        written = cache.db.upsert_xuid_gamertags(mapping)
        result.resolved += written
        result.unresolvable += len(chunk) - len(mapping)

        print(
            f"[PARTICIPANTS] {result.resolved}/{result.attempted} named "
            f"({result.unresolvable} did not resolve)"
        )

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit", type=int, default=DEFAULT_LIMIT,
        help=f"How many unnamed participants to attempt, most-seen first (default {DEFAULT_LIMIT})",
    )
    args = parser.parse_args()

    outcome = asyncio.run(backfill_participant_gamertags(limit=args.limit))
    print(
        f"Attempted {outcome.attempted} unnamed participants: "
        f"{outcome.resolved} named, {outcome.unresolvable} did not resolve."
    )
