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

ONE PASS NEVER FINISHES THE JOB, and that is by design rather than a fault.
Roughly half of each batch comes back 429 and is deferred, so a pass halves
what is left rather than emptying it. --until-converged chains passes until one
of them gains almost nothing, which is the only sensible stopping point: either
everything nameable is named, or the endpoint is throttling hard enough that
continuing just burns quota.

Measured over the first full convergence (2026-09-04, 455,602 participants):

    pass 1  21800 -> 11267    pass 4   3000 -> 2300
    pass 2  11267 ->  5700    pass 5   2300 ->  400
    pass 3   5700 ->  3000    pass 6    400 ->  400   <- stop

455,202 of 455,602 named, 99.9%.

Two outcomes that look alike and are not:

  - A 429 or a DNS blip means the request never got an answer. Nothing was
    learned, so those ids are simply left for the next pass.
  - An id the endpoint ANSWERED for and did not know is a deleted or banned
    account. Those go in xuid_unresolvable and are never asked about again -
    the website renders them as a dash rather than inventing a name.

Conflating the two is what stopped an earlier version converging: it counted
every id missing from a response as unresolvable, which made throttling look
permanent. Worth noting that across ~150k attempted ids in that first run, the
dead count was ZERO - every single loss was throttling. xuid_unresolvable is
still the right guard, but it protects against a rarer case than expected.
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

# --until-converged bounds. Eight passes is well past the six the first full
# convergence needed, and 200 is below the smallest useful pass observed (700).
DEFAULT_MAX_PASSES = 8
DEFAULT_MIN_GAIN = 200


@dataclass
class ParticipantBackfillResult:
    unnamed_before: int = 0
    attempted: int = 0
    resolved: int = 0
    # Answered for and unknown: deleted or banned. Recorded, never retried.
    dead: int = 0
    # Not answered for at all - a 429, a DNS blip, a dropped connection. Left
    # for the next run, because nothing was learned about them.
    skipped: int = 0


def _unnamed_participants(conn, limit: int) -> List[str]:
    """Participant xuids with no name, and not already known to be unnameable.

    Ordered by how many scoreboards they appear on, so a bounded run buys the
    most visible names first - the same "most-played first" reasoning as the
    map backfill.

    Excluding xuid_unresolvable is what stops a dead account being re-requested
    on every future run forever. It is a small population in practice - see the
    module docstring - but an unbounded cost without this.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT mp.xuid, COUNT(*) AS appearances
        FROM match_participants mp
        LEFT JOIN players p ON p.xuid = mp.xuid
        LEFT JOIN xuid_gamertags xg ON xg.xuid = mp.xuid
        LEFT JOIN xuid_unresolvable xu ON xu.xuid = mp.xuid
        WHERE (p.xuid IS NULL OR p.gamertag IS NULL)
          AND xg.xuid IS NULL
          AND xu.xuid IS NULL
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

        # Ids the endpoint ANSWERED for and did not know, as opposed to ids a
        # failed request never asked about. Only the client can tell those
        # apart - it adds the former to _unresolvable_xuids when a 200 comes
        # back without them - and the difference is the whole game here: one
        # is permanent and one is worth retrying. Reaching into that set is
        # deliberate; the alternative is re-deriving it from the same 200s the
        # client has already parsed.
        dead = [x for x in chunk if x in client._unresolvable_xuids]
        if dead:
            cache.db.mark_xuids_unresolvable(dead)
        result.dead += len(dead)
        result.skipped += len(chunk) - len(mapping) - len(dead)

        print(
            f"[PARTICIPANTS] {result.resolved}/{result.attempted} named, "
            f"{result.dead} do not exist, {result.skipped} deferred to a later run"
        )

    return result


async def backfill_until_converged(
    db_path: Optional[str] = None,
    limit: int = DEFAULT_LIMIT,
    max_passes: int = DEFAULT_MAX_PASSES,
    min_gain: int = DEFAULT_MIN_GAIN,
) -> List[ParticipantBackfillResult]:
    """Re-run the backfill until a pass stops being worth running.

    Returns one result per pass, so a caller can see the shape of the descent
    rather than just the total.

    Each pass builds a fresh HaloAPIClient, which drops that client's in-memory
    record of ids it found unresolvable. That is fine and deliberate: those are
    persisted to xuid_unresolvable as they are found, and the next pass's query
    excludes them from the database instead. Nothing is re-asked.

    Bounded by max_passes so a throttled endpoint cannot spin here forever, and
    stopped early by min_gain because a pass that names almost nobody will not
    be rescued by another one.
    """
    passes: List[ParticipantBackfillResult] = []

    for n in range(1, max_passes + 1):
        outcome = await backfill_participant_gamertags(db_path=db_path, limit=limit)
        passes.append(outcome)

        if outcome.unnamed_before == 0:
            print(f"[CONVERGE] pass {n}: nothing left to name")
            break

        print(
            f"[CONVERGE] pass {n}: {outcome.unnamed_before} unnamed -> "
            f"named {outcome.resolved}, {outcome.dead} do not exist, "
            f"{outcome.skipped} deferred"
        )

        if outcome.resolved < min_gain:
            print(f"[CONVERGE] stopping: pass {n} named only {outcome.resolved}")
            break

    return passes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit", type=int, default=DEFAULT_LIMIT,
        help=f"How many unnamed participants to attempt per pass, most-seen "
             f"first (default {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--until-converged", action="store_true",
        help="Re-run until a pass names fewer than --min-gain. One pass never "
             "finishes the job: about half of each batch is deferred by a 429, "
             "so a pass halves the remainder rather than emptying it.",
    )
    parser.add_argument(
        "--max-passes", type=int, default=DEFAULT_MAX_PASSES,
        help=f"Ceiling on --until-converged, so a throttled endpoint cannot "
             f"spin forever (default {DEFAULT_MAX_PASSES})",
    )
    parser.add_argument(
        "--min-gain", type=int, default=DEFAULT_MIN_GAIN,
        help=f"Stop --until-converged once a pass names fewer than this "
             f"(default {DEFAULT_MIN_GAIN})",
    )
    args = parser.parse_args()

    if args.until_converged:
        results = asyncio.run(backfill_until_converged(
            limit=args.limit, max_passes=args.max_passes, min_gain=args.min_gain,
        ))
        print(
            f"Converged after {len(results)} pass(es): "
            f"{sum(r.resolved for r in results)} named, "
            f"{sum(r.dead for r in results)} do not exist, "
            f"{results[-1].skipped if results else 0} still deferred."
        )
    else:
        outcome = asyncio.run(backfill_participant_gamertags(limit=args.limit))
        print(
            f"Attempted {outcome.attempted} unnamed participants: "
            f"{outcome.resolved} named, {outcome.dead} do not exist (recorded, "
            f"never retried), {outcome.skipped} deferred to a later run. "
            f"Re-run, or use --until-converged, to pick up the deferred ones."
        )
