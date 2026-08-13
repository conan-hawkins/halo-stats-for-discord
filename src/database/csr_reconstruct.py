#!/usr/bin/env python3
"""Rebuild CSR for RETIRED seasons from per-match recaps.

Once 343 retires a CsrSeason, `skill.svc/hi/playlist/{id}/csrs?season=...`
answers 404 forever - CsrSeason1-1 and CsrSeason2-1 are both gone, which is why
the site can only show the second half of Season 1 and reports a season peak
far below what players actually hit. Measured on one account: the playlist
endpoint reports 1328 for CsrSeason1-2 while the player's real Season 1 peak
was 1620.

The per-match recap endpoint still answers for those same matches. So the
history is recoverable, just not by asking about the season - you have to walk
the matches and take the best CSR seen.

    python -m src.database.csr_reconstruct --dry-run     # counts only
    python -m src.database.csr_reconstruct               # harvest
    python -m src.database.csr_reconstruct --aggregate   # build season rows

WHY IT IS BUILT THIS WAY

* One request per MATCH, not per player. get_match_skill covers the whole
  roster in a single call, so a 8-player match costs one request.

* Raw observations are stored, not season aggregates. Season boundaries are
  inferred (343 does not publish them), so keeping (xuid, match, date, csr)
  lets the windows be re-cut later without re-fetching a single match.

* Writes go to a SEPARATE database. The bot is the only writer of
  halo_stats_v2.db and the website reads it; this never takes a write lock on
  it. Reads from it are time-sliced so no read snapshot is held for long -
  a long snapshot blocks WAL checkpointing and that is felt as site latency.

* CONCURRENCY IS DELIBERATELY LOW. The Spartan account pool and its rate
  limiter are SHARED with the live bot. Saturating them would slow every
  user-facing lookup on the site, so this leaves most of the pool free and
  would rather take longer.

* A match that fails is never marked done. _fetch_skill answers None for both
  "no data" and "the request failed", so this checks for None itself and treats
  it as failure - otherwise a transient blip silently becomes "this player had
  no CSR", permanently, which is exactly the bug that made this work necessary.
  It reads the recap straight off _fetch_skill rather than going through
  get_match_skill so that it depends on nothing but the raw call, and so a
  failed request can never be mistaken for an empty one.
"""
import argparse
import asyncio
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from src.api.client import HaloAPIClient

# Kept well below the client's 25-slot ceiling on purpose: the pool is shared
# with the live bot. See the module docstring.
DEFAULT_CONCURRENCY = 3

# Launch-era Arena ran under three asset ids and each is kept SEPARATE.
#
# It is tempting to merge them: players from the launch ids demonstrably hold
# CSR on the live id (measured 7/8, 7/8, 8/8), so AllTimeMax is consolidated
# onto the current id. That is a fact about AllTimeMax and NOT about seasons.
# Merging them produced 19 players whose reconstructed CsrSeason1-2 exceeded
# the official one - impossible, since reconstruction can only undershoot - and
# every single one of those peaks came from a launch-era id. Splitting the ids
# takes that to zero and RAISES the exact-match count. Per-season records stay
# with the asset id that owned the match.
ARENA_LIVE = "edfef3ac-9cbe-4fa2-b949-8f29deafd483"
HARVEST_IDS = [
    ARENA_LIVE,
    "f7f30787-f607-436b-bdec-44c65bc2ecef",
    "f7eb8c71-fedb-4696-8c0f-96025e285ffd",
]

# The whole of Season 1, both splits. The served half (CsrSeason1-2) is
# harvested too, deliberately: reconstructing a season the API still answers
# for is the only way to prove the method reproduces official figures before
# trusting it on the half that is gone.
DEFAULT_START = "2021-11-01"
DEFAULT_END = "2022-06-01"

# Both boundaries are derived from the data, because 343 publishes neither.
#
# S1_SPLIT: scanning candidate start dates against the official CsrSeason1-2
# season_max shows a clean cliff - 2022-02-22 still admits 11 impossible
# values, 2022-03-01 admits none and simultaneously maximises exact matches.
# The true reset therefore falls in (2022-02-22, 2022-03-01].
#
# S2_START: 2022-05-03 is Halo Season 2. Running the window past it pulled
# Season 2 matches into Season 1 and produced 146 impossible values.
S1_SPLIT = "2022-03-01"
S2_START = "2022-05-03"
SEASON_WINDOWS = [
    ("CsrSeason1-1", DEFAULT_START, S1_SPLIT),
    ("CsrSeason1-2", S1_SPLIT, S2_START),
]

SLICE_DAYS = 3


@dataclass
class ReconResult:
    slices_done: int = 0
    slices_skipped: int = 0
    matches_seen: int = 0
    requests: int = 0
    observations: int = 0
    unavailable: int = 0
    failed: int = 0
    per_month: Dict[str, int] = field(default_factory=dict)


async def _skill_status(client: HaloAPIClient, url: str, xuids: List[str]
                        ) -> Tuple[int, Optional[List[Dict]]]:
    """(status, value) for a skill.svc GET.

    This module has to tell a permanent 404 - a match whose recap 343 no longer
    serves - from a retryable failure, or it retries dead matches forever and
    the slice never finishes. _fetch_skill collapses both into None.

    Uses the client's own _fetch_skill_ex where the deployment has it, and
    otherwise issues the equivalent request here, through the SAME shared rate
    limiter so it still yields to live site traffic.
    """
    ex = getattr(client, "_fetch_skill_ex", None)
    if ex is not None:
        return await ex(url, xuids)

    import aiohttp
    from urllib.parse import urlencode
    from src.api.rate_limiters import halo_stats_rate_limiter, BUCKET_SKILL

    full = f"{url}?{urlencode([('players', f'xuid({x})') for x in xuids])}"
    try:
        async with halo_stats_rate_limiter.slot(bucket=BUCKET_SKILL) as account_index:
            token = client.get_next_spartan_token(account_index)
            if isinstance(token, dict):
                token = token.get("token")
            if not token:
                return 0, None
            headers = {"Authorization": f"Spartan {token}",
                       "x-343-authorization-spartan": token,
                       "User-Agent": client.user_agent,
                       "Accept": "application/json"}
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(full, headers=headers) as r:
                    if r.status == 200:
                        halo_stats_rate_limiter.note_result(BUCKET_SKILL, rate_limited=False)
                        payload = await r.json()
                        v = payload.get("Value") if isinstance(payload, dict) else None
                        return 200, (v if isinstance(v, list) else None)
                    if r.status == 429:
                        halo_stats_rate_limiter.note_result(BUCKET_SKILL, rate_limited=True)
                        halo_stats_rate_limiter.set_backoff(
                            account_index=account_index, seconds=5.0)
                    return r.status, None
    except Exception as exc:
        print(f"[RECON] request failed: {exc}")
        return 0, None


def _open_output(path: str) -> sqlite3.Connection:
    out = sqlite3.connect(path)
    out.row_factory = sqlite3.Row
    out.execute("PRAGMA journal_mode=WAL")
    out.executescript("""
        CREATE TABLE IF NOT EXISTS csr_observation (
            xuid TEXT NOT NULL,
            ladder_id TEXT NOT NULL,
            match_id TEXT NOT NULL,
            started TEXT NOT NULL,
            pre_csr INTEGER,
            post_csr INTEGER,
            -- Placement state, straight from the recap. A player is not ranked
            -- until MeasurementMatchesRemaining hits 0, and CSR reads -1 the
            -- whole way there. Storing it turns "-1, we know nothing" into
            -- "provably unranked, N of 10 placement matches played", which is
            -- a fact worth showing rather than a gap.
            placement_left INTEGER,
            placement_total INTEGER,
            PRIMARY KEY (xuid, match_id)
        );
        CREATE INDEX IF NOT EXISTS idx_obs_ladder_started
            ON csr_observation (ladder_id, started);
        -- One row per finished time slice. Written in the SAME transaction as
        -- that slice's observations, so a slice is never marked done without
        -- its data. A slice that failed simply has no row and is retried.
        CREATE TABLE IF NOT EXISTS recon_progress (
            slice_start TEXT PRIMARY KEY,
            matches INTEGER NOT NULL,
            done_at TEXT NOT NULL
        );
        -- Matches whose recap the API no longer serves at all (404). Recorded
        -- so they are not mistaken for failures and retried forever.
        CREATE TABLE IF NOT EXISTS recon_unavailable (
            match_id TEXT PRIMARY KEY,
            started TEXT NOT NULL,
            seen_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS recon_season (
            xuid TEXT NOT NULL,
            ladder_id TEXT NOT NULL,
            season_id TEXT NOT NULL,
            csr INTEGER,
            season_max INTEGER,
            matches INTEGER,
            -- What this row is allowed to claim. See classify() - publishing a
            -- 'floor' as if it were a peak is what would make the site less
            -- accurate, not more.
            status TEXT,
            placement_played INTEGER,
            derived_at TEXT NOT NULL,
            PRIMARY KEY (xuid, ladder_id, season_id)
        );
    """)
    # Migrations. The observations are expensive (one API request each) and the
    # season rows are not (pure derivation), so columns are ADDED to the former
    # and the latter is simply rebuilt.
    cols = {r[1] for r in out.execute("PRAGMA table_info(csr_observation)")}
    for col in ("placement_left", "placement_total"):
        if col not in cols:
            out.execute(f"ALTER TABLE csr_observation ADD COLUMN {col} INTEGER")
    if "status" not in {r[1] for r in out.execute("PRAGMA table_info(recon_season)")}:
        out.execute("DROP TABLE recon_season")
        out.execute("""CREATE TABLE recon_season (
            xuid TEXT NOT NULL, ladder_id TEXT NOT NULL, season_id TEXT NOT NULL,
            csr INTEGER, season_max INTEGER, matches INTEGER, status TEXT,
            placement_played INTEGER, derived_at TEXT NOT NULL,
            PRIMARY KEY (xuid, ladder_id, season_id))""")
    out.commit()
    return out


def _slices(start: str, end: str, days: int = SLICE_DAYS) -> List[Tuple[str, str]]:
    a = datetime.fromisoformat(start)
    z = datetime.fromisoformat(end)
    out = []
    while a < z:
        b = min(a + timedelta(days=days), z)
        out.append((a.date().isoformat(), b.date().isoformat()))
        a = b
    return out


def _matches_with_rosters(db: sqlite3.Connection, a: str, b: str
                          ) -> List[Tuple[str, str, str, List[str]]]:
    """Matches in [a, b) that we hold a roster for, with that roster.

    Index-backed both ways: idx_matches_playlist_start for the range, then
    match_participants by match_id. Only ~0.07% of launch-era matches have a
    roster, so this returns very little for the volume it walks - which is why
    it is sliced rather than run as one query over the whole era.
    """
    out: List[Tuple[str, str, str, List[str]]] = []
    for asset_id in HARVEST_IDS:
        rows = db.execute(
            "SELECT match_id, playlist_id, start_time FROM matches"
            " WHERE playlist_id = ? AND start_time >= ? AND start_time < ?",
            (asset_id, a, b)).fetchall()
        for mid, pid, started in rows:
            who = [str(x) for (x,) in db.execute(
                "SELECT xuid FROM match_participants WHERE match_id = ?", (mid,))]
            if who:
                out.append((mid, pid, started, who))
    return out


async def harvest(db_path: str, out_path: str, start: str, end: str,
                  concurrency: int, dry_run: bool = False) -> ReconResult:
    result = ReconResult()
    out = _open_output(out_path)
    done = {r["slice_start"] for r in out.execute("SELECT slice_start FROM recon_progress")}

    client: Optional[HaloAPIClient] = None
    if not dry_run:
        client = HaloAPIClient()
        if not await client.ensure_valid_tokens():
            raise RuntimeError("no valid Spartan tokens - start the bot once first")

    sem = asyncio.Semaphore(concurrency)
    slices = _slices(start, end)
    print(f"[RECON] {len(slices)} slices of {SLICE_DAYS}d, {len(done)} already done, "
          f"concurrency={concurrency}")

    for a, b in slices:
        if a in done:
            result.slices_skipped += 1
            continue

        # Short-lived read connection: opened, used, closed. Keeps no snapshot
        # open across the network calls below, so WAL checkpointing - and with
        # it site latency - is never held up by this job.
        db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        db.execute("PRAGMA query_only=ON")
        try:
            batch = _matches_with_rosters(db, a, b)
        finally:
            db.close()

        result.matches_seen += len(batch)
        result.per_month[a[:7]] = result.per_month.get(a[:7], 0) + len(batch)
        if dry_run:
            if batch:
                print(f"[RECON] {a} -> {len(batch)} matches with rosters")
            continue
        if not batch:
            with out:
                out.execute("INSERT OR REPLACE INTO recon_progress VALUES (?,?,?)",
                            (a, 0, datetime.now().isoformat()))
            continue

        rows: List[Tuple] = []
        gone: List[Tuple] = []
        failed = 0

        async def one(mid: str, ladder: str, started: str, who: List[str]):
            nonlocal failed
            async with sem:
                status, got = await _skill_status(
                    client, f"{client.SKILL_URL}/hi/matches/{mid}/skill", who)
            result.requests += 1
            if status == 404:
                # Permanently gone, not a fault: a few launch-era matches no
                # longer have a recap at all. Recording them stops the slice
                # being retried forever over matches that will never answer.
                gone.append((mid, started, datetime.now().isoformat()))
                return
            if got is None:
                failed += 1
                return
            for entry in got:
                xuid = client._unwrap_player_id(entry)
                res = entry.get("Result") if isinstance(entry, dict) else None
                if not xuid or not isinstance(res, dict):
                    continue
                recap = res.get("RankRecap") or {}
                post_block = recap.get("PostMatchCsr") or {}
                pre = (recap.get("PreMatchCsr") or {}).get("Value")
                post = post_block.get("Value")
                left = post_block.get("MeasurementMatchesRemaining")
                total = post_block.get("InitialMeasurementMatches")
                # -1 is "not ranked yet" and 0/0 is "unranked match"; neither is
                # a rank. But the placement counters alongside them ARE
                # meaningful, so a -1 row is kept when it can say WHY.
                pre = pre if isinstance(pre, int) and pre > 0 else None
                post = post if isinstance(post, int) and post > 0 else None
                placement = isinstance(left, int) and isinstance(total, int) and total > 0
                if pre is None and post is None and not placement:
                    continue
                rows.append((str(xuid), ladder, mid, started, pre, post,
                             left if placement else None,
                             total if placement else None))

        await asyncio.gather(*(one(*m) for m in batch))
        result.failed += failed

        # Slice marker and its rows commit together. If some matches in the
        # slice failed, the slice is NOT marked done, so a re-run retries it
        # rather than leaving a permanent hole nothing records.
        with out:
            out.executemany(
                "INSERT OR REPLACE INTO csr_observation VALUES (?,?,?,?,?,?,?,?)", rows)
            out.executemany(
                "INSERT OR REPLACE INTO recon_unavailable VALUES (?,?,?)", gone)
            if not failed:
                out.execute("INSERT OR REPLACE INTO recon_progress VALUES (?,?,?)",
                            (a, len(batch), datetime.now().isoformat()))
        result.observations += len(rows)
        result.unavailable += len(gone)
        result.slices_done += 1
        print(f"[RECON] {a} {len(batch):>4} matches -> {len(rows):>5} observations"
              f"{f'  ({len(gone)} gone/404)' if gone else ''}"
              f"{f'  ({failed} FAILED, slice left open)' if failed else ''}"
              f"   total={result.observations:,}")

    return result


def _repair_ladders(out: sqlite3.Connection, db_path: str) -> int:
    """Point every observation at the asset id its match actually belongs to.

    An earlier version canonicalised the three Arena ids onto the live one.
    That was wrong for seasons (see HARVEST_IDS), and the raw observations are
    kept precisely so a mistake like this costs a regroup rather than another
    4,350 requests.
    """
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    db.execute("PRAGMA query_only=ON")
    fixed = 0
    try:
        mids = [r["match_id"] for r in out.execute(
            "SELECT DISTINCT match_id FROM csr_observation")]
        for mid in mids:
            row = db.execute("SELECT playlist_id FROM matches WHERE match_id=?",
                             (mid,)).fetchone()
            if not row:
                continue
            with out:
                cur = out.execute(
                    "UPDATE csr_observation SET ladder_id=? "
                    " WHERE match_id=? AND ladder_id<>?", (row[0], mid, row[0]))
                fixed += cur.rowcount
    finally:
        db.close()
    return fixed


def aggregate(out_path: str, db_path: Optional[str] = None) -> None:
    """Cut the stored observations into season rows, and check them.

    The check is the point: CsrSeason1-2 is still served by the API, so if this
    method is sound it must reproduce the official season_max for it. Only then
    is the CsrSeason1-1 figure - which nothing can be compared against, because
    343 no longer serves it - worth believing.
    """
    out = _open_output(out_path)
    if db_path:
        fixed = _repair_ladders(out, db_path)
        print(f"[RECON] ladder repair: {fixed:,} observations re-pointed")

    live = None
    if db_path:
        live = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        live.execute("PRAGMA query_only=ON")

    now = datetime.now().isoformat()
    for season, a, b in SEASON_WINDOWS:
        rows = out.execute(
            "SELECT xuid, ladder_id,"
            "       MAX(MAX(COALESCE(pre_csr,-1), COALESCE(post_csr,-1))) AS peak,"
            "       MIN(COALESCE(placement_left, 99)) AS min_left,"
            "       MAX(COALESCE(placement_total, 0)) AS total,"
            "       COUNT(*) AS n"
            "  FROM csr_observation"
            " WHERE started >= ? AND started < ?"
            " GROUP BY xuid, ladder_id", (a, b)).fetchall()

        keep = []
        tally = {"exact": 0, "floor": 0, "unranked": 0}
        for r in rows:
            peak = r["peak"] if (r["peak"] or -1) > 0 else None
            if peak is None:
                # No CSR ever seen. If the recaps showed placement counters,
                # that is not missing data - the player provably never ranked
                # here that season. Their K/D still stands; their rank does
                # not exist. Anything else is a gap, so record nothing.
                if r["total"]:
                    played = r["total"] - min(r["min_left"], r["total"])
                    keep.append((r["xuid"], r["ladder_id"], season, None, None,
                                 r["n"], "unranked", played, now))
                    tally["unranked"] += 1
                continue
            status = "floor"
            if live is not None:
                atm = live.execute(
                    "SELECT all_time_max FROM player_playlist_csr"
                    " WHERE xuid=? AND playlist_asset_id=?",
                    (r["xuid"], r["ladder_id"])).fetchone()
                # Reconstruction can only undershoot, so reaching the
                # authoritative all-time max proves the true peak was found.
                if atm and atm[0] is not None and peak == atm[0]:
                    status = "exact"
            keep.append((r["xuid"], r["ladder_id"], season, None, peak,
                         r["n"], status, None, now))
            tally[status] += 1

        with out:
            # Rebuild the season from scratch. INSERT OR REPLACE alone would
            # leave behind rows keyed on an asset id this run no longer
            # produces - which is exactly how the first corrected aggregate
            # still reported 127 impossible values from the previous, wrongly
            # canonicalised run.
            out.execute("DELETE FROM recon_season WHERE season_id = ?", (season,))
            out.executemany(
                "INSERT OR REPLACE INTO recon_season VALUES (?,?,?,?,?,?,?,?,?)", keep)
        print(f"[RECON] {season}: {len(keep):,} rows  "
              f"exact={tally['exact']:,}  floor={tally['floor']:,}  "
              f"unranked={tally['unranked']:,}")
    if live is not None:
        live.close()


def merge(out_path: str, db_path: str) -> None:
    """Publish reconstructed seasons into the live DB, in their own table.

    Deliberately NOT written into player_csr_season. Most reconstructed values
    are floors, not peaks (see bot_docs/CSR_SEASON1_RECONSTRUCTION.md), and
    anything reading player_csr_season today would present them as peaks - a
    player who peaked at 1700 would be shown as 1250. A separate table with an
    explicit `status` means no existing reader changes behaviour, and the site
    can adopt it deliberately, honouring the claim each row is allowed to make.

    The bot is the single writer of halo_stats_v2.db. This takes one short
    transaction with a long busy timeout rather than requiring it to be stopped.
    """
    out = _open_output(out_path)
    rows = out.execute(
        "SELECT xuid, ladder_id, season_id, season_max, matches, status,"
        "       placement_played, derived_at FROM recon_season").fetchall()

    live = sqlite3.connect(db_path, timeout=30)
    live.execute("PRAGMA busy_timeout=30000")
    live.execute("""
        CREATE TABLE IF NOT EXISTS player_csr_season_derived (
            xuid TEXT NOT NULL,
            playlist_asset_id TEXT NOT NULL,
            season_id TEXT NOT NULL,
            season_max INTEGER,
            matches_observed INTEGER,
            -- 'exact'    : reached all_time_max, provably the true peak
            -- 'floor'    : a real CSR, but coverage cannot prove it is the peak
            --              -> may only ever be shown as "at least X"
            -- 'unranked' : placement counters present, never placed
            status TEXT NOT NULL,
            placement_played INTEGER,
            derived_at TEXT NOT NULL,
            PRIMARY KEY (xuid, playlist_asset_id, season_id)
        )""")
    live.execute("CREATE INDEX IF NOT EXISTS idx_csr_derived_xuid"
                 " ON player_csr_season_derived (xuid)")
    with live:
        live.execute("DELETE FROM player_csr_season_derived")
        live.executemany(
            "INSERT OR REPLACE INTO player_csr_season_derived VALUES (?,?,?,?,?,?,?,?)",
            [tuple(r) for r in rows])
    n = live.execute("SELECT COUNT(*) FROM player_csr_season_derived").fetchone()[0]
    by = dict(live.execute("SELECT status, COUNT(*) FROM player_csr_season_derived"
                           " GROUP BY status").fetchall())
    live.close()
    print(f"[RECON] merged {n:,} rows into player_csr_season_derived  {by}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db-path", help="live stats DB (read-only)")
    ap.add_argument("--out", help="output DB (default DATA_DIR/csr_reconstruct.db)")
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=DEFAULT_END)
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                    help=f"shared with the live bot - keep it low (default {DEFAULT_CONCURRENCY})")
    ap.add_argument("--dry-run", action="store_true", help="count matches, fetch nothing")
    ap.add_argument("--aggregate", action="store_true", help="build season rows from observations")
    ap.add_argument("--merge", action="store_true",
                    help="publish season rows into the live DB (own table, not player_csr_season)")
    args = ap.parse_args()

    from src.config import DATA_DIR
    db_path = args.db_path or str(Path(DATA_DIR) / "halo_stats_v2.db")
    out_path = args.out or str(Path(DATA_DIR) / "csr_reconstruct.db")

    if args.aggregate:
        aggregate(out_path, db_path)
        return 0

    if args.merge:
        merge(out_path, db_path)
        return 0

    r = asyncio.run(harvest(db_path, out_path, args.start, args.end,
                            args.concurrency, args.dry_run))
    print("\n" + "=" * 62)
    print(f"  slices done/skipped : {r.slices_done} / {r.slices_skipped}")
    print(f"  matches with roster : {r.matches_seen:,}")
    print(f"  requests issued     : {r.requests:,}")
    print(f"  observations stored : {r.observations:,}")
    print(f"  recap gone (404)    : {r.unavailable:,} (permanent, not retried)")
    print(f"  FAILED matches      : {r.failed:,} (their slices left open; re-run)")
    if args.dry_run:
        for m in sorted(r.per_month):
            print(f"    {m}  {r.per_month[m]:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
