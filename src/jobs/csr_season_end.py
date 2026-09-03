"""Season-end CSR for the reconstructed Season 1 splits.

WHAT THIS ADDS

`csr_reconstruct` derives a season PEAK. It cannot derive a season END, because
it only ever sees matches whose rosters we happened to store - 0.07% of the
launch era - so "the last match it observed" is, for the median player, one
arbitrary game at an unknown point in a four-month season.

Measured against CsrSeason1-2, which 343 still serves and which therefore has a
known answer, taking the last OBSERVED match reproduces the official
end-of-season CSR only 2.8% of the time, mean error 51 CSR. Broken down by how
close that match sits to the season boundary:

    last observed match     players   within 50 CSR   mean error
    within 3 days                48            98%           14
    within a week                41            83%           24
    over a month early          761            58%           55

The method is sound; the input is stale. 84% of players' "last observed match"
is over a month before the season actually ended.

WHY THIS JOB IS CHEAP ANYWAY

We do not need to discover the last match - for players anyone has ever looked
up we already hold it. `player_match` carries their own full history, and a
300-player sample of reconstructed Season 1-1 rows found:

  * 73 of 300 (24%) have their own ranked matches stored for the window
  * for those, we hold 591 ranked matches on average
  * in 73 of 73 cases our stored last match is LATER than anything the
    reconstruction observed - by an average of 45 days

So the true final game is already known locally. All that is missing is its
CSR, and a skill recap covers a whole roster in ONE request. Players who shared
their last match cost one request between them.

The remaining 76% are roster-only players nobody ever looked up, so we hold
none of their matches. They are left alone: finding their last game means
crawling ~250 requests of match list each, which is the multi-day path
csr_reconstruct's notes deliberately reject in favour of doing it on demand.

WHY IT IS BUILT THIS WAY

* Phase 1 (find) touches no network and is resumable per batch. It walks
  players in small batches and CLOSES the read connection between them, so no
  read snapshot is ever held for long - the same rule csr_reconstruct follows,
  and the reason WAL checkpointing (and site latency) is never held up.

* Phase 2 (fetch) issues one request per distinct match, through the shared
  rate limiter at low concurrency, because the Spartan pool is shared with the
  live bot.

* A season end is written ONLY where a recap actually answered. A blank stays
  blank; nothing is estimated. `status` still describes the PEAK, which is a
  separate claim - a floor peak with a known end is entirely normal.
"""
import argparse
import asyncio
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.api.client import HaloAPIClient
from src.jobs.csr_reconstruct import (
    DEFAULT_CONCURRENCY,
    SEASON_WINDOWS,
    _skill_status,
)

# Players per read batch. Small on purpose: each batch opens the live DB, runs,
# and closes it, so this bounds how long any single read snapshot lives.
PLAYER_BATCH = 200


def _open_output(path: str) -> sqlite3.Connection:
    out = sqlite3.connect(path)
    out.row_factory = sqlite3.Row
    out.execute("PRAGMA journal_mode=WAL")
    out.executescript("""
        -- One row per (player, ladder, season) whose last match we located.
        CREATE TABLE IF NOT EXISTS season_end (
            xuid TEXT NOT NULL,
            ladder_id TEXT NOT NULL,
            season_id TEXT NOT NULL,
            match_id TEXT NOT NULL,
            started TEXT NOT NULL,
            -- NULL until phase 2 has fetched the recap, and still NULL if the
            -- recap is gone or carried no rank. Never guessed.
            end_csr INTEGER,
            fetched_at TEXT,
            PRIMARY KEY (xuid, ladder_id, season_id)
        );
        CREATE INDEX IF NOT EXISTS idx_se_match ON season_end (match_id);
        -- Resume markers for phase 1, so a interrupted find does not restart.
        CREATE TABLE IF NOT EXISTS find_progress (
            batch_key TEXT PRIMARY KEY,
            done_at TEXT NOT NULL
        );
        -- Matches whose recap 343 no longer serves. Permanent, not retried.
        CREATE TABLE IF NOT EXISTS se_unavailable (
            match_id TEXT PRIMARY KEY,
            noted_at TEXT NOT NULL
        );
    """)
    return out


def _derived_targets(db_path: str) -> List[Tuple[str, str, str]]:
    """(xuid, ladder_id, season_id) for every reconstructed row that could have
    an end - i.e. everything except 'unranked', which by definition never had a
    rank to finish on."""
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    db.execute("PRAGMA query_only=ON")
    try:
        return [(r[0], r[1], r[2]) for r in db.execute(
            "SELECT xuid, playlist_asset_id, season_id FROM player_csr_season_derived"
            " WHERE status <> 'unranked'")]
    finally:
        db.close()


def find_last_matches(db_path: str, out_path: str, limit: Optional[int] = None,
                      only_season: Optional[str] = None) -> None:
    """Phase 1: locate each player's genuine last match of the season, locally.

    No network. Drives from the players rather than from `matches`: the Season 1
    Arena window holds 6.5M matches, so scanning it would be far more work than
    walking 18k players' own histories through idx_player_match_xuid.
    """
    out = _open_output(out_path)
    targets = _derived_targets(db_path)

    # Group by season so each batch runs one window, and index by player so a
    # batch is a contiguous slice of players.
    by_season: Dict[str, List[Tuple[str, str]]] = {}
    for xuid, ladder, season in targets:
        by_season.setdefault(season, []).append((xuid, ladder))

    windows = {s: (a, b) for s, a, b in SEASON_WINDOWS}
    done = {r["batch_key"] for r in out.execute("SELECT batch_key FROM find_progress")}
    found = skipped = 0
    batches_run = 0

    for season, pairs in sorted(by_season.items()):
        if only_season and season != only_season:
            continue
        if season not in windows:
            print(f"[SEASON-END] no window for {season}, skipping {len(pairs)} rows")
            continue
        a, b = windows[season]
        # Deterministic order, so batch keys mean the same thing across runs.
        pairs.sort()
        players = sorted({x for x, _ in pairs})
        ladders_for = {}
        for x, lad in pairs:
            ladders_for.setdefault(x, set()).add(lad)

        for i in range(0, len(players), PLAYER_BATCH):
            key = f"{season}:{i}"
            if key in done:
                skipped += 1
                continue
            if limit is not None and batches_run >= limit:
                print(f"[SEASON-END] stopping at --limit {limit} batches")
                out.close()
                return
            chunk = players[i : i + PLAYER_BATCH]

            db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            db.execute("PRAGMA query_only=ON")
            try:
                ph = ",".join("?" * len(chunk))
                rows = db.execute(
                    f"""
                    SELECT pm.xuid, m.playlist_id, m.match_id, m.start_time
                      FROM player_match pm
                      JOIN matches m ON m.match_id = pm.match_id
                     WHERE pm.xuid IN ({ph})
                       AND m.start_time >= ? AND m.start_time < ?
                    """,
                    (*chunk, a, b),
                ).fetchall()
            finally:
                # Closed before anything else happens. See the module docstring.
                db.close()

            # Latest match per (player, ladder we actually have a row for).
            best: Dict[Tuple[str, str], Tuple[str, str]] = {}
            for xuid, playlist, mid, started in rows:
                if not playlist or playlist not in ladders_for.get(xuid, ()):
                    continue
                cur = best.get((xuid, playlist))
                if cur is None or started > cur[1]:
                    best[(xuid, playlist)] = (mid, started)

            with out:
                out.executemany(
                    "INSERT INTO season_end (xuid, ladder_id, season_id, match_id, started)"
                    " VALUES (?,?,?,?,?)"
                    " ON CONFLICT(xuid, ladder_id, season_id) DO UPDATE SET"
                    "   match_id=excluded.match_id, started=excluded.started,"
                    "   end_csr=NULL, fetched_at=NULL"
                    " WHERE season_end.match_id <> excluded.match_id",
                    [(x, lad, season, mid, st) for (x, lad), (mid, st) in best.items()])
                out.execute("INSERT OR REPLACE INTO find_progress VALUES (?,?)",
                            (key, datetime.now().isoformat()))
            found += len(best)
            batches_run += 1
            print(f"[SEASON-END] {key}: {len(chunk)} players -> {len(best)} last matches"
                  f"  (total {found:,})")

    total = out.execute("SELECT COUNT(*) FROM season_end").fetchone()[0]
    print(f"[SEASON-END] phase 1 done: {found:,} located this run, {total:,} stored, "
          f"{skipped} batches already done")
    out.close()


async def fetch_end_csr(out_path: str, concurrency: int, limit: Optional[int] = None,
                        only_season: Optional[str] = None) -> None:
    """Phase 2: one skill recap per distinct match, filling in the end CSR.

    A recap covers the whole roster, so players who finished their season in the
    same match are answered together by a single request.
    """
    out = _open_output(out_path)
    gone = {r["match_id"] for r in out.execute("SELECT match_id FROM se_unavailable")}

    pending: Dict[str, List[str]] = {}
    q = "SELECT match_id, xuid FROM season_end WHERE fetched_at IS NULL"
    params: Tuple = ()
    if only_season:
        q += " AND season_id = ?"
        params = (only_season,)
    for r in out.execute(q, params):
        if r["match_id"] in gone:
            continue
        pending.setdefault(r["match_id"], []).append(r["xuid"])

    mids = sorted(pending)
    if limit is not None:
        mids = mids[:limit]
    if not mids:
        print("[SEASON-END] nothing to fetch")
        out.close()
        return

    client = HaloAPIClient()
    if not await client.ensure_valid_tokens():
        raise RuntimeError("no valid Spartan tokens - start the bot once first")

    sem = asyncio.Semaphore(concurrency)
    print(f"[SEASON-END] fetching {len(mids):,} recaps for "
          f"{sum(len(v) for v in pending.values()):,} player-seasons, "
          f"concurrency={concurrency}")

    filled = blank = failed = 0
    lock = asyncio.Lock()

    async def one(mid: str, who: List[str]) -> None:
        nonlocal filled, blank, failed
        async with sem:
            status, got = await _skill_status(
                client, f"{client.SKILL_URL}/hi/matches/{mid}/skill", who)
        now = datetime.now().isoformat()
        if status == 404:
            async with lock:
                with out:
                    out.execute("INSERT OR REPLACE INTO se_unavailable VALUES (?,?)",
                                (mid, now))
            return
        if got is None:
            # A transient failure must NOT be recorded as "no CSR" - that is the
            # exact bug csr_reconstruct's docstring warns about. Left pending.
            async with lock:
                failed += 1
            return

        found: List[Tuple[int, str, str]] = []
        for entry in got:
            xuid = client._unwrap_player_id(entry)
            res = entry.get("Result") if isinstance(entry, dict) else None
            if not xuid or not isinstance(res, dict):
                continue
            recap = res.get("RankRecap") or {}
            post = (recap.get("PostMatchCsr") or {}).get("Value")
            # -1 is "not ranked yet" and 0 is an unranked match; neither is a
            # rank, so neither becomes an end-of-season CSR.
            if isinstance(post, int) and post > 0:
                found.append((post, str(xuid), now))

        async with lock:
            with out:
                for post, xuid, ts in found:
                    out.execute(
                        "UPDATE season_end SET end_csr=?, fetched_at=?"
                        " WHERE match_id=? AND xuid=?", (post, ts, mid, xuid))
                # Everyone else on this roster got a real answer of "no rank
                # here"; mark them fetched so the match is not requested again.
                out.execute(
                    "UPDATE season_end SET fetched_at=? WHERE match_id=? AND fetched_at IS NULL",
                    (now, mid))
            filled += len(found)
            blank += max(0, len(who) - len(found))

    step = 200
    for i in range(0, len(mids), step):
        await asyncio.gather(*(one(m, pending[m]) for m in mids[i : i + step]))
        print(f"[SEASON-END] {min(i + step, len(mids)):,}/{len(mids):,} matches  "
              f"filled={filled:,} no-rank={blank:,} failed={failed:,}")

    print(f"[SEASON-END] phase 2 done: {filled:,} end CSRs, {blank:,} had no rank, "
          f"{failed:,} failed (left pending, re-run)")
    out.close()


def merge(out_path: str, db_path: str) -> None:
    """Publish end CSRs into player_csr_season_derived.

    Adds a column rather than a table: the row already exists, this is one more
    fact about it. `status` is untouched, because it describes the PEAK - a
    floor peak alongside a known end is the normal case, not a contradiction.
    """
    out = _open_output(out_path)
    rows = out.execute(
        "SELECT xuid, ladder_id, season_id, end_csr FROM season_end"
        " WHERE end_csr IS NOT NULL").fetchall()
    out.close()

    live = sqlite3.connect(db_path, timeout=30)
    live.execute("PRAGMA busy_timeout=30000")
    cols = {r[1] for r in live.execute("PRAGMA table_info(player_csr_season_derived)")}
    if "season_end_csr" not in cols:
        live.execute("ALTER TABLE player_csr_season_derived ADD COLUMN season_end_csr INTEGER")
        print("[SEASON-END] added column player_csr_season_derived.season_end_csr")

    with live:
        live.executemany(
            "UPDATE player_csr_season_derived SET season_end_csr = ?"
            " WHERE xuid = ? AND playlist_asset_id = ? AND season_id = ?",
            [(r["end_csr"], r["xuid"], r["ladder_id"], r["season_id"]) for r in rows])

    n = live.execute("SELECT COUNT(*) FROM player_csr_season_derived"
                     " WHERE season_end_csr IS NOT NULL").fetchone()[0]
    live.close()
    print(f"[SEASON-END] merged {len(rows):,} values; {n:,} rows now carry a season end")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db-path", help="live stats DB")
    ap.add_argument("--out", help="scratch DB (default DATA_DIR/csr_season_end.db)")
    ap.add_argument("--find", action="store_true",
                    help="phase 1: locate each player's last match locally (no network)")
    ap.add_argument("--fetch", action="store_true",
                    help="phase 2: one skill recap per distinct match")
    ap.add_argument("--merge", action="store_true",
                    help="publish end CSRs into player_csr_season_derived")
    ap.add_argument("--season", help="restrict to one season id, e.g. CsrSeason1-2")
    ap.add_argument("--limit", type=int,
                    help="stop after N batches (--find) or N matches (--fetch)")
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                    help=f"shared with the live bot - keep it low (default {DEFAULT_CONCURRENCY})")
    args = ap.parse_args()

    from src.config import DATA_DIR
    db_path = args.db_path or str(Path(DATA_DIR) / "halo_stats_v2.db")
    out_path = args.out or str(Path(DATA_DIR) / "csr_season_end.db")

    if args.find:
        find_last_matches(db_path, out_path, args.limit, args.season)
        return 0
    if args.fetch:
        asyncio.run(fetch_end_csr(out_path, args.concurrency, args.limit, args.season))
        return 0
    if args.merge:
        merge(out_path, db_path)
        return 0

    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
