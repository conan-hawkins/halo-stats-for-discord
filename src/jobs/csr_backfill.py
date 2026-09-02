"""
One-time backfill of historic CSR from skill.svc into a standalone SQLite file.

Writes to its OWN database, not the live one, for two reasons: the run takes
hours and must not hold the bot's single write connection for that long, and a
separate file makes resume trivial. Merge it in afterwards with
`python -m src.jobs.csr_merge`.

Run everything:      python -m src.jobs.csr_backfill
Dry run:             python -m src.jobs.csr_backfill --limit-players 320
Resume:              re-run the same command; completed work is skipped
Specific playlists:  python -m src.jobs.csr_backfill --playlists <id> <id>

Three phases:

  A. discover which CsrSeason ids exist (the space is irregular - 1-1 and 2-1
     do not exist while 1-2, 2-2 and 2-3 do, and only season 13 has three
     sub-seasons, so this is probed rather than computed)
  B. scope: one pass over every ranked player per playlist, current season
     only. A player with no CSR this season STILL reports their all-time peak,
     so this single pass answers "was this player ever ranked here"
  C. sweep: the remaining seasons, for qualifying (player, playlist) pairs only

Phase B is what makes this affordable. Without it, every player would need
every season queried: ~341,000 requests instead of ~50,000.

Safe to re-run. Every unit of work is recorded in csr_progress in the same
transaction as the rows it produced, so a kill -9 loses at most one chunk and
a restart re-issues no completed request.
"""
from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from src.api.client import HaloAPIClient, SkillFetchError
from src.api.utils import is_token_valid, safe_read_json
from src.config import get_token_cache_path

# The template's value. The real pacing is BUCKET_SKILL inside the client;
# this only bounds how many chunks are in flight at once.
CONCURRENCY = 5

# Probed once at startup, then cached.
#
# The minor bound was 4, which was wrong: CsrSeason13-5 through 13-11 all exist
# and were never probed, so the discovery pass could not have found them. They
# happen to be empty today - 343 serves them as future slots, and no player has
# a rank in any of them - so nothing was actually lost. But 343 resets CSR
# roughly every four months (Nov 18 2025, Mar 3 2026, Jul 7 2026), each reset
# opening the next minor, so 13-5 becomes real in due course and a bound of 4
# would have silently dropped it.
#
# Probing is cheap and one-off (cached in csr_seasons afterwards), and the space
# is irregular enough - 1-1 and 2-1 do not exist while 1-2, 2-2 and 2-3 do -
# that it has to be probed rather than computed. So set the bound well clear of
# anything 343 is likely to reach.
SEASON_MAJOR_MAX = 20
SEASON_MINOR_MAX = 16

# Marks a phase-B (current season) unit in csr_progress, where season_id has no
# meaningful value. Empty string rather than NULL so it participates in the PK.
CURRENT_SEASON = ""


@dataclass
class BackfillResult:
    seasons_discovered: int = 0
    playlists: int = 0
    players: int = 0
    requests: int = 0
    units_skipped: int = 0
    playlist_rows: int = 0
    season_rows: int = 0
    qualifying_pairs: int = 0
    failed_chunks: int = 0
    gone_chunks: int = 0
    per_phase: Dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# output database
# ---------------------------------------------------------------------------

def _open_output(path: str) -> sqlite3.Connection:
    """The standalone result file. Mirrors the two live tables exactly so the
    merge is a plain INSERT ... SELECT with no column mapping."""
    conn = sqlite3.connect(path, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS player_playlist_csr (
            xuid TEXT NOT NULL,
            playlist_asset_id TEXT NOT NULL,
            current_csr INTEGER,
            current_tier TEXT,
            current_sub_tier INTEGER,
            all_time_max INTEGER,
            last_updated TEXT NOT NULL,
            PRIMARY KEY (xuid, playlist_asset_id)
        );
        CREATE TABLE IF NOT EXISTS player_csr_season (
            xuid TEXT NOT NULL,
            playlist_asset_id TEXT NOT NULL,
            season_id TEXT NOT NULL,
            csr INTEGER,
            tier TEXT,
            sub_tier INTEGER,
            season_max INTEGER,
            last_updated TEXT NOT NULL,
            PRIMARY KEY (xuid, playlist_asset_id, season_id)
        );
        -- One row per completed chunk. Written in the same transaction as the
        -- data it covers, so "recorded" and "durable" cannot disagree.
        CREATE TABLE IF NOT EXISTS csr_progress (
            playlist_asset_id TEXT NOT NULL,
            season_id TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            done_at TEXT NOT NULL,
            PRIMARY KEY (playlist_asset_id, season_id, chunk_index)
        );
        CREATE TABLE IF NOT EXISTS csr_seasons (
            season_id TEXT PRIMARY KEY,
            discovered_at TEXT NOT NULL
        );
    """)
    conn.commit()
    return conn


def _completed_units(conn: sqlite3.Connection) -> set:
    return {(r["playlist_asset_id"], r["season_id"], r["chunk_index"])
            for r in conn.execute("SELECT * FROM csr_progress")}


# ---------------------------------------------------------------------------
# inputs, read from the live database read-only
# ---------------------------------------------------------------------------

def _ranked_players(db_path: str, limit: Optional[int]) -> List[str]:
    """Players with ranked games. Ordered, because chunk_index is only a
    stable identity across runs if the list is stable."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=60)
    conn.execute("PRAGMA query_only=ON")
    sql = ("SELECT xuid FROM player_mode_stats "
           "WHERE game_mode='ranked' AND games_played > 0 ORDER BY xuid")
    if limit:
        sql += f" LIMIT {int(limit)}"
    out = [str(r[0]) for r in conn.execute(sql)]
    conn.close()
    return out


def _ranked_playlists(db_path: str) -> List[str]:
    """Resolved ranked playlists, plus the two hardcoded ids that never get a
    metadata row because the classifier short-circuits them."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=60)
    conn.execute("PRAGMA query_only=ON")
    ids = {r[0] for r in conn.execute(
        "SELECT playlist_asset_id FROM playlist_metadata "
        "WHERE resolution_status='resolved' AND is_ranked=1")}
    conn.close()
    ids |= set(HaloAPIClient.RANKED_PLAYLIST_IDS)
    return sorted(ids)


def _load_cached_spartan_accounts() -> List[Dict]:
    """Whatever valid Spartan tokens already exist on disk, with no refresh and
    no interactive login - this is a batch job and must never block on a
    browser popup. Same helper shape as reclassify_playlists_backfill."""
    accounts = []
    for i in range(1, 6):
        cache = safe_read_json(get_token_cache_path(i), default={})
        spartan = cache.get("spartan") if cache else None
        if spartan and is_token_valid(spartan):
            accounts.append({'id': f'account{i}', 'token': spartan.get("token"),
                             'name': f'Account {i}'})
    return accounts


# ---------------------------------------------------------------------------
# phase A - which seasons exist
# ---------------------------------------------------------------------------

async def _discover_seasons(client: HaloAPIClient, out: sqlite3.Connection,
                            playlist_id: str, sample: List[str]) -> List[str]:
    cached = [r["season_id"] for r in out.execute(
        "SELECT season_id FROM csr_seasons ORDER BY season_id")]
    if cached:
        # The cache is keyed on nothing but its own existence, so a run that
        # discovered under an older, narrower bound would keep reporting that
        # narrower answer forever. Re-probe when the cache predates the bounds.
        print(f"[CSR] Using {len(cached)} cached season ids "
              f"(delete from csr_seasons to re-probe after a bound change)")
        return cached

    print("[CSR] Discovering season ids (the space is irregular, so probe it)...")
    found: List[str] = []
    for major in range(1, SEASON_MAJOR_MAX + 1):
        for minor in range(1, SEASON_MINOR_MAX + 1):
            season = f"CsrSeason{major}-{minor}"
            got = await client._fetch_skill(
                f"{client.SKILL_URL}/hi/playlist/{playlist_id}/csrs",
                sample, [("season", season)])
            if got is not None:
                found.append(season)

    now = datetime.now().isoformat()
    out.executemany("INSERT OR REPLACE INTO csr_seasons VALUES (?, ?)",
                    [(s, now) for s in found])
    out.commit()
    print(f"[CSR] {len(found)} seasons: {', '.join(found)}")
    return found


# ---------------------------------------------------------------------------
# the work
# ---------------------------------------------------------------------------

def _chunks(items: Sequence[str], size: int) -> List[List[str]]:
    return [list(items[i:i + size]) for i in range(0, len(items), size)]


def _persist(out: sqlite3.Connection, playlist: str, season: str,
             chunk_index: int, found: Dict[str, Dict]) -> Tuple[int, int]:
    """Write one chunk's rows and its progress marker atomically.

    The progress row goes in the SAME transaction as the data, so a crash can
    never leave a chunk marked done with its rows missing.
    """
    now = datetime.now().isoformat()
    playlist_rows = season_rows = 0
    with out:  # implicit transaction; commits on clean exit
        if season == CURRENT_SEASON:
            for xuid, v in found.items():
                # Only players with a history here are worth a row. A player
                # who has never ranked in this playlist has nothing to show.
                if v.get("all_time_max") is None and v.get("csr") is None:
                    continue
                out.execute("""INSERT OR REPLACE INTO player_playlist_csr
                    (xuid, playlist_asset_id, current_csr, current_tier,
                     current_sub_tier, all_time_max, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (xuid, playlist, v.get("csr"), v.get("tier"),
                     v.get("sub_tier"), v.get("all_time_max"), now))
                playlist_rows += 1
        else:
            for xuid, v in found.items():
                # Absence is the signal for "did not play ranked that season",
                # so a row without a CSR would be noise.
                if v.get("csr") is None:
                    continue
                out.execute("""INSERT OR REPLACE INTO player_csr_season
                    (xuid, playlist_asset_id, season_id, csr, tier, sub_tier,
                     season_max, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (xuid, playlist, season, v.get("csr"), v.get("tier"),
                     v.get("sub_tier"), v.get("season_max"), now))
                season_rows += 1
        out.execute("INSERT OR REPLACE INTO csr_progress VALUES (?, ?, ?, ?)",
                    (playlist, season, chunk_index, now))
    return playlist_rows, season_rows


_last_token_reload = 0.0
_token_reload_lock: Optional[asyncio.Lock] = None


async def _reload_tokens(client: HaloAPIClient) -> bool:
    """Re-read the Spartan tokens the bot keeps refreshed on disk.

    The backfill snapshots tokens once at startup and holds them for the whole
    run. A full sweep takes over an hour, Spartan tokens do not last that long,
    and the bot rewrites the caches as it refreshes them - so the run carries on
    presenting an in-memory copy that expired, and every request 401s from there
    on. Measured: a single sweep lost 18,099 chunks that way, then 10,664 on the
    next pass, converging only because each re-run re-read the cache at startup.

    Reloading on the first 401 collapses that into one run. Rate-limited so a
    burst of concurrent 401s reloads once, not once per chunk.
    """
    global _last_token_reload, _token_reload_lock
    if _token_reload_lock is None:
        _token_reload_lock = asyncio.Lock()
    async with _token_reload_lock:
        now = time.monotonic()
        if now - _last_token_reload < 30:
            return False          # someone else just did it; use theirs
        accounts = _load_cached_spartan_accounts()
        if not accounts:
            return False
        client.spartan_accounts = accounts
        from src.api.rate_limiters import halo_stats_rate_limiter
        halo_stats_rate_limiter.set_num_accounts(len(accounts))
        _last_token_reload = now
        print(f"[CSR] reloaded {len(accounts)} Spartan token(s) from cache")
        return True


async def _run_units(client: HaloAPIClient, out: sqlite3.Connection,
                     units: List[Tuple[str, str, int, List[str]]],
                     result: BackfillResult, label: str) -> None:
    """Fetch units concurrently, persist them serially.

    Writes are serialised behind a lock rather than done concurrently: SQLite
    would serialise them anyway, and doing it explicitly keeps the transaction
    boundaries in _persist meaningful.
    """
    if not units:
        print(f"[CSR] {label}: nothing to do")
        return

    sem = asyncio.Semaphore(CONCURRENCY)
    write_lock = asyncio.Lock()
    done = 0
    failed = 0
    gone = 0
    total = len(units)

    async def one(unit):
        nonlocal done, failed, gone
        playlist, season, idx, players = unit

        async def attempt():
            return await client.get_playlist_csr(
                playlist, players,
                season_id=season or None,
                include_unranked=(season == CURRENT_SEASON),
                strict=True,
            )

        async with sem:
            try:
                found = await attempt()
            except SkillFetchError as exc:
                if exc.permanent:
                    # HTTP 404: the playlist has no CSR ladder at all, or the
                    # season is retired. It will never answer, so mark it done
                    # with no rows - here the absence IS the answer. Retrying
                    # is why a sweep could never reach zero failures: the
                    # retired playlists (Survivors, FFA, Squad Battle, and the
                    # rotated asset ids) 404 on every single attempt, so every
                    # run burned thousands of requests re-asking them.
                    gone += 1
                    found = {}
                else:
                    try:
                        # Overwhelmingly this is tokens ageing out mid-run, so
                        # re-read them and try once more before writing it off.
                        if not await _reload_tokens(client):
                            raise exc
                        found = await attempt()
                    except SkillFetchError as exc2:
                        # Deliberately do NOT persist. _persist writes the
                        # progress marker in the same transaction as the rows,
                        # so recording a failed chunk would mark it done with
                        # nothing in it - and the resume logic would skip it
                        # forever. Those players would show "no CSR that
                        # season" with nothing anywhere to say the answer was
                        # never actually received. Leave it incomplete.
                        failed += 1
                        print(f"[CSR] {label}: chunk {idx} FAILED, left for a "
                              f"later run - {exc2}")
                        return
        result.requests += 1
        async with write_lock:
            p_rows, s_rows = _persist(out, playlist, season, idx, found)
            result.playlist_rows += p_rows
            result.season_rows += s_rows
            done += 1
            if done % 50 == 0 or done == total:
                print(f"[CSR] {label}: {done}/{total} chunks "
                      f"(+{result.playlist_rows} current, +{result.season_rows} season)")

    await asyncio.gather(*(one(u) for u in units))
    result.per_phase[label] = done
    result.failed_chunks += failed
    result.gone_chunks += gone
    if gone:
        print(f"[CSR] {label}: {gone} chunk(s) 404'd (no ladder / retired "
              f"season) and are recorded as empty, not retried.")
    if failed:
        print(f"[CSR] {label}: {failed} chunk(s) got no answer and were NOT "
              f"marked done. Re-run to fill them.")


async def backfill_csr(db_path: Optional[str] = None,
                       out_path: Optional[str] = None,
                       playlists: Optional[Sequence[str]] = None,
                       limit_players: Optional[int] = None) -> BackfillResult:
    from src.config import DATA_DIR

    db_path = db_path or str(Path(DATA_DIR) / "halo_stats_v2.db")
    out_path = out_path or str(Path.home() / "csr_backfill.db")
    result = BackfillResult()

    client = HaloAPIClient()
    client.spartan_accounts = _load_cached_spartan_accounts()
    if not client.spartan_accounts:
        raise RuntimeError(
            "No valid cached Spartan tokens in data/auth/token_cache*.json - "
            "start the bot once so it refreshes them, then re-run.")
    from src.api.rate_limiters import halo_stats_rate_limiter
    halo_stats_rate_limiter.set_num_accounts(len(client.spartan_accounts))
    print(f"[CSR] {len(client.spartan_accounts)} Spartan account(s)")

    players = _ranked_players(db_path, limit_players)
    playlist_ids = list(playlists) if playlists else _ranked_playlists(db_path)
    result.players = len(players)
    result.playlists = len(playlist_ids)
    print(f"[CSR] {len(players):,} ranked players x {len(playlist_ids)} playlists")
    if not players or not playlist_ids:
        return result

    out = _open_output(out_path)
    print(f"[CSR] Output: {out_path}")

    seasons = await _discover_seasons(client, out, playlist_ids[0], players[:8])
    result.seasons_discovered = len(seasons)

    done_units = _completed_units(out)
    chunk_size = HaloAPIClient.PLAYLIST_CSR_BATCH_MAX

    # ---- phase B: current season, everyone -------------------------------
    scope_units = []
    for playlist in playlist_ids:
        for idx, chunk in enumerate(_chunks(players, chunk_size)):
            if (playlist, CURRENT_SEASON, idx) in done_units:
                result.units_skipped += 1
                continue
            scope_units.append((playlist, CURRENT_SEASON, idx, chunk))
    await _run_units(client, out, scope_units, result, "scope")

    # ---- phase C: the rest of the seasons, qualifying players only -------
    sweep_units = []
    for playlist in playlist_ids:
        qualifying = [r[0] for r in out.execute(
            "SELECT xuid FROM player_playlist_csr WHERE playlist_asset_id = ? "
            "ORDER BY xuid", (playlist,))]
        result.qualifying_pairs += len(qualifying)
        if not qualifying:
            continue
        for season in seasons:
            for idx, chunk in enumerate(_chunks(qualifying, chunk_size)):
                if (playlist, season, idx) in done_units:
                    result.units_skipped += 1
                    continue
                sweep_units.append((playlist, season, idx, chunk))
    await _run_units(client, out, sweep_units, result, "sweep")

    out.close()
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db-path", help="live halo_stats_v2.db (read-only)")
    ap.add_argument("--out", help="output SQLite file (default ~/csr_backfill.db)")
    ap.add_argument("--playlists", nargs="*", help="restrict to these asset ids")
    ap.add_argument("--limit-players", type=int,
                    help="cap the player list; use for a dry run")
    args = ap.parse_args()

    r = asyncio.run(backfill_csr(db_path=args.db_path, out_path=args.out,
                                 playlists=args.playlists,
                                 limit_players=args.limit_players))
    print("\n" + "=" * 60)
    print(f"  seasons discovered : {r.seasons_discovered}")
    print(f"  playlists x players: {r.playlists} x {r.players:,}")
    print(f"  requests issued    : {r.requests:,}")
    print(f"  units skipped      : {r.units_skipped:,} (already done)")
    print(f"  404 chunks         : {r.gone_chunks:,} (no ladder/retired; recorded empty)")
    print(f"  FAILED chunks      : {r.failed_chunks:,} (no answer; re-run to fill)")
    print(f"  qualifying pairs   : {r.qualifying_pairs:,}")
    print(f"  current-rank rows  : {r.playlist_rows:,}")
    print(f"  season rows        : {r.season_rows:,}")
    print("=" * 60)
    print("Now merge: python -m src.jobs.csr_merge")
    return 0


if __name__ == "__main__":
    sys.exit(main())
