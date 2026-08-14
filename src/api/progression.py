"""Career rank and Xbox gamerpic upkeep.

Both are per-player facts that live outside match history, and they go stale for
DIFFERENT reasons - which is why they refresh on different triggers:

  career rank  changes only when the player earns XP, i.e. only when they play.
               Refreshed when a refresh actually brought in new matches, so an
               idle player costs nothing and an active one is never wrong.

  gamerpic     changes whenever the player changes their Xbox avatar, which has
               nothing to do with playing. A match-driven trigger would leave
               a player who never plays again showing a years-old avatar, so it
               refreshes on page load instead, throttled by age.

Both are written by the bot rather than read live by the API service, because
that service holds no Halo credentials and cannot resolve either itself.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

# A page load should not re-resolve an avatar that was checked minutes ago; a
# gamerpic changes rarely and the request is not free. Long enough to collapse
# a browsing session into one lookup, short enough that a changed avatar shows
# up the same day.
GAMERPIC_MAX_AGE = timedelta(hours=12)


def _fresh(updated_at: Optional[str], max_age: timedelta) -> bool:
    if not updated_at:
        return False
    try:
        return datetime.now() - datetime.fromisoformat(updated_at) < max_age
    except (TypeError, ValueError):
        return False


def read_state(conn: sqlite3.Connection, xuid: str) -> Dict:
    row = conn.execute(
        "SELECT gamerpic, gamerpic_updated_at, career_rank, career_rank_updated_at"
        "  FROM players WHERE xuid = ?", (str(xuid),)).fetchone()
    if not row:
        return {}
    keys = ("gamerpic", "gamerpic_updated_at", "career_rank", "career_rank_updated_at")
    return dict(zip(keys, row))


def gamerpic_is_stale(conn: sqlite3.Connection, xuid: str) -> bool:
    state = read_state(conn, xuid)
    if not state:
        return False                      # unknown player; nothing to attach to
    return not _fresh(state.get("gamerpic_updated_at"), GAMERPIC_MAX_AGE)


async def refresh_gamerpics(client, conn: sqlite3.Connection,
                            xuids: Iterable[str]) -> int:
    """Resolve and store avatars. Returns how many were written."""
    wanted = [str(x) for x in dict.fromkeys(xuids) if x]
    if not wanted:
        return 0
    resolved = await client.get_gamerpics(wanted)
    if not resolved:
        return 0
    now = datetime.now().isoformat()
    with conn:
        conn.executemany(
            "UPDATE players SET gamerpic = ?, gamerpic_updated_at = ? WHERE xuid = ?",
            [(url, now, xuid) for xuid, url in resolved.items()])
    return len(resolved)


async def refresh_career_ranks(client, conn: sqlite3.Connection,
                               xuids: Iterable[str]) -> int:
    """Fetch and store career ranks. Returns how many real ranks were written.

    Three outcomes, and they must not be conflated:

      a real rank      stored, with the timestamp
      no career rank   the endpoint answered but omitted this player, or gave
                       the rank-0 sentinel. Stored as NULL WITH a timestamp, so
                       we record that we asked and stop asking every run.
      request failed   nothing written at all. strict=True makes the client
                       raise rather than return an empty dict, because
                       otherwise a 401 looks exactly like "nobody has a rank"
                       and would stamp the whole batch as answered - which is
                       precisely how the CSR backfill destroyed 13,889 rows.
    """
    wanted = [str(x) for x in dict.fromkeys(xuids) if x]
    if not wanted:
        return 0

    resolved = await client.get_career_ranks(wanted, strict=True)

    now = datetime.now().isoformat()
    absent = [x for x in wanted if x not in resolved]
    with conn:
        if resolved:
            conn.executemany(
                "UPDATE players SET career_rank = ?, career_partial_progress = ?,"
                "       career_rank_updated_at = ? WHERE xuid = ?",
                [(v["rank"], v.get("partial_progress"), now, xuid)
                 for xuid, v in resolved.items()])
        if absent:
            conn.executemany(
                "UPDATE players SET career_rank = NULL,"
                "       career_partial_progress = NULL,"
                "       career_rank_updated_at = ? WHERE xuid = ?",
                [(now, xuid) for xuid in absent])
    return len(resolved)


async def on_player_viewed(client, conn: sqlite3.Connection, xuid: str,
                           new_matches: Optional[int]) -> Dict[str, int]:
    """Upkeep for one player after a page-load refresh.

    `new_matches` comes from the stats refresh that just ran: None means we
    could not tell, in which case the career rank is left alone rather than
    re-fetched on every page load for a player who has not touched the game.
    """
    done = {"gamerpic": 0, "career_rank": 0}
    try:
        if gamerpic_is_stale(conn, xuid):
            done["gamerpic"] = await refresh_gamerpics(client, conn, [xuid])

        state = read_state(conn, xuid)
        never_fetched = state and not state.get("career_rank_updated_at")
        if (new_matches or 0) > 0 or never_fetched:
            try:
                done["career_rank"] = await refresh_career_ranks(client, conn, [xuid])
            except Exception as e:
                # A failed fetch writes nothing, so the next view retries. Only
                # an actual answer is ever recorded.
                print(f"[PROGRESSION] career rank fetch failed for {xuid}: {e}")
    except Exception as e:
        # Upkeep must never break the refresh the page is actually waiting on.
        print(f"[PROGRESSION] upkeep failed for {xuid}: {e}")
    return done


def seed_rank_definitions(conn: sqlite3.Connection, ranks: List[Dict]) -> int:
    """Store the static rank table. Idempotent."""
    def text(value):
        if isinstance(value, dict):
            return value.get("value")
        return value if isinstance(value, str) else (
            str(value) if value is not None else None)

    rows = []
    for r in ranks:
        rank = r.get("Rank")
        if not isinstance(rank, int):
            continue
        rows.append((
            rank,
            text(r.get("RankTitle")),
            text(r.get("RankSubTitle")),
            text(r.get("RankTier")),
            r.get("XpRequiredForRank"),
            r.get("RankIcon"),
            r.get("RankLargeIcon"),
            r.get("RankAdornmentIcon"),
        ))
    if not rows:
        return 0
    with conn:
        conn.executemany(
            "INSERT OR REPLACE INTO career_rank_defs"
            " (rank, title, subtitle, tier, xp_required, icon_path,"
            "  large_icon_path, adornment_icon_path)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    return len(rows)
