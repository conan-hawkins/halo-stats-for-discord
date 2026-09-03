"""Loopback-only internal endpoint that lets the stats website trigger a live
Halo refresh through the bot.

Why it lives inside the bot (not the API): the bot already owns the single
DB-writer thread, the Halo rate limiters, AND the global freshness signal
(`_history_checked_at`). Running this on the bot's own event loop means
web-triggered refreshes reuse exactly that machinery.

Refresh is **freshness-coalesced**: the website fires this on every player open
and every ~5 minutes per open tab, but a player is only actually fetched from
Halo if nobody (web OR Discord) has API-checked them within
WEB_AUTOREFRESH_FRESHNESS_SECONDS. That, plus a global per-minute fetch cap,
bounds Halo calls by *distinct stale players* rather than by viewer count - which
is what makes a public, no-token auto-refresh safe.

Security: binds to INTERNAL_API_HOST, which defaults to 127.0.0.1. Widen it only
when the port is confined to a private container network and is never published
to the host - the Docker deploy sets 0.0.0.0 and puts the bot and the stats API
alone on an `internal: true` compose network. Either way every request must
present the shared INTERNAL_STATS_REFRESH_TOKEN (API->bot auth), compared with
hmac.compare_digest.
"""
from __future__ import annotations

import asyncio
import hmac
import re
import time
from collections import deque

from aiohttp import web

from src.api.client import api_client
from src.config import settings

# Guard against starting twice on Discord reconnects (on_ready can fire again).
_web_server_started = False

# Bound concurrent live fetches; coalesce concurrent refreshes for the SAME
# gamertag into one fetch. Both created on the event loop in start_internal_api().
_refresh_semaphore: asyncio.Semaphore | None = None
_inflight: dict[str, asyncio.Task] = {}
_inflight_lock: asyncio.Lock | None = None

# monotonic timestamps of actual Halo fetches in the last minute (global cap).
# Only mutated inside _inflight_lock, so it needs no separate lock.
_global_fetch_times: deque[float] = deque()

# "Processed 42 matches (3 new)" -> (42, 3)
_CACHE_INFO_RE = re.compile(r"Processed\s+(\d+)\s+matches\s+\((\d+)\s+new\)")


def _token_ok(request: web.Request) -> bool:
    expected = settings.INTERNAL_STATS_REFRESH_TOKEN
    if not expected:
        return False
    supplied = request.headers.get("X-Internal-Token", "")
    return hmac.compare_digest(supplied, expected)


def _fetch_cap_ok_and_consume() -> bool:
    """True if a real fetch is within the global per-minute cap, consuming a slot.
    Caller must hold _inflight_lock."""
    now = time.monotonic()
    cutoff = now - 60.0
    while _global_fetch_times and _global_fetch_times[0] < cutoff:
        _global_fetch_times.popleft()
    if len(_global_fetch_times) >= settings.WEB_REFRESH_MAX_FETCHES_PER_MINUTE:
        return False
    _global_fetch_times.append(now)
    return True


async def _do_refresh(gamertag: str, xuid: str | None) -> dict:
    """One live fetch under the concurrency semaphore. force_full_fetch=False so
    it still honours the bot's own freshness logic; we only reach here when the
    web freshness window already judged the player stale. Never raises."""
    assert _refresh_semaphore is not None
    async with _refresh_semaphore:
        try:
            result = await api_client.get_player_stats(
                gamertag,
                "overall",
                matches_to_process=None,   # None => full incremental history
                force_full_fetch=False,    # respect the shared freshness signal
                xuid=xuid,
            )
        except Exception as e:  # defensive: never leak a raw stacktrace to HTTP
            return {"error": 4, "message": f"Refresh failed: {e}"}

    # Progression upkeep, OUTSIDE the semaphore so it never holds a live-fetch
    # slot, and after the fetch so it can see whether new matches arrived.
    await _refresh_progression(result, xuid)
    return result


async def _refresh_progression(result: dict, xuid: str | None) -> None:
    """Keep the avatar and career rank current for a player just looked at.

    The two refresh on different triggers because they go stale for different
    reasons. A gamerpic changes when the player changes their Xbox avatar, which
    has nothing to do with playing - so it refreshes on page load, throttled by
    age. A career rank only moves when XP is earned - so it refreshes only when
    this fetch actually brought new matches in, which costs nothing for the
    overwhelming majority of views, where nothing has changed.

    Never raises. It runs after the stats result is already in hand, so a
    failure here cannot affect the answer the page is waiting on, and leaves the
    stored values alone rather than clearing them.
    """
    if result.get("error"):
        return
    player_xuid = xuid or result.get("xuid")
    if not player_xuid:
        return

    new_matches = None
    m = _CACHE_INFO_RE.search(result.get("cache_info", "") or "")
    if m:
        new_matches = int(m.group(2))

    try:
        from src.api import progression
        from src.database.cache import get_cache
        conn = get_cache().db._get_connection()
        await progression.on_player_viewed(
            api_client, conn, str(player_xuid), new_matches)
    except Exception as e:
        print(f"[PROGRESSION] upkeep skipped for {player_xuid}: {e}")


def _log_refresh_task_end(task: asyncio.Task) -> None:
    """Say something when a refresh ends abnormally.

    _do_refresh catches Exception, but CancelledError is a BaseException and
    slips straight past it, so a cancelled crawl used to vanish with no log at
    all - the bot simply went quiet mid-crawl and nothing was ever saved. A
    background task that can die silently is a background task you cannot
    debug.
    """
    if task.cancelled():
        print("⚠ Refresh task was CANCELLED before it finished - nothing saved.")
        return
    exc = task.exception()
    if exc is not None:
        print(f"⚠ Refresh task died with {type(exc).__name__}: {exc}")


async def _refresh_coalesced(gamertag: str, xuid: str | None) -> dict:
    """One in-flight fetch per gamertag; concurrent callers await the same task.
    The global fetch cap is consumed only when a NEW fetch is actually started,
    so coalesced awaiters don't burn cap slots. Returns {"_rate_limited": True}
    when the cap is exhausted."""
    assert _inflight_lock is not None
    key = gamertag.lower()
    async with _inflight_lock:
        task = _inflight.get(key)
        if task is None or task.done():
            if not _fetch_cap_ok_and_consume():
                return {"_rate_limited": True}
            task = asyncio.create_task(_do_refresh(gamertag, xuid))
            task.add_done_callback(_log_refresh_task_end)
            _inflight[key] = task
    try:
        # SHIELDED. Awaiting a task normally forwards the awaiter's
        # cancellation into it, so a caller giving up would kill the crawl -
        # and with coalescing, kill it for every other caller too. The website
        # aborts at 250s and the API at 235s, while a first crawl of a player
        # with ~10,000 matches takes far longer than either, so the request
        # that starts such a crawl is guaranteed to walk away from it. It has
        # to keep running: the whole "it may still finish in the background"
        # contract, and the site's "Still updating..." state, depend on it.
        return await asyncio.shield(task)
    finally:
        async with _inflight_lock:
            # Only retire it if it really finished. Popping a still-running
            # task would let the next caller start a SECOND crawl of the same
            # player alongside the first.
            if _inflight.get(key) is task and task.done():
                _inflight.pop(key, None)


async def handle_refresh(request: web.Request) -> web.Response:
    if not _token_ok(request):
        return web.json_response(
            {"ok": False, "error_code": 1, "message": "unauthorized"}, status=401
        )

    try:
        body = await request.json()
    except Exception:
        body = {}
    gamertag = str(body.get("gamertag") or "").strip()
    if not gamertag:
        return web.json_response(
            {"ok": False, "error_code": 3, "message": "gamertag required"}, status=400
        )

    # Resolve xuid from the DB cache only (no API call). The gamertag came from
    # search, so it's in the DB; if somehow not, treat as stale and let the fetch
    # resolve it.
    try:
        xuid = api_client.stats_cache.resolve_xuid_by_gamertag(gamertag)
    except Exception:
        xuid = None

    # Freshness gate: served straight from cache, zero Halo calls, no cap consumed.
    if xuid:
        age = api_client.history_checked_age_seconds(xuid)
        if age is not None and age < settings.WEB_AUTOREFRESH_FRESHNESS_SECONDS:
            payload = {
                "ok": True, "skipped": True, "reason": "fresh", "age_seconds": int(age),
            }
            # Carry the last check's verdict on the skip. The gate is global, so
            # whoever opens a private player first consumes the one refresh that
            # would have reported "private" - without this, every other viewer
            # inside the freshness window is told nothing and their page cannot
            # explain the empty history. `last_reason` is separate from `reason`
            # because `reason` says why we skipped, and clients switch on it.
            #
            # Omitted when the check found a real history: reaching here proves a
            # check completed, so its absence is itself the answer ("not empty"),
            # which is what retires a stale notice on the site.
            last = api_client.last_history_visibility(xuid)
            if last in ("private", "no_games"):
                payload["last_reason"] = last
            return web.json_response(payload, status=200)

    result = await _refresh_coalesced(gamertag, xuid)

    if result.get("_rate_limited"):
        # Graceful: the page keeps showing cached data instead of erroring.
        # Unlike the freshness skip this proves nothing about a check having
        # run, so a missing verdict here means "we don't know" and the site is
        # expected to keep whatever it already had.
        payload = {"ok": True, "skipped": True, "reason": "rate_limited"}
        last = api_client.last_history_visibility(xuid) if xuid else None
        if last in ("private", "no_games"):
            payload["last_reason"] = last
        return web.json_response(payload, status=200)

    error = result.get("error", 4)
    if error == 0:
        matches_processed = new_matches = None
        m = _CACHE_INFO_RE.search(result.get("cache_info", "") or "")
        if m:
            matches_processed, new_matches = int(m.group(1)), int(m.group(2))
        # Only set when the history came back empty, and only to a value we
        # actually established: "private" (Xbox says we may not view this
        # player's game history) or "no_games" (we may, and there are none).
        # Absent means we could not tell - the website must then stay vague
        # rather than accuse a real account of being empty.
        payload = {
            "ok": True,
            "refreshed": True,
            "matches_processed": matches_processed,
            "new_matches": new_matches,
            "age_seconds": 0,
        }
        visibility = result.get("history_visibility")
        if visibility in ("private", "no_games"):
            payload["reason"] = visibility
        return web.json_response(payload, status=200)
    if error == 2:
        return web.json_response(
            {"ok": False, "error_code": 2, "message": result.get("message", "gamertag not found")},
            status=404,
        )
    if error == 4:
        return web.json_response(
            {"ok": False, "error_code": 4, "message": result.get("message", "upstream failure")},
            status=502,
        )
    return web.json_response(
        {"ok": False, "error_code": error, "message": result.get("message", "unexpected error")},
        status=500,
    )


async def start_internal_api() -> None:
    """Start the loopback internal API on the bot's event loop. Idempotent, and
    a no-op (with a warning) if the shared token is unset."""
    global _web_server_started, _refresh_semaphore, _inflight_lock
    if _web_server_started:
        return
    if not settings.INTERNAL_STATS_REFRESH_TOKEN:
        print("⚠ Internal stats refresh API NOT started: INTERNAL_STATS_REFRESH_TOKEN is unset.")
        return

    _refresh_semaphore = asyncio.Semaphore(settings.REFRESH_MAX_CONCURRENCY)
    _inflight_lock = asyncio.Lock()

    app = web.Application()
    app.router.add_post("/internal/refresh-player", handle_refresh)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=settings.INTERNAL_API_HOST, port=settings.INTERNAL_API_PORT)
    await site.start()

    _web_server_started = True
    print(
        f"✓ Internal stats refresh API on "
        f"http://{settings.INTERNAL_API_HOST}:{settings.INTERNAL_API_PORT}"
    )
