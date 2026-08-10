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

Security: bound to 127.0.0.1 only, and every request must present the shared
INTERNAL_STATS_REFRESH_TOKEN (loopback API->bot auth), compared with
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
            return await api_client.get_player_stats(
                gamertag,
                "overall",
                matches_to_process=None,   # None => full incremental history
                force_full_fetch=False,    # respect the shared freshness signal
                xuid=xuid,
            )
        except Exception as e:  # defensive: never leak a raw stacktrace to HTTP
            return {"error": 4, "message": f"Refresh failed: {e}"}


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
            _inflight[key] = task
    try:
        return await task
    finally:
        async with _inflight_lock:
            if _inflight.get(key) is task:
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
            return web.json_response(
                {"ok": True, "skipped": True, "reason": "fresh", "age_seconds": int(age)},
                status=200,
            )

    result = await _refresh_coalesced(gamertag, xuid)

    if result.get("_rate_limited"):
        # Graceful: the page keeps showing cached data instead of erroring.
        return web.json_response(
            {"ok": True, "skipped": True, "reason": "rate_limited"}, status=200
        )

    error = result.get("error", 4)
    if error == 0:
        matches_processed = new_matches = None
        m = _CACHE_INFO_RE.search(result.get("cache_info", "") or "")
        if m:
            matches_processed, new_matches = int(m.group(1)), int(m.group(2))
        return web.json_response(
            {
                "ok": True,
                "refreshed": True,
                "matches_processed": matches_processed,
                "new_matches": new_matches,
                "age_seconds": 0,
            },
            status=200,
        )
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
    site = web.TCPSite(runner, host="127.0.0.1", port=settings.INTERNAL_API_PORT)
    await site.start()

    _web_server_started = True
    print(f"✓ Internal stats refresh API on http://127.0.0.1:{settings.INTERNAL_API_PORT}")
