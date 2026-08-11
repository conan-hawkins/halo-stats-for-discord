#!/usr/bin/env python3
"""Second-round probes: characterise the failures the first round found.

Round one established that /matches/count exists but does NOT equal the length
of the list /matches paginates. That is only actionable if we know *how* they
differ - a stable lower bound is usable for sizing a crawl window, noise is
not. Likewise "?type= is validated" (a bogus value 400s) is not the same as
"?type= actually filters", which needs a case where the filtered list is
demonstrably shorter.

Read-only. Same pacing and token-verification rules as endpoint_probe.py.
"""

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

USER_AGENT = "HaloWaypoint/2021.01.10.01"
STATS = "https://halostats.svc.halowaypoint.com"
SKILL = "https://skill.svc.halowaypoint.com"
PROFILE = "https://profile.svc.halowaypoint.com"

_DELAY = 1.0
_REQUESTS = 0
_RATE_LIMITED = 0


def _get(url: str, token: str, params: Optional[List[Tuple[str, str]]] = None):
    global _REQUESTS, _RATE_LIMITED
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("x-343-authorization-spartan", token)
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Accept", "application/json")
    _REQUESTS += 1
    try:
        with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=30) as r:
            return r.status, json.loads(r.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        if e.code == 429:
            _RATE_LIMITED += 1
        body = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(body)
        except json.JSONDecodeError:
            return e.code, body
    except Exception as e:  # noqa: BLE001
        return -1, f"{type(e).__name__}: {e}"
    finally:
        time.sleep(_DELAY)


def _rows(token: str, xuid: str, start: int, count: int = 1, mtype: Optional[str] = None) -> int:
    """Number of rows at an offset, or -1 if the request failed."""
    params = [("start", str(start)), ("count", str(count))]
    if mtype:
        params.append(("type", mtype))
    status, payload = _get(f"{STATS}/hi/players/xuid({xuid})/matches", token, params)
    if status != 200 or not isinstance(payload, dict):
        return -1
    return len(payload.get("Results", []))


def list_length(token: str, xuid: str, mtype: Optional[str], hi: int = 32768) -> Optional[int]:
    """Exact length of a match list, by binary search on the last populated offset.

    Cheaper than crawling: ~15 single-row requests for a history of any size.
    Invariant: rows(start) >= 1 for start < length, 0 for start >= length.
    """
    if _rows(token, xuid, 0, 1, mtype) < 1:
        return 0
    lo = 0                      # known populated
    while _rows(token, xuid, hi, 1, mtype) >= 1:
        lo = hi
        hi *= 2
        if hi > 2 ** 20:
            return None
    while hi - lo > 1:
        mid = (lo + hi) // 2
        n = _rows(token, xuid, mid, 1, mtype)
        if n < 0:
            return None
        if n >= 1:
            lo = mid
        else:
            hi = mid
    return lo + 1


def main() -> int:
    global _DELAY
    ap = argparse.ArgumentParser()
    ap.add_argument("--xuid", required=True)
    ap.add_argument("--pool", default="/tmp/xuid_pool.json",
                    help="JSON list of xuids for the batch ceiling test")
    ap.add_argument("--delay", type=float, default=1.0)
    args = ap.parse_args()
    _DELAY = args.delay
    xuid = args.xuid

    # Reuse round one's token selection: first cache that really authenticates.
    token = None
    import glob as _glob
    for path in sorted(_glob.glob("/app/data/auth/token_cache*.json")):
        try:
            cache = json.load(open(path))
        except Exception:  # noqa: BLE001
            continue
        cand = (cache.get("spartan") or {}).get("token")
        if cand and _rows(cand, xuid, 0, 1) >= 0:
            status, _ = _get(f"{STATS}/hi/players/xuid({xuid})/matches", cand,
                             [("start", "0"), ("count", "1")])
            if status == 200:
                token = cand
                print(f"Token: {os.path.basename(path)}\n")
                break
    if not token:
        print("no working token", file=sys.stderr)
        return 2

    # -- S1: how do the counts relate to the real list lengths? --------------
    print("=" * 78)
    print("S1  count endpoint vs. true list length")
    print("=" * 78)
    status, counts = _get(f"{STATS}/hi/players/xuid({xuid})/matches/count", token)
    print(f"    /matches/count -> {json.dumps(counts)}")
    lengths = {}
    for mtype in (None, "matchmaking", "custom", "local"):
        n = list_length(token, xuid, mtype)
        lengths[mtype or "all"] = n
        print(f"    true length type={mtype or '(absent)':12s} = {n}")
    if isinstance(counts, dict):
        pairs = [
            ("all", "MatchesPlayedCount"),
            ("matchmaking", "MatchmadeMatchesPlayedCount"),
            ("custom", "CustomMatchesPlayedCount"),
            ("local", "LocalMatchesPlayedCount"),
        ]
        print()
        for key, cfield in pairs:
            actual, claimed = lengths.get(key), counts.get(cfield)
            if isinstance(actual, int) and isinstance(claimed, int):
                delta = actual - claimed
                rel = "count is a LOWER bound" if delta >= 0 else "count OVERSTATES the list"
                print(f"    {cfield:32s} claimed={claimed:6d} actual={actual:6d} "
                      f"delta={delta:+d}  ({rel})")
        s = sum(lengths.get(k) or 0 for k in ("matchmaking", "custom", "local"))
        print(f"    filtered lengths sum to {s}; unfiltered list is {lengths.get('all')} "
              f"-> partitions {'cover' if s == lengths.get('all') else 'DO NOT cover'} the list")

    # -- S2: service record subqueries as a playlist/ranked source -----------
    print("\n" + "=" * 78)
    print("S2  service record Subqueries (candidate replacement for RANKED_PLAYLIST_IDS)")
    print("=" * 78)
    status, sr = _get(f"{STATS}/hi/players/xuid({xuid})/Matchmade/servicerecord", token)
    ranked_playlists = []
    if status == 200 and isinstance(sr, dict):
        sub = sr.get("Subqueries") or {}
        print(f"    top-level keys: {sorted(sr.keys())}")
        print(f"    SeasonIds ({len(sub.get('SeasonIds') or [])}): "
              f"{(sub.get('SeasonIds') or [])[:4]} ...")
        print(f"    GameVariantCategories: {sub.get('GameVariantCategories')}")
        print(f"    GameplayInteractions: {sub.get('GameplayInteractions')}")
        print(f"    IsRanked: {sub.get('IsRanked')}")
        pls = sub.get("PlaylistAssetIds") or []
        print(f"    PlaylistAssetIds ({len(pls)}): {pls[:6]}")

        # Does isRanked=true actually narrow the playlist set? That is the
        # property that would let us stop hardcoding ranked asset IDs.
        season = (sub.get("SeasonIds") or [None])[-1]
        if season:
            st, ranked_sr = _get(
                f"{STATS}/hi/players/xuid({xuid})/Matchmade/servicerecord", token,
                [("seasonId", season), ("gameVariantCategory", "6"), ("isRanked", "true")])
            print(f"\n    filtered (season={season}, category=6, isRanked=true) -> HTTP {st}")
            if st == 200 and isinstance(ranked_sr, dict):
                rsub = ranked_sr.get("Subqueries") or {}
                ranked_playlists = rsub.get("PlaylistAssetIds") or []
                print(f"      MatchesCompleted={ranked_sr.get('MatchesCompleted')} "
                      f"Wins={ranked_sr.get('Wins')}")
                print(f"      ranked PlaylistAssetIds ({len(ranked_playlists)}): "
                      f"{ranked_playlists[:6]}")

    # -- S3: playlist CSR, using a genuinely ranked playlist -----------------
    print("\n" + "=" * 78)
    print("S3  skill.svc playlist CSR")
    print("=" * 78)
    for pid in (ranked_playlists or [])[:2] or ["edfef3ac-9cbe-4fa2-b949-8f29deafd483"]:
        st, payload = _get(f"{SKILL}/hi/playlist/{pid}/csrs", token,
                           [("players", f"xuid({xuid})")])
        detail = ""
        if st == 200 and isinstance(payload, dict):
            val = (payload.get("Value") or [{}])[0].get("Result", {})
            cur = val.get("Current") or {}
            detail = (f"csr={cur.get('Value')} tier={cur.get('Tier')} "
                      f"subtier={cur.get('SubTier')} season_max="
                      f"{(val.get('SeasonMax') or {}).get('Value')}")
        else:
            detail = str(payload)[:160]
        print(f"    playlist {pid} -> HTTP {st}  {detail}")

    # -- S4: profile batch ceiling ------------------------------------------
    print("\n" + "=" * 78)
    print("S4  profile.svc /users?xuids= batch ceiling")
    print("=" * 78)
    pool: List[str] = []
    if os.path.exists(args.pool):
        pool = json.load(open(args.pool))
    if not pool:
        print("    no xuid pool available; skipped")
    else:
        for size in (1, 10, 25, 50, 100, 200, 300):
            if size > len(pool):
                break
            batch = pool[:size]
            t0 = time.monotonic()
            st, payload = _get(f"{PROFILE}/users", token, [("xuids", x) for x in batch])
            ms = (time.monotonic() - t0) * 1000
            got = len(payload) if isinstance(payload, list) else -1
            note = ""
            if st == 200 and got < size:
                note = "  <- fewer returned than sent (silent truncation or unknown xuids)"
            print(f"    sent={size:4d} -> HTTP {st} returned={got:4d} ({ms:.0f}ms){note}")
            if st != 200:
                print(f"         body={str(payload)[:160]}")
                break

    print("\n" + "=" * 78)
    print(f"{_REQUESTS} requests, {_RATE_LIMITED} rate-limited")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
