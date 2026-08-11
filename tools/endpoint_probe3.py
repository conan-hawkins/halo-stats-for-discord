#!/usr/bin/env python3
"""Third round: is each surviving candidate SAFE, not just present?

Round two killed ?type= and reduced /matches/count from "exact total" to
"some other number". What is left to establish:

  A. Is /matches/count a reliable LOWER bound on the match list? That is the
     only property that makes it usable for sizing a crawl window. One player
     showing count < length proves nothing; one player showing count > length
     would disqualify it outright, because sizing a window past the end of the
     list is exactly the 429 storm the slow-start exists to avoid.
     Two requests per player settle it - no binary search needed:
         rows(count-1) >= 1  =>  list is at least `count` long  (safe)
         rows(count-1) == 0  =>  count overstates the list       (unsafe)

  B. Where exactly does the profile batch cap fall, and does an unknown xuid
     poison the whole batch or just go missing?

  C. Can isRanked on the service record actually name the ranked playlists,
     or does it only filter aggregates? This decides whether
     RANKED_PLAYLIST_IDS can stop being hardcoded.

  D. Does skill.svc return real CSR for a player who plays ranked? The DB has
     almost no CSR recorded, so this is the difference between a real feature
     and a wasted request.

Read-only throughout.
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
from typing import List, Optional, Tuple

USER_AGENT = "HaloWaypoint/2021.01.10.01"
STATS = "https://halostats.svc.halowaypoint.com"
SKILL = "https://skill.svc.halowaypoint.com"
PROFILE = "https://profile.svc.halowaypoint.com"

_DELAY = 1.0
_REQUESTS = 0


def _get(url: str, token: str, params: Optional[List[Tuple[str, str]]] = None):
    global _REQUESTS
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
        body = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(body)
        except json.JSONDecodeError:
            return e.code, body
    except Exception as e:  # noqa: BLE001
        return -1, f"{type(e).__name__}: {e}"
    finally:
        time.sleep(_DELAY)


def _rows(token: str, xuid: str, start: int) -> int:
    st, p = _get(f"{STATS}/hi/players/xuid({xuid})/matches", token,
                 [("start", str(start)), ("count", "1")])
    if st != 200 or not isinstance(p, dict):
        return -1
    return len(p.get("Results", []))


def pick_token(xuid: str) -> Optional[str]:
    import glob as _glob
    for path in sorted(_glob.glob("/app/data/auth/token_cache*.json")):
        try:
            cache = json.load(open(path))
        except Exception:  # noqa: BLE001
            continue
        tok = (cache.get("spartan") or {}).get("token")
        if not tok:
            continue
        st, _ = _get(f"{STATS}/hi/players/xuid({xuid})/matches", tok,
                     [("start", "0"), ("count", "1")])
        if st == 200:
            print(f"Token: {os.path.basename(path)}\n")
            return tok
    return None


def main() -> int:
    global _DELAY
    ap = argparse.ArgumentParser()
    ap.add_argument("--xuids", required=True, help="comma-separated xuids for test A")
    ap.add_argument("--pool", default="/tmp/xuid_pool.json")
    ap.add_argument("--delay", type=float, default=1.0)
    args = ap.parse_args()
    _DELAY = args.delay

    xuids = [x.strip() for x in args.xuids.split(",") if x.strip()]
    token = pick_token(xuids[0])
    if not token:
        print("no working token", file=sys.stderr)
        return 2

    # ---- A: is count a safe lower bound, across players? -------------------
    print("=" * 78)
    print("A  /matches/count as a LOWER bound on the match list")
    print("=" * 78)
    print(f"    {'xuid':>18}  {'count':>7} {'mm':>7}  {'row@c-1':>8} {'row@c':>6}  verdict")
    unsafe = 0
    for x in xuids:
        st, counts = _get(f"{STATS}/hi/players/xuid({x})/matches/count", token)
        if st != 200 or not isinstance(counts, dict):
            print(f"    {x:>18}  HTTP {st}")
            continue
        n = counts.get("MatchesPlayedCount")
        mm = counts.get("MatchmadeMatchesPlayedCount")
        if not isinstance(n, int):
            print(f"    {x:>18}  no MatchesPlayedCount")
            continue
        if n == 0:
            at_last, at_n = 0, _rows(token, x, 0)
            verdict = "count=0 and list empty" if at_n == 0 else "count=0 but LIST HAS ROWS - unsafe"
            if at_n != 0:
                unsafe += 1
            print(f"    {x:>18}  {n:>7} {mm:>7}  {'-':>8} {at_n:>6}  {verdict}")
            continue
        at_last = _rows(token, x, n - 1)
        at_n = _rows(token, x, n)
        if at_last >= 1:
            verdict = "SAFE lower bound" + (" (list continues past count)" if at_n >= 1 else " (exact)")
        else:
            verdict = "UNSAFE - count overstates the list"
            unsafe += 1
        print(f"    {x:>18}  {n:>7} {mm:>7}  {at_last:>8} {at_n:>6}  {verdict}")
    print(f"\n    -> {'ALL SAFE' if not unsafe else f'{unsafe} player(s) UNSAFE'}")

    # ---- B: profile batch ceiling and unknown-xuid behaviour ---------------
    print("\n" + "=" * 78)
    print("B  profile.svc /users?xuids= ceiling and unknown-xuid handling")
    print("=" * 78)
    pool: List[str] = json.load(open(args.pool)) if os.path.exists(args.pool) else []
    if pool:
        for size in (100, 101, 128, 150):
            if size > len(pool):
                print(f"    sent={size:4d} -> pool too small")
                continue
            st, payload = _get(f"{PROFILE}/users", token, [("xuids", x) for x in pool[:size]])
            got = len(payload) if isinstance(payload, list) else -1
            print(f"    sent={size:4d} -> HTTP {st} returned={got}")
            if st != 200:
                break
        # One nonexistent xuid mixed into a good batch: dropped, or fatal?
        mixed = pool[:9] + ["1234567890123456"]
        st, payload = _get(f"{PROFILE}/users", token, [("xuids", x) for x in mixed])
        got = len(payload) if isinstance(payload, list) else -1
        print(f"    9 real + 1 bogus xuid -> HTTP {st} returned={got}  "
              f"({'bogus silently dropped' if st == 200 and got == 9 else 'see body'})")
        if st != 200:
            print(f"      body={str(payload)[:200]}")
    else:
        print("    no pool; skipped")

    # ---- C: can isRanked name the ranked playlists? ------------------------
    print("\n" + "=" * 78)
    print("C  service record isRanked -> can it enumerate ranked playlists?")
    print("=" * 78)
    x = xuids[0]
    for label, params in (
        ("category=6 only", [("gameVariantCategory", "6")]),
        ("category=6 isRanked=true", [("gameVariantCategory", "6"), ("isRanked", "true")]),
        ("category=6 isRanked=false", [("gameVariantCategory", "6"), ("isRanked", "false")]),
    ):
        st, sr = _get(f"{STATS}/hi/players/xuid({x})/Matchmade/servicerecord", token, params)
        if st != 200 or not isinstance(sr, dict):
            print(f"    {label:28s} -> HTTP {st} {str(sr)[:120]}")
            continue
        sub = sr.get("Subqueries") or {}
        pls = sub.get("PlaylistAssetIds") or []
        print(f"    {label:28s} -> matches={sr.get('MatchesCompleted')} "
              f"wins={sr.get('Wins')} playlists={len(pls)} {pls[:3]}")

    # ---- D: real CSR from skill.svc ---------------------------------------
    print("\n" + "=" * 78)
    print("D  skill.svc playlist CSR against known ranked playlists")
    print("=" * 78)
    ranked = [
        ("Ranked Arena", "edfef3ac-9cbe-4fa2-b949-8f29deafd483"),
        ("Ranked Tactical", "57e417dd-7366-4dda-9bdd-2802151d5e81"),
        ("Ranked FFA", "71734db4-4b8e-4682-9206-62b6eff92582"),
    ]
    found = False
    for x in xuids[:4]:
        for name, pid in ranked:
            st, payload = _get(f"{SKILL}/hi/playlist/{pid}/csrs", token,
                               [("players", f"xuid({x})")])
            if st != 200 or not isinstance(payload, dict):
                print(f"    {x} {name:16s} -> HTTP {st}")
                continue
            res = (payload.get("Value") or [{}])[0].get("Result", {})
            cur = res.get("Current") or {}
            allt = res.get("SeasonMax") or {}
            v = cur.get("Value")
            marker = ""
            if isinstance(v, int) and v > 0:
                found = True
                marker = "  <-- REAL CSR"
            print(f"    {x} {name:16s} -> csr={v} tier={cur.get('Tier')!r} "
                  f"season_max={allt.get('Value')}{marker}")
            if found:
                break
        if found:
            break
    if not found:
        print("    no player in this sample has ranked CSR in these playlists "
              "(csr=-1 means 'no data', not 'error')")

    print("\n" + "=" * 78)
    print(f"{_REQUESTS} requests")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
