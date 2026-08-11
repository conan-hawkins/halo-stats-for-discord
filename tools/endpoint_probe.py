#!/usr/bin/env python3
"""Validate candidate Halo API endpoints before committing code to them.

Read-only. Every request is a GET; nothing is written to the API, to the token
cache, or to the database. The Spartan token is read but never printed.

The point is not "does the endpoint return 200" - the 401-vs-404 probe already
answered that without a token. The point is whether each endpoint's *semantics*
match what the crawl would assume, because a wrong assumption here does not
fail loudly, it silently truncates match history or forces a full re-crawl on
every sync. Each test below prints its own verdict and the evidence for it.

Run it where a valid token already lives, e.g. inside the bot container:

    docker compose cp tools/endpoint_probe.py bot:/tmp/endpoint_probe.py
    docker compose exec bot python /tmp/endpoint_probe.py --xuid <XUID>

Requests are serialised with a delay because this process is NOT inside the
bot's in-process rate limiter - it spends from the same per-account budget the
live bot is using, without coordinating with it. Keep --delay >= 1.0 unless the
bot is stopped.

Stdlib only, so it runs anywhere the token does.
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

DEFAULT_TOKEN_CACHE_GLOBS = [
    "/app/data/auth/token_cache*.json",
    "data/auth/token_cache*.json",
    os.path.expanduser("~/halo-stats-for-discord/data/auth/token_cache*.json"),
]

# Filled in by main(); every request goes through _get so pacing and the 429
# tally are impossible to bypass by accident.
_DELAY = 1.0
_RATE_LIMITED = 0
_REQUESTS = 0


class Result:
    """One test's verdict plus the evidence a reader needs to disagree with it."""

    def __init__(self, name: str):
        self.name = name
        self.verdict = "UNKNOWN"
        self.detail = ""

    def set(self, verdict: str, detail: str) -> "Result":
        self.verdict = verdict
        self.detail = detail
        return self


def _get(url: str, token: str, params: Optional[List[Tuple[str, str]]] = None) -> Tuple[int, Any, float]:
    """GET a URL. Returns (status, parsed_json_or_text, elapsed_ms).

    Repeated query keys are passed as a list of pairs because both the skill
    and profile batch endpoints take `players=`/`xuids=` more than once, which
    a dict cannot express.
    """
    global _RATE_LIMITED, _REQUESTS
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Spartan {token}")
    req.add_header("x-343-authorization-spartan", token)
    req.add_header("User-Agent", USER_AGENT)
    req.add_header("Accept", "application/json")

    ctx = ssl.create_default_context()
    started = time.monotonic()
    _REQUESTS += 1
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            body = resp.read().decode("utf-8", "replace")
            elapsed = (time.monotonic() - started) * 1000
            try:
                return resp.status, json.loads(body), elapsed
            except json.JSONDecodeError:
                return resp.status, body, elapsed
    except urllib.error.HTTPError as e:
        elapsed = (time.monotonic() - started) * 1000
        if e.code == 429:
            _RATE_LIMITED += 1
        body = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(body), elapsed
        except json.JSONDecodeError:
            return e.code, body, elapsed
    except Exception as e:  # noqa: BLE001 - a probe must never abort the run
        elapsed = (time.monotonic() - started) * 1000
        return -1, f"{type(e).__name__}: {e}", elapsed
    finally:
        time.sleep(_DELAY)


def _match_ids(payload: Any) -> List[str]:
    if not isinstance(payload, dict):
        return []
    return [m.get("MatchId") for m in payload.get("Results", []) if isinstance(m, dict)]


def _matches(token: str, xuid: str, start: int, count: int, mtype: Optional[str] = None):
    params = [("start", str(start)), ("count", str(count))]
    if mtype is not None:
        params.append(("type", mtype))
    return _get(f"{STATS}/hi/players/xuid({xuid})/matches", token, params)


# --------------------------------------------------------------------------
# T1 - does /matches/count exist, and what is in it?
# --------------------------------------------------------------------------
def t1_match_count(token: str, xuid: str) -> Tuple[Result, Optional[Dict]]:
    r = Result("T1  /matches/count returns counts")
    status, payload, ms = _get(f"{STATS}/hi/players/xuid({xuid})/matches/count", token)
    if status != 200:
        return r.set("FAIL", f"HTTP {status}: {str(payload)[:200]}"), None
    if not isinstance(payload, dict):
        return r.set("FAIL", f"non-dict payload: {str(payload)[:200]}"), None
    # Do the parts sum to the whole? If they do not, the sub-counts mean
    # something other than "this many rows in that filtered list", and sizing a
    # filtered crawl from them would be wrong.
    total = payload.get("MatchesPlayedCount")
    parts = [payload.get(k) for k in ("MatchmadeMatchesPlayedCount",
                                      "CustomMatchesPlayedCount",
                                      "LocalMatchesPlayedCount")]
    summed = sum(p for p in parts if isinstance(p, int))
    sums_ok = isinstance(total, int) and summed == total
    r.set("PASS", f"{ms:.0f}ms  {json.dumps(payload)}  "
                  f"parts sum to total={sums_ok} ({summed} vs {total})")
    return r, payload


# --------------------------------------------------------------------------
# T2 - THE decisive one. Is MatchesPlayedCount exactly the length of the list
# that /matches paginates?
#
# This is the assumption the whole plan rests on. planner.decide_full_history_sync
# falls back to a FULL re-crawl whenever cache_plus_new < total_matches_hint, so
# a count that runs even one higher than the list turns every incremental sync
# into a full crawl - far worse than today. Two requests settle it without
# crawling anything: the last row must exist and the row after it must not.
# --------------------------------------------------------------------------
def t2_count_reconciles(token: str, xuid: str, counts: Optional[Dict]) -> Result:
    r = Result("T2  count == match list length (CRITICAL)")
    if not counts:
        return r.set("SKIP", "T1 did not return counts")
    n = counts.get("MatchesPlayedCount")
    if not isinstance(n, int) or n <= 0:
        return r.set("SKIP", f"MatchesPlayedCount not a positive int: {n!r}")

    s_last, p_last, _ = _matches(token, xuid, n - 1, 1)
    s_past, p_past, _ = _matches(token, xuid, n, 1)
    if s_last != 200 or s_past != 200:
        return r.set("FAIL", f"HTTP {s_last} at start={n-1}, HTTP {s_past} at start={n}")

    last_n, past_n = len(_match_ids(p_last)), len(_match_ids(p_past))
    ev = f"count={n}; start={n-1} returned {last_n} row(s); start={n} returned {past_n} row(s)"
    if last_n == 1 and past_n == 0:
        return r.set("PASS", ev + "  -> count is exactly the list length; safe as total_matches_hint")
    if past_n > 0:
        return r.set(
            "FAIL",
            ev + "  -> list is LONGER than count. Feeding this as total_matches_hint would let "
            "the planner declare completeness early and TRUNCATE history. Do not use as a hint.",
        )
    return r.set(
        "FAIL",
        ev + "  -> list is SHORTER than count (count likely includes matches the list omits). "
        "cache_plus_new can never reach it, so every sync would fall back to a full re-crawl.",
    )


# --------------------------------------------------------------------------
# T3 - is ?type= actually honoured, or silently ignored?
#
# Silently ignored is the dangerous outcome: sizing a matchmade crawl by
# MatchmadeMatchesPlayedCount while the listing keeps returning customs too
# would truncate. The bogus-value probe distinguishes "validated" from
# "ignored" - if garbage is accepted and returns the full list, the parameter
# is not being parsed and must not be trusted.
# --------------------------------------------------------------------------
def t3_type_param(token: str, xuid: str) -> Result:
    r = Result("T3  ?type= filters server-side")
    s_all, p_all, _ = _matches(token, xuid, 0, 25, "all")
    s_mm, p_mm, _ = _matches(token, xuid, 0, 25, "matchmaking")
    s_cu, p_cu, _ = _matches(token, xuid, 0, 25, "custom")
    s_bogus, p_bogus, _ = _matches(token, xuid, 0, 25, "definitely-not-a-type")

    if 200 not in (s_all, s_mm):
        return r.set("FAIL", f"HTTP all={s_all} matchmaking={s_mm}")

    ids_all, ids_mm = _match_ids(p_all), _match_ids(p_mm)
    ids_cu = _match_ids(p_cu) if s_cu == 200 else []
    same_as_all = ids_mm == ids_all
    mm_subset = set(ids_mm).issubset(set(ids_all))
    cu_overlap = set(ids_cu) & set(ids_mm)

    ev = (
        f"all={len(ids_all)} matchmaking={len(ids_mm)} custom={len(ids_cu)}(HTTP {s_cu}) "
        f"bogus=HTTP {s_bogus}"
        + (f"/{len(_match_ids(p_bogus))} rows" if s_bogus == 200 else "")
        + f"; matchmaking subset of all={mm_subset}; custom∩matchmaking={len(cu_overlap)}"
    )
    if s_bogus == 200 and len(_match_ids(p_bogus)) == len(ids_all) and same_as_all:
        return r.set(
            "FAIL",
            ev + "  -> a bogus type was accepted and every type returned identical rows: "
            "the parameter is being IGNORED. Do not filter server-side.",
        )
    if same_as_all and not ids_cu:
        return r.set(
            "UNKNOWN",
            ev + "  -> matchmaking == all, but this player may simply have no customs "
            "in the first page. Re-run against a player known to have custom games.",
        )
    if mm_subset and not cu_overlap:
        return r.set("PASS", ev + "  -> type is honoured and the partitions are disjoint")
    return r.set("FAIL", ev + "  -> partitions overlap or matchmaking is not a subset of all")


# --------------------------------------------------------------------------
# T4 - same reconciliation as T2, for the matchmade sub-list.
# --------------------------------------------------------------------------
def t4_matchmade_count(token: str, xuid: str, counts: Optional[Dict]) -> Result:
    r = Result("T4  matchmade count == type=matchmaking length")
    if not counts:
        return r.set("SKIP", "T1 did not return counts")
    n = counts.get("MatchmadeMatchesPlayedCount")
    if not isinstance(n, int) or n <= 0:
        return r.set("SKIP", f"MatchmadeMatchesPlayedCount not a positive int: {n!r}")

    s_last, p_last, _ = _matches(token, xuid, n - 1, 1, "matchmaking")
    s_past, p_past, _ = _matches(token, xuid, n, 1, "matchmaking")
    if s_last != 200 or s_past != 200:
        return r.set("FAIL", f"HTTP {s_last} at start={n-1}, HTTP {s_past} at start={n}")
    last_n, past_n = len(_match_ids(p_last)), len(_match_ids(p_past))
    ev = f"matchmade count={n}; start={n-1} -> {last_n} row(s); start={n} -> {past_n} row(s)"
    if last_n == 1 and past_n == 0:
        return r.set("PASS", ev + "  -> exact; safe to size a matchmade crawl")
    return r.set("FAIL", ev + "  -> does not reconcile; do not size a crawl from it")


# --------------------------------------------------------------------------
# T5 - service record: does it work, and does it carry the Subqueries block
# that would replace the hardcoded RANKED_PLAYLIST_IDS set?
# --------------------------------------------------------------------------
def t5_service_record(token: str, xuid: str) -> Result:
    r = Result("T5  servicerecord + Subqueries")
    out = []
    sub_ok = False
    for mtype in ("Matchmade", "Custom", "Local"):
        status, payload, ms = _get(f"{STATS}/hi/players/xuid({xuid})/{mtype}/servicerecord", token)
        if status != 200 or not isinstance(payload, dict):
            out.append(f"{mtype}=HTTP {status}")
            continue
        subq = payload.get("Subqueries") or payload.get("subqueries")
        core = payload.get("CoreStats") or {}
        out.append(
            f"{mtype}=200({ms:.0f}ms, matches={payload.get('MatchesCompleted')}, "
            f"wins={payload.get('Wins')}, kills={core.get('Kills')})"
        )
        if mtype == "Matchmade" and isinstance(subq, dict):
            sub_ok = True
            out.append(
                "Subqueries keys=" + ",".join(sorted(subq.keys()))
                + f" seasons={len(subq.get('SeasonIds') or [])}"
                + f" playlists={len(subq.get('PlaylistAssetIds') or [])}"
                + f" categories={len(subq.get('GameVariantCategories') or [])}"
                + f" ranked={subq.get('IsRanked')}"
            )
    verdict = "PASS" if sub_ok else ("PARTIAL" if any("200" in o for o in out) else "FAIL")
    return r.set(verdict, "; ".join(out))


# --------------------------------------------------------------------------
# T6 - profile batch lookup, and where its ceiling is.
#
# The ceiling matters: the graph crawler would chunk by it, and a silent
# truncation (N sent, fewer returned, HTTP 200) is worse than an error.
# --------------------------------------------------------------------------
def t6_profile_batch(token: str, xuids: List[str]) -> Result:
    r = Result("T6  profile.svc batch xuid lookup")
    if len(xuids) < 2:
        return r.set("SKIP", "need --xuid2 (or more) to test batching")
    status, payload, ms = _get(f"{PROFILE}/users", token, [("xuids", x) for x in xuids])
    if status != 200:
        return r.set("FAIL", f"HTTP {status}: {str(payload)[:200]}")
    n = len(payload) if isinstance(payload, list) else -1
    sample = ""
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        sample = " keys=" + ",".join(sorted(payload[0].keys()))
    ev = f"{ms:.0f}ms sent={len(xuids)} returned={n}{sample}"
    if n == len(xuids):
        return r.set("PASS", ev + "  -> one call resolves the whole batch")
    if n > 0:
        return r.set("PARTIAL", ev + "  -> fewer returned than sent; chunk conservatively and "
                                     "treat missing entries as unresolved, not as nonexistent")
    return r.set("FAIL", ev)


# --------------------------------------------------------------------------
# T7/T8 - skill endpoints, against a real match taken from the player's history.
# --------------------------------------------------------------------------
def t7_match_skill(token: str, xuid: str, match_id: Optional[str]) -> Tuple[Result, Optional[str]]:
    r = Result("T7  skill.svc per-match CSR/MMR")
    if not match_id:
        return r.set("SKIP", "no match id available"), None
    status, payload, ms = _get(f"{SKILL}/hi/matches/{match_id}/skill", token,
                               [("players", f"xuid({xuid})")])
    if status != 200:
        return r.set("FAIL", f"HTTP {status}: {str(payload)[:200]}"), None
    results = payload.get("Value") if isinstance(payload, dict) else None
    if not results:
        return r.set("FAIL", f"200 but no Value array: {str(payload)[:200]}"), None
    first = results[0].get("Result", {}) if isinstance(results[0], dict) else {}
    counterfactuals = "Counterfactuals" in first
    pre = (first.get("RankRecap") or {}).get("PreMatchCsr", {})
    post = (first.get("RankRecap") or {}).get("PostMatchCsr", {})
    return r.set(
        "PASS",
        f"{ms:.0f}ms  TeamMmr={first.get('TeamMmr')} preCsr={pre.get('Value')} "
        f"postCsr={post.get('Value')} tier={post.get('Tier')} counterfactuals={counterfactuals}",
    ), None


def t8_playlist_csr(token: str, xuid: str, playlist_id: Optional[str]) -> Result:
    r = Result("T8  skill.svc batch playlist CSR")
    if not playlist_id:
        return r.set("SKIP", "no ranked playlist id available from history")
    status, payload, ms = _get(f"{SKILL}/hi/playlist/{playlist_id}/csrs", token,
                               [("players", f"xuid({xuid})")])
    if status != 200:
        return r.set("FAIL", f"HTTP {status}: {str(payload)[:200]}")
    results = payload.get("Value") if isinstance(payload, dict) else None
    if not results:
        return r.set("FAIL", f"200 but no Value array: {str(payload)[:200]}")
    res = results[0].get("Result", {}) if isinstance(results[0], dict) else {}
    current = res.get("Current") or {}
    return r.set("PASS", f"{ms:.0f}ms  csr={current.get('Value')} tier={current.get('Tier')} "
                         f"subtier={current.get('SubTier')}")


# --------------------------------------------------------------------------
# T9 - what does matches-privacy actually answer, and for whom?
# --------------------------------------------------------------------------
def t9_matches_privacy(token: str, xuid: str) -> Result:
    r = Result("T9  matches-privacy semantics")
    status, payload, ms = _get(f"{STATS}/hi/players/xuid({xuid})/matches-privacy", token)
    if status != 200:
        return r.set("FAIL", f"HTTP {status} for a non-self player: {str(payload)[:160]}  "
                             "-> likely self-only; keep the Xbox privacy call")
    return r.set("PASS", f"{ms:.0f}ms  {json.dumps(payload)[:300]}  "
                         "-> compare against can_view_game_history for the same player")


def load_token(path: Optional[str], probe_xuid: str) -> Optional[str]:
    """Find a Spartan token that actually authenticates.

    `expires_at` is not trustworthy on its own: a cache can carry a token that
    is hours from its recorded expiry and still 401 (observed on account 1,
    which the bot survives only because it rotates accounts on 401). So every
    candidate is spent on one real request against a known-good endpoint, and
    the first that returns 200 wins. Without this the whole run reports a
    uniform wall of 401s that looks like "these endpoints don't work".
    """
    env = os.environ.get("SPARTAN_TOKEN")
    if env:
        print("Token source: $SPARTAN_TOKEN (not verified)")
        return env

    candidates: List[str] = []
    if path:
        candidates = [path]
    else:
        import glob as _glob
        for pattern in DEFAULT_TOKEN_CACHE_GLOBS:
            candidates.extend(sorted(_glob.glob(pattern)))

    for candidate in candidates:
        try:
            with open(candidate, "r", encoding="utf-8") as f:
                cache = json.load(f)
        except Exception as e:  # noqa: BLE001
            print(f"  {os.path.basename(candidate)}: unreadable ({e})")
            continue
        spartan = cache.get("spartan") or {}
        token, expires = spartan.get("token"), spartan.get("expires_at", 0)
        left = (expires - time.time()) / 60
        if not token:
            print(f"  {os.path.basename(candidate)}: no spartan token")
            continue
        if left <= 0:
            print(f"  {os.path.basename(candidate)}: expired {abs(left):.0f} min ago")
            continue
        status, _, _ = _matches(token, probe_xuid, 0, 1)
        if status == 200:
            print(f"Token source: {os.path.basename(candidate)} "
                  f"(verified live, {left:.0f} min of recorded life left)")
            return token
        print(f"  {os.path.basename(candidate)}: recorded valid for {left:.0f} min "
              f"but returned HTTP {status} - skipping")
    return None


def main() -> int:
    global _DELAY
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--xuid", required=True,
                    help="XUID with a real match history (bare digits, no xuid() wrapper)")
    ap.add_argument("--xuid2", action="append", default=[],
                    help="additional XUID(s) for the batch profile test; repeatable")
    ap.add_argument("--token-cache", help="path to token_cache.json")
    ap.add_argument("--delay", type=float, default=1.0,
                    help="seconds between requests (default 1.0; lower only if the bot is stopped)")
    args = ap.parse_args()
    _DELAY = args.delay

    xuid = args.xuid.strip().strip("()").replace("xuid", "")
    print(f"Probing as xuid({xuid}), {args.delay}s between requests\n")

    token = load_token(args.token_cache, xuid)
    if not token:
        print("\nNo Spartan token authenticated. Set $SPARTAN_TOKEN or pass --token-cache "
              "pointing at a cache whose spartan token still works.", file=sys.stderr)
        return 2
    print()

    results: List[Result] = []

    r1, counts = t1_match_count(token, xuid)
    results.append(r1)
    results.append(t2_count_reconciles(token, xuid, counts))
    results.append(t3_type_param(token, xuid))
    results.append(t4_matchmade_count(token, xuid, counts))
    results.append(t5_service_record(token, xuid))
    results.append(t6_profile_batch(token, [xuid] + args.xuid2))

    # Pull a real match (and its playlist) out of page 0 for the skill tests.
    match_id, playlist_id = None, None
    status, page0, _ = _matches(token, xuid, 0, 25)
    if status == 200:
        ids = [m for m in _match_ids(page0) if m]
        match_id = ids[0] if ids else None
        if match_id:
            s, stats, _ = _get(f"{STATS}/hi/matches/{match_id}/stats", token)
            if s == 200 and isinstance(stats, dict):
                playlist_id = ((stats.get("MatchInfo") or {}).get("Playlist") or {}).get("AssetId")

    r7, _ = t7_match_skill(token, xuid, match_id)
    results.append(r7)
    results.append(t8_playlist_csr(token, xuid, playlist_id))
    results.append(t9_matches_privacy(token, xuid))

    print("\n" + "=" * 78)
    print(f"SUMMARY   {_REQUESTS} requests, {_RATE_LIMITED} rate-limited (429)")
    print("=" * 78)
    for r in results:
        print(f"{r.verdict:8s} {r.name}")
        print(f"         {r.detail}")
    print("=" * 78)
    if _RATE_LIMITED:
        print("NOTE: 429s here mean the probe competed with the live bot for the same "
              "per-account budget. Re-run with a larger --delay before trusting timings.")

    blocking = [r for r in results if r.verdict == "FAIL" and "CRITICAL" in r.name]
    return 1 if blocking else 0


if __name__ == "__main__":
    sys.exit(main())
