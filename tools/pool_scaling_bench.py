#!/usr/bin/env python3
"""Does account count actually raise match-stats throughput?

Read-only. Every request is a GET; nothing is written to the API, to the token
cache, or to the database.

    docker compose exec -T bot python tools/pool_scaling_bench.py --dry-run
    docker compose exec -T bot python tools/pool_scaling_bench.py

WHY THIS EXISTS
---------------
The bot's throughput ceiling has long been assumed to be the number of Xbox
accounts in the pool, but that has never been measured: every production
instrumentation round so far exercised the match-detail path at N=0 or N=1
requests, which measures nothing. This settles it in ~700 requests.

WHY THE OBVIOUS TEST CANNOT ANSWER IT
-------------------------------------
Sweeping set_num_accounts(k) and plotting throughput looks like the right
experiment and is worthless. set_num_accounts(k) sets the semaphore to k*5
(rate_limiters.py, set_num_accounts) AND the aggregate rate to k*6, because
BUCKET_MATCH_STATS is pinned at 6/account (rate_limiters.py, module scope). The
local ceiling is therefore min(6k, 5k/L) - linear in k BY CONSTRUCTION. If Halo
never pushes back the curve comes out perfectly straight, and that straight line
is our own arithmetic rather than evidence about Halo. It would read as
confirmation and mean nothing.

So this holds concurrency and aggregate offered rate FIXED and varies only the
number of distinct identities signing the requests:

    baseline   I=1  C=1   R=6/s    sequential, pure service latency L
    cell A     I=1  C=25  R=30/s   one identity carries the whole load
    cell B     I=5  C=25  R=30/s   five identities, IDENTICAL offered load
    cell A2    I=1  C=25  R=30/s   repeat of A, drift control

B >> A means a per-identity limit is real and more accounts scale throughput.
B ~= A means the binding limit is not per-identity - and since every request
leaves the same box, a per-IP limit would look exactly like that.

Cell A drives one identity at 30 req/s, above the 6/s production pace for a
single account. The AGGREGATE is what production already sustains, and this
repo's own note records match_stats serving 60/60 clean at 15, 30 and 50 req/s.
That is a stated, sourced risk rather than an accident; --max-per-account-rate
clamps it and --account-index chooses which account takes the load.

TWO ISOLATION RULES THAT ARE NOT OPTIONAL
-----------------------------------------
1. `api_client = HaloAPIClient()` is MODULE-LEVEL in src/api/client.py, and
   HaloAPIClient.__init__ calls get_cache() -> _init_db(), which ends in
   _populate_medal_types() + conn.commit(): ~150 INSERT OR IGNORE + UPDATE
   statements against the 64GB production DB, taking the write lock while the
   live bot's single-writer executor may be mid-commit. So merely IMPORTING
   src.api.client writes to production. The in-memory cache singleton below is
   seeded BEFORE that import, and asserted. Do not reorder those lines.

2. _get_match_date returns Optional[datetime], collapsing 200-without-StartTime,
   404, 429 and 401 all into None. Without the aiohttp TraceConfig there is no
   429 count at all, and a cell that 429s half its traffic would score as fast.
   Throughput here is HTTP 200s per second, never attempts.

This runs in a separate process from the bot (docker compose exec), so it has
its own limiter and does NOT coordinate with the live one: for the duration Halo
sees the bot's traffic plus ours, on the same identities and the same IP. Run it
when quiet; the preflight says whether it is, and the A-vs-A2 drift check says
afterwards whether it mattered.
"""

import sys

sys.path.insert(0, ".")

# --- isolation, before anything can import src.api.client (see rule 1) -------
from src.database.cache import PlayerStatsCacheV2
import src.database.cache as _cache_mod

_cache_mod._cache_instance = PlayerStatsCacheV2(":memory:")
if _cache_mod._cache_instance.db_path != ":memory:":
    raise SystemExit("refusing to run: cache singleton is not in-memory")
# ---------------------------------------------------------------------------

import argparse
import asyncio
import hashlib
import random
import sqlite3
import statistics
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import aiohttp

from src.api.client import HaloAPIClient
from src.api.rate_limiters import BUCKET_MATCH_STATS, halo_stats_rate_limiter
from src.api.utils import is_token_valid, safe_read_json
from src.config import DATABASE_FILE, get_token_cache_path

STATS_HOST = "https://halostats.svc.halowaypoint.com"


def rule(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def pct(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    i = min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1))))
    return s[i]


def fingerprint(token):
    if not token:
        return "none"
    return hashlib.sha256(token.encode()).hexdigest()[:8]


class Trace:
    """Per-request truth, collected out of aiohttp rather than out of
    _get_match_date - which cannot tell us a status code (see rule 2)."""

    def __init__(self):
        self.by_url = {}
        self.in_flight = 0
        self.peak_in_flight = 0
        self.connector_queued = 0
        self.dns_lookups = 0

    def config(self):
        tc = aiohttp.TraceConfig()

        async def on_start(session, ctx, params):
            self.in_flight += 1
            if self.in_flight > self.peak_in_flight:
                self.peak_in_flight = self.in_flight
            self.by_url[str(params.url)] = {
                "t_send": time.monotonic(),
                "identity": fingerprint(
                    params.headers.get("x-343-authorization-spartan")
                ),
                "status": None,
                "bytes": 0,
                "exc": None,
            }

        async def on_end(session, ctx, params):
            self.in_flight -= 1
            rec = self.by_url.get(str(params.url))
            if rec is not None:
                rec["status"] = params.response.status
                rec["bytes"] = int(params.response.headers.get("Content-Length") or 0)

        async def on_exc(session, ctx, params):
            self.in_flight -= 1
            rec = self.by_url.get(str(params.url))
            if rec is not None:
                rec["exc"] = type(params.exception).__name__

        async def on_queued(session, ctx, params):
            self.connector_queued += 1

        async def on_dns(session, ctx, params):
            self.dns_lookups += 1

        tc.on_request_start.append(on_start)
        tc.on_request_end.append(on_end)
        tc.on_request_exception.append(on_exc)
        tc.on_connection_queued_start.append(on_queued)
        tc.on_dns_resolvehost_start.append(on_dns)
        return tc


class Budget:
    """The only hard stop. Because the bench deliberately never calls
    note_result - a controlled test needs a fixed offered load, and feeding 429s
    into AIMD would decay the rate and contaminate every later cell - there is no
    adaptive brake. This and --abort-429 are it."""

    def __init__(self, limit, abort_429):
        self.limit = limit
        self.abort_429 = abort_429
        self.dispatched = 0
        self.rate_limited = 0
        self.aborted = None

    def take(self):
        if self.aborted:
            return False
        if self.dispatched >= self.limit:
            self.aborted = "request limit %d reached" % self.limit
            return False
        self.dispatched += 1
        return True

    def note_429(self):
        self.rate_limited += 1
        if self.rate_limited >= self.abort_429 and not self.aborted:
            self.aborted = "429 budget %d exhausted" % self.abort_429


def load_accounts():
    """Whatever valid Spartan tokens are already on disk. No refresh, no
    interactive login. Same shape as csr_backfill._load_cached_spartan_accounts,
    copied rather than imported because that one hardcodes range(1,6) and drops
    the expiry this needs."""
    out = []
    now = time.time()
    for i in range(1, 9):
        cache = safe_read_json(get_token_cache_path(i), default={})
        spartan = cache.get("spartan") if cache else None
        if spartan and is_token_valid(spartan):
            out.append({
                "id": "account%d" % i,
                "token": spartan.get("token"),
                "seconds_left": float(spartan.get("expires_at", 0)) - now,
            })
    return out


def load_match_ids(db_path, count, offset, seed):
    """Read-only. idx_matches_start_time covers this ORDER BY, so it is a
    bounded index scan and not a walk of the 64GB table."""
    conn = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True, timeout=60)
    conn.execute("PRAGMA query_only=ON")
    rows = conn.execute(
        "SELECT match_id FROM matches ORDER BY start_time DESC LIMIT ? OFFSET ?",
        (count, offset),
    ).fetchall()
    conn.close()
    ids = [str(r[0]) for r in rows if r[0]]
    # Shuffled here, dealt round-robin by the caller. Contiguous slices would let
    # one cell draw the big 8v8 matches and look slow for a reason that has
    # nothing to do with identity count.
    random.Random(seed).shuffle(ids)
    return ids


def recent_bot_activity(db_path, minutes=10):
    """players.last_processed_at is written from datetime.now().isoformat() -
    local and naive - so the cutoff is computed in Python. SQL datetime('now') is
    UTC and would silently mis-window by the BST offset."""
    cutoff = (datetime.now() - timedelta(minutes=minutes)).isoformat()
    conn = sqlite3.connect("file:%s?mode=ro" % db_path, uri=True, timeout=60)
    conn.execute("PRAGMA query_only=ON")
    n = conn.execute(
        "SELECT COUNT(*) FROM players WHERE last_processed_at >= ?", (cutoff,)
    ).fetchone()[0]
    conn.close()
    return int(n)


def configure(client, tokens, identities, concurrency, rate_total):
    """Pin the limiter to an exact offered load. Returns the per-account rate.

    set_num_accounts clears neither _account_backoff nor _next_free nor the
    bucket's success counter, so without the explicit reset below a 429 in one
    cell would silently handicap the next."""
    per_acct = rate_total / identities
    halo_stats_rate_limiter.set_bucket_rate(
        BUCKET_MATCH_STATS, per_acct, floor=per_acct, ceiling=per_acct
    )
    halo_stats_rate_limiter.set_num_accounts(identities)

    # set_num_accounts hardwires the semaphore to identities*5 and there is no
    # public setter, so this private poke is how C is held fixed while I varies -
    # which is the whole experiment. slot() releases the object it acquired, so
    # swapping it cannot leak a permit. Asserted so a future refactor of
    # rate_limiters.py fails loudly instead of silently running the wrong test.
    halo_stats_rate_limiter._semaphore = asyncio.Semaphore(concurrency)
    assert halo_stats_rate_limiter._semaphore._value == concurrency

    halo_stats_rate_limiter._account_backoff.clear()
    halo_stats_rate_limiter._next_free.clear()
    halo_stats_rate_limiter._global_backoff_until = 0.0
    halo_stats_rate_limiter._bucket_successes[BUCKET_MATCH_STATS] = 0

    client.spartan_accounts = tokens[:identities]
    client.current_account_index = 0
    return per_acct


async def one_request(client, session, match_id, trace, budget, results):
    if not budget.take():
        return
    t_enqueue = time.monotonic()
    await client._get_match_date(match_id, session)
    t_done = time.monotonic()

    url = "%s/hi/matches/%s/stats" % (STATS_HOST, match_id)
    rec = trace.by_url.get(url)
    if rec is None:
        results.append({"status": None, "exc": "no-trace", "identity": "?",
                        "bytes": 0, "service": 0.0, "queue_wait": 0.0})
        return
    if rec["status"] == 429:
        budget.note_429()
    results.append({
        "status": rec["status"],
        "exc": rec["exc"],
        "identity": rec["identity"],
        "bytes": rec["bytes"],
        "service": t_done - rec["t_send"],
        "queue_wait": rec["t_send"] - t_enqueue,
    })


async def run_cell(name, client, session, ids, tokens, identities, concurrency,
                   rate_total, trace, budget, sequential=False):
    per_acct = configure(client, tokens, identities, concurrency, rate_total)
    trace.peak_in_flight = 0
    trace.connector_queued = 0
    dns_before = trace.dns_lookups
    results = []

    t0 = time.monotonic()
    if sequential:
        for mid in ids:
            await one_request(client, session, mid, trace, budget, results)
    else:
        await asyncio.gather(*(
            one_request(client, session, mid, trace, budget, results)
            for mid in ids
        ))
    wall = time.monotonic() - t0

    ok = [r for r in results if r["status"] == 200]
    services = [r["service"] for r in ok]
    used = {}
    for r in results:
        used[r["identity"]] = used.get(r["identity"], 0) + 1

    return {
        "name": name,
        "identities": identities,
        "concurrency": concurrency,
        "rate_total": rate_total,
        "per_acct": per_acct,
        "requests": len(results),
        "ok200": len(ok),
        "http429": sum(1 for r in results if r["status"] == 429),
        "http404": sum(1 for r in results if r["status"] == 404),
        "http401": sum(1 for r in results if r["status"] == 401),
        "exc": sum(1 for r in results if r["exc"]),
        "wall": wall,
        "throughput_ok": (len(ok) / wall) if wall > 0 else 0.0,
        "service_mean": (statistics.mean(services) * 1000) if services else 0.0,
        "service_p50": pct(services, 50) * 1000,
        "service_p95": pct(services, 95) * 1000,
        "queue_p50": pct([r["queue_wait"] for r in ok], 50) * 1000,
        "in_flight_mean": (sum(services) / wall) if wall > 0 else 0.0,
        "in_flight_peak": trace.peak_in_flight,
        "connector_queued": trace.connector_queued,
        "dns": trace.dns_lookups - dns_before,
        "bytes_mean": (statistics.mean([r["bytes"] for r in ok]) / 1024.0) if ok else 0.0,
        "identities_used": used,
    }


def print_cell(c, baseline_p50=None):
    ref = ""
    if baseline_p50:
        ref = "  (baseline L p50=%.0fms)" % baseline_p50
    print("\n[BENCH] cell=%s identities=%d C=%d R_total=%.1f per_acct=%.2f" % (
        c["name"], c["identities"], c["concurrency"], c["rate_total"], c["per_acct"]))
    print("        requests=%d ok200=%d http429=%d http404=%d http401=%d exc=%d" % (
        c["requests"], c["ok200"], c["http429"], c["http404"], c["http401"], c["exc"]))
    print("        wall=%.2fs throughput_ok=%.2f/s (offered cap %.1f/s)" % (
        c["wall"], c["throughput_ok"], c["rate_total"]))
    print("        service_ms mean=%.0f p50=%.0f p95=%.0f%s" % (
        c["service_mean"], c["service_p50"], c["service_p95"], ref))
    print("        queue_wait_ms p50=%.0f" % c["queue_p50"])
    print("        in_flight mean=%.1f peak=%d cap=%d" % (
        c["in_flight_mean"], c["in_flight_peak"], c["concurrency"]))
    print("        identities used: %s" % " ".join(
        "%s=%d" % (k, v) for k, v in sorted(c["identities_used"].items())))
    print("        bytes_mean=%.0fKB connector_queued=%d dns=%d" % (
        c["bytes_mean"], c["connector_queued"], c["dns"]))
    if c["wall"] < 5:
        print("        WARNING: under 5s wall - mostly ramp, not steady state")
    if c["connector_queued"]:
        print("        VOID: connector queued requests - it was the bottleneck, not Halo")


def verdict(a, b, a2):
    rule("VERDICT")
    ratio = (b["throughput_ok"] / a["throughput_ok"]) if a["throughput_ok"] else 0.0
    drift_pct = 0.0
    if a["throughput_ok"]:
        drift_pct = abs(a["throughput_ok"] - a2["throughput_ok"]) / a["throughput_ok"] * 100

    print("  A  (1 identity)   throughput_ok = %6.2f/s   429s=%d" % (
        a["throughput_ok"], a["http429"]))
    print("  B  (5 identities) throughput_ok = %6.2f/s   429s=%d" % (
        b["throughput_ok"], b["http429"]))
    print("  A2 (repeat of A)  throughput_ok = %6.2f/s   429s=%d" % (
        a2["throughput_ok"], a2["http429"]))
    print("\n  B/A ratio = %.2fx     A-vs-A2 drift = %.0f%%" % (ratio, drift_pct))

    if a["http401"] or b["http401"] or a2["http401"]:
        print("\n  RUN VOID: a 401 appeared - a token rotated mid-run.")
        return
    if drift_pct > 20:
        print("\n  RUN CONTAMINATED: the two A cells disagree by >20%. The live bot")
        print("  almost certainly interfered. Re-run in a quieter window; do not")
        print("  draw any conclusion from the B/A ratio above.")
        return

    if (a["throughput_ok"] >= 0.95 * a["rate_total"]
            and b["throughput_ok"] >= 0.95 * b["rate_total"]):
        print("\n  INCONCLUSIVE - both cells sat at OUR OWN offered-rate cap, so neither")
        print("  ever asked Halo a hard question. Re-run with a higher --rate.")
        return
    if a["in_flight_mean"] >= 0.9 * a["concurrency"]:
        print("\n  INCONCLUSIVE - cell A was semaphore-bound (in_flight ~= C), so it")
        print("  measured C/L rather than Halo. Re-run with a higher --concurrency.")
        return

    if ratio >= 3.0:
        print("\n  ACCOUNTS ARE THE CONSTRAINT. A per-identity limit is real and more")
        print("  accounts scale throughput close to linearly. Adding the 2-3 accounts")
        print("  you control is justified.")
    elif ratio >= 1.3:
        print("\n  SUB-LINEAR. A per-identity limit exists but something else also binds.")
        print("  Worth adding a couple of accounts you own; the gain does not justify a")
        print("  user-facing account-collection feature.")
    else:
        print("\n  ACCOUNTS ARE NOT THE CONSTRAINT. One identity carried the same offered")
        print("  load as five. Adding accounts will not raise throughput - and since every")
        print("  request leaves the same box, a per-IP limit would look exactly like this.")
        print("  Look elsewhere for the bottleneck.")


async def main():
    p = argparse.ArgumentParser(description="Does account count raise throughput?")
    p.add_argument("--dry-run", action="store_true",
                   help="resolve everything, print the budget, fire zero requests")
    p.add_argument("--limit", type=int, default=1000, help="hard cap on total requests")
    p.add_argument("--n", type=int, default=300, help="requests per measured cell")
    p.add_argument("--concurrency", type=int, default=25)
    p.add_argument("--rate", type=float, default=30.0, help="aggregate offered req/s")
    p.add_argument("--max-per-account-rate", type=float, default=30.0)
    p.add_argument("--account-index", type=int, default=0,
                   help="which account plays the single-identity role")
    p.add_argument("--abort-429", type=int, default=25)
    p.add_argument("--db", default=str(DATABASE_FILE))
    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--offset", type=int, default=0)
    args = p.parse_args()

    rule("PREFLIGHT")
    print("  utc          %s" % datetime.now(timezone.utc).isoformat())
    print("  db           %s (read-only)" % args.db)
    print("  cache        in-memory (%s)" % _cache_mod._cache_instance.db_path)

    baseline_n = 20
    warmup_n = args.concurrency
    planned = warmup_n + baseline_n + args.n * 3
    if planned > args.limit:
        print("\n  planned %d requests exceeds --limit %d. Raise it or lower --n."
              % (planned, args.limit))
        return 2

    accounts = load_accounts()
    if len(accounts) < 5:
        print("\n  need 5 valid cached Spartan tokens, found %d." % len(accounts))
        print("  Run the bot's auth flow first; this script never logs in.")
        return 2
    if args.account_index >= len(accounts):
        print("\n  --account-index %d out of range" % args.account_index)
        return 2

    est_seconds = planned / max(args.rate, 1.0) + 60
    print("\n  tokens (%d):" % len(accounts))
    for i, acc in enumerate(accounts):
        print("    [%d] %s  expires in %6.1f min"
              % (i, fingerprint(acc["token"]), acc["seconds_left"] / 60))
    for i, acc in enumerate(accounts[:5]):
        if acc["seconds_left"] < est_seconds + 300:
            print("\n  token %d expires within the run window + 5 min. A token that" % i)
            print("  expires mid-run turns a cell into 401s and reads as 'slow'.")
            print("  Refresh tokens and re-run.")
            return 2

    if args.rate > args.max_per_account_rate:
        print("\n  cell A would drive one identity at %.1f req/s, above"
              " --max-per-account-rate %.1f." % (args.rate, args.max_per_account_rate))
        return 2

    pool = load_match_ids(args.db, planned * 2, args.offset, args.seed)
    if len(pool) < planned:
        print("\n  only %d match ids available, need %d" % (len(pool), planned))
        return 2

    busy = recent_bot_activity(args.db)
    print("\n  bot activity: %d players refreshed in the last 10 min" % busy)
    if busy:
        print("    the bot shares these identities, this IP and this CPU.")
        print("    cross-check: docker compose logs --since 5m bot"
              " | grep -c 'phase=match_details'")
        print("    the A-vs-A2 drift check will say if it contaminated the run.")

    print("\n  plan: warmup %d, baseline %d, then A/B/A2 at %d each = %d requests"
          % (warmup_n, baseline_n, args.n, planned))
    print("  cells: A(I=1,C=%d,R=%.0f)  B(I=5,C=%d,R=%.0f)  A2=A"
          % (args.concurrency, args.rate, args.concurrency, args.rate))
    print("  est wall ~%.1f min" % (est_seconds / 60))

    if args.dry_run:
        print("\n  --dry-run: nothing fired.")
        return 0

    # Dealt round-robin so payload size and match age are distributed evenly
    # across cells, and so no id is reused - no later cell gets a server-side
    # warm-cache advantage, and the URL stays a unique correlation key.
    dealt = pool[:planned]
    warm_ids = dealt[:warmup_n]
    base_ids = dealt[warmup_n:warmup_n + baseline_n]
    rest = dealt[warmup_n + baseline_n:]
    a_ids, b_ids, a2_ids = rest[0::3], rest[1::3], rest[2::3]

    trace = Trace()
    budget = Budget(args.limit, args.abort_429)
    client = HaloAPIClient()
    tokens = accounts[:5]
    single = [accounts[args.account_index]]

    connector = aiohttp.TCPConnector(limit=max(args.concurrency * 2, 64))
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_connect=10, sock_read=20)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout,
                                     trace_configs=[trace.config()]) as session:
        rule("WARMUP (untimed - pays DNS and TLS once, so cell A does not)")
        await run_cell("warmup", client, session, warm_ids, tokens, 5,
                       args.concurrency, args.rate, trace, budget)
        print("  done. dns lookups so far=%d" % trace.dns_lookups)

        rule("BASELINE - sequential, pure service latency")
        base = await run_cell("baseline", client, session, base_ids, single, 1, 1,
                              6.0, trace, budget, sequential=True)
        print_cell(base)

        rule("CELL A - ONE identity, full load")
        a = await run_cell("A", client, session, a_ids, single, 1,
                           args.concurrency, args.rate, trace, budget)
        print_cell(a, base["service_p50"])
        if budget.aborted:
            print("\n  ABORTED: %s" % budget.aborted)
            return 3

        await asyncio.sleep(2)
        rule("CELL B - FIVE identities, identical load")
        b = await run_cell("B", client, session, b_ids, tokens, 5,
                           args.concurrency, args.rate, trace, budget)
        print_cell(b, base["service_p50"])
        if budget.aborted:
            print("\n  ABORTED: %s" % budget.aborted)
            return 3

        await asyncio.sleep(2)
        rule("CELL A2 - repeat of A, drift control")
        a2 = await run_cell("A2", client, session, a2_ids, single, 1,
                            args.concurrency, args.rate, trace, budget)
        print_cell(a2, base["service_p50"])

    verdict(a, b, a2)
    print("\n  dispatched %d requests, %d were 429s"
          % (budget.dispatched, budget.rate_limited))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        print("\ninterrupted")
        sys.exit(130)
