"""Measure what HaloStatsRateLimiter actually does under the deployed shape.

A diagnostic, not a test: it currently reports failing numbers by design, which
is the point. Promote its expectations into tests/ once the limiter is fixed.

5 accounts, 25 concurrent callers - exactly the full-crawl burst that produced
the 429 storm on 2026-08-11. It differs from tests/test_rate_limiter_stress.py
in the two ways that make the defects observable at all:

  1. it uses the DEPLOYED rate of 3 req/s per account, not 1000. At 1000 the
     minimum interval is 1ms and the thundering herd has nothing to form around.
  2. it HOLDS each slot for a simulated request duration. wait_if_needed()
     releases its semaphore before returning, so unless something occupies the
     slot afterwards, the concurrency cap's failure is invisible.

    PYTHONIOENCODING=utf-8 .venv/Scripts/python.exe tools/rl_repro.py

Correct limiter: peak in-flight <= 25, zero pacing violations, ~1.6s for 25
requests. Measured 2026-08-11: 20 of 25 issued within the same 0.1ms, 19
violations, 0.347s wall.
"""
import asyncio
import sys
import time

sys.path.insert(0, ".")

from src.api.rate_limiters import HaloStatsRateLimiter

REQUEST_SECONDS = 0.25   # pretend the Halo call takes 250ms


async def main():
    rl = HaloStatsRateLimiter(requests_per_second_per_account=3)
    rl.set_num_accounts(5)

    t0 = time.time()
    inflight = 0
    peak_inflight = 0
    events = []      # (issued_at, account)
    lock = asyncio.Lock()

    async def caller(page):
        nonlocal inflight, peak_inflight
        acct = await rl.wait_if_needed()
        async with lock:
            inflight += 1
            peak_inflight = max(peak_inflight, inflight)
        events.append((time.time() - t0, acct))
        await asyncio.sleep(REQUEST_SECONDS)     # the actual HTTP request
        async with lock:
            inflight -= 1

    await asyncio.gather(*(caller(i) for i in range(25)))

    print(f"intended: 5 accounts x 3 req/s = 15 req/s, {5*5} max concurrent\n")

    print(f"PEAK CONCURRENT IN-FLIGHT REQUESTS: {peak_inflight}")
    print("  (the semaphore is meant to cap this at 25; the real question is")
    print("   whether it caps anything at all)\n")

    events.sort()
    first_100ms = [e for e in events if e[0] < 0.100]
    print(f"requests issued in the first 100ms: {len(first_100ms)} of 25")
    print(f"  accounts used in that window: {sorted({a for _, a in first_100ms})}")

    burst_rate = len(first_100ms) / 0.100 if first_100ms else 0
    print(f"  => effective rate in that window: {burst_rate:.0f} req/s "
          f"(intended 15)\n")

    per_account = {}
    for at, acct in events:
        per_account.setdefault(acct, []).append(at)

    print("per-account issue times (s since start):")
    for acct in sorted(per_account):
        times = per_account[acct]
        gaps = [round(b - a, 4) for a, b in zip(times, times[1:])]
        violations = [g for g in gaps if g < 0.333 - 1e-6]
        print(f"  account {acct}: {len(times)} requests, "
              f"{len(violations)} gap(s) under the 0.333s minimum")
        print(f"    at {[round(t, 4) for t in times]}")

    total_violations = sum(
        1 for acct in per_account
        for a, b in zip(per_account[acct], per_account[acct][1:])
        if b - a < 0.333 - 1e-6
    )
    print(f"\nTOTAL per-account pacing violations: {total_violations}")
    print(f"total wall time for 25 requests: {max(t for t, _ in events):.3f}s")
    print(f"  (at the intended 15 req/s, 25 requests should span ~1.6s)")


asyncio.run(main())
